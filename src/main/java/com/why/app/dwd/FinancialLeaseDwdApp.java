package com.why.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.why.bean.dwd.*;
import com.why.common.FinancialLeaseCommon;
import com.why.util.CreateEnvUtil;
import com.why.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Objects;

/**
 * Created by WHY on 2024/9/6.
 * Functions: DWD层处理逻辑
 */
public class FinancialLeaseDwdApp {
    public static void main(String[] args) throws Exception {
        // TODO 1 创建流环境
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(8082, "financial_lease_dwd_app");
        env.setParallelism(1);

        //TODO 2 获取Kafka数据源 读取ods层数据
        KafkaSource<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(FinancialLeaseCommon.KAFKA_ODS_TOPIC, "financial_lease_dwd_app", OffsetsInitializer.earliest());
        //创建数据流
        DataStreamSource<String> odsStream = env.fromSource(kafkaConsumer, WatermarkStrategy.noWatermarks(), "financial_lease_dwd_app");

        //TODO 3 筛选出业务过程相关的事实表
        // credit | credit_facility | credit_facility_status | contract | reply
        SingleOutputStreamOperator<JSONObject> flatStream = odsStream.flatMap(new FlatMapFunction<String, JSONObject>() {

            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(value);
                String tableName = jsonObject.getString("table");
                //根据表名进行筛选 同时根据授信申请id 授信id是否为空来判断数据是否完整
                //credit | credit_facility_status  | reply 这三张表依据credit_facility_id来进行判断
                if ("credit".equals(tableName) || "credit_facility_status".equals(tableName) || "reply".equals(tableName)) {
                    if (!Objects.isNull(jsonObject.getJSONObject("data").getString("credit_facility_id"))) {
                        //数据发送到下游
                        out.collect(jsonObject);
                    }
                }
                //credit_facility依据自身的id来进行判断
                else if ("credit_facility".equals(tableName) && !Objects.isNull(jsonObject.getJSONObject("data").getString("id"))) {
                    out.collect(jsonObject);
                }
                //contract依据自身的credit_id来进行判断
                else if ("contract".equals(tableName) && !Objects.isNull(jsonObject.getJSONObject("data").getString("credit_id"))) {
                    out.collect(jsonObject);
                }
            }
        });

        //将授信审批相关的业务表和合同表分成两个数据流进行处理
        //分流依据：表名
        // 3.1 contractStream: contract
        SingleOutputStreamOperator<JSONObject> contractStream = flatStream.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                return "contract".equals(value.getString("table"));
            }
        });
//        contractStream.print("contractStream>>>");

        //3.2 mainStream: credit | credit_facility | credit_facility_status | reply
        SingleOutputStreamOperator<JSONObject> mainStream = flatStream.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                return !"contract".equals(value.getString("table"));
            }
        });

        //TODO 4 按照授信申请id进行分组
        KeyedStream<JSONObject, String> keyedStream = mainStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                String tableName = value.getString("table");
                if ("credit_facility".equals(tableName)) {
                    return value.getJSONObject("data").getString("id");
                } else {
                    return value.getJSONObject("data").getString("credit_facility_id");
                }
            }
        });

//        keyedStream.print(">>>");

        //TODO 5 动态分流
        // 5.1 定义侧输出流标签 将审批通过的数据放入主输出流 审批取消/审批拒绝的数据放入侧输出流

        //审批域
        //审批取消侧输出流标签
        OutputTag<DwdAuditCancelBean> auditCancelTag = new OutputTag<DwdAuditCancelBean>("cancel_stream"){};
        //审批拒绝测输出流标签
        OutputTag<DwdAuditRejectBean> auditRejectTag = new OutputTag<DwdAuditRejectBean>("reject_stream"){};

        // 授信域
        // 授信新增
        OutputTag<DwdCreditAddBean> creditAddTag = new OutputTag<DwdCreditAddBean>("credit_add_stream") {
        };
        // 授信完成
        OutputTag<DwdCreditOccupyBean> creditOccupyTag = new OutputTag<DwdCreditOccupyBean>("credit_occupy_stream") {
        };

        // 租赁域
        // 合同制作
        OutputTag<DwdLeaseContractProducedBean> contractProducedTag = new OutputTag<DwdLeaseContractProducedBean>("contract_produced_stream") {
        };

        //TODO 5.2 对主输出流的数据进行处理
        SingleOutputStreamOperator<DwdAuditApprovalBean> processStream = keyedStream.process(new KeyedProcessFunction<String, JSONObject, DwdAuditApprovalBean>() {
            //设置状态
            //信审经办Id
            private ValueState<String> auditManIdStatus;
            //申请通过状态
            private ValueState<DwdAuditApprovalBean> auditApprovalStatus;
            //申请批复状态
            private ValueState<DwdAuditReplyBean> auditReplyStatus;

            //新增授信状态
            private ValueState<DwdCreditAddBean> creditAddStatus;
            //授信占用状态
            private ValueState<DwdCreditOccupyBean> creditOccupyStatus;
            //合同制作状态
            private ValueState<DwdLeaseContractProducedBean> contractProducedStatus;

            @Override
            public void open(Configuration parameters) throws Exception {
                auditManIdStatus = getRuntimeContext().getState(new ValueStateDescriptor<String>("audit_man_id", String.class));
                auditApprovalStatus = getRuntimeContext().getState(new ValueStateDescriptor<DwdAuditApprovalBean>("audit_approval_status", DwdAuditApprovalBean.class));
                auditReplyStatus = getRuntimeContext().getState(new ValueStateDescriptor<DwdAuditReplyBean>("audit_reply_status", DwdAuditReplyBean.class));

                ValueStateDescriptor<DwdCreditAddBean> creditAddStatusDescriptor = new ValueStateDescriptor<>("credit_add_status", DwdCreditAddBean.class);
                creditAddStatusDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(30L)).build());
                creditAddStatus = getRuntimeContext().getState(creditAddStatusDescriptor);

                ValueStateDescriptor<DwdCreditOccupyBean> creditOccupyStatusDescriptor = new ValueStateDescriptor<>("credit_occupy_status", DwdCreditOccupyBean.class);
                creditOccupyStatusDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(30L)).build());
                creditOccupyStatus = getRuntimeContext().getState(creditOccupyStatusDescriptor);


                ValueStateDescriptor<DwdLeaseContractProducedBean> contractProducedStatusDescriptor = new ValueStateDescriptor<>("contract_produced_status", DwdLeaseContractProducedBean.class);
                contractProducedStatusDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(30L)).build());
                contractProducedStatus = getRuntimeContext().getState(contractProducedStatusDescriptor);
            }

            @Override
            public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, DwdAuditApprovalBean>.Context ctx, Collector<DwdAuditApprovalBean> out) throws Exception {
                //对不同表格进行处理
                String tableName = value.getString("table");
                //授信申请状态表
                if ("credit_facility_status".equals(tableName)) {
                    //因为指标汇总中有信审经办统计粒度的指标，所以需要记录信审经办审批状态\
                    //具体操作为：过滤出处于信审经办审核状态的数据，并获取到信审经办id，将其存入到状态中，供授信申请表处理时进行使用
                    //5 信审经办审核通过
                    //6 信审经办审核复议
                    //20 信审经办审批拒绝
                    String status = value.getJSONObject("data").getString("status");
                    if ("5".equals(status) || "6".equals(status) || "20".equals(status)) {
                        String employeeId = value.getJSONObject("data").getString("employee_id");
                        auditManIdStatus.update(employeeId);
                    }
                }
                //授信申请表
                else if ("credit_facility".equals(tableName)) {
                    //判断授信申请的状态
                    //16 授信申请通过
                    //20 授信申请拒绝
                    //21 授信申请取消
                    //（根据授信申请的最终状态来进行汇总，如果是17授信申请复议的话，需要继续进行授信申请处理，无需进行汇总）
                    String type = value.getString("type");
                    String status = value.getJSONObject("data").getString("status");
                    if ("update".equals(type)) {
                        switch (status) {
                            case "16": {
                                DwdAuditApprovalBean auditApprovalBean = value.getObject("data", DwdAuditApprovalBean.class);
                                //补全信审经办信息
                                auditApprovalBean.setAuditManId(auditManIdStatus.value());
                                auditManIdStatus.clear();
                                //补全其他信息 审批通过时间 | 申请授信金额
                                auditApprovalBean.setApproveTime(value.getJSONObject("data").getString("update_time"));
                                auditApprovalBean.setApplyAmount(value.getJSONObject("data").getBigDecimal("credit_amount"));
                                //TODO 补全批复信息
                                // 需要考虑数据先后问题 是申请通过数据先到 还是申请批复数据先到
                                // 首先获取存储的申请批复状态，根据其是否为null，来判断数据先后问题
                                DwdAuditReplyBean auditReplyBean = auditReplyStatus.value();
                                if (Objects.isNull(auditReplyBean)) {
                                    //申请通过数据先到，更新其状态即可，批复信息的补全等到批复数据到达后再进行
                                    auditApprovalStatus.update(auditApprovalBean);
                                } else {
                                    //申请批复数据先到，进行批复信息的补全
                                    auditApprovalBean.setReplyId(auditReplyBean.getId());
                                    auditApprovalBean.setReplyAmount(auditReplyBean.getReplyAmount());
                                    auditApprovalBean.setReplyTime(auditReplyBean.getReplyTime());
                                    auditApprovalBean.setIrr(auditReplyBean.getIrr());
                                    auditApprovalBean.setPeriod(auditReplyBean.getPeriod());
                                    auditReplyStatus.clear();

                                    //更新申请通过状态，下游可能用到其中的数据
                                    auditApprovalStatus.update(auditApprovalBean);

                                    //数据写出到下游
                                    out.collect(auditApprovalBean);

                                    // 如果新增授信的数据先到 已经存在了状态中 这里需要进行处理 将对应的数据写出到侧输出流
                                    DwdCreditAddBean creditAddBean = creditAddStatus.value();
                                    if (!Objects.isNull(creditAddBean)) {
                                        //写入申请金额和批复金额（注意：这里的前提条件是申请批复数据先于申请成功数据到达）
                                        creditAddBean.setApplyAmount(auditApprovalBean.getApplyAmount());
                                        creditAddBean.setReplyAmount(auditApprovalBean.getReplyAmount());
                                        ctx.output(creditAddTag, creditAddBean);
                                        creditAddStatus.clear();
                                    }

                                    // 如果完成授信占用的数据先到 已经存在状态中 这里需要进行处理 将对应的数据写到侧输出流
                                    DwdCreditOccupyBean creditOccupyBean = creditOccupyStatus.value();
                                    if (creditOccupyBean != null) {
                                        creditOccupyBean.setApplyAmount(auditApprovalBean.getApplyAmount());
                                        creditOccupyBean.setReplyAmount(auditApprovalBean.getReplyAmount());
                                        ctx.output(creditOccupyTag, creditOccupyBean);
                                        creditOccupyStatus.clear();
                                    }

                                    // 如果完成合同制作的数据先到 已经存在状态中 这里需要进行处理 将对应的数据写到侧输出流
                                    DwdLeaseContractProducedBean contractProducedBean = contractProducedStatus.value();
                                    if (contractProducedBean != null) {
                                        contractProducedBean.setApplyAmount(auditApprovalBean.getApplyAmount());
                                        contractProducedBean.setReplyAmount(auditApprovalBean.getReplyAmount());
                                        ctx.output(contractProducedTag, contractProducedBean);
                                        contractProducedStatus.clear();
                                    }
                                }
                                break;
                            }
                            case "20":{
                                DwdAuditRejectBean auditRejectBean = value.getObject("data", DwdAuditRejectBean.class);
                                //补全信审经办信息
                                auditRejectBean.setAuditManId(auditManIdStatus.value());
                                auditManIdStatus.clear();
                                //补全其他信息
                                auditRejectBean.setRejectTime(value.getJSONObject("data").getString("update_time"));
                                auditRejectBean.setApplyAmount(value.getJSONObject("data").getBigDecimal("credit_amount"));
                                //写出到侧输出流
                                ctx.output(auditRejectTag, auditRejectBean);
                                //申请拒绝之后可能会复议，因此需要将之前的状态清空
                                auditApprovalStatus.clear();
                                break;
                            }
                            case "21":{
                                DwdAuditCancelBean auditCancelBean = value.getObject("data", DwdAuditCancelBean.class);
                                //补全信审经办信息
                                auditCancelBean.setAuditManId(auditManIdStatus.value());
                                auditManIdStatus.clear();
                                //补全其他信息
                                auditCancelBean.setCancelTime(value.getJSONObject("data").getString("update_time"));
                                auditCancelBean.setApplyAmount(value.getJSONObject("data").getBigDecimal("credit_amount"));
                                //写出到侧输出流
                                ctx.output(auditCancelTag, auditCancelBean);
                                //申请取消，不需要后续状态，因此清空
                                auditApprovalStatus.clear();
                                break;
                            }
                        }
                    }

                }
                //授信批复表
                else if ("reply".equals(tableName)) {
                    DwdAuditReplyBean auditReplyBean = value.getObject("data", DwdAuditReplyBean.class);
                    //添加信息
                    auditReplyBean.setReplyAmount(value.getJSONObject("data").getBigDecimal("credit_amount"));
                    auditReplyBean.setReplyTime(value.getJSONObject("data").getString("create_time"));

                    //判断是申请通过的数据先到还是批复数据先到
                    DwdAuditApprovalBean auditApprovalBean = auditApprovalStatus.value();
                    if (Objects.isNull(auditApprovalBean)) {
                        //批复数据先到
                        auditReplyStatus.update(auditReplyBean);
                    } else {
                        //申请通过的数据先到
                        //补全批复信息
                        auditApprovalBean.setReplyId(auditReplyBean.getId());
                        auditApprovalBean.setReplyAmount(auditReplyBean.getReplyAmount());
                        auditApprovalBean.setReplyTime(auditReplyBean.getReplyTime());
                        auditApprovalBean.setIrr(auditReplyBean.getIrr());
                        auditApprovalBean.setPeriod(auditReplyBean.getPeriod());

                        // 将申请通过的数据当做主流写出
                        out.collect(auditApprovalBean);
                        auditApprovalStatus.update(auditApprovalBean);

                        // 如果新增授信的数据先到 已经存在了状态中 这里需要进行处理 将对应的数据写出到侧输出流
                        DwdCreditAddBean creditAddBean = creditAddStatus.value();
                        if (!Objects.isNull(creditAddBean)) {
                            //写入申请金额和批复金额（注意：这里的前提条件是申请批复数据先于申请成功数据到达）
                            creditAddBean.setApplyAmount(auditApprovalBean.getApplyAmount());
                            creditAddBean.setReplyAmount(auditApprovalBean.getReplyAmount());
                            ctx.output(creditAddTag, creditAddBean);
                            creditAddStatus.clear();
                        }

                        // 如果完成授信占用的数据先到 已经存在状态中 这里需要进行处理 将对应的数据写到侧输出流
                        DwdCreditOccupyBean creditOccupyBean = creditOccupyStatus.value();
                        if (creditOccupyBean != null) {
                            creditOccupyBean.setApplyAmount(auditApprovalBean.getApplyAmount());
                            creditOccupyBean.setReplyAmount(auditApprovalBean.getReplyAmount());
                            ctx.output(creditOccupyTag, creditOccupyBean);
                            creditOccupyStatus.clear();
                        }

                        // 如果完成合同制作的数据先到 已经存在状态中 这里需要进行处理 将对应的数据写到侧输出流
                        DwdLeaseContractProducedBean contractProducedBean = contractProducedStatus.value();
                        if (contractProducedBean != null) {
                            contractProducedBean.setApplyAmount(auditApprovalBean.getApplyAmount());
                            contractProducedBean.setReplyAmount(auditApprovalBean.getReplyAmount());
                            ctx.output(contractProducedTag, contractProducedBean);
                            contractProducedStatus.clear();
                        }
                    }
                }
                //授信表
                else {
                    //需要判断 新增授信 授信占用 合同制作三种状态
                    String type = value.getString("type");
                    DwdAuditApprovalBean auditApprovalBean = auditApprovalStatus.value(); //从中获取申请金额和批复金额
                    if ("insert".equals(type)) {
                        //新增授信数据
                        DwdCreditAddBean creditAddBean = value.getObject("data", DwdCreditAddBean.class);
                        //补全 新增授信时间
                        creditAddBean.setAddTime(value.getJSONObject("data").getString("create_time"));
                        if (!Objects.isNull(auditApprovalBean)) {
                            if (Objects.isNull(auditApprovalBean.getReplyAmount())) {
                                //批复数据还没到，先存入状态
                                creditAddStatus.update(creditAddBean);
                            } else {
                                //批复的数据到了，数据完整
                                //补全申请金额 | 批复金额 | 新增授信时间
                                creditAddBean.setReplyAmount(auditApprovalBean.getReplyAmount());
                                creditAddBean.setApplyAmount(auditApprovalBean.getApplyAmount());
                                ctx.output(creditAddTag, creditAddBean);
                                auditApprovalStatus.clear();
                            }
                        }
                    } else {
                        String status = value.getJSONObject("data").getString("status");
                        if ("2".equals(status)) {
                            // 此时为授信占用数据
                            DwdCreditOccupyBean creditOccupyBean = value.getObject("data", DwdCreditOccupyBean.class);
                            //补全 完成授信占用时间
                            creditOccupyBean.setOccupyTime(value.getJSONObject("data").getString("credit_occupy_time"));
                            if (!Objects.isNull(auditApprovalBean)) {
                                // 审批通过的数据先到
                                if (auditApprovalBean.getReplyAmount() == null) {
                                    // 批复的数据未到
                                    creditOccupyStatus.update(creditOccupyBean);
                                } else {
                                    // 批复的数据也到了 审批的数据完整
                                    //补全 申请金额 | 批复金额
                                    creditOccupyBean.setApplyAmount(auditApprovalBean.getApplyAmount());
                                    creditOccupyBean.setReplyAmount(auditApprovalBean.getReplyAmount());
                                    ctx.output(creditOccupyTag, creditOccupyBean);
                                    auditApprovalStatus.clear();
                                }
                            }
                        } else if ("3".equals(status)) {
                            // 此时为制作合同
                            DwdLeaseContractProducedBean contractProducedBean = value.getObject("data", DwdLeaseContractProducedBean.class);
                            contractProducedBean.setId(value.getJSONObject("data").getString("contract_id"));
                            contractProducedBean.setCreditId(value.getJSONObject("data").getString("id"));
                            contractProducedBean.setProducedTime(value.getJSONObject("data").getString("contract_produce_time"));

                            if (auditApprovalBean != null) {
                                // 审批通过的数据先到
                                if (auditApprovalBean.getReplyAmount() == null) {
                                    // 批复的数据未到
                                    contractProducedStatus.update(contractProducedBean);
                                } else {
                                    // 批复的数据也到了 审批的数据完整
                                    contractProducedBean.setApplyAmount(auditApprovalBean.getApplyAmount());
                                    contractProducedBean.setReplyAmount(auditApprovalBean.getReplyAmount());
                                    ctx.output(contractProducedTag, contractProducedBean);
                                    auditApprovalStatus.clear();
                                }
                            }
                        }
                    }
                }
            }
        });

//        processStream.print("process>>>");
        //经过上面的操作，已经完成了数据的处理并分流，接下来需要将各个数据流写入到Kafka对用的主题中
        //TODO 6 将审批域写出到kafka对应的主题中
        String approveTopic = "financial_dwd_audit_approve";
        String cancelTopic = "financial_dwd_audit_cancel";
        String rejectTopic = "financial_dwd_audit_reject";

        processStream.print("approval>>>");
        //将申请通过数据写入到Kafka中
        processStream.map(new MapFunction<DwdAuditApprovalBean, String>() {
            @Override
            public String map(DwdAuditApprovalBean value) throws Exception {
                return JSON.toJSONString(value);
            }
        }).sinkTo(KafkaUtil.getKafkaProducer(approveTopic, approveTopic + "_sink")).name("approve_stream_sink");

        //将申请取消数据写入到Kafka中
        SideOutputDataStream<DwdAuditCancelBean> cancelStream = processStream.getSideOutput(auditCancelTag);
        cancelStream.print("cancel>>>");
        cancelStream.map(new MapFunction<DwdAuditCancelBean, String>() {
            @Override
            public String map(DwdAuditCancelBean value) throws Exception {
                return JSON.toJSONString(value);
            }
        }).sinkTo(KafkaUtil.getKafkaProducer(cancelTopic, cancelTopic + "_sink")).name("cancel_stream_sink");

        //将申请拒绝的数据写入到Kafka中
        SideOutputDataStream<DwdAuditRejectBean> rejectStream = processStream.getSideOutput(auditRejectTag);
        rejectStream.print("reject>>>");
        rejectStream.map(new MapFunction<DwdAuditRejectBean, String>() {
                    @Override
                    public String map(DwdAuditRejectBean value) throws Exception {
                        return JSON.toJSONString(value);
                    }
                })
                .sinkTo(KafkaUtil.getKafkaProducer(rejectTopic, rejectTopic + "_sink"))
                .name("reject_stream_sink");
//
        //TODO 7 将授信域写出到Kafka对应的主题中
        String creditAddTopic = "financial_dwd_credit_add";
        String creditOccupyTopic = "financial_dwd_credit_occupy";

        // 7.1 新增授信数据
        SideOutputDataStream<DwdCreditAddBean> creditAddStream = processStream.getSideOutput(creditAddTag);
        creditAddStream.print("add>>>");
        creditAddStream.map(new MapFunction<DwdCreditAddBean, String>() {
                    @Override
                    public String map(DwdCreditAddBean value) throws Exception {
                        return JSON.toJSONString(value);
                    }
                })
                .sinkTo(KafkaUtil.getKafkaProducer(creditAddTopic, creditAddTopic + "_sink"))
                .name("credit_add_sink");

        // 7.2 完成授信占用
        SideOutputDataStream<DwdCreditOccupyBean> creditOccupyStream = processStream.getSideOutput(creditOccupyTag);
        creditOccupyStream.print("occupy>>>");
        creditOccupyStream.map(new MapFunction<DwdCreditOccupyBean, String>() {
                    @Override
                    public String map(DwdCreditOccupyBean value) throws Exception {
                        return JSON.toJSONString(value);
                    }
                })
                .sinkTo(KafkaUtil.getKafkaProducer(creditOccupyTopic, creditOccupyTopic + "_sink"))
                .name("credit_occupy_sink");

        // TODO 8 将合同制作状态的数据写出到Kafka对应的主题中
        // 8.1 合同制作数据
        String leaseContractProducedTopic = "financial_dwd_lease_contract_produce";
        SideOutputDataStream<DwdLeaseContractProducedBean> contractProducedStream = processStream.getSideOutput(contractProducedTag);
        contractProducedStream.print("contract>>>");
        contractProducedStream.map(JSON::toJSONString)
                .sinkTo(KafkaUtil.getKafkaProducer(leaseContractProducedTopic, leaseContractProducedTopic + "_sink"))
                .name("contract_produce_sink");

        //TODO 9 合并租赁域合同制作数据和合同表的数据
        //9.1 合同制作数据
        KeyedStream<DwdLeaseContractProducedBean, String> contractProducedKeyedStream = contractProducedStream.keyBy(new KeySelector<DwdLeaseContractProducedBean, String>() {
            @Override
            public String getKey(DwdLeaseContractProducedBean value) throws Exception {
                return value.getCreditId();
            }
        });
        //9.2 合同表数据
        KeyedStream<JSONObject, String> contractKeyedStream = contractStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getJSONObject("data").getString("credit_id");
            }
        });

        //9.3 合并
        ConnectedStreams<DwdLeaseContractProducedBean, JSONObject> connectStream = contractProducedKeyedStream.connect(contractKeyedStream);

        // TODO 10 数据分流 处理签约和起租数据
        // 以签约流为主流 起租数据写入到侧输出流
        // 起租流标签
        OutputTag<DwdLeaseExecutionBean> executionStreamTag = new OutputTag<DwdLeaseExecutionBean>("execution_stream"){};
        // 对合并后的流进行处理
        SingleOutputStreamOperator<DwdLeaseSignedBean> signedBeanStream = connectStream.process(new KeyedCoProcessFunction<String, DwdLeaseContractProducedBean, JSONObject, DwdLeaseSignedBean>() {
            // 合同制作数据
            private ValueState<DwdLeaseContractProducedBean> contractProducedStatus;
            // 签约数据
            private ValueState<DwdLeaseSignedBean> signedStatus;
            // 起租数据
            private ValueState<DwdLeaseExecutionBean> executionStatus;

            @Override
            public void open(Configuration parameters) throws Exception {
                contractProducedStatus = getRuntimeContext().getState(new ValueStateDescriptor<DwdLeaseContractProducedBean>("contract_produced_state", DwdLeaseContractProducedBean.class));
                signedStatus = getRuntimeContext().getState(new ValueStateDescriptor<DwdLeaseSignedBean>("sign_state", DwdLeaseSignedBean.class));
                executionStatus = getRuntimeContext().getState(new ValueStateDescriptor<DwdLeaseExecutionBean>("execution_state", DwdLeaseExecutionBean.class));
            }

            @Override
            public void processElement1(DwdLeaseContractProducedBean value, KeyedCoProcessFunction<String, DwdLeaseContractProducedBean, JSONObject, DwdLeaseSignedBean>.Context ctx, Collector<DwdLeaseSignedBean> out) throws Exception {
                //当前为合同制作数据
                DwdLeaseSignedBean signedBean = signedStatus.value();
                DwdLeaseExecutionBean executionBean = executionStatus.value();
                if (signedBean == null || executionBean == null) {
                    //有任意一个为空 说明数据不完整 需要等待合同表到来再进行进一步的处理 此时选择将合同制作数据保存到相应状态中
                    contractProducedStatus.update(value);
                }
                // 合同表数据先到 更新过签约状态 但数据不完整
                if (signedBean != null) {
                    signedBean.setCreditFacilityId(value.getCreditFacilityId());
                    signedBean.setApplyAmount(value.getApplyAmount());
                    signedBean.setReplyAmount(value.getReplyAmount());
                    signedBean.setCreditAmount(value.getCreditAmount());
                    out.collect(signedBean);
                    signedStatus.clear();
                }
                // 合同表数据先到 更新过起租状态 但数据不完整
                if (executionBean != null) {
                    executionBean.setCreditFacilityId(value.getCreditFacilityId());
                    executionBean.setApplyAmount(value.getApplyAmount());
                    executionBean.setReplyAmount(value.getReplyAmount());
                    executionBean.setCreditAmount(value.getCreditAmount());
                    ctx.output(executionStreamTag, executionBean);
                    executionStatus.clear();
                }
            }

            @Override
            public void processElement2(JSONObject value, KeyedCoProcessFunction<String, DwdLeaseContractProducedBean, JSONObject, DwdLeaseSignedBean>.Context ctx, Collector<DwdLeaseSignedBean> out) throws Exception {
                //当前为合同表数据
                DwdLeaseContractProducedBean contractProducedBean = contractProducedStatus.value();

                String type = value.getString("type");
                if ("update".equals(type)) {
                    String status = value.getJSONObject("data").getString("status");
                    if ("2".equals(status)) {
                        // 当前为签约数据
                        DwdLeaseSignedBean leaseSignedBean = value.getObject("data", DwdLeaseSignedBean.class);
                        if (Objects.isNull(contractProducedBean)) {
                            signedStatus.update(leaseSignedBean);
                        } else {
                            // 合同制作数据先到  需要将签约数据写出到主流
                            leaseSignedBean.setCreditFacilityId(contractProducedBean.getCreditFacilityId());
                            leaseSignedBean.setApplyAmount(contractProducedBean.getApplyAmount());
                            leaseSignedBean.setReplyAmount(contractProducedBean.getReplyAmount());
                            leaseSignedBean.setCreditAmount(contractProducedBean.getCreditAmount());
                            out.collect(leaseSignedBean);
                        }

                    } else if ("3".equals(status)) {
                        // 当前为起租数据
                        DwdLeaseExecutionBean executionBean = value.getObject("data", DwdLeaseExecutionBean.class);
                        if (Objects.isNull(contractProducedBean)) {
                            executionStatus.update(executionBean);
                        } else {
                            // 合同制作数据先到  需要将起租数据写出到侧输出流
                            executionBean.setCreditFacilityId(contractProducedBean.getCreditFacilityId());
                            executionBean.setApplyAmount(contractProducedBean.getApplyAmount());
                            executionBean.setReplyAmount(contractProducedBean.getReplyAmount());
                            executionBean.setCreditAmount(contractProducedBean.getCreditAmount());
                            ctx.output(executionStreamTag, executionBean);
                        }
                    }
                }
            }
        });




        // TODO 11 提取起租流 将签约和起租数据写出到对应的kafka主题
        // 11.1 写出签约数据
        String signedTopic = "financial_dwd_lease_sign";
        signedBeanStream.print("sign>>>");
        signedBeanStream.map(new MapFunction<DwdLeaseSignedBean, String>() {
                    @Override
                    public String map(DwdLeaseSignedBean value) throws Exception {
                        return JSON.toJSONString(value);
                    }
                })
                .sinkTo(KafkaUtil.getKafkaProducer(signedTopic, signedTopic + "_sink"))
                .name("sign_sink");

        // 11.2 写出起租数据
        // 获取起租流
        SideOutputDataStream<DwdLeaseExecutionBean> executionStream = signedBeanStream.getSideOutput(executionStreamTag);
        executionStream.print("execution>>>");
        String executionTopic = "financial_dwd_lease_execution";
        executionStream.map(new MapFunction<DwdLeaseExecutionBean, String>() {
                    @Override
                    public String map(DwdLeaseExecutionBean value) throws Exception {
                        return JSON.toJSONString(value);
                    }
                })
                .sinkTo(KafkaUtil.getKafkaProducer(executionTopic, executionTopic + "_sink"))
                .name("execution_sink");

        //TODO 12 执行
        env.execute();
    }
}
