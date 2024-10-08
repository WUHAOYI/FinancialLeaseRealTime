package com.why.app.test;
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
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Created by WHY on 2024/9/6.
 * Functions:
 */
public class DwdReference {
    public static void main(String[] args) throws Exception {
        // TODO 1 初始化流环境
        String appName = "dwd_base_app1";
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(8082, appName);
        env.setParallelism(1);

        // TODO 2 从ods层读取数据 topic_db
        KafkaSource<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(FinancialLeaseCommon.KAFKA_ODS_TOPIC, appName, OffsetsInitializer.earliest());

        DataStreamSource<String> odsStream = env.fromSource(kafkaConsumer, WatermarkStrategy.noWatermarks(), appName);

        // TODO 3 筛选表示业务过程的表格 5张表
        // credit、credit facility、credit facility status、reply、contract
        SingleOutputStreamOperator<JSONObject> flatMapStream = odsStream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(value);
                String tableName = jsonObject.getString("table");
                // 添加ETL 保证下游数据是完整
                if ("credit".equals(tableName) || "credit_facility_status".equals(tableName) || "reply".equals(tableName)) {
                    if (jsonObject.getJSONObject("data").getString("credit_facility_id") != null) {
                        out.collect(jsonObject);
                    }

                } else if ("credit_facility".equals(tableName) && jsonObject.getJSONObject("data").getString("id") != null) {
                    out.collect(jsonObject);
                } else if ("contract".equals(tableName)) {
                    if (jsonObject.getJSONObject("data").getString("credit_id") != null) {
                        out.collect(jsonObject);
                    }
                }
            }
        });
        // 3.1 筛选contract表数据
        SingleOutputStreamOperator<JSONObject> contractStream = flatMapStream.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {

                return "contract".equals(value.getString("table"));
            }
        });
//        contractStream.print("contractStream>>>>");

        // 3.2 筛选出credit、credit facility、credit facility status、reply
        SingleOutputStreamOperator<JSONObject> mainStream = flatMapStream.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {

                return !"contract".equals(value.getString("table"));
            }
        });

        // TODO 4 按照授信id进行分组
        KeyedStream<JSONObject, String> keyedStream = mainStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                String tableName = value.getString("table");
                if ("credit_facility".equals(tableName)) {
                    return value.getJSONObject("data").getString("id");
                }
                return value.getJSONObject("data").getString("credit_facility_id");
            }
        });

//        keyedStream.print("keyed>>>");

        // TODO 5 动态分流
        // 5.1 定义侧输出流标签
        // 审批域
        // 审批拒绝侧输出流标签
        OutputTag<DwdAuditRejectBean> rejectStreamTag = new OutputTag<DwdAuditRejectBean>("reject_stream") {
        };

        // 审批取消侧输出流标签
        OutputTag<DwdAuditCancelBean> cancelStreamTag = new OutputTag<DwdAuditCancelBean>("cancel_stream") {
        };

        // 授信域
        // 授信新增
        OutputTag<DwdCreditAddBean> creditAddTag = new OutputTag<DwdCreditAddBean>("credit_add_stream") {
        };

        // 授信完成
        OutputTag<DwdCreditOccupyBean> creditOccupyTag = new OutputTag<DwdCreditOccupyBean>("credit_occupy_stream") {
        };

        // 租赁域
        OutputTag<DwdLeaseContractProducedBean> contractProducedTag = new OutputTag<DwdLeaseContractProducedBean>("contract_produced_stream") {
        };


        SingleOutputStreamOperator<DwdAuditApprovalBean> processStream = keyedStream.process(new KeyedProcessFunction<String, JSONObject, DwdAuditApprovalBean>() {

            // 审批信审经办员工ID
            private ValueState<String> auditManIdStatus;
            // 批复信息记录
            private ValueState<DwdAuditReplyBean> replyBeanStatus;
            // 审批通过信息记录
            private ValueState<DwdAuditApprovalBean> approvalBeanStatus;
            // 新增授信
            private ValueState<DwdCreditAddBean> creditAddBeanStatus;
            // 完成授信
            private ValueState<DwdCreditOccupyBean> creditOccupyBeanStatus;
            // 制作合同
            private ValueState<DwdLeaseContractProducedBean> contractProductBeanStatus;


            @Override
            public void open(Configuration parameters) throws Exception {
                // 信审经办id状态
                auditManIdStatus = getRuntimeContext().getState(new ValueStateDescriptor<String>("audit_man_id", String.class));

                replyBeanStatus = getRuntimeContext().getState(new ValueStateDescriptor<DwdAuditReplyBean>("audit_reply_status", DwdAuditReplyBean.class));

                approvalBeanStatus = getRuntimeContext().getState(new ValueStateDescriptor<DwdAuditApprovalBean>("audit_approval_status", DwdAuditApprovalBean.class));

                // 设置状态的存活时间
                ValueStateDescriptor<DwdCreditAddBean> auditAddStatusDescriptor = new ValueStateDescriptor<>("audit_add_status", DwdCreditAddBean.class);
                auditAddStatusDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(30L)).build());
                creditAddBeanStatus = getRuntimeContext().getState(auditAddStatusDescriptor);


                ValueStateDescriptor<DwdCreditOccupyBean> auditOccupyStatusDescriptor = new ValueStateDescriptor<>("audit_occupy_status", DwdCreditOccupyBean.class);
                auditOccupyStatusDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(30L)).build());
                creditOccupyBeanStatus = getRuntimeContext().getState(auditOccupyStatusDescriptor);

                ValueStateDescriptor<DwdLeaseContractProducedBean> contractProducedStatusDescriptor = new ValueStateDescriptor<>("contract_produced_status", DwdLeaseContractProducedBean.class);
                contractProducedStatusDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(30L)).build());
                contractProductBeanStatus = getRuntimeContext().getState(contractProducedStatusDescriptor);
            }

            @Override
            public void processElement(JSONObject jsonObject, Context ctx, Collector<DwdAuditApprovalBean> out) throws Exception {
                String tableName = jsonObject.getString("table");
                // 不同的业务表格需要进行不同的处理方式
                if ("credit_facility_status".equals(tableName)) {
                    // 审批申请状态表
                    String status = jsonObject.getJSONObject("data").getString("status");
                    // 记录信审经办审批状态
                    // 5 审批通过
                    // 6 审批不通过 但是申请复议
                    // 20 审批不通过
                    if ("5".equals(status) || "6".equals(status) || "20".equals(status)) {
                        String employeeId = jsonObject.getJSONObject("data").getString("employee_id");
                        auditManIdStatus.update(employeeId);
                    }
                } else if ("credit_facility".equals(tableName)) {
                    String type = jsonObject.getString("type");
                    String status = jsonObject.getJSONObject("data").getString("status");
                    // 判断审批最终的状态
                    // 16 通过
                    // 20 拒绝
                    // 21 取消
                    if ("update".equals(type)) {
                        switch (status) {
                            case "16": {
                                DwdAuditApprovalBean auditApprovalBean = jsonObject.getObject("data", DwdAuditApprovalBean.class);
                                // 补全信息
                                String auditManId = auditManIdStatus.value();
                                auditApprovalBean.setAuditManId(auditManId);
                                auditManIdStatus.clear();


                                auditApprovalBean.setApproveTime(jsonObject.getJSONObject("data").getString("update_time"));
                                auditApprovalBean.setApplyAmount(jsonObject.getJSONObject("data").getBigDecimal("credit_amount"));
                                // TODO 补全批复信息
                                DwdAuditReplyBean replyBean = replyBeanStatus.value();

                                // 数据先后问题
                                if (replyBean == null) {
                                    approvalBeanStatus.update(auditApprovalBean);
                                } else {

                                    auditApprovalBean.setReplyId(replyBean.getId());
                                    auditApprovalBean.setReplyAmount(replyBean.getReplyAmount());
                                    auditApprovalBean.setReplyTime(replyBean.getReplyTime());
                                    auditApprovalBean.setIrr(replyBean.getIrr());
                                    auditApprovalBean.setPeriod(replyBean.getPeriod());
                                    replyBeanStatus.clear();

                                    // 下游也可能需要用到审批通过的数据
                                    approvalBeanStatus.update(auditApprovalBean);

                                    out.collect(auditApprovalBean);

                                    // 如果新增授信的数据先到 已经存在了状态中 这里需要进行处理 将对应的数据写出到侧输出流
                                    DwdCreditAddBean creditAddBean = creditAddBeanStatus.value();
                                    if (creditAddBean != null) {
                                        creditAddBean.setApplyAmount(auditApprovalBean.getApplyAmount());
                                        creditAddBean.setReplyAmount(auditApprovalBean.getReplyAmount());
                                        ctx.output(creditAddTag, creditAddBean);
                                        creditAddBeanStatus.clear();
                                    }
                                    // 如果完成授信占用的数据先到 已经存在状态中 这里需要进行处理 将对应的数据写到侧输出流
                                    DwdCreditOccupyBean creditOccupyBean = creditOccupyBeanStatus.value();
                                    if (creditOccupyBean != null) {
                                        creditOccupyBean.setApplyAmount(auditApprovalBean.getApplyAmount());
                                        creditOccupyBean.setReplyAmount(auditApprovalBean.getReplyAmount());
                                        ctx.output(creditOccupyTag, creditOccupyBean);
                                        creditOccupyBeanStatus.clear();
                                    }

                                    // 如果完成合同制作的数据先到 已经存在状态中 这里需要进行处理 将对应的数据写到侧输出流
                                    DwdLeaseContractProducedBean contractProducedBean = contractProductBeanStatus.value();
                                    if (contractProducedBean != null) {
                                        contractProducedBean.setApplyAmount(auditApprovalBean.getApplyAmount());
                                        contractProducedBean.setReplyAmount(auditApprovalBean.getReplyAmount());
                                        ctx.output(contractProducedTag, contractProducedBean);
                                        contractProductBeanStatus.clear();
                                    }
                                }


                                break;
                            }
                            case "20": {
                                DwdAuditRejectBean auditRejectBean = jsonObject.getObject("data", DwdAuditRejectBean.class);
                                String auditManId = auditManIdStatus.value();
                                auditRejectBean.setAuditManId(auditManId);
                                auditManIdStatus.clear();
                                auditRejectBean.setRejectTime(jsonObject.getJSONObject("data").getString("update_time"));
                                auditRejectBean.setApplyAmount(jsonObject.getJSONObject("data").getBigDecimal("credit_amount"));

                                ctx.output(rejectStreamTag, auditRejectBean);

                                // 审批拒绝之后 可能出现复议 需要将之前的状态清空
                                approvalBeanStatus.clear();

                                break;
                            }
                            case "21": {
                                DwdAuditCancelBean auditCancelBean = jsonObject.getObject("data", DwdAuditCancelBean.class);
                                String auditManId = auditManIdStatus.value();
                                auditCancelBean.setAuditManId(auditManId);
                                auditManIdStatus.clear();
                                auditCancelBean.setCancelTime(jsonObject.getJSONObject("data").getString("update_time"));
                                auditCancelBean.setApplyAmount(jsonObject.getJSONObject("data").getBigDecimal("credit_amount"));
                                ctx.output(cancelStreamTag, auditCancelBean);
                                // 审批取消 不再需要后续 状态清空
                                approvalBeanStatus.clear();
                                break;
                            }
                        }
                    }
                } else if ("reply".equals(tableName)) {
                    // 记录批复信息
                    DwdAuditReplyBean auditReplyBean = jsonObject.getObject("data", DwdAuditReplyBean.class);
                    // 手动添加特殊字段名称的数据
                    auditReplyBean.setReplyTime(jsonObject.getJSONObject("data").getString("create_time"));
                    auditReplyBean.setReplyAmount(jsonObject.getJSONObject("data").getBigDecimal("credit_amount"));

                    // 如果审批通过的数据先到 需要在批复数据的位置完成数据的合并及输出
                    DwdAuditApprovalBean auditApprovalBean = approvalBeanStatus.value();
                    if (auditApprovalBean != null) {
                        auditApprovalBean.setReplyId(auditReplyBean.getId());
                        auditApprovalBean.setReplyAmount(auditReplyBean.getReplyAmount());
                        auditApprovalBean.setReplyTime(auditReplyBean.getReplyTime());
                        auditApprovalBean.setIrr(auditReplyBean.getIrr());
                        auditApprovalBean.setPeriod(auditReplyBean.getPeriod());

                        // 将审批通过的数据当做主流写出
                        out.collect(auditApprovalBean);
                        approvalBeanStatus.update(auditApprovalBean);

                        // 此时审批通过的数据才完整
                        // 如果新增授信的数据先到 已经存在了状态中 这里需要进行处理 将对应的数据写出到侧输出流
                        DwdCreditAddBean creditAddBean = creditAddBeanStatus.value();
                        if (creditAddBean != null) {
                            creditAddBean.setApplyAmount(auditApprovalBean.getApplyAmount());
                            creditAddBean.setReplyAmount(auditApprovalBean.getReplyAmount());
                            ctx.output(creditAddTag, creditAddBean);
                            creditAddBeanStatus.clear();
                        }
                        // 如果完成授信占用的数据先到 已经存在状态中 这里需要进行处理 将对应的数据写到侧输出流
                        DwdCreditOccupyBean creditOccupyBean = creditOccupyBeanStatus.value();
                        if (creditOccupyBean != null) {
                            creditOccupyBean.setApplyAmount(auditApprovalBean.getApplyAmount());
                            creditOccupyBean.setReplyAmount(auditApprovalBean.getReplyAmount());
                            ctx.output(creditOccupyTag, creditOccupyBean);
                            creditOccupyBeanStatus.clear();
                        }

                        // 如果完成合同制作的数据先到 已经存在状态中 这里需要进行处理 将对应的数据写到侧输出流
                        DwdLeaseContractProducedBean contractProducedBean = contractProductBeanStatus.value();
                        if (contractProducedBean != null) {
                            contractProducedBean.setApplyAmount(auditApprovalBean.getApplyAmount());
                            contractProducedBean.setReplyAmount(auditApprovalBean.getReplyAmount());
                            ctx.output(contractProducedTag, contractProducedBean);
                            contractProductBeanStatus.clear();
                        }

                    } else {
                        replyBeanStatus.update(auditReplyBean);
                    }

                } else {
                    // credit 授信表
                    // 获取type 判断是否为新增

                    String type = jsonObject.getString("type");
                    DwdAuditApprovalBean auditApprovalBean = approvalBeanStatus.value();

                    if ("insert".equals(type)) {
                        // 此时为新增授信数据
                        DwdCreditAddBean creditAddBean = jsonObject.getObject("data", DwdCreditAddBean.class);
                        creditAddBean.setAddTime(jsonObject.getJSONObject("data").getString("create_time"));
                        if (auditApprovalBean != null) {
                            // 审批通过的数据先到
                            if (auditApprovalBean.getReplyAmount() == null) {
                                // 批复的数据未到
                                creditAddBeanStatus.update(creditAddBean);
                            } else {
                                // 批复的数据也到了 审批的数据完整
                                creditAddBean.setApplyAmount(auditApprovalBean.getApplyAmount());
                                creditAddBean.setReplyAmount(auditApprovalBean.getReplyAmount());
                                ctx.output(creditAddTag, creditAddBean);
                                approvalBeanStatus.clear();
                            }
                        }
                    } else {
                        String status = jsonObject.getJSONObject("data").getString("status");
                        if ("2".equals(status)) {
                            // 此时为授信占用数据
                            DwdCreditOccupyBean creditOccupyBean = jsonObject.getObject("data", DwdCreditOccupyBean.class);
                            creditOccupyBean.setOccupyTime(jsonObject.getJSONObject("data").getString("credit_occupy_time"));
                            if (auditApprovalBean != null) {
                                // 审批通过的数据先到
                                if (auditApprovalBean.getReplyAmount() == null) {
                                    // 批复的数据未到
                                    creditOccupyBeanStatus.update(creditOccupyBean);
                                } else {
                                    // 批复的数据也到了 审批的数据完整
                                    creditOccupyBean.setApplyAmount(auditApprovalBean.getApplyAmount());
                                    creditOccupyBean.setReplyAmount(auditApprovalBean.getReplyAmount());
                                    ctx.output(creditOccupyTag, creditOccupyBean);
                                    approvalBeanStatus.clear();
                                }
                            }
                        } else if ("3".equals(status)) {
                            // 此时为制作合同
                            DwdLeaseContractProducedBean contractProducedBean = jsonObject.getObject("data", DwdLeaseContractProducedBean.class);
                            contractProducedBean.setId(jsonObject.getJSONObject("data").getString("contract_id"));
                            contractProducedBean.setCreditId(jsonObject.getJSONObject("data").getString("id"));
                            contractProducedBean.setProducedTime(jsonObject.getJSONObject("data").getString("contract_produce_time"));

                            if (auditApprovalBean != null) {
                                // 审批通过的数据先到
                                if (auditApprovalBean.getReplyAmount() == null) {
                                    // 批复的数据未到
                                    contractProductBeanStatus.update(contractProducedBean);
                                } else {
                                    // 批复的数据也到了 审批的数据完整
                                    contractProducedBean.setApplyAmount(auditApprovalBean.getApplyAmount());
                                    contractProducedBean.setReplyAmount(auditApprovalBean.getReplyAmount());
                                    ctx.output(contractProducedTag, contractProducedBean);
                                    approvalBeanStatus.clear();
                                }
                            }
                        }
                    }
                }
            }
        });
        processStream.print("process>>>");

        // TODO 6 将审批域写出到kafka对应的主题中
        String approveTopic = "financial_dwd_audit_approve";
        String cancelTopic = "financial_dwd_audit_cancel";
        String rejectTopic = "financial_dwd_audit_reject";

        // 获取侧输出流
        SideOutputDataStream<DwdAuditCancelBean> cancelStream = processStream.getSideOutput(cancelStreamTag);


        cancelStream.print("cancel>>>");
        SideOutputDataStream<DwdAuditRejectBean> rejectStream = processStream.getSideOutput(rejectStreamTag);

        // 6.1 审批通过数据
        processStream.map(new MapFunction<DwdAuditApprovalBean, String>() {
                    @Override
                    public String map(DwdAuditApprovalBean value) throws Exception {
                        return JSON.toJSONString(value);
                    }
                })
                .sinkTo(KafkaUtil.getKafkaProducer(approveTopic, approveTopic + "_sink"))
                .name("approve_stream_sink");
//        // 6.2 审批拒绝数据
//        rejectStream.map(new MapFunction<DwdAuditRejectBean, String>() {
//                    @Override
//                    public String map(DwdAuditRejectBean value) throws Exception {
//                        return JSON.toJSONString(value);
//                    }
//                })
//                .sinkTo(KafkaUtil.getKafkaProducer(rejectTopic, rejectTopic + "_sink"))
//                .name("reject_stream_sink");
//        // 6.3 审批取消数据
//        cancelStream.map(new MapFunction<DwdAuditCancelBean, String>() {
//                    @Override
//                    public String map(DwdAuditCancelBean value) throws Exception {
//                        return JSON.toJSONString(value);
//                    }
//                })
//                .sinkTo(KafkaUtil.getKafkaProducer(cancelTopic, cancelTopic + "_sink"))
//                .name("cancel_stream_sink");
//
//        // TODO 7 将授信域写出到kafka对应的主题
//        String creditAddTopic = "financial_dwd_credit_add";
//        String creditOccupyTopic = "financial_dwd_credit_occupy";
//
//        SideOutputDataStream<DwdCreditAddBean> creditAddStream = processStream.getSideOutput(creditAddTag);
//        SideOutputDataStream<DwdCreditOccupyBean> creditOccupyStream = processStream.getSideOutput(creditOccupyTag);
//
//        // 7.1 新增授信数据
//        creditAddStream.map(new MapFunction<DwdCreditAddBean, String>() {
//                    @Override
//                    public String map(DwdCreditAddBean value) throws Exception {
//                        return JSON.toJSONString(value);
//                    }
//                })
//                .sinkTo(KafkaUtil.getKafkaProducer(creditAddTopic, creditAddTopic + "_sink"))
//                .name("credit_add_sink");
//
//        // 7.2 完成授信占用
//        creditOccupyStream.map(new MapFunction<DwdCreditOccupyBean, String>() {
//                    @Override
//                    public String map(DwdCreditOccupyBean value) throws Exception {
//                        return JSON.toJSONString(value);
//                    }
//                })
//                .sinkTo(KafkaUtil.getKafkaProducer(creditOccupyTopic, creditOccupyTopic + "_sink"))
//                .name("credit_occupy_sink");
//
//        // TODO 8 过滤合同制作状态的授信数据
//        // 8.1 合同制作数据
//        String leaseContractProducedTopic = "financial_dwd_lease_contract_produce";
//        SideOutputDataStream<DwdLeaseContractProducedBean> contractProducedStream = processStream.getSideOutput(contractProducedTag);
//        contractProducedStream.map(JSON::toJSONString)
//                .sinkTo(KafkaUtil.getKafkaProducer(leaseContractProducedTopic, leaseContractProducedTopic + "_sink"))
//                .name("contract_produce_sink");
//
//
//        // TODO 9 合并租赁域合同制作数据和合同表数据
//        // 根据分组的主键作为关联的条件
//        KeyedStream<DwdLeaseContractProducedBean, String> producedBeanStringKeyedStream = contractProducedStream.keyBy(new KeySelector<DwdLeaseContractProducedBean, String>() {
//            @Override
//            public String getKey(DwdLeaseContractProducedBean value) throws Exception {
//                return value.getCreditId();
//            }
//        });
//        // 合同表数据
//        KeyedStream<JSONObject, String> contractKeyedStream = contractStream.keyBy(new KeySelector<JSONObject, String>() {
//            @Override
//            public String getKey(JSONObject value) throws Exception {
//                return value.getJSONObject("data").getString("credit_id");
//            }
//        });
//
//        ConnectedStreams<DwdLeaseContractProducedBean, JSONObject> connectStream = producedBeanStringKeyedStream.connect(contractKeyedStream);
//
//
//        // TODO 10 数据分流
//        OutputTag<DwdLeaseExecutionBean> executionTag = new OutputTag<DwdLeaseExecutionBean>("execution_produced_stream") {
//        };
//
//        // 主流为签约流
//        SingleOutputStreamOperator<DwdLeaseSignedBean> signedBeanStream = connectStream.process(new KeyedCoProcessFunction<String, DwdLeaseContractProducedBean, JSONObject, DwdLeaseSignedBean>() {
//
//            // 合同制作数据
//            private ValueState<DwdLeaseContractProducedBean> contractProducedState;
//
//            // 签约数据
//            private ValueState<DwdLeaseSignedBean> signState;
//
//            // 起租数据
//            private ValueState<DwdLeaseExecutionBean> executionState;
//
//            @Override
//            public void open(Configuration parameters) throws Exception {
//                contractProducedState = getRuntimeContext().getState(new ValueStateDescriptor<DwdLeaseContractProducedBean>("contract_produced_state", DwdLeaseContractProducedBean.class));
//
//                signState = getRuntimeContext().getState(new ValueStateDescriptor<DwdLeaseSignedBean>("sign_state", DwdLeaseSignedBean.class));
//
//                executionState = getRuntimeContext().getState(new ValueStateDescriptor<DwdLeaseExecutionBean>("execution_state", DwdLeaseExecutionBean.class));
//            }
//
//            @Override
//            public void processElement1(DwdLeaseContractProducedBean bean, Context ctx, Collector<DwdLeaseSignedBean> out) throws Exception {
//                // 当前为合同制作数据  判断合同表数据是否先到 进行处理
//                DwdLeaseSignedBean signedBean = signState.value();
//                DwdLeaseExecutionBean executionBean = executionState.value();
//                if (signedBean == null || executionBean == null) {
//                    // 任意一个为空 表示数据不完整 后续需要使用到合同制作的数据
//                    contractProducedState.update(bean);
//                }
//
//                // 如果签约数据不为空
//                if (signedBean != null) {
//                    // 此时签约的数据完整 需要写出到主流
//                    signedBean.setCreditFacilityId(bean.getCreditFacilityId());
//                    signedBean.setApplyAmount(bean.getApplyAmount());
//                    signedBean.setReplyAmount(bean.getReplyAmount());
//                    signedBean.setCreditAmount(bean.getCreditAmount());
//                    out.collect(signedBean);
//                    signState.clear();
//                }
//                // 如果起租数据不为空
//                if (executionBean != null) {
//                    // 此时起租数据完整 需要写出到侧输出流
//                    executionBean.setCreditFacilityId(bean.getCreditFacilityId());
//                    executionBean.setApplyAmount(bean.getApplyAmount());
//                    executionBean.setReplyAmount(bean.getReplyAmount());
//                    executionBean.setCreditAmount(bean.getCreditAmount());
//                    ctx.output(executionTag, executionBean);
//                    executionState.clear();
//                }
//
//            }
//
//            @Override
//            public void processElement2(JSONObject jsonObject, Context ctx, Collector<DwdLeaseSignedBean> out) throws Exception {
//                // 当前为合同表代表签约和起租  判断合同制作数据是否先到 进行处理
//                DwdLeaseContractProducedBean contractProducedBean = contractProducedState.value();
//
//                String type = jsonObject.getString("type");
//                if ("update".equals(type)) {
//                    String status = jsonObject.getJSONObject("data").getString("status");
//                    if ("2".equals(status)) {
//                        // 当前为签约数据
//                        DwdLeaseSignedBean leaseSignedBean = jsonObject.getObject("data", DwdLeaseSignedBean.class);
//                        if (contractProducedBean == null) {
//                            signState.update(leaseSignedBean);
//                        } else {
//                            // 合同制作数据先到  需要将签约数据写出到主流
//                            leaseSignedBean.setCreditFacilityId(contractProducedBean.getCreditFacilityId());
//                            leaseSignedBean.setApplyAmount(contractProducedBean.getApplyAmount());
//                            leaseSignedBean.setReplyAmount(contractProducedBean.getReplyAmount());
//                            leaseSignedBean.setCreditAmount(contractProducedBean.getCreditAmount());
//                            out.collect(leaseSignedBean);
//                        }
//
//                    } else if ("3".equals(status)) {
//                        // 当前为起租数据
//                        DwdLeaseExecutionBean executionBean = jsonObject.getObject("data", DwdLeaseExecutionBean.class);
//                        if (contractProducedBean == null) {
//                            executionState.update(executionBean);
//                        } else {
//                            // 合同制作数据先到  需要将起租数据写出到侧输出流
//                            executionBean.setCreditFacilityId(contractProducedBean.getCreditFacilityId());
//                            executionBean.setApplyAmount(contractProducedBean.getApplyAmount());
//                            executionBean.setReplyAmount(contractProducedBean.getReplyAmount());
//                            executionBean.setCreditAmount(contractProducedBean.getCreditAmount());
//                            ctx.output(executionTag, executionBean);
//                        }
//                    }
//                }
//            }
//        });
//
//        // 获取起租流
//        SideOutputDataStream<DwdLeaseExecutionBean> executionStream = signedBeanStream.getSideOutput(executionTag);
//
//
//        // TODO 11 提取起租流 将签约和起租数据写出到对应的kafka主题
//        // 11.1 写出签约数据
//        String signedTopic = "financial_dwd_lease_sign";
//
//        signedBeanStream.map(new MapFunction<DwdLeaseSignedBean, String>() {
//                    @Override
//                    public String map(DwdLeaseSignedBean value) throws Exception {
//                        return JSON.toJSONString(value);
//                    }
//                })
//                .sinkTo(KafkaUtil.getKafkaProducer(signedTopic,signedTopic+"_sink"))
//                .name("sign_sink");
//
//        // 11.2 写出起租数据
//        String executionTopic = "financial_dwd_lease_execution";
//        executionStream.map(new MapFunction<DwdLeaseExecutionBean, String>() {
//                    @Override
//                    public String map(DwdLeaseExecutionBean value) throws Exception {
//                        return JSON.toJSONString(value);
//                    }
//                })
//                .sinkTo(KafkaUtil.getKafkaProducer(executionTopic,executionTopic+"_sink"))
//                .name("execution_sink");

        // TODO 12 执行任务
        env.execute();

    }
}