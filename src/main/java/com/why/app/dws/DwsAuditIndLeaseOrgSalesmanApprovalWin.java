package com.why.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.why.app.func.AsyncDimFunctionHBase;
import com.why.bean.DwdAuditApprovalBean;
import com.why.bean.DwsAuditIndLeaseOrgSalesmanApprovalBean;
import com.why.util.*;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.shaded.com.nimbusds.jose.util.JSONStringUtils;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Created by WHY on 2024/9/7.
 * Functions: 审批域行业业务方向业务经办粒度审批通过各窗口汇总 主程序
 */
public class DwsAuditIndLeaseOrgSalesmanApprovalWin {
    public static void main(String[] args) throws Exception {
        String appName = "dws_audit_industry_lease_organization_salesman_approval_window";
        //TODO 1 创建环境
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(8083, appName);
        env.setParallelism(1);

        //TODO 2 获取Kafka数据
        String approveTopic = "financial_dwd_audit_approve";
        KafkaSource<String> kafkaSource = KafkaUtil.getKafkaConsumer(approveTopic, appName, OffsetsInitializer.earliest());
        DataStreamSource<String> kafkaSourceStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source");

        //TODO 3 转换数据结构（从Kafka中读取的数据是DwdAuditApprovalBean格式的字符串，需要将其转换为dws层的bean结构
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanApprovalBean> dwsBeanStream = kafkaSourceStream.map(new MapFunction<String, DwsAuditIndLeaseOrgSalesmanApprovalBean>() {
            @Override
            public DwsAuditIndLeaseOrgSalesmanApprovalBean map(String value) throws Exception {

                //对应的字段会自动转化
                DwsAuditIndLeaseOrgSalesmanApprovalBean bean = JSONObject.parseObject(value, DwsAuditIndLeaseOrgSalesmanApprovalBean.class);
                //填充其余字段
                bean.setIndustry3Id(JSONObject.parseObject(value).getString("industryId"));
                bean.setApplyCount(1L);
                return bean;
            }
        });

//        dwsBeanStream.print("dws>>>");

        //TODO 4 引入水位线
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanApprovalBean> streamWithWatermark = dwsBeanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<DwsAuditIndLeaseOrgSalesmanApprovalBean>forBoundedOutOfOrderness(
                Duration.ofSeconds(5L)
        ).withTimestampAssigner(new SerializableTimestampAssigner<DwsAuditIndLeaseOrgSalesmanApprovalBean>() {
            @Override
            public long extractTimestamp(DwsAuditIndLeaseOrgSalesmanApprovalBean element, long recordTimestamp) {
                //从数据中提取字段作为水位线的时间戳
                return element.getTs();
            }
        }));

        //TODO 5 分组
        //分组依据：行业 业务方向 业务经办id
        KeyedStream<DwsAuditIndLeaseOrgSalesmanApprovalBean, String> keyedStream = streamWithWatermark.keyBy(new KeySelector<DwsAuditIndLeaseOrgSalesmanApprovalBean, String>() {
            @Override
            public String getKey(DwsAuditIndLeaseOrgSalesmanApprovalBean bean) throws Exception {
                return bean.getLeaseOrganization() + ":" + bean.getIndustry3Id() + ":" + bean.getSalesmanId();
            }
        });

        //TODO 6 开窗
        // 开窗的时间决定了最终结果的最小时间范围  越小精度越高  越大越省资源
        WindowedStream<DwsAuditIndLeaseOrgSalesmanApprovalBean, String, TimeWindow> windowStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10L)));

        //TODO 7 聚合 先聚合再进行维度关联，可以减少处理的数据
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanApprovalBean> reducedStream = windowStream.reduce(new ReduceFunction<DwsAuditIndLeaseOrgSalesmanApprovalBean>() {
            @Override
            public DwsAuditIndLeaseOrgSalesmanApprovalBean reduce(DwsAuditIndLeaseOrgSalesmanApprovalBean value1, DwsAuditIndLeaseOrgSalesmanApprovalBean value2) throws Exception {
                // 聚合逻辑
                value1.setApplyCount(value1.getApplyCount() + value2.getApplyCount());
                value1.setApplyAmount(value1.getApplyAmount().add(value2.getApplyAmount()));
                value1.setReplyAmount(value1.getReplyAmount().add(value2.getReplyAmount()));
                return value1;
            }
        }, new ProcessWindowFunction<DwsAuditIndLeaseOrgSalesmanApprovalBean, DwsAuditIndLeaseOrgSalesmanApprovalBean, String, TimeWindow>() {
            @Override
            public void process(String s, ProcessWindowFunction<DwsAuditIndLeaseOrgSalesmanApprovalBean, DwsAuditIndLeaseOrgSalesmanApprovalBean, String, TimeWindow>.Context context, Iterable<DwsAuditIndLeaseOrgSalesmanApprovalBean> elements, Collector<DwsAuditIndLeaseOrgSalesmanApprovalBean> out) throws Exception {
                String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                String curDate = DateFormatUtil.toDate(context.window().getStart());
                for (DwsAuditIndLeaseOrgSalesmanApprovalBean element : elements) {
                    element.setStt(stt);
                    element.setEdt(edt);
                    element.setCurDate(curDate);
                    out.collect(element);
                }
            }
        });

//        reducedStream.print(">>>>");

        //TODO 8 补全维度信息
        //需要用到Flink的异步流处理
        //使用RichAsyncFunction的原因是需要在open/close方法中开启/关闭HBase的异步连接
        //为了逻辑复用，自己实现一个RichAsyncFunction来应用于所有的维度信息补全操作

        //8.1 关联三级行业名称及二级行业ID
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanApprovalBean> C3NameC2IdStream = AsyncDataStream.unorderedWait(reducedStream, new AsyncDimFunctionHBase<DwsAuditIndLeaseOrgSalesmanApprovalBean>() {
            @Override
            public String getTable() {
                return "dim_industry";
            }

            @Override
            public String getId(DwsAuditIndLeaseOrgSalesmanApprovalBean bean) {
                return bean.getIndustry3Id();
            }

            @Override
            public void addDim(DwsAuditIndLeaseOrgSalesmanApprovalBean bean, JSONObject dim) {
                bean.setIndustry3Name(dim.getString("industry_name"));
                bean.setIndustry2Id(dim.getString("superior_industry_id"));
            }
        }, 60L, TimeUnit.SECONDS);

//        C3NameC2IdStream.print("C3NameC2IdStream>>>");

        //8.2 关联二级行业名称及一级行业ID
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanApprovalBean> C2NameC1IdStream = AsyncDataStream.unorderedWait(C3NameC2IdStream, new AsyncDimFunctionHBase<DwsAuditIndLeaseOrgSalesmanApprovalBean>() {
            @Override
            public String getTable() {
                return "dim_industry";
            }

            @Override
            public String getId(DwsAuditIndLeaseOrgSalesmanApprovalBean bean) {
                return bean.getIndustry2Id();
            }

            @Override
            public void addDim(DwsAuditIndLeaseOrgSalesmanApprovalBean bean, JSONObject dim) {
                bean.setIndustry2Name(dim.getString("industry_name"));
                bean.setIndustry1Id(dim.getString("superior_industry_id"));
            }
        }, 60L, TimeUnit.SECONDS);

        // 8.3 关联一级行业名称
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanApprovalBean> C1NameStream = AsyncDataStream.unorderedWait(C2NameC1IdStream, new AsyncDimFunctionHBase<DwsAuditIndLeaseOrgSalesmanApprovalBean>() {
            @Override
            public String getTable() {
                return "dim_industry";
            }

            @Override
            public String getId(DwsAuditIndLeaseOrgSalesmanApprovalBean bean) {
                return bean.getIndustry1Id();
            }

            @Override
            public void addDim(DwsAuditIndLeaseOrgSalesmanApprovalBean bean, JSONObject dim) {
                bean.setIndustry1Name(dim.getString("industry_name"));
            }
        }, 60L, TimeUnit.SECONDS);

        // 8.4 关联业务经办姓名及三级部门id
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanApprovalBean> SalesNameD1IdStream = AsyncDataStream.unorderedWait(C1NameStream, new AsyncDimFunctionHBase<DwsAuditIndLeaseOrgSalesmanApprovalBean>() {
            @Override
            public String getTable() {
                return "dim_employee";
            }

            @Override
            public String getId(DwsAuditIndLeaseOrgSalesmanApprovalBean bean) {
                return bean.getSalesmanId();
            }

            @Override
            public void addDim(DwsAuditIndLeaseOrgSalesmanApprovalBean bean, JSONObject dim) {
                bean.setSalesmanName(dim.getString("name"));
                bean.setDepartment3Id(dim.getString("department_id"));
            }
        }, 60L, TimeUnit.SECONDS);

        // 8.5 关联三级部门名称及二级部门id
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanApprovalBean> D3NameD2IdStream = AsyncDataStream.unorderedWait(SalesNameD1IdStream, new AsyncDimFunctionHBase<DwsAuditIndLeaseOrgSalesmanApprovalBean>() {
            @Override
            public String getTable() {
                return "dim_department";
            }

            @Override
            public String getId(DwsAuditIndLeaseOrgSalesmanApprovalBean bean) {
                return bean.getDepartment3Id();
            }

            @Override
            public void addDim(DwsAuditIndLeaseOrgSalesmanApprovalBean bean, JSONObject dim) {
                bean.setDepartment3Name(dim.getString("department_name"));
                bean.setDepartment2Id(dim.getString("superior_department_id"));
            }
        }, 60L, TimeUnit.SECONDS);

        // 8.6 关联二级部门名称及一级部门id
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanApprovalBean> D2NameD1IdStream = AsyncDataStream.unorderedWait(D3NameD2IdStream, new AsyncDimFunctionHBase<DwsAuditIndLeaseOrgSalesmanApprovalBean>() {
            @Override
            public String getTable() {
                return "dim_department";
            }

            @Override
            public String getId(DwsAuditIndLeaseOrgSalesmanApprovalBean bean) {
                return bean.getDepartment2Id();
            }

            @Override
            public void addDim(DwsAuditIndLeaseOrgSalesmanApprovalBean bean, JSONObject dim) {
                bean.setDepartment2Name(dim.getString("department_name"));
                bean.setDepartment1Id(dim.getString("superior_department_id"));
            }
        }, 60L, TimeUnit.SECONDS);

        // 8.7 关联一级部门名称
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanApprovalBean> D1NameStream = AsyncDataStream.unorderedWait(D2NameD1IdStream, new AsyncDimFunctionHBase<DwsAuditIndLeaseOrgSalesmanApprovalBean>() {
            @Override
            public String getTable() {
                return "dim_department";
            }

            @Override
            public String getId(DwsAuditIndLeaseOrgSalesmanApprovalBean bean) {
                return bean.getDepartment1Id();
            }

            @Override
            public void addDim(DwsAuditIndLeaseOrgSalesmanApprovalBean bean, JSONObject dim) {
                bean.setDepartment1Name(dim.getString("department_name"));
            }
        }, 60L, TimeUnit.SECONDS);

//        D1NameStream.print();
        //TODO 9 将数据写出到Doris
        //现在数据流中数据的格式是DwsAuditIndLeaseOrgSalesmanApprovalBean类型，需要将其转化为JSONString类型，再进行写入
        D1NameStream.map(new MapFunction<DwsAuditIndLeaseOrgSalesmanApprovalBean, String>() {
            @Override
            public String map(DwsAuditIndLeaseOrgSalesmanApprovalBean bean) throws Exception {
                return Bean2JSONUtil.Bean2Json(bean);
            }
        }).sinkTo(DorisUtil.getDorisSink("financial_lease_realtime.dws_audit_industry_lease_organization_salesman_approval_win","dws_audit_industry_lease_organization_salesman_approval_win"));

        //TODO 执行
        env.execute();
    }
}
