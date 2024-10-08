package com.why.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.why.app.func.AsyncDimFunctionHBase;
import com.why.bean.dws.DwsAuditIndLeaseOrgSalesmanCancelBean;
import com.why.util.*;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Created by WHY on 2024/9/8.
 * Functions:审批域行业业务方向业务经办粒度审批取消汇总
 */
public class DwsAuditIndLeaseOrgSalesmanCancelWin {
    public static void main(String[] args) throws Exception {

        String appName = "dws_audit_industry_lease_organization_salesman_cancel_window";
        //TODO 1 创建流环境
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(8084, appName);
        env.setParallelism(1);

        //TODO 2 从Kafka中读取对应主题的数据
        String cancelTopic = "financial_dwd_audit_cancel";
        KafkaSource<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(cancelTopic, appName, OffsetsInitializer.earliest());
        DataStreamSource<String> kafkaSourceStream = env.fromSource(kafkaConsumer, WatermarkStrategy.noWatermarks(), "kafka_source");

        //TODO 3 转换数据结构，得到相应的bean
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanCancelBean> mapStream = kafkaSourceStream.map(new MapFunction<String, DwsAuditIndLeaseOrgSalesmanCancelBean>() {
            @Override
            public DwsAuditIndLeaseOrgSalesmanCancelBean map(String value) throws Exception {
                DwsAuditIndLeaseOrgSalesmanCancelBean bean = JSONObject.parseObject(value, DwsAuditIndLeaseOrgSalesmanCancelBean.class);
                //补充参数
                bean.setIndustry3Id(JSONObject.parseObject(value).getString("industryId"));
                bean.setApplyCount(1L);
                return bean;
            }
        });

        //TODO 4 注册水位线
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanCancelBean> withWatermarkStream = mapStream.assignTimestampsAndWatermarks(WatermarkStrategy.
                <DwsAuditIndLeaseOrgSalesmanCancelBean>forBoundedOutOfOrderness(Duration.ofSeconds(10L))
                .withTimestampAssigner(new SerializableTimestampAssigner<DwsAuditIndLeaseOrgSalesmanCancelBean>() {
                    @Override
                    public long extractTimestamp(DwsAuditIndLeaseOrgSalesmanCancelBean element, long recordTimestamp) {
                        return element.getTs();
                    }
                }));

        //TODO 5 分组
        //分组依据 业务方向 部门 业务经办员id
        KeyedStream<DwsAuditIndLeaseOrgSalesmanCancelBean, String> keyedStream = withWatermarkStream.keyBy(new KeySelector<DwsAuditIndLeaseOrgSalesmanCancelBean, String>() {
            @Override
            public String getKey(DwsAuditIndLeaseOrgSalesmanCancelBean bean) throws Exception {
                return bean.getLeaseOrganization() + ":" + bean.getIndustry3Id() + ":" + bean.getSalesmanId();
            }
        });

        //TODO 6 开窗
        WindowedStream<DwsAuditIndLeaseOrgSalesmanCancelBean, String, TimeWindow> windowStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10L)));

        //TODO 7 聚合
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanCancelBean> reducedStream = windowStream.reduce(new ReduceFunction<DwsAuditIndLeaseOrgSalesmanCancelBean>() {
            @Override
            public DwsAuditIndLeaseOrgSalesmanCancelBean reduce(DwsAuditIndLeaseOrgSalesmanCancelBean value1, DwsAuditIndLeaseOrgSalesmanCancelBean value2) throws Exception {
                value1.setApplyAmount(value1.getApplyAmount().add(value2.getApplyAmount()));
                value1.setApplyCount(value1.getApplyCount() + value2.getApplyCount());
                return value1;
            }
        }, new ProcessWindowFunction<DwsAuditIndLeaseOrgSalesmanCancelBean, DwsAuditIndLeaseOrgSalesmanCancelBean, String, TimeWindow>() {
            @Override
            public void process(String s, ProcessWindowFunction<DwsAuditIndLeaseOrgSalesmanCancelBean, DwsAuditIndLeaseOrgSalesmanCancelBean, String, TimeWindow>.Context context, Iterable<DwsAuditIndLeaseOrgSalesmanCancelBean> elements, Collector<DwsAuditIndLeaseOrgSalesmanCancelBean> out) throws Exception {
                String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                String curDate = DateFormatUtil.toDate(context.window().getStart());
                for (DwsAuditIndLeaseOrgSalesmanCancelBean element : elements) {
                    element.setStt(stt);
                    element.setEdt(edt);
                    element.setCurDate(curDate);
                    out.collect(element);
                }
            }
        });

        //TODO 8 添加维度信息
        //8.1 补充三级行业名称以及二级行业Id
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanCancelBean> I3NameI2IdStream = AsyncDataStream.unorderedWait(reducedStream, new AsyncDimFunctionHBase<DwsAuditIndLeaseOrgSalesmanCancelBean>() {
            @Override
            public String getTable() {
                return "dim_industry";
            }

            @Override
            public String getId(DwsAuditIndLeaseOrgSalesmanCancelBean bean) {
                return bean.getIndustry3Id();
            }

            @Override
            public void addDim(DwsAuditIndLeaseOrgSalesmanCancelBean bean, JSONObject dim) {
                bean.setIndustry3Name(dim.getString("industry_name"));
                bean.setIndustry2Id(dim.getString("superior_industry_id"));
            }
        }, 60L, TimeUnit.SECONDS);

        //8.2补充二级行业名称以及一级行业id
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanCancelBean> I2NameI1IdStream = AsyncDataStream.unorderedWait(I3NameI2IdStream, new AsyncDimFunctionHBase<DwsAuditIndLeaseOrgSalesmanCancelBean>() {
            @Override
            public String getTable() {
                return "dim_industry";
            }

            @Override
            public String getId(DwsAuditIndLeaseOrgSalesmanCancelBean bean) {
                return bean.getIndustry2Id();
            }

            @Override
            public void addDim(DwsAuditIndLeaseOrgSalesmanCancelBean bean, JSONObject dim) {
                bean.setIndustry2Name(dim.getString("industry_name"));
                bean.setIndustry1Id(dim.getString("superior_industry_id"));
            }
        }, 60L, TimeUnit.SECONDS);

        //8.3补充一级行业名称
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanCancelBean> I1NameStream = AsyncDataStream.unorderedWait(I2NameI1IdStream, new AsyncDimFunctionHBase<DwsAuditIndLeaseOrgSalesmanCancelBean>() {
            @Override
            public String getTable() {
                return "dim_industry";
            }

            @Override
            public String getId(DwsAuditIndLeaseOrgSalesmanCancelBean bean) {
                return bean.getIndustry1Id();
            }

            @Override
            public void addDim(DwsAuditIndLeaseOrgSalesmanCancelBean bean, JSONObject dim) {
                bean.setIndustry1Name(dim.getString("industry_name"));
            }
        }, 60L, TimeUnit.SECONDS);

        //8.4 补充业务经办姓名和三级部门id
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanCancelBean> SalesNameStream = AsyncDataStream.unorderedWait(I1NameStream, new AsyncDimFunctionHBase<DwsAuditIndLeaseOrgSalesmanCancelBean>() {
            @Override
            public String getTable() {
                return "dim_employee";
            }

            @Override
            public String getId(DwsAuditIndLeaseOrgSalesmanCancelBean bean) {
                return bean.getSalesmanId();
            }

            @Override
            public void addDim(DwsAuditIndLeaseOrgSalesmanCancelBean bean, JSONObject dim) {
                bean.setSalesmanName(dim.getString("name"));
                bean.setDepartment3Id(dim.getString("department_id"));
            }
        }, 60L, TimeUnit.SECONDS);

        //8.5 补充三级部门名称和二级部门id
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanCancelBean> D3NameD2IdStream = AsyncDataStream.unorderedWait(SalesNameStream, new AsyncDimFunctionHBase<DwsAuditIndLeaseOrgSalesmanCancelBean>() {
            @Override
            public String getTable() {
                return "dim_department";
            }

            @Override
            public String getId(DwsAuditIndLeaseOrgSalesmanCancelBean bean) {
                return bean.getDepartment3Id();
            }

            @Override
            public void addDim(DwsAuditIndLeaseOrgSalesmanCancelBean bean, JSONObject dim) {
                bean.setDepartment3Name(dim.getString("department_name"));
                bean.setDepartment2Id(dim.getString("superior_department_id"));
            }
        }, 60L, TimeUnit.SECONDS);

        //8.6 补充二级部门名称和一级部门id
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanCancelBean> D2NameD1IdStream = AsyncDataStream.unorderedWait(D3NameD2IdStream, new AsyncDimFunctionHBase<DwsAuditIndLeaseOrgSalesmanCancelBean>() {
            @Override
            public String getTable() {
                return "dim_department";
            }

            @Override
            public String getId(DwsAuditIndLeaseOrgSalesmanCancelBean bean) {
                return bean.getDepartment2Id();
            }

            @Override
            public void addDim(DwsAuditIndLeaseOrgSalesmanCancelBean bean, JSONObject dim) {
                bean.setDepartment2Name(dim.getString("department_name"));
                bean.setDepartment1Id(dim.getString("superior_department_id"));
            }
        }, 60L, TimeUnit.SECONDS);

        //8.7 补充一级部门名称
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanCancelBean> D1NameStream = AsyncDataStream.unorderedWait(D2NameD1IdStream, new AsyncDimFunctionHBase<DwsAuditIndLeaseOrgSalesmanCancelBean>() {
            @Override
            public String getTable() {
                return "dim_department";
            }

            @Override
            public String getId(DwsAuditIndLeaseOrgSalesmanCancelBean bean) {
                return bean.getDepartment1Id();
            }

            @Override
            public void addDim(DwsAuditIndLeaseOrgSalesmanCancelBean bean, JSONObject dim) {
                bean.setDepartment1Name(dim.getString("department_name"));
            }
        }, 60L, TimeUnit.SECONDS);

//        D1NameStream.print("dim>>>");

        //TODO 9 写出到Doris表中
        D1NameStream.map(new MapFunction<DwsAuditIndLeaseOrgSalesmanCancelBean, String>() {
            @Override
            public String map(DwsAuditIndLeaseOrgSalesmanCancelBean bean) throws Exception {
                return Bean2JSONUtil.Bean2Json(bean);
            }
        }).sinkTo(DorisUtil.getDorisSink("financial_lease_realtime.dws_audit_industry_lease_organization_salesman_cancel_win","dws_audit_industry_lease_organization_salesman_cancel_win"));

        env.execute();
    }

}
