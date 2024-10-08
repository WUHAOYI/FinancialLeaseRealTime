package com.why.app.dws;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.why.app.func.AsyncDimFunctionHBase;
import com.why.bean.dws.DwsAuditIndLeaseOrgSalesmanRejectBean;
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
 * Functions: 审批域行业业务方向业务经办粒度审批拒绝各窗口汇总
 */
public class DwsAuditIndLeaseOrgSalesmanRejectWin {
    public static void main(String[] args) throws Exception {
        String appName = "dws_audit_industry_lease_organization_salesman_reject_window";
        // TODO 1 初始化流环境
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(8085, appName);
        env.setParallelism(1);
        // TODO 2 读取对应主题kafka的数据
        String rejectTopic = "financial_dwd_audit_reject";
        KafkaSource<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(rejectTopic, appName, OffsetsInitializer.earliest());
        DataStreamSource<String> kafkaSource = env.fromSource(kafkaConsumer, WatermarkStrategy.noWatermarks(), "kafka_source");

        // TODO 3 转换数据结构
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanRejectBean> rejectBeanStream = kafkaSource.map(new MapFunction<String, DwsAuditIndLeaseOrgSalesmanRejectBean>() {
            @Override
            public DwsAuditIndLeaseOrgSalesmanRejectBean map(String value) throws Exception {
                DwsAuditIndLeaseOrgSalesmanRejectBean rejectBean = JSON.parseObject(value, DwsAuditIndLeaseOrgSalesmanRejectBean.class);
                rejectBean.setApplyCount(1L);
                rejectBean.setIndustry3Id(JSON.parseObject(value).getString("industryId"));
                return rejectBean;
            }
        });

        // TODO 4 引入水位线
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanRejectBean> withWaterMarkStream = rejectBeanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<DwsAuditIndLeaseOrgSalesmanRejectBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L)).withTimestampAssigner(new SerializableTimestampAssigner<DwsAuditIndLeaseOrgSalesmanRejectBean>() {
            @Override
            public long extractTimestamp(DwsAuditIndLeaseOrgSalesmanRejectBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        // TODO 5 按照 业务方向 + 三级行业id + 业务经办员id 分组
        KeyedStream<DwsAuditIndLeaseOrgSalesmanRejectBean, String> keyedStream = withWaterMarkStream.keyBy(new KeySelector<DwsAuditIndLeaseOrgSalesmanRejectBean, String>() {
            @Override
            public String getKey(DwsAuditIndLeaseOrgSalesmanRejectBean bean) throws Exception {
                return bean.getLeaseOrganization() + ":" + bean.getIndustry3Id() + ":" + bean.getSalesmanId();
            }
        });

        // TODO 6 开窗
        WindowedStream<DwsAuditIndLeaseOrgSalesmanRejectBean, String, TimeWindow> windowStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10L)));

        // TODO 7 聚合
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanRejectBean> reduceStream = windowStream.reduce(new ReduceFunction<DwsAuditIndLeaseOrgSalesmanRejectBean>() {
            @Override
            public DwsAuditIndLeaseOrgSalesmanRejectBean reduce(DwsAuditIndLeaseOrgSalesmanRejectBean value1, DwsAuditIndLeaseOrgSalesmanRejectBean value2) throws Exception {
                value1.setApplyCount(value1.getApplyCount() + value2.getApplyCount());
                value1.setApplyAmount(value1.getApplyAmount().add(value2.getApplyAmount()));

                return value1;
            }
        }, new ProcessWindowFunction<DwsAuditIndLeaseOrgSalesmanRejectBean, DwsAuditIndLeaseOrgSalesmanRejectBean, String, TimeWindow>() {
            @Override
            public void process(String s, Context context, Iterable<DwsAuditIndLeaseOrgSalesmanRejectBean> elements, Collector<DwsAuditIndLeaseOrgSalesmanRejectBean> out) throws Exception {
                String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                String curDate = DateFormatUtil.toDate(context.window().getStart());
                for (DwsAuditIndLeaseOrgSalesmanRejectBean element : elements) {
                    element.setStt(stt);
                    element.setEdt(edt);
                    element.setCurDate(curDate);
                    out.collect(element);
                }
            }
        });
//        reduceStream.print("reduce>>>");


        // TODO 8 关联维度信息
        // 8.1 关联二级行业id 和三级行业名称
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanRejectBean> c3NameC2IdStream = AsyncDataStream.unorderedWait(reduceStream, new AsyncDimFunctionHBase<DwsAuditIndLeaseOrgSalesmanRejectBean>() {
            @Override
            public String getTable() {
                return "dim_industry";
            }

            @Override
            public String getId(DwsAuditIndLeaseOrgSalesmanRejectBean bean) {
                return bean.getIndustry3Id();
            }

            @Override
            public void addDim(DwsAuditIndLeaseOrgSalesmanRejectBean bean, JSONObject dim) {
                bean.setIndustry3Name(dim.getString("industry_name"));
                bean.setIndustry2Id(dim.getString("superior_industry_id"));
            }
        }, 60, TimeUnit.SECONDS);

        // 8.2 关联一级行业id 和二级行业名称
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanRejectBean> c2NameC1IdStream = AsyncDataStream.unorderedWait(c3NameC2IdStream, new AsyncDimFunctionHBase<DwsAuditIndLeaseOrgSalesmanRejectBean>() {
            @Override
            public String getTable() {
                return "dim_industry";
            }

            @Override
            public String getId(DwsAuditIndLeaseOrgSalesmanRejectBean bean) {
                return bean.getIndustry2Id();
            }

            @Override
            public void addDim(DwsAuditIndLeaseOrgSalesmanRejectBean bean, JSONObject dim) {
                bean.setIndustry2Name(dim.getString("industry_name"));
                bean.setIndustry1Id(dim.getString("superior_industry_id"));
            }
        }, 60, TimeUnit.SECONDS);

        // 8.3 关联一级行业名称
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanRejectBean> c1NameStream = AsyncDataStream.unorderedWait(c2NameC1IdStream, new AsyncDimFunctionHBase<DwsAuditIndLeaseOrgSalesmanRejectBean>() {
            @Override
            public String getTable() {
                return "dim_industry";
            }

            @Override
            public String getId(DwsAuditIndLeaseOrgSalesmanRejectBean bean) {
                return bean.getIndustry1Id();
            }

            @Override
            public void addDim(DwsAuditIndLeaseOrgSalesmanRejectBean bean, JSONObject dim) {
                bean.setIndustry1Name(dim.getString("industry_name"));

            }
        }, 60, TimeUnit.SECONDS);

        // 8.4 关联业务经办员名称和三级部门id
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanRejectBean> d3IdStream = AsyncDataStream.unorderedWait(c1NameStream, new AsyncDimFunctionHBase<DwsAuditIndLeaseOrgSalesmanRejectBean>() {
            @Override
            public String getTable() {
                return "dim_employee";
            }

            @Override
            public String getId(DwsAuditIndLeaseOrgSalesmanRejectBean bean) {
                return bean.getSalesmanId();
            }

            @Override
            public void addDim(DwsAuditIndLeaseOrgSalesmanRejectBean bean, JSONObject dim) {
                bean.setSalesmanName(dim.getString("name"));
                bean.setDepartment3Id(dim.getString("department_id"));
            }
        }, 60, TimeUnit.SECONDS);

        // 8.5 关联二级部门id和三级部门名称
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanRejectBean> d2IdD3NameStream = AsyncDataStream.unorderedWait(d3IdStream, new AsyncDimFunctionHBase<DwsAuditIndLeaseOrgSalesmanRejectBean>() {
            @Override
            public String getTable() {
                return "dim_department";
            }

            @Override
            public String getId(DwsAuditIndLeaseOrgSalesmanRejectBean bean) {
                return bean.getDepartment3Id();
            }

            @Override
            public void addDim(DwsAuditIndLeaseOrgSalesmanRejectBean bean, JSONObject dim) {
                bean.setDepartment3Name(dim.getString("department_name"));
                bean.setDepartment2Id(dim.getString("superior_department_id"));
            }
        }, 60, TimeUnit.SECONDS);

        // 8.6 关联一级部门id和二级部门名称
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanRejectBean> d1IdStream = AsyncDataStream.unorderedWait(d2IdD3NameStream, new AsyncDimFunctionHBase<DwsAuditIndLeaseOrgSalesmanRejectBean>() {
            @Override
            public String getTable() {
                return "dim_department";
            }

            @Override
            public String getId(DwsAuditIndLeaseOrgSalesmanRejectBean bean) {
                return bean.getDepartment2Id();
            }

            @Override
            public void addDim(DwsAuditIndLeaseOrgSalesmanRejectBean bean, JSONObject dim) {
                bean.setDepartment2Name(dim.getString("department_name"));
                bean.setDepartment1Id(dim.getString("superior_department_id"));
            }
        }, 60, TimeUnit.SECONDS);

        // 8.7 关联一级部门名称
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanRejectBean> d1NameStream = AsyncDataStream.unorderedWait(d1IdStream, new AsyncDimFunctionHBase<DwsAuditIndLeaseOrgSalesmanRejectBean>() {
            @Override
            public String getTable() {
                return "dim_department";
            }

            @Override
            public String getId(DwsAuditIndLeaseOrgSalesmanRejectBean bean) {
                return bean.getDepartment1Id();
            }

            @Override
            public void addDim(DwsAuditIndLeaseOrgSalesmanRejectBean bean, JSONObject dim) {
                bean.setDepartment1Name(dim.getString("department_name"));
            }
        }, 60, TimeUnit.SECONDS);

//        d1NameStream.print("dim>>>");

        // TODO 9 写出到doris
        d1NameStream.map(new MapFunction<DwsAuditIndLeaseOrgSalesmanRejectBean, String>() {
            @Override
            public String map(DwsAuditIndLeaseOrgSalesmanRejectBean value) throws Exception {
                return Bean2JSONUtil.Bean2Json(value);
            }
        }).sinkTo(DorisUtil.getDorisSink("financial_lease_realtime.dws_audit_industry_lease_organization_salesman_reject_win","dws_audit_industry_lease_organization_salesman_reject_win"));

        // TODO 10 执行任务
        env.execute();
    }
}