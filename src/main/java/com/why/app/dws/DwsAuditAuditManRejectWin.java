package com.why.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.why.app.func.AsyncDimFunctionHBase;
import com.why.bean.dws.DwsAuditAuditManRejectBean;
import com.why.util.*;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
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
 * Created by WHY on 2024/10/8.
 * Functions: 审批域信审经办粒度审批拒绝各窗口汇总
 */
public class DwsAuditAuditManRejectWin {
    public static void main(String[] args) throws Exception {
        String appName = "dws_audit_audit_man_reject_window";
        // TODO 1 初始化流环境
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(8087, appName);
        env.setParallelism(1);
        // TODO 2 从kafka读取数据
        String rejectTopic = "financial_dwd_audit_reject";
        KafkaSource<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(rejectTopic, appName, OffsetsInitializer.earliest());
        DataStreamSource<String> kafkaSource = env.fromSource(kafkaConsumer, WatermarkStrategy.noWatermarks(), "kafka_source");

        // TODO 3 转换结构 同时过滤信审经办id为空的数据
        SingleOutputStreamOperator<DwsAuditAuditManRejectBean> beanStream = kafkaSource.flatMap(new FlatMapFunction<String, DwsAuditAuditManRejectBean>() {
            @Override
            public void flatMap(String value, Collector<DwsAuditAuditManRejectBean> out) throws Exception {
                DwsAuditAuditManRejectBean manApprovalBean = JSON.parseObject(value, DwsAuditAuditManRejectBean.class);
                // 有具体的信审经办id才需要往下游写
                if (manApprovalBean.getAuditManId() != null) {
                    manApprovalBean.setApplyCount(1L);
                    out.collect(manApprovalBean);
                }
            }
        });

        // TODO 4 引入水位线
        SingleOutputStreamOperator<DwsAuditAuditManRejectBean> withWaterMarkStream = beanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<DwsAuditAuditManRejectBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L)).withTimestampAssigner(new SerializableTimestampAssigner<DwsAuditAuditManRejectBean>() {
            @Override
            public long extractTimestamp(DwsAuditAuditManRejectBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        // TODO 5 按照信审经办id分组
        KeyedStream<DwsAuditAuditManRejectBean, String> keyedStream = withWaterMarkStream.keyBy(new KeySelector<DwsAuditAuditManRejectBean, String>() {
            @Override
            public String getKey(DwsAuditAuditManRejectBean value) throws Exception {
                return value.getAuditManId();
            }
        });

        // TODO 6 开窗
        WindowedStream<DwsAuditAuditManRejectBean, String, TimeWindow> windowStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10L)));

        // TODO 7 聚合
        SingleOutputStreamOperator<DwsAuditAuditManRejectBean> reduceStream = windowStream.reduce(new ReduceFunction<DwsAuditAuditManRejectBean>() {
            @Override
            public DwsAuditAuditManRejectBean reduce(DwsAuditAuditManRejectBean value1, DwsAuditAuditManRejectBean value2) throws Exception {
                value1.setApplyCount(value1.getApplyCount() + value2.getApplyCount());
                value1.setApplyAmount(value1.getApplyAmount().add(value2.getApplyAmount()));

                return value1;
            }
        }, new ProcessWindowFunction<DwsAuditAuditManRejectBean, DwsAuditAuditManRejectBean, String, TimeWindow>() {
            @Override
            public void process(String s, Context context, Iterable<DwsAuditAuditManRejectBean> elements, Collector<DwsAuditAuditManRejectBean> out) throws Exception {
                String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                String curDate = DateFormatUtil.toDate(context.window().getStart());
                for (DwsAuditAuditManRejectBean element : elements) {
                    element.setStt(stt);
                    element.setEdt(edt);
                    element.setCurDate(curDate);
                    out.collect(element);
                }
            }
        });
        reduceStream.print("reduce>>>");


        // TODO 8 关联维度数据 关联信审经办员名称
        SingleOutputStreamOperator<DwsAuditAuditManRejectBean> dimStream = AsyncDataStream.unorderedWait(reduceStream, new AsyncDimFunctionHBase<DwsAuditAuditManRejectBean>() {
            @Override
            public String getTable() {

                return "dim_employee";
            }

            @Override
            public String getId(DwsAuditAuditManRejectBean bean) {
                return bean.getAuditManId();
            }

            @Override
            public void addDim(DwsAuditAuditManRejectBean bean, JSONObject dim) {
                bean.setAuditManName(dim.getString("name"));
            }
        }, 60, TimeUnit.SECONDS);

        // TODO 9 写出到doris
        dimStream.map(new MapFunction<DwsAuditAuditManRejectBean, String>() {
            @Override
            public String map(DwsAuditAuditManRejectBean value) throws Exception {
                return Bean2JSONUtil.Bean2Json(value);
            }
        }).sinkTo(DorisUtil.getDorisSink("financial_lease_realtime.dws_audit_audit_man_reject_win","dws_audit_audit_man_reject_win"));

        // TODO 10 执行任务
        env.execute();
    }
}
