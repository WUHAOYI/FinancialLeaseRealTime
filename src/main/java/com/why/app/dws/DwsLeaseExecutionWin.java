package com.why.app.dws;

import com.alibaba.fastjson.JSON;
import com.why.bean.dws.DwsLeaseExecutionBean;
import com.why.util.*;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Created by WHY on 2024/10/8.
 * Functions: 租赁域起租窗口汇总
 */
public class DwsLeaseExecutionWin {
    public static void main(String[] args) throws Exception {
        String appName = "dws_lease_execution_window";
        // TODO 1 初始化流环境
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(8092, appName);
        env.setParallelism(1);

        // TODO 2 从kafka读取对应主题的dwd层数据
        String executionTopic = "financial_dwd_lease_execution";
        KafkaSource<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(executionTopic, appName, OffsetsInitializer.earliest());
        DataStreamSource<String> kafkaSource = env.fromSource(kafkaConsumer, WatermarkStrategy.noWatermarks(),"kafka_source");

        // TODO 3 转换数据结构
        SingleOutputStreamOperator<DwsLeaseExecutionBean> beanStream = kafkaSource.map(new MapFunction<String, DwsLeaseExecutionBean>() {
            @Override
            public DwsLeaseExecutionBean map(String value) throws Exception {
                DwsLeaseExecutionBean creditAddBean = JSON.parseObject(value, DwsLeaseExecutionBean.class);
                creditAddBean.setApplyCount(1L);
                return creditAddBean;
            }
        });

        // TODO 4 引入水位线
        SingleOutputStreamOperator<DwsLeaseExecutionBean> withWaterMarkStream = beanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<DwsLeaseExecutionBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L)).withTimestampAssigner(new SerializableTimestampAssigner<DwsLeaseExecutionBean>() {
            @Override
            public long extractTimestamp(DwsLeaseExecutionBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        // TODO 5 开窗
        AllWindowedStream<DwsLeaseExecutionBean, TimeWindow> windowStream = withWaterMarkStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(10L)));

        // TODO 6 聚合
        SingleOutputStreamOperator<DwsLeaseExecutionBean> reduceStream = windowStream.reduce(new ReduceFunction<DwsLeaseExecutionBean>() {
            @Override
            public DwsLeaseExecutionBean reduce(DwsLeaseExecutionBean value1, DwsLeaseExecutionBean value2) throws Exception {
                value1.setApplyCount(value1.getApplyCount() + value2.getApplyCount());
                value1.setApplyAmount(value1.getApplyAmount().add(value2.getApplyAmount()));
                value1.setReplyAmount(value1.getReplyAmount().add(value2.getReplyAmount()));
                value1.setCreditAmount(value1.getCreditAmount().add(value2.getCreditAmount()));
                return value1;
            }
        }, new ProcessAllWindowFunction<DwsLeaseExecutionBean, DwsLeaseExecutionBean, TimeWindow>() {
            @Override
            public void process(Context context, Iterable<DwsLeaseExecutionBean> elements, Collector<DwsLeaseExecutionBean> out) throws Exception {
                String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                String curDate = DateFormatUtil.toDate(context.window().getStart());
                for (DwsLeaseExecutionBean element : elements) {
                    element.setStt(stt);
                    element.setEdt(edt);
                    element.setCurDate(curDate);
                    out.collect(element);
                }
            }
        });

        // TODO 7 写出数据到doris
        reduceStream.map(new MapFunction<DwsLeaseExecutionBean, String>() {
            @Override
            public String map(DwsLeaseExecutionBean value) throws Exception {
                return Bean2JSONUtil.Bean2Json(value);
            }
        }).sinkTo(DorisUtil.getDorisSink("financial_lease_realtime.dws_lease_execution_win","dws_lease_execution_win"));

        // TODO 8 执行任务
        env.execute();
    }
}
