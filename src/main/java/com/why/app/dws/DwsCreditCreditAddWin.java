package com.why.app.dws;

import com.alibaba.fastjson.JSON;
import com.why.bean.dws.DwsCreditCreditAddBean;
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
 * Functions: 授信域新增授信窗口汇总
 */
public class DwsCreditCreditAddWin {
    public static void main(String[] args) throws Exception {
        String appName = "dws_credit_credit_add_window";
        // TODO 1 初始化流环境
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(8088, appName);
        env.setParallelism(1);

        // TODO 2 从kafka读取对应主题的dwd层数据
        String creditAddTopic = "financial_dwd_credit_add";
        KafkaSource<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(creditAddTopic, appName, OffsetsInitializer.earliest());
        DataStreamSource<String> kafkaSource = env.fromSource(kafkaConsumer, WatermarkStrategy.noWatermarks(),"kafka_source");

        // TODO 3 转换数据结构
        SingleOutputStreamOperator<DwsCreditCreditAddBean> beanStream = kafkaSource.map(new MapFunction<String, DwsCreditCreditAddBean>() {
            @Override
            public DwsCreditCreditAddBean map(String value) throws Exception {
                DwsCreditCreditAddBean creditAddBean = JSON.parseObject(value, DwsCreditCreditAddBean.class);
                creditAddBean.setApplyCount(1L);
                return creditAddBean;
            }
        });

        // TODO 4 引入水位线
        SingleOutputStreamOperator<DwsCreditCreditAddBean> withWaterMarkStream = beanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<DwsCreditCreditAddBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L)).withTimestampAssigner(new SerializableTimestampAssigner<DwsCreditCreditAddBean>() {
            @Override
            public long extractTimestamp(DwsCreditCreditAddBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        // TODO 5 开窗
        AllWindowedStream<DwsCreditCreditAddBean, TimeWindow> windowStream = withWaterMarkStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(10L)));

        // TODO 6 聚合
        SingleOutputStreamOperator<DwsCreditCreditAddBean> reduceStream = windowStream.reduce(new ReduceFunction<DwsCreditCreditAddBean>() {
            @Override
            public DwsCreditCreditAddBean reduce(DwsCreditCreditAddBean value1, DwsCreditCreditAddBean value2) throws Exception {
                value1.setApplyCount(value1.getApplyCount() + value2.getApplyCount());
                value1.setApplyAmount(value1.getApplyAmount().add(value2.getApplyAmount()));
                value1.setReplyAmount(value1.getReplyAmount().add(value2.getReplyAmount()));
                value1.setCreditAmount(value1.getCreditAmount().add(value2.getCreditAmount()));
                return value1;
            }
        }, new ProcessAllWindowFunction<DwsCreditCreditAddBean, DwsCreditCreditAddBean, TimeWindow>() {
            @Override
            public void process(Context context, Iterable<DwsCreditCreditAddBean> elements, Collector<DwsCreditCreditAddBean> out) throws Exception {
                String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                String curDate = DateFormatUtil.toDate(context.window().getStart());
                for (DwsCreditCreditAddBean element : elements) {
                    element.setStt(stt);
                    element.setEdt(edt);
                    element.setCurDate(curDate);
                    out.collect(element);
                }
            }
        });
//        reduceStream.print();
        // TODO 7 写出数据到doris
        reduceStream.map(new MapFunction<DwsCreditCreditAddBean, String>() {
            @Override
            public String map(DwsCreditCreditAddBean value) throws Exception {
                return Bean2JSONUtil.Bean2Json(value);
            }
        }).sinkTo(DorisUtil.getDorisSink("financial_lease_realtime.dws_credit_credit_add_win","dws_credit_credit_add_win"));

        // TODO 8 执行任务
        env.execute();
    }
}
