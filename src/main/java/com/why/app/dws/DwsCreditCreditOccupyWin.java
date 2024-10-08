package com.why.app.dws;

import com.alibaba.fastjson.JSON;
import com.why.bean.dws.DwsCreditCreditOccupyBean;
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
 * Functions: 授信域完成授信占用窗口汇总
 */
public class DwsCreditCreditOccupyWin {
    public static void main(String[] args) throws Exception {
        String appName = "dws_credit_credit_occupy_window";
        // TODO 1 初始化流环境
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(8089, appName);
        env.setParallelism(1);

        // TODO 2 从kafka读取对应主题的dwd层数据
        String creditOccupyTopic = "financial_dwd_credit_occupy";

        KafkaSource<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(creditOccupyTopic, appName, OffsetsInitializer.earliest());
        if (kafkaConsumer == null) {
            System.out.println("kafkaConsumer is null");
            System.exit(-1);
        }
        DataStreamSource<String> kafkaSource = env.fromSource(kafkaConsumer, WatermarkStrategy.noWatermarks(),"kafka_source");
        // TODO 3 转换数据结构
        SingleOutputStreamOperator<DwsCreditCreditOccupyBean> beanStream = kafkaSource.map(new MapFunction<String, DwsCreditCreditOccupyBean>() {
            @Override
            public DwsCreditCreditOccupyBean map(String value) throws Exception {
                DwsCreditCreditOccupyBean creditAddBean = JSON.parseObject(value, DwsCreditCreditOccupyBean.class);
                creditAddBean.setApplyCount(1L);
                return creditAddBean;
            }
        });

        // TODO 4 引入水位线
        SingleOutputStreamOperator<DwsCreditCreditOccupyBean> withWaterMarkStream = beanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<DwsCreditCreditOccupyBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L)).withTimestampAssigner(new SerializableTimestampAssigner<DwsCreditCreditOccupyBean>() {
            @Override
            public long extractTimestamp(DwsCreditCreditOccupyBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        // TODO 5 开窗
        AllWindowedStream<DwsCreditCreditOccupyBean, TimeWindow> windowStream = withWaterMarkStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(10L)));

        // TODO 6 聚合
        SingleOutputStreamOperator<DwsCreditCreditOccupyBean> reduceStream = windowStream.reduce(new ReduceFunction<DwsCreditCreditOccupyBean>() {
            @Override
            public DwsCreditCreditOccupyBean reduce(DwsCreditCreditOccupyBean value1, DwsCreditCreditOccupyBean value2) throws Exception {
                value1.setApplyCount(value1.getApplyCount() + value2.getApplyCount());
                value1.setApplyAmount(value1.getApplyAmount().add(value2.getApplyAmount()));
                value1.setReplyAmount(value1.getReplyAmount().add(value2.getReplyAmount()));
                value1.setCreditAmount(value1.getCreditAmount().add(value2.getCreditAmount()));
                return value1;
            }
        }, new ProcessAllWindowFunction<DwsCreditCreditOccupyBean, DwsCreditCreditOccupyBean, TimeWindow>() {
            @Override
            public void process(Context context, Iterable<DwsCreditCreditOccupyBean> elements, Collector<DwsCreditCreditOccupyBean> out) throws Exception {
                String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                String curDate = DateFormatUtil.toDate(context.window().getStart());
                for (DwsCreditCreditOccupyBean element : elements) {
                    element.setStt(stt);
                    element.setEdt(edt);
                    element.setCurDate(curDate);
                    out.collect(element);
                }
            }
        });

        reduceStream.print();

        // TODO 7 写出数据到doris
        reduceStream.map(new MapFunction<DwsCreditCreditOccupyBean, String>() {
            @Override
            public String map(DwsCreditCreditOccupyBean value) throws Exception {
                return Bean2JSONUtil.Bean2Json(value);
            }
        }).sinkTo(DorisUtil.getDorisSink("financial_lease_realtime.dws_credit_credit_occupy_win","dws_credit_credit_occupy_win"));

        // TODO 8 执行任务
        env.execute();
    }
}
