package com.why.app.dws;

import com.alibaba.fastjson.JSON;
import com.why.bean.dws.DwsLeaseContractProduceBean;
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
 * Functions: 租赁域完成合同制作窗口汇总
 */
public class DwsLeaseContractProduceWin {
    public static void main(String[] args) throws Exception {
        String appName = "dws_lease_contract_produce_window";
        // TODO 1 初始化流环境
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(8090, appName);
        env.setParallelism(1);

        // TODO 2 从kafka读取对应主题的dwd层数据
        String leaseContractProducedTopic = "financial_dwd_lease_contract_produce";
        KafkaSource<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(leaseContractProducedTopic, appName, OffsetsInitializer.earliest());
        DataStreamSource<String> kafkaSource = env.fromSource(kafkaConsumer, WatermarkStrategy.noWatermarks(),"kafka_source");

        // TODO 3 转换数据结构
        SingleOutputStreamOperator<DwsLeaseContractProduceBean> beanStream = kafkaSource.map(new MapFunction<String, DwsLeaseContractProduceBean>() {
            @Override
            public DwsLeaseContractProduceBean map(String value) throws Exception {
                DwsLeaseContractProduceBean creditAddBean = JSON.parseObject(value, DwsLeaseContractProduceBean.class);
                creditAddBean.setApplyCount(1L);
                return creditAddBean;
            }
        });

        // TODO 4 引入水位线
        SingleOutputStreamOperator<DwsLeaseContractProduceBean> withWaterMarkStream = beanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<DwsLeaseContractProduceBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L)).withTimestampAssigner(new SerializableTimestampAssigner<DwsLeaseContractProduceBean>() {
            @Override
            public long extractTimestamp(DwsLeaseContractProduceBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        // TODO 5 开窗
        AllWindowedStream<DwsLeaseContractProduceBean, TimeWindow> windowStream = withWaterMarkStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(10L)));

        // TODO 6 聚合
        SingleOutputStreamOperator<DwsLeaseContractProduceBean> reduceStream = windowStream.reduce(new ReduceFunction<DwsLeaseContractProduceBean>() {
            @Override
            public DwsLeaseContractProduceBean reduce(DwsLeaseContractProduceBean value1, DwsLeaseContractProduceBean value2) throws Exception {
                value1.setApplyCount(value1.getApplyCount() + value2.getApplyCount());
                value1.setApplyAmount(value1.getApplyAmount().add(value2.getApplyAmount()));
                value1.setReplyAmount(value1.getReplyAmount().add(value2.getReplyAmount()));
                value1.setCreditAmount(value1.getCreditAmount().add(value2.getCreditAmount()));
                return value1;
            }
        }, new ProcessAllWindowFunction<DwsLeaseContractProduceBean, DwsLeaseContractProduceBean, TimeWindow>() {
            @Override
            public void process(Context context, Iterable<DwsLeaseContractProduceBean> elements, Collector<DwsLeaseContractProduceBean> out) throws Exception {
                String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                String curDate = DateFormatUtil.toDate(context.window().getStart());
                for (DwsLeaseContractProduceBean element : elements) {
                    element.setStt(stt);
                    element.setEdt(edt);
                    element.setCurDate(curDate);
                    out.collect(element);
                }
            }
        });

        // TODO 7 写出数据到doris
        reduceStream.map(new MapFunction<DwsLeaseContractProduceBean, String>() {
            @Override
            public String map(DwsLeaseContractProduceBean value) throws Exception {
                return Bean2JSONUtil.Bean2Json(value);
            }
        }).sinkTo(DorisUtil.getDorisSink("financial_lease_realtime.dws_lease_contract_produce_win","dws_lease_contract_produce_win"));

        // TODO 8 执行任务
        env.execute();
    }
}
