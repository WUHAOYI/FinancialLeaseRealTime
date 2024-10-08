package com.why.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.why.app.func.AsyncDimFunctionHBase;
import com.why.bean.dws.DwsAuditAuditManCancelBean;
import com.why.util.*;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
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
 * Functions: 审批域信审经办粒度审批取消各窗口汇总
 */
public class DwsAuditAuditManCancelWin {
    public static void main(String[] args) throws Exception {
        String appName = "dws_audit_audit_man_cancel_win";
        // TODO 1. 初始化流处理环境
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(8087, appName);
        env.setParallelism(1);

        // TODO 2. 从 Kafka 目标主题消费数据
        String topic = "financial_dwd_audit_cancel";
        KafkaSource<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, appName, OffsetsInitializer.earliest());
        SingleOutputStreamOperator<String> kafkaSource = env.fromSource(kafkaConsumer, WatermarkStrategy.noWatermarks(), "source_operator");

        // TODO 3. 清洗数据并转换数据结构
        // 取消时可能尚未分配信审经办，因此 auditManId 可能为 null，这部分数据是我们不需要的，舍弃
        SingleOutputStreamOperator<DwsAuditAuditManCancelBean> flatMappedStream = kafkaSource.flatMap(new FlatMapFunction<String, DwsAuditAuditManCancelBean>() {
            @Override
            public void flatMap(String jsonStr, Collector<DwsAuditAuditManCancelBean> out) throws Exception {
                DwsAuditAuditManCancelBean bean = JSON.parseObject(jsonStr, DwsAuditAuditManCancelBean.class);
                if (bean.getAuditManId() != null) {
                    bean.setApplyCount(1L);
                    out.collect(bean);
                }
            }
        });
        // TODO 4. 引入水位线
        SingleOutputStreamOperator<DwsAuditAuditManCancelBean> withWatermarkStream = flatMappedStream.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<DwsAuditAuditManCancelBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                        .withTimestampAssigner(new SerializableTimestampAssigner<DwsAuditAuditManCancelBean>() {
                            @Override
                            public long extractTimestamp(DwsAuditAuditManCancelBean bean, long recordTimestamp) {
                                return bean.getTs();
                            }
                        }));


        // TODO 5. 按照信审经办 ID 分组
        KeyedStream<DwsAuditAuditManCancelBean, String> keyedStream = withWatermarkStream.keyBy(new KeySelector<DwsAuditAuditManCancelBean, String>() {
            @Override
            public String getKey(DwsAuditAuditManCancelBean bean) throws Exception {
                return bean.getAuditManId();
            }
        });

        // TODO 6. 开窗
        WindowedStream<DwsAuditAuditManCancelBean, String, TimeWindow> windowStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10L)));

        // TODO 7 聚合
        SingleOutputStreamOperator<DwsAuditAuditManCancelBean> reduceStream = windowStream.reduce(new ReduceFunction<DwsAuditAuditManCancelBean>() {
            @Override
            public DwsAuditAuditManCancelBean reduce(DwsAuditAuditManCancelBean value1, DwsAuditAuditManCancelBean value2) throws Exception {
                value1.setApplyCount(value1.getApplyCount() + value2.getApplyCount());
                value1.setApplyAmount(value1.getApplyAmount().add(value2.getApplyAmount()));
                return value1;
            }
        }, new ProcessWindowFunction<DwsAuditAuditManCancelBean, DwsAuditAuditManCancelBean, String, TimeWindow>() {
            @Override
            public void process(String s, Context context, Iterable<DwsAuditAuditManCancelBean> elements, Collector<DwsAuditAuditManCancelBean> out) throws Exception {
                String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                String curDate = DateFormatUtil.toDate(context.window().getStart());
                for (DwsAuditAuditManCancelBean element : elements) {
                    element.setStt(stt);
                    element.setEdt(edt);
                    element.setCurDate(curDate);
                    out.collect(element);
                }
            }
        });

        reduceStream.print("reduce>>>");

        // TODO 8. 关联维度信息
        // 关联获取信审经办姓名
        SingleOutputStreamOperator<DwsAuditAuditManCancelBean> dimStream = AsyncDataStream.unorderedWait(reduceStream, new AsyncDimFunctionHBase<DwsAuditAuditManCancelBean>() {
                    @Override
                    public String getTable() {
                        return "dim_employee";
                    }

                    @Override
                    public String getId(DwsAuditAuditManCancelBean bean) {
                        return bean.getAuditManId();
                    }

                    @Override
                    public void addDim(DwsAuditAuditManCancelBean bean, JSONObject dim) {
                        bean.setAuditManName(dim.getString("name"));
                    }
                }, 60, TimeUnit.SECONDS);

        // TODO 9. 写入 Doris
        SingleOutputStreamOperator<String> dorisStream = dimStream.map(Bean2JSONUtil::Bean2Json);

        DorisSink<String> dorisSink = DorisUtil.getDorisSink
                ("financial_lease_realtime.dws_audit_audit_man_cancel_win",
                        "dws_audit_audit_man_cancel_win");
        dorisStream.sinkTo(dorisSink);

        env.execute();
    }
}
