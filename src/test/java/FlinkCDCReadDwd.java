import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

/**
 * Created by WHY on 2024/9/6.
 * Functions:
 */
public class FlinkCDCReadDwd {
    public static void main(String[] args) throws Exception {
        //1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //配置kafka source
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop108:9092")
                .setTopics("financial_dwd_credit_add")
                .setGroupId("flink_cdc_kafka1")
                .setValueOnlyDeserializer(new SimpleStringSchema()) //设置序列化
                .setStartingOffsets(OffsetsInitializer.earliest()) //读取之前的数据
                .build();

        //读取数据并进行打印
        DataStreamSource<String> kafkaSourceData = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource");
        SingleOutputStreamOperator<Long> countedStream = kafkaSourceData.map(new RichMapFunction<String, Long>() {
            private long count;

            @Override
            public void open(Configuration parameters) throws Exception {
                count = 0;
            }

            @Override
            public Long map(String value) throws Exception {
                System.out.println(count + ": " + value);
                count++;
                return count;
            }
        });
        countedStream.print("total count: ");

//        kafkaSourceData.print("kafka>>>");

        //执行
        env.execute();

    }
}
