import com.why.common.FinancialLeaseCommon;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.hadoop.metrics2.sink.FileSink;

import java.util.concurrent.TimeUnit;

/**
 * Created by WHY on 2024/9/5.
 * Functions:
 */
public class FlickCDCDemoKafka {
    public static void main(String[] args) throws Exception {
        //1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String mainTopic = "topic_db";
        String approveTopic = "financial_dwd_audit_approve";
        String cancelTopic = "financial_dwd_audit_cancel";
        String rejectTopic = "financial_dwd_audit_reject";
        String transactionTopic = "__transaction_state";

        //配置kafka source
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop108:9092")
                .setTopics(transactionTopic)
                .setGroupId("flink_cdc_kafka")
                .setValueOnlyDeserializer(new SimpleStringSchema()) //设置序列化
                .setStartingOffsets(OffsetsInitializer.earliest()) //读取之前的数据
                .build();

        //读取数据并进行打印
        DataStreamSource<String> kafkaSourceStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource");

        //添加HDFS访问权限
        System.setProperty("HADOOP_USER_NAME", FinancialLeaseCommon.HADOOP_USER_NAME);

        //写出到指定路径
        StreamingFileSink<String> fileSinkTopicDb = StreamingFileSink
                .<String>forRowFormat(new Path("hdfs://hadoop108/kafkaSourceOutput/topic_db"),
                        new SimpleStringEncoder<>("UTF-8"))

                .withRollingPolicy( //.withRollingPolicy()方法指定了一个“滚动策略”(因为文件会有内容持续不断地写入，所以我们应该给一个标准，到什么时候就开启新的文件，将之前的内容归档保存)
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15)) //至少包含 15 分钟的数据
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5)) //最近 5 分钟没有收到新的数据
                                .withMaxPartSize(1024 * 1024 * 1024) //文件大小已达到 1 GB
                                .build()
                )
                .build();

        StreamingFileSink<String> fileSinkTopicReject = StreamingFileSink
                .<String>forRowFormat(new Path("hdfs://hadoop108/kafkaSourceOutput/reject"),
                        new SimpleStringEncoder<>("UTF-8"))

                .withRollingPolicy( //.withRollingPolicy()方法指定了一个“滚动策略”(因为文件会有内容持续不断地写入，所以我们应该给一个标准，到什么时候就开启新的文件，将之前的内容归档保存)
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15)) //至少包含 15 分钟的数据
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5)) //最近 5 分钟没有收到新的数据
                                .withMaxPartSize(1024 * 1024 * 1024) //文件大小已达到 1 GB
                                .build()
                )
                .build();

        StreamingFileSink<String> fileSinkTopicApprove = StreamingFileSink
                .<String>forRowFormat(new Path("hdfs://hadoop108/kafkaSourceOutput/approve"),
                        new SimpleStringEncoder<>("UTF-8"))

                .withRollingPolicy( //.withRollingPolicy()方法指定了一个“滚动策略”(因为文件会有内容持续不断地写入，所以我们应该给一个标准，到什么时候就开启新的文件，将之前的内容归档保存)
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15)) //至少包含 15 分钟的数据
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5)) //最近 5 分钟没有收到新的数据
                                .withMaxPartSize(1024 * 1024 * 1024) //文件大小已达到 1 GB
                                .build()
                )
                .build();

        StreamingFileSink<String> fileSinkTopicTrans = StreamingFileSink
                .<String>forRowFormat(new Path("hdfs://hadoop108/kafkaSourceOutput/trans"),
                        new SimpleStringEncoder<>("UTF-8"))

                .withRollingPolicy( //.withRollingPolicy()方法指定了一个“滚动策略”(因为文件会有内容持续不断地写入，所以我们应该给一个标准，到什么时候就开启新的文件，将之前的内容归档保存)
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15)) //至少包含 15 分钟的数据
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5)) //最近 5 分钟没有收到新的数据
                                .withMaxPartSize(1024 * 1024 * 1024) //文件大小已达到 1 GB
                                .build()
                )
                .build();

        kafkaSourceStream.addSink(fileSinkTopicTrans);

        kafkaSourceStream.print("kafka>>>");

        //执行
        env.execute();

    }

}
