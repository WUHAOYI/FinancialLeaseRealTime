import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by WHY on 2024/9/5.
 * Functions:
 */
public class FlinkCDCDemoMySQL {
    public static void main(String[] args) throws Exception {
        //1.初始化流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        //2.检查点
//        //启用检查点
//        env.enableCheckpointing(10 * 1000L);
//        //设置相邻两次检查点的最小间隔
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30 * 1000L);
//        //设置取消Job时检查点的清理模式
//        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        //设置状态后端的类型
//        env.setStateBackend(new HashMapStateBackend());
//        //设置检查点存储路径
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop108:8020/flinkCDC");

        //3.创建 Flink-MySQL-CDC 的 Source
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop108")
                .port(3306)
                .databaseList("financial_lease_config") // set captured database
                .tableList("financial_lease_config.table_process") // set captured table
                .username("root")
                .password("hadoop")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .startupOptions(StartupOptions.initial())
                .build();

        //4.使用CDC Source从MySQL读取数据
        DataStreamSource<String> mysqlDS =
                env.fromSource(
                        mySqlSource,
                        WatermarkStrategy.noWatermarks(),
                        "MysqlSource");

        //5.打印输出
        mysqlDS.print();

        //6.执行任务
        env.execute();
    }
}
