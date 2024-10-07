package com.why.util;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.why.common.FinancialLeaseCommon;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by WHY on 2024/9/5.
 * Functions: 封装创建流环境的方法
 */
public class CreateEnvUtil {
    /**
     * 创建环境
     * @param port
     * @param storagePath
     * @return
     */
    public static StreamExecutionEnvironment getStreamEnv(Integer port, String storagePath)
    {
        //1.创建流环境
        Configuration conf = new Configuration();
        conf.setInteger(RestOptions.PORT,port); //设置 Flink 的 REST API 服务器的端口号，以便通过 REST API 与 Flink 集群进行交互
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.设置检查点checkpoints
        env.enableCheckpointing(10 * 1000L);
        //设置相邻的两个检查点最小的时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10 * 1000L);
        //设置检查点清除机制(作业取消后删除检查点)
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        //设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        //设置检查点存储路径
        env.getCheckpointConfig().setCheckpointStorage(FinancialLeaseCommon.HDFS_URI_PREFIX + storagePath);

        //3.设置HDFS访问权限
        System.setProperty("HADOOP_USER_NAME",FinancialLeaseCommon.HADOOP_USER_NAME);

        //4.返回环境
        return env;
    }

}
