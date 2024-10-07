package com.why.app.dim;

import com.alibaba.fastjson.JSONObject;
import com.why.app.func.DimSinkFunc;
import com.why.bean.TableProcess;
import com.why.common.FinancialLeaseCommon;
import com.why.util.CreateEnvUtil;
import com.why.util.HBaseUtil;
import com.why.util.KafkaUtil;
import com.why.util.MySQLUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Objects;

/**
 * Created by WHY on 2024/9/5.
 * Functions: DIM层数据处理
 */
public class FinancialLeaseDimApp {
    public static void main(String[] args) throws Exception {
        //TODO 1 创建新环境
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(8081, "financial_lease_dim_app");

        //TODO 2 从Kafka中读取数据（创建KafkaSource）
        KafkaSource<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(FinancialLeaseCommon.KAFKA_ODS_TOPIC, "financial_lease_dim_app", OffsetsInitializer.earliest());
        DataStreamSource<String> kafkaSource = env.fromSource(kafkaConsumer, WatermarkStrategy.noWatermarks(), "financial_lease_dim_app");
//        kafkaSource.print("kafka>>>");

        //TODO 3 从MySQL中读取配置表数据
        DataStreamSource<String> mysqlSource = env.fromSource(MySQLUtil.getMysqlSource(), WatermarkStrategy.noWatermarks(), "financial_lease_dim_app");
//        mysqlSource.print("mysql>>>");

        //TODO 4 在hbase中创建维度表
        //调用底层的处理函数来对数据流进行处理
        SingleOutputStreamOperator<TableProcess> processStream = mysqlSource.process(new ProcessFunction<String, TableProcess>() {

            private Connection hbaseConnection = null;

            /**
             * 创建连接
             * @param parameters The configuration containing the parameters attached to the contract.
             * @throws Exception
             */
            @Override
            public void open(Configuration parameters) throws Exception {
                hbaseConnection = HBaseUtil.getHBaseConnection();
            }

            /**
             * 处理数据
             * @param jsonStr The input value.
             * @param ctx A {@link Context} that allows querying the timestamp of the element and getting a
             *     {@link TimerService} for registering timers and querying the time. The context is only
             *     valid during the invocation of this method, do not store it.
             * @param out The collector for returning result values.
             * @throws Exception
             */
            @Override
            public void processElement(String jsonStr, ProcessFunction<String, TableProcess>.Context ctx, Collector<TableProcess> out) throws Exception {
                //获取JSON格式数据
                JSONObject jsonObject = JSONObject.parseObject(jsonStr);
                String op = jsonObject.getString("op");
                TableProcess tableProcess = jsonObject.getObject("after", TableProcess.class);
                //根据操作符来判断MySQL中执行了什么操作，从而决定在hbase中进行什么样的操作
                if ("r".equals(op) || "c".equals(op)) {
                    //read or create 创建表格
                    //获取创建表格所需的信息：命名空间/表名/列族，从after字段中查找
                    HBaseUtil.createTable(hbaseConnection, FinancialLeaseCommon.HBASE_NAMESPACE, tableProcess.getSinkTable(), tableProcess.getSinkFamily().split(","));
                } else if ("d".equals(op)) {
                    //delete 删除表格
                    //获取删除表格所需信息，从before字段中查找
                    tableProcess = jsonObject.getObject("before", TableProcess.class);
                    HBaseUtil.deleteTable(hbaseConnection, FinancialLeaseCommon.HBASE_NAMESPACE, tableProcess.getSinkTable());
                } else if ("u".equals(op)) {
                    //先删除，再创建
                    HBaseUtil.createTable(hbaseConnection, FinancialLeaseCommon.HBASE_NAMESPACE, tableProcess.getSinkTable(), tableProcess.getSinkFamily().split(","));
                    tableProcess = jsonObject.getObject("before", TableProcess.class);
                    HBaseUtil.deleteTable(hbaseConnection, FinancialLeaseCommon.HBASE_NAMESPACE, tableProcess.getSinkTable());
                }
                //添加操作符信息
                tableProcess.setOperateType(op);
                //数据继续向下游传递
                out.collect(tableProcess);
            }

            /**
             * 关闭连接
             * @throws Exception
             */
            @Override
            public void close() throws Exception {
                HBaseUtil.closeHBaseConnection(hbaseConnection);
            }
        });

        //TODO 5 广播配置流，然后进行双流合并：配置表数据流（MySQL）和主数据流（Kafka）进行合并
        //创建映射状态描述器
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<String, TableProcess>("broadcast_state", String.class, TableProcess.class);
        //创建广播流
        BroadcastStream<TableProcess> broadcastStream = processStream.broadcast(mapStateDescriptor);
        //双流连接
        BroadcastConnectedStream<String, TableProcess> connectedStream = kafkaSource.connect(broadcastStream);

        //TODO 6 处理合并后的数据，得到维度表数据
        //返回值类型 Tuple3 <维度数据处理类型（bootstrap-insert delete update） 维度表数据 维度表元数据（tableProcess）>
        //基于合并流调用process方法进行处理
        SingleOutputStreamOperator<Tuple3<String, JSONObject, TableProcess>> dimProcessStream = connectedStream.process(new BroadcastProcessFunction<String, TableProcess, Tuple3<String, JSONObject, TableProcess>>() {

            //根据处理逻辑，需要在广播流处理完成，将维度表信息写入广播状态后，主流的处理逻辑才可以正常地从广播状态中查询元数据信息并进行判断
            //如果主流比广播流先进行处理，会导致所有数据都被判定为不是维度表数据
            //因此需要提前读取MySQL中的维度表信息并进行封装

            private HashMap<String, TableProcess> configMap = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                configMap = new HashMap<>();
                java.sql.Connection connection = MySQLUtil.getConnection();
                PreparedStatement preparedStatement = connection.prepareStatement("select * from financial_lease_config.table_process");
                ResultSet resultSet = preparedStatement.executeQuery();
                while (resultSet.next()) {
                    TableProcess tableProcess = new TableProcess();
                    tableProcess.setSourceTable(resultSet.getString(1));
                    tableProcess.setSinkTable(resultSet.getString(2));
                    tableProcess.setSinkFamily(resultSet.getString(3));
                    tableProcess.setSinkColumns(resultSet.getString(4));
                    tableProcess.setSinkRowKey(resultSet.getString(5));
                    configMap.put(tableProcess.getSourceTable(), tableProcess);
                }
                preparedStatement.close();
                connection.close();
            }

            //处理主流
            @Override
            public void processElement(String value, BroadcastProcessFunction<String, TableProcess, Tuple3<String, JSONObject, TableProcess>>.ReadOnlyContext ctx, Collector<Tuple3<String, JSONObject, TableProcess>> out) throws Exception {
                //判断当前数据是否是维度表的数据，如果是的话就保留数据写入到下游
                //判断依据：广播流中的维度表信息

                //获取广播状态
                ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                //将数据转化为JSON格式
                JSONObject data = JSONObject.parseObject(value);
                //获取该条数据的操作类型
                String type = data.getString("type");
                //maxwell的数据类型有6种  bootstrap-start bootstrap-complete bootstrap-insert  insert update delete
                //其中bootstrap-start bootstrap-complete 没有具体数据
                if ("bootstrap-start".equals(type) || "bootstrap-complete".equals(type)) {
                    return;
                } else {
                    String tableName = data.getString("table");
                    //根据表名从广播状态中查找对应的元数据
                    TableProcess tableProcess = broadcastState.get(tableName);
                    if (Objects.isNull(tableProcess)) //元数据为空，说明广播状态中没有该数据
                    {
                        tableProcess = configMap.get(tableName);
                    }
                    //再从configMap进行判断，避免主流数据到达太早的情况
                    if (Objects.isNull(tableProcess)) {
                        return; //确定当前数据不是维度表
                    } else {
                        //需要过滤掉dim表不需要的数据
                        String[] columns = tableProcess.getSinkColumns().split(",");
                        JSONObject dataInner = data.getJSONObject("data");
                        if ("delete".equals(type)) //如果是删除操作，需要从old中查找表中数据
                        {
                            dataInner = data.getJSONObject("old");
                        }
                        //过滤掉多余的行信息
                        dataInner.keySet().removeIf(key -> !Arrays.asList(columns).contains(key));

                        //数据写出到下游
                        out.collect(Tuple3.of(type, dataInner, tableProcess));
                    }
                }
            }

            //处理广播流
            @Override
            public void processBroadcastElement(TableProcess value, BroadcastProcessFunction<String, TableProcess, Tuple3<String, JSONObject, TableProcess>>.Context ctx, Collector<Tuple3<String, JSONObject, TableProcess>> out) throws Exception {
                //将配置表的信息写入到广播状态中去
                //读取广播状态
                BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

                //获取操作状态，据此判断是否需要写入到广播状态中去
                String op = value.getOperateType();
                if ("d".equals(op)) //删除
                {
                    //删除当前的广播状态（说明维度表的信息在之前的处理中已经写入到广播状态了，因为删除了该维度表，所以应该删除该表对应的广播状态
                    broadcastState.remove(value.getSourceTable());
                    //删除configMap中的数据
                    configMap.remove(value.getSourceTable());
                } else {
                    //添加数据到广播状态中
                    broadcastState.put(value.getSourceTable(), value);
                }
            }
        });

        dimProcessStream.print("dim>>>>");

        //TODO 7 将数据写出到HBase
        dimProcessStream.addSink(new DimSinkFunc());

        //TODO 8 执行任务
        env.execute();
    }

}
