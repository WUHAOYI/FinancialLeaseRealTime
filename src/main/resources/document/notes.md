#环境准本（Hadoop，hive，Kafka，zookeeper，Flume）、业务数据模拟、数据采集通道等与离线数仓一致

# 集群资源规划
| **服务****名称** | **子****服务** | **服务器**<br/>**hadoop108** | **服务器**<br/>**hadoop109** | **服务器**<br/>**hadoop110** |
| --- | --- | --- | --- | --- |
| **HDFS** | NameNode | √ |  |  |
| **** | DataNode | √ | √ | √ |
| **** | SecondaryNameNode |  |  | √ |
| **Yarn** | NodeManager | √ | √ | √ |
| **** | Resourcemanager |  | √ |  |
| **Zookeeper** | Zookeeper Server | √ | √ | √ |
| **Kafka** | Kafka | √ | √ | √ |
| **My****SQL** | MySQL | √ |  |  |
| **HBase** | HMaster | √ |  |  |
| **** | HRegionServer | √ | √ | √ |
| **Maxwell** |  | √ |  |  |
| **Redis** |  | √ |  |  |
| **Flink** | JobManager | √ |  |  |
| **** | TaskManager | √ | √ | √ |
| **D****o****ris** | Frontends | √ |  |  |
|  | Backends | √ | √ | √ |


# 数仓架构
系统流程如下所示：

![](https://cdn.nlark.com/yuque/0/2024/png/2675852/1725281023903-21e0a31d-c950-461c-a9e4-2c64613d6d50.png)

首先，原始数据（ods层）缓存在kafka中，以数据流的形式由Flink来进行消费

然后通过数据清洗等操作，以及MySQL中的配置信息，来构建维度表，将维度表数据存储到HBase中；

然后Flink继续消费Kafka的数据，进行事实维度关联，构建dwd层

再将结果缓存到Kafka中，由另一个Flink Job进行消费，进行数据聚合，构建dws层

最后将结果存储到Doris中，进行可视化展示；

![](https://cdn.nlark.com/yuque/0/2024/png/2675852/1725281750232-52697e21-d8f6-4c9a-926d-8963903b2b1e.png)

# 数仓分层
## 1.ODS层
采集数据写入的**Kafka topic_db主题**即为实时数仓的ODS 层，这一层的作用是对数据做原样展示和备份

准备：

1. 重置financial_lease中的数据，删除所有表
2. 重置maxwell监控的数据变更信息，删除所有表，再启动maxwell
3. 删除kafka中的主题`topic_db`，然后再重新创建（防止有遗留数据）：`kafka-topics.sh --bootstrap-server hadoop108:9092 --delete --topic topic_db`（启动消费者即可自动创建不存在的主题：`kafka-console-consumer.sh --bootstrap-server hadoop108:9092 --topic topic_db`
4. 模拟数据生成（从8.1开始）

## 2.DIM层
DIM层的设计依据是维度建模理论，该层存储维度模型的维度表，存储在HBase数据库中

> 选用HBase数据库的原因：DIM 层表是用于维度关联的，要**通过主键去获取相关维度信息**，这种场景下** K-V 类型数据库的效率较高**。常见的 K-V 类型数据库有 Redis、HBase，而 Redis 的数据常驻内存，会给内存造成较大压力，因而选用 HBase 存储维度数据；而且获取维度信息时一般会选取特定列的信息，而不是整行的信息，在这种情况下HBase的列式存储会带来性能上的优势
>

### 2.1 维度表动态拆分
目的：**动态更新维度数据**，并写入到相应的表中

> 在实际业务中，维度表并不是一成不变的，可以随着业务的变化，会有新的表成为维度表，这就需要实时更新维度表信息
>

实现方式：在 `MySQL` 中构建一张配置表，通过`Flink CDC`将配置表信息读取到程序中

然后就可以通过双流连结的方式进行事实表和维度表的关联，这样当维度表的信息发生变化时，我们可以第一时间在数据流处理过程中捕获到相应的变化并进行处理；

相关流程：

![](https://cdn.nlark.com/yuque/0/2024/png/2675852/1725512657335-1859d591-aa79-4d68-995b-2e048905c92d.png)

具体实现：

首先创建数据库financial_lease_config，并开启binlog（通过Maxwell监控binlog变化，从而将数据同步到Kafka中）：

![](https://cdn.nlark.com/yuque/0/2024/png/2675852/1725512597803-5fc9ac81-75a8-48a5-9929-45bcd9b0f11a.png)

创建MySQL配置表table_process，信息如下：

```sql
DROP TABLE IF EXISTS `table_process`;
CREATE TABLE `table_process` (
    `source_table` varchar(200) NOT NULL COMMENT '来源表',
    `sink_table` varchar(200) NOT NULL COMMENT '输出表',
    `sink_family` varchar(200) COMMENT '输出到 hbase 的列族',
    `sink_columns` varchar(2000) COMMENT '输出字段',
    `sink_row_key` varchar(200) COMMENT 'rowkey',
    PRIMARY KEY (`sink_table`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
```

向其中插入维度表信息数据：

```sql
INSERT INTO `table_process`(`source_table`, `sink_table`, `sink_family`, `sink_columns`, `sink_row_key`) VALUES 
('business_partner', 'dim_business_partner', 'info', 'id,create_time,update_time,name', 'id'),
('department', 'dim_department', 'info', 'id,create_time,update_time,department_level,department_name,superior_department_id', 'id'),
('employee', 'dim_employee', 'info', 'id,create_time,update_time,name,type,department_id', 'id'),
('industry', 'dim_industry', 'info', 'id,create_time,update_time,industry_level,industry_name,superior_industry_id', 'id');
```

![](https://cdn.nlark.com/yuque/0/2024/png/2675852/1725512472534-3796df0c-fe9e-469f-ae91-ce2eb83ed4d4.png)

然后将维度表的信息保存到HBase中即可

### 2.2 FlinkCDC读取Kafka数据（Demo）
```java
    public static void main(String[] args) throws Exception {
        //1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //配置kafka source
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop108:9092")
                .setTopics("topic_db")
                .setGroupId("flink_cdc_kafka")
                .setValueOnlyDeserializer(new SimpleStringSchema()) //设置序列化
                .setStartingOffsets(OffsetsInitializer.earliest()) //读取之前的数据
                .build();

        //读取数据并进行打印
        DataStreamSource<String> kafkaSourceData = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource");
        kafkaSourceData.print();

        //执行
        env.execute();
    }
```

### 2.3 数据格式说明
#### 2.3.1 Kafka CDC Source（Maxwell采集数据的格式）
```json
{
	"database": "financial_lease",
	"table": "credit_facility_status",
	"type": "update",
	"ts": 1728199784,
	"xid": 156043,
	"xoffset": 6577,
	"data": {
		"id": 24749,
		"create_time": "2024-08-06 16:55:00.000000",
		"update_time": "2024-08-06 16:55:00.000000",
		"action_taken": 1,
		"status": 1,
		"credit_facility_id": 2591,
		"employee_id": 645,
		"signatory_id": null
	},
	"old": {
		"credit_facility_id": null
	}
}
```

#### 2.3.2 MySQL CDC Source
```json
{
	"before": null,
	"after": {
		"source_table": "business_partner",
		"sink_table": "dim_business_partner",
		"sink_family": "info",
		"sink_columns": "id,create_time,update_time,name",
		"sink_row_key": "id"
	},
	"source": {
		"version": "1.6.4.Final",
		"connector": "mysql",
		"name": "mysql_binlog_source",
		"ts_ms": 0,
		"snapshot": "false",
		"db": "financial_lease_config",
		"sequence": null,
		"table": "table_process",
		"server_id": 0,
		"gtid": null,
		"file": "",
		"pos": 0,
		"row": 0,
		"thread": null,
		"query": null
	},
	"op": "r",
	"ts_ms": 1728199839963,
	"transaction": null
}
```

重点：

1. `op`：操作类型
2. `before`：变更之前的数据
3. `after`：变更之后的数据

### 2.4 代码实现
#### 2.4.1 整体实现流程
1. 创建流环境
2. 从Kafka中读取ods层（得到主数据流）
3. 从MySQL中读取配置表数据
4. 在hbase中创建维度表
5. 广播配置表数据流，然后与主流进行双流合并
6. 处理广播连接流，得到维度表数据
7. 将维度表数据写出到hbase

#### 2.4.2 创建流环境
工具类：`CreateEnvUtil`

main函数：

```java
//TODO 1 创建新环境
StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(8081, "financial_lease_dim_app");
```

`CreateEnvUtil`方法-`getStreamEnv`：

```java
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
```

#### 2.4.3 从Kafka中读取topic_db原始数据
工具类：`KafkaUtil`

main函数：

```java
//TODO 2 从Kafka中读取数据（创建KafkaSource）
KafkaSource<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(FinancialLeaseCommon.KAFKA_ODS_TOPIC, "financial_lease_dim_app", OffsetsInitializer.earliest());
DataStreamSource<String> kafkaSource = env.fromSource(kafkaConsumer, WatermarkStrategy.noWatermarks(), "financial_lease_dim_app");
//kafkaSource.print("kafka>>>");
```

得到的`kafkaSource`就是主数据流

`KafkaUtil`方法-`getKafkaConsumer`：

```java
 public static KafkaSource<String> getKafkaConsumer(String topicName,String groupId, OffsetsInitializer initializer)
    {
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(FinancialLeaseCommon.KAFKA_BOOTSTRAP_SERVERS)
                .setTopics(topicName)
                .setGroupId(groupId)
                .setStartingOffsets(initializer)
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                    //自定义Kafka消息中value的反序列化器
                    @Override
                    public String deserialize(byte[] message) throws IOException {
                        if(message != null && message.length != 0)
                        {
                            return new String(message);
                        }
                        return null;
                    }

                    @Override
                    public boolean isEndOfStream(String nextElement) {
                        return false;
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return BasicTypeInfo.STRING_TYPE_INFO;
                    }
                })
                .build();

        return kafkaSource;
    }
```

注意：这里我们自己模拟simple反序列器自己实现了反序列话的方法，原因是：![](https://cdn.nlark.com/yuque/0/2024/png/2675852/1725519312426-d99bd08c-ee41-482b-9355-fa27a677377a.png)

在`SimpleStringSchema`中，反序列化方法没有对空值进行处理，如果遇到空值就会报错，所以需要手动进行空值的判定



#### 2.4.4 从MySQL中读取配置表数据
工具类：`MySQLUtil`

main函数：

```java
//TODO 3 从MySQL中读取配置表数据
DataStreamSource<String> mysqlSource = env.fromSource(MySQLUtil.getMysqlSource(), WatermarkStrategy.noWatermarks(), "financial_lease_dim_app");
//mysqlSource.print("mysql>>>");
```

得到的`mysqlSource`就是配置信息数据流

`MySQLUtil`方法-`getMysqlSource`：

```java
    /**
     * 创建MySQL Source
     * @return
     */
    public static MySqlSource<String> getMysqlSource()
    {
        return MySqlSource.<String>builder()
                .hostname(FinancialLeaseCommon.MYSQL_HOSTNAME)
                .port(FinancialLeaseCommon.MYSQL_PORT)
                .databaseList(FinancialLeaseCommon.MEDICAL_CONFIG_DATABASE) // set captured database
                .tableList(FinancialLeaseCommon.MEDICAL_CONFIG_TABLE) // set captured table
                .username(FinancialLeaseCommon.MYSQL_USERNAME)
                .password(FinancialLeaseCommon.MYSQL_PASSWD)
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .startupOptions(StartupOptions.initial())
                .build();
    }
```

#### 2.4.5 在hbase中创建维度表
工具类：`HBaseUtil`

main函数：

```java
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
```

处理逻辑：

1. 基于配置信息数据流（String类型）调用`process`方法，对每一条数据进行处理
2. 在`open`方法中创建`HBase`连接
3. 在`processElement`方法中编写具体的数据处理逻辑
+ 得到JSON格式数据
+ 得到当前数据的操作类型（详情参考2.3.1数据格式说明）
+ 得到当前数据的具体数据值（即维度表的配置信息），存储在一个`bean`中：`TableProcess`
+ 根据操作类型来判断如何对HBase中的表格进行操作
+ 如果当前操作类型是`r`（读取）或者`c`（创建），则新建HBase维度表
+ 如果当前数据类型是`d`（删除），则删除HBase中相应的维度表
+ 如果当前数据类型是`u`（更新），说明HBase中维度表的配置信息发生了变化，所以创建新的维度表，并删除旧的维度表
4. 在`close`方法中关闭`HBase`连接

`HBaseUtil`方法：

```java
    /**
     * 创建HBase连接
     *
     * @return
     */
    public static Connection getHBaseConnection() {
        Configuration conf = HBaseConfiguration.create();
        //设置zookeeper服务的主机名以及端口
        conf.set(FinancialLeaseCommon.HBASE_ZOOKEEPER_QUORUM, FinancialLeaseCommon.HBASE_ZOOKEEPER_QUORUM_HOST);
        conf.set(FinancialLeaseCommon.HBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT, FinancialLeaseCommon.HBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT_VALUE);
        try {
            return ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 关闭HBase连接
     *
     * @param connection
     */
    public static void closeHBaseConnection(Connection connection) {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 创建表格
     *
     * @param hbaseConnection hbase连接
     * @param namespace       命名空间
     * @param table           表名
     * @param familyNames     列族
     * @throws IOException
     */
    public static void createTable(Connection hbaseConnection, String namespace, String table, String... familyNames) throws IOException {
        //判断列族数量，不能为0
        if (Objects.isNull(familyNames) || familyNames.length == 0) {
            throw new RuntimeException("列族至少需要有一个");
        }

        // hbase的api分两类DDL和DML
        // DDL 使用的是admin
        // DML 使用的是table
        Admin admin = hbaseConnection.getAdmin();

        // 检查命名空间是否存在
//        try {
//            admin.getNamespaceDescriptor(namespace);
//        } catch (NamespaceNotFoundException e) {
//            // 如果命名空间不存在，则创建它
//            NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();
//            admin.createNamespace(namespaceDescriptor);
//            System.out.println("命名空间 " + namespace + " 创建成功");
//        }

        TableName tableName = TableName.valueOf(namespace, table);
        //判断表格是否存在
        if (admin.tableExists(tableName)) {
            System.out.println("表格" + namespace + ":" + table + "已经存在");
        } else {
            try {
                TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
                //添加列族信息
                for (String familyName : familyNames) {
                    ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(familyName));
                    tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
                }

                admin.createTable(tableDescriptorBuilder.build());
                System.out.println("表格" + namespace + ":" + table + "创建成功");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        admin.close();

    }

    /**
     * 删除表格
     *
     * @param hbaseConnection
     * @param namespace
     * @param table
     * @throws IOException
     */
    public static void deleteTable(Connection hbaseConnection, String namespace, String table) throws IOException {
        Admin admin = hbaseConnection.getAdmin();
        try {
            TableName tableName = TableName.valueOf(namespace, table);
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("表格" + namespace + ":" + table + "删除成功");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        admin.close();
    }
```

重点说明：

1. 创建表格之前必须要先创建好相应的命名空间（namespace）
2. 创建/删除表格等DDL操作均通过`admin`来实现
+ 通过命名空间和表名来构建`TableName`，用于判断表格是否存在
+ 通过`TableName`来构建`TableDescriptorBuilder`，用于添加表格的其他信息（如列族）
+ 通过`TableDescriptorBuilder.setColumnFamily`方法来添加列族信息
3. 创建表格时并不指定列名，而是写入数据时再将`列名:数据`写入到相应的列族中

#### 2.4.6 广播配置表数据流，然后与主流进行双流合并
main函数：

```java
        //TODO 5 广播配置流，然后进行双流合并：配置表数据流（MySQL）和主数据流（Kafka）进行合并
        //创建映射状态描述器
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<String, TableProcess>("broadcast_state", String.class, TableProcess.class);
        //创建广播流
        BroadcastStream<TableProcess> broadcastStream = processStream.broadcast(mapStateDescriptor);
        //双流连接
        BroadcastConnectedStream<String, TableProcess> connectedStream = kafkaSource.connect(broadcastStream);
```

重点说明：

1. 广播的作用：将状态广播出去，这样所有的并行子任务都能从状态中获取数据
2. `mapStateDescriptor`：Map类型的状态描述符，以k-v格式存储数据
3. 如何构建广播流：基于数据流调用`broadcast`方法，传入一个`mapStateDescriptor`，即可构建广播流，将`mapStateDescriptor`中保存的数据广播出去
4. 双流连接：`主数据流.connect(广播数据流)`，得到一个`BroadcastConnectedStream（广播连接流）`，基于这个数据流调用`process`方法，能对广播数据流和主数据流进行处理

#### 2.4.7 处理广播连接流，得到维度表数据
main函数：

```java
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
```

处理逻辑：

1. 基于广播连接流调用了`process`方法，传入了一个广播处理函数：`BroadcastProcessFunction`，参数分别是`IN1``IN2`和`OUT`，即主流输入数据、广播流输入数据和输出数据
2. 在`processBroadcastElement`方法中对广播流数据（即HBase表的配置信息）进行处理，这里输入的数据是`TableProcess`格式，处理逻辑如下：
+ 从ctx（上下文）中获取广播状态
+ 获取操作类型
+ 如果操作类型是`d`（删除），说明应该从广播状态中删除相应的数据
+ 如果是其他操作类型，则需要把数据添加到广播状态中，数据的格式是`k-v`类型，需要根据业务逻辑要求选择合适的k和v，这里的k选为表名，v则是全部的配置信息
3. 在`processElement`中对主流数据进行处理：
+ 获取广播状态
+ 从主流数据中获取操作类型（maxwell共有六种操作类型）
+ 根据主流数据中的表名信息获取状态中保存的配置信息
+ 根据操作类型判断具体的处理逻辑：
+ 如果操作类型是`bootstrap-start`或者`bootstrap-complete`，则这条记录中没有具体的数据，跳过即可
+ 如果是其他的操作类型，则需要判断这条记录是否是维度表信息记录【根据状态中的配置信息判断】
    - 如果不是维度表信息记录，则跳过即可
    - 如果是维度表信息记录，则获取到具体的维度表数据（参考2.3.1）
+ 对于维度表数据，根据配置信息中`column`的定义来过滤多余的行
+ 得到最终的维度表所需信息
3. `configMap`的作用：在我们的处理逻辑中，需要先处理广播流的数据，将配置信息写入到状态中，然后在主流中获取状态，根据状态中保存的配置信息来筛选维度表数据，但如果主流数据比广播流数据先处理，此时状态中没有配置信息，就会导致所有的主流数据都被判定为不是维度表数据；因此在`open`方法中连接MySQL数据库，读取一遍配置信息，保存在`configMap`中，起到和状态数据相似的效果

#### 2.4.8 将数据写出到HBase
工具类：`DimSinkFunc`、`HBaseUtil`

main函数：

```java
        //TODO 7 将数据写出到HBase
        dimProcessStream.addSink(new DimSinkFunc());
```

自定义了一个Sink方法用于写出数据到HBase中

`DimSinkFunc`工具类：

```java
public class DimSinkFunc extends RichSinkFunction<Tuple3<String, JSONObject, TableProcess>> {

    private Connection hBaseConnection = null;

    private Jedis jedis;
    /**
     * 启动
     * @param parameters The configuration containing the parameters attached to the contract.
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        hBaseConnection = HBaseUtil.getHBaseConnection();
        jedis = RedisUtil.getRedisClient(); //创建redis连接
    }

    /**
     * 处理函数具体逻辑 数据的删除和更新
     * @param value The input record.
     * @param context Additional context about the input record.
     * @throws Exception
     */
    @Override
    public void invoke(Tuple3<String, JSONObject, TableProcess> value, Context context) throws Exception {
        String type = value.f0; //操作类型
        JSONObject data = value.f1; //数据
        TableProcess tableProcess = value.f2; //元数据

        String sinkTable = tableProcess.getSinkTable();
        String sinkRowKeyName = tableProcess.getSinkRowKey();
        String sinkRowKey = data.getString(sinkRowKeyName);
        String sinkFamily = tableProcess.getSinkFamily();

        String[] columns = tableProcess.getSinkColumns().split(",");
        String[] values = new String[columns.length];
        for (int i = 0; i < columns.length; i++) {
            values[i] = data.getString(columns[i]);
        }

        if("delete".equals(type))
        {
            HBaseUtil.deleteRow(hBaseConnection, FinancialLeaseCommon.HBASE_NAMESPACE,sinkTable,sinkRowKey);
        }else {
            HBaseUtil.putRow(hBaseConnection,FinancialLeaseCommon.HBASE_NAMESPACE,sinkTable,sinkRowKey,sinkFamily,columns,values);
        }

//        当需要在HBase中删除更新数据时，应当同时删除redis中的数据缓存。以免导致数据不一致的问题
        if ("delete".equals(type) || "update".equals(type))
        {
            jedis.del(sinkTable + ":" + sinkRowKey);
        }
    }

    /**
     * 关闭
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        HBaseUtil.closeHBaseConnection(hBaseConnection);
    }
}

```

重点说明：

1. 继承了`RichSinkFunction`类，在`invoke`方法中处理具体的sink逻辑，其中方法的参数分为`IN`和`context`，`IN`的格式和2.4.7中的到的维度表数据格式相同
2. 通过`redis`进行缓存操作，如果要删除或者更新HBase表中的数据，应该对redis中的缓存也进行相应的操作

`HBaseUtil`工具类-`deleteRow`/`putRow`方法：

```java
    /**
     * 删除数据
     *
     * @param hbaseConnection
     * @param namespace
     * @param sinkTable
     * @param rowKey
     * @throws IOException
     */
    public static void deleteRow(Connection hbaseConnection, String namespace, String sinkTable, String rowKey) throws IOException {
        TableName tableName = TableName.valueOf(namespace, sinkTable);
        Table table = hbaseConnection.getTable(tableName);

        Delete delete = new Delete(Bytes.toBytes(rowKey));
        try {
            table.delete(delete);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        table.close();
    }

    /**
     * 新增数据
     *
     * @param hbaseConnection
     * @param namespace
     * @param sinkTable
     * @param rowKey
     * @param sinkFamily
     * @param columns
     * @param values
     * @throws IOException
     */
    public static void putRow(Connection hbaseConnection, String namespace, String sinkTable, String rowKey, String sinkFamily, String[] columns, String[] values) throws IOException {
        TableName tableName = TableName.valueOf(namespace, sinkTable);
        Table table = hbaseConnection.getTable(tableName);

        Put put = new Put(Bytes.toBytes(rowKey));
        for (int i = 0; i < columns.length; i++) {
            if (values[i] == null) {
                values[i] = "";
            }
            put.addColumn(Bytes.toBytes(sinkFamily), Bytes.toBytes(columns[i]), Bytes.toBytes(values[i]));
        }
        try {
            table.put(put);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        table.close();
    }
```

重点说明：

1. 新增/删除数据属于DML，通过`Table`来进行实现【基于`Table`对象调用`put/delete`方法】
2. 通过`TableName`来构建`Table`对象

#### 2.4.9 执行任务
```java
//TODO 8 执行任务
env.execute();
```

Flink有延迟执行的机制，只有显式地调用执行环境的 execute()方法，才能触发程序的执行；

### 2.5 测试
#### 2.5.1 环境准备
1. 启动Hadoop集群
2. 启动zookeeper
3. 启动Kafka
4. 启动HBase
5. 启动Redis

#### 2.5.2 HBase历史数据处理
如果之前已经做过测试，可以先删除相应的namespace和table，然后在创建好namespace，即可运行程序进行表的创建和dim层数据的写入，相关脚本如下：

```shell
#!/bin/bash

# 要删除的命名空间
NAMESPACE="FINANCIAL_LEASE_REALTIME"

for TABLE in "FINANCIAL_LEASE_REALTIME:dim_business_partner" "FINANCIAL_LEASE_REALTIME:dim_department" "FINANCIAL_LEASE_REALTIME:dim_employee" "FINANCIAL_LEASE_REALTIME:dim_industry"
do
  echo "禁用表: $TABLE"
  echo "disable '$TABLE'" | hbase shell -n

  echo "删除表: $TABLE"
  echo "drop '$TABLE'" | hbase shell -n
done

# 删除命名空间
echo "删除命名空间: $NAMESPACE"
echo "drop_namespace '$NAMESPACE'" | hbase shell -n
echo "create_namespace '$NAMESPACE'" | hbase shell -n
```

#### 2.5.3 执行程序
## 3.DWD层
DIM层中对维度表进行了构建和数据导入，在DWD层中，我们需要根据维度建模理论，进行事实表的构建

### 3.1 实体类说明
使用实体类来进行事实表数据的存储

首先需要根据业务逻辑以及维度建模理论，确定有哪些事实表需要构建，并确定其所需数据

要进行事实表的构建，需要关注业务过程，在金融租赁业务中，正常的业务流程是这样的：

**授信申请——层层审核——申请通过（取消/拒绝）——新增授信——开始授信占用——合同制作——签约——起租**

这里我们使用`事务型事实表`来记录各个业务过程，以`授信申请通过`为例，说明事实表的设计流程

1. 选择业务过程：授信申请业务
2. 声明粒度：所谓粒度即是事实表中每行数据表示的内容，我们对授信申请业务进行分析，经过层层审核之后，一条授信申请记录可能到达三个状态：通过、取消或者拒绝，因此我们可以将这三个状态划分为三个事实表来进行数据的存储，例如在授信申请通过事实表中，一行数据就记录了一条授信申请通过的信息
3. 确定维度：根据维度表的内容，可以进行关联的维度信息有部门、业务方向、业务经办、信审经办等；部门维度说明了该授信申请通过记录是哪个部门发起的，业务经办维度说明了该授信申请通过记录经由了哪个业务经办进行审核；
4. 确定事实：即业务过程的度量值，授信申请通过可以使用申请授信金额、批复金额等数据进行度量

根据以上事实表构建过程以及业务流程，可以将整个业务过程划分为如下事实，并通过相应的bean进行信息的存储：

1. 授信申请通过信息：`DwdAuditApprovalBean`
2. 授信申请取消信息：`DwdAuditCancelBean`
3. 授信申请拒绝信息：`DwdAuditRejectBean`
4. 授信申请批复信息：`DwdAuditReplyBean`
5. 新增授信信息：`DwdCreditAddBean`
6. 授信占用信息：`DwdCreditOccupyBean`
7. 合同制作信息：`DwdLeaseContractProducedBean`
8. 签约信息：`DwdLeaseSignedBean`
9. 起租信息：`DwdLeaseExecutionBean`

> 这里需要搞清楚三个金额：
>
> 1. 申请金额：指的是授信申请发起者所申请的金额
> 2. 批复金额：指的是对授信申请进行批复，所允许申请人使用的金额
> 3. 授信金额：指的是新增授信时，该授信申请占用的金额
>

各个bean对应的Kafka主题如下：

1. 授信申请通过：financial_dwd_audit_approve
2. 授信申请取消：financial_dwd_audit_cancel
3. 授信申请拒绝：financial_dwd_audit_reject
4. 授信申请批复：
5. 新增授信：financial_dwd_credit_add
6. 授信占用：financial_dwd_credit_occupy
7. 合同制作：financial_dwd_lease_contract_produce
8. 签约：financial_dwd_lease_sign
9. 起租：financial_dwd_lease_execution

### 3.2 代码实现
在DIM层我们对维度表进行了处理，从数据流中筛选出维度信息存储到HBase中，在DWD层我们主要关心事实表的处理

#### 3.2.1 整体实现流程
1. 创建流环境
2. 从Kafka中读取ods层数据（得到主数据流）
3. 从主数据流中筛选出与业务过程相关的表（credit | credit_facility | credit_facility_status | contract | reply）
4. 按照授信申请id进行分组
5. 动态分流：
    1. 定义测输出流，将审批通过的数据放入主输出流，将审批取消/拒绝的数据放入侧输出流
    2. 对主输出流中的数据进行处理
6. 将审批域（审批通过/取消/拒绝）写出到kafka对应的主题中
7. 将授信域（授信新增/授信占用）写出到Kafka对应的主题中
8. 将合同制作状态的数据写出到Kafka对应的主题中
9. 合并租赁域合同制作数据和合同表的数据
10. 数据分流，处理签约和起租数据
11. 将签约和起租数据写出到对应的kafka主题
12. 执行任务

#### 3.2.2 从主数据流中筛选出与业务过程相关的表
创建流环境 & 从Kafka中读取ods层数据的处理方法与DIM层相同

main函数：

```java
        //TODO 3 筛选出业务过程相关的事实表
        // credit | credit_facility | credit_facility_status | contract | reply
        SingleOutputStreamOperator<JSONObject> flatStream = odsStream.flatMap(new FlatMapFunction<String, JSONObject>() {

            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(value);
                String tableName = jsonObject.getString("table");
                //根据表名进行筛选 同时根据授信申请id 授信id是否为空来判断数据是否完整
                //credit | credit_facility_status  | reply 这三张表依据credit_facility_id来进行判断
                if ("credit".equals(tableName) || "credit_facility_status".equals(tableName) || "reply".equals(tableName)) {
                    if (!Objects.isNull(jsonObject.getJSONObject("data").getString("credit_facility_id"))) {
                        //数据发送到下游
                        out.collect(jsonObject);
                    }
                }
                //credit_facility依据自身的id来进行判断
                else if ("credit_facility".equals(tableName) && !Objects.isNull(jsonObject.getJSONObject("data").getString("id"))) {
                    out.collect(jsonObject);
                }
                //contract依据自身的credit_id来进行判断
                else if ("contract".equals(tableName) && !Objects.isNull(jsonObject.getJSONObject("data").getString("credit_id"))) {
                    out.collect(jsonObject);
                }
            }
        });

        //将授信审批相关的业务表和合同表分成两个数据流进行处理
        //分流依据：表名
        // 3.1 contractStream: contract
        SingleOutputStreamOperator<JSONObject> contractStream = flatStream.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                return "contract".equals(value.getString("table"));
            }
        });

        //3.2 mainStream: credit | credit_facility | credit_facility_status | reply
        SingleOutputStreamOperator<JSONObject> mainStream = flatStream.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                return !"contract".equals(value.getString("table"));
            }
        });
```

逻辑说明：

1. 通过`flatMap`进行业务过程相关表的筛选，同时进行数据清洗，过滤掉不完整的数据
2. 通过`filter`将授信审批相关的业务表（mainStream）和合同表（contractStream）分成两个数据流进行处理

#### 3.2.3 对mainStream按照授信申请id进行分组
main函数：

```java
        //TODO 4 按照授信申请id进行分组
        KeyedStream<JSONObject, String> keyedStream = mainStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                String tableName = value.getString("table");
                if ("credit_facility".equals(tableName)) {
                    return value.getJSONObject("data").getString("id");
                } else {
                    return value.getJSONObject("data").getString("credit_facility_id");
                }
            }
        });
```

使用`keyBy`方法指定相应的key进行分组，授信申请id相同的数据处于同一个授信申请流程中

#### 3.2.4 对mainStream进行动态分流
mainStream中是授信审批相关的业务表数据，在这一步中我们把授信审批通过的数据放入主流中，并且将其余数据筛选出来，放入侧输出流中。

main函数：

```java
//TODO 5 动态分流
        // 5.1 定义侧输出流标签 将审批通过的数据放入主输出流 审批取消/审批拒绝的数据放入侧输出流

        //审批域
        //审批取消侧输出流标签
        OutputTag<DwdAuditCancelBean> auditCancelTag = new OutputTag<DwdAuditCancelBean>("cancel_stream"){};
        //审批拒绝测输出流标签
        OutputTag<DwdAuditRejectBean> auditRejectTag = new OutputTag<DwdAuditRejectBean>("reject_stream"){};

        // 授信域
        // 授信新增
        OutputTag<DwdCreditAddBean> creditAddTag = new OutputTag<DwdCreditAddBean>("credit_add_stream") {
        };
        // 授信完成
        OutputTag<DwdCreditOccupyBean> creditOccupyTag = new OutputTag<DwdCreditOccupyBean>("credit_occupy_stream") {
        };

        // 租赁域
        // 合同制作
        OutputTag<DwdLeaseContractProducedBean> contractProducedTag = new OutputTag<DwdLeaseContractProducedBean>("contract_produced_stream") {
        };

        //TODO 5.2 对主输出流的数据进行处理
        SingleOutputStreamOperator<DwdAuditApprovalBean> processStream = keyedStream.process(new KeyedProcessFunction<String, JSONObject, DwdAuditApprovalBean>() {
            //设置状态
            //信审经办Id
            private ValueState<String> auditManIdStatus;
            //申请通过状态
            private ValueState<DwdAuditApprovalBean> auditApprovalStatus;
            //申请批复状态
            private ValueState<DwdAuditReplyBean> auditReplyStatus;
            //新增授信状态
            private ValueState<DwdCreditAddBean> creditAddStatus;
            //授信占用状态
            private ValueState<DwdCreditOccupyBean> creditOccupyStatus;
            //合同制作状态
            private ValueState<DwdLeaseContractProducedBean> contractProducedStatus;

            @Override
            public void open(Configuration parameters) throws Exception {
                auditManIdStatus = getRuntimeContext().getState(new ValueStateDescriptor<String>("audit_man_id", String.class));
                auditApprovalStatus = getRuntimeContext().getState(new ValueStateDescriptor<DwdAuditApprovalBean>("audit_approval_status", DwdAuditApprovalBean.class));
                auditReplyStatus = getRuntimeContext().getState(new ValueStateDescriptor<DwdAuditReplyBean>("audit_reply_status", DwdAuditReplyBean.class));
                creditAddStatus = getRuntimeContext().getState(new ValueStateDescriptor<DwdCreditAddBean>("credit_add_status", DwdCreditAddBean.class));
                creditOccupyStatus = getRuntimeContext().getState(new ValueStateDescriptor<DwdCreditOccupyBean>("credit_occupy_status", DwdCreditOccupyBean.class));
                contractProducedStatus = getRuntimeContext().getState(new ValueStateDescriptor<DwdLeaseContractProducedBean>("contract_produced_status", DwdLeaseContractProducedBean.class));
            }

            @Override
            public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, DwdAuditApprovalBean>.Context ctx, Collector<DwdAuditApprovalBean> out) throws Exception {
                //对不同表格进行处理
                String tableName = value.getString("table");
                //授信申请状态表
                if ("credit_facility_status".equals(tableName)) {
                    //因为指标汇总中有信审经办统计粒度的指标，所以需要记录信审经办审批状态\
                    //具体操作为：过滤出处于信审经办审核状态的数据，并获取到信审经办id，将其存入到状态中，供授信申请表处理时进行使用
                    //5 信审经办审核通过
                    //6 信审经办审核复议
                    //20 信审经办审批拒绝
                    String status = value.getJSONObject("data").getString("status");
                    if ("5".equals(status) || "6".equals(status) || "20".equals(status)) {
                        String employeeId = value.getJSONObject("data").getString("employee_id");
                        auditManIdStatus.update(employeeId);
                    }
                }
                //授信申请表
                else if ("credit_facility".equals(tableName)) {
                    //判断授信申请的状态
                    //16 授信申请通过
                    //20 授信申请拒绝
                    //21 授信申请取消
                    //（根据授信申请的最终状态来进行汇总，如果是17授信申请复议的话，需要继续进行授信申请处理，无需进行汇总）
                    String type = value.getString("type");
                    String status = value.getJSONObject("data").getString("status");
                    if ("update".equals(type)) {
                        switch (status) {
                            case "16":
                                DwdAuditApprovalBean auditApprovalBean = value.getObject("data", DwdAuditApprovalBean.class);
                                //补全信审经办信息
                                auditApprovalBean.setAuditManId(auditManIdStatus.value());
                                auditManIdStatus.clear();
                                //补全其他信息 审批通过时间 | 申请授信金额
                                auditApprovalBean.setApproveTime(value.getJSONObject("data").getString("update_time"));
                                auditApprovalBean.setApplyAmount(value.getJSONObject("data").getBigDecimal("credit_amount"));
                                //TODO 补全批复信息
                                // 需要考虑数据先后问题 是申请通过数据先到 还是申请批复数据先到
                                // 首先获取存储的申请批复状态，根据其是否为null，来判断数据先后问题
                                DwdAuditReplyBean auditReplyBean = auditReplyStatus.value();
                                if (Objects.isNull(auditReplyBean)) {
                                    //申请通过数据先到，更新其状态即可，批复信息的补全等到批复数据到达后再进行
                                    auditApprovalStatus.update(auditApprovalBean);
                                } else {
                                    //申请批复数据先到，进行批复信息的补全
                                    auditApprovalBean.setReplyId(auditReplyBean.getId());
                                    auditApprovalBean.setReplyAmount(auditReplyBean.getReplyAmount());
                                    auditApprovalBean.setReplyTime(auditReplyBean.getReplyTime());
                                    auditApprovalBean.setIrr(auditReplyBean.getIrr());
                                    auditApprovalBean.setPeriod(auditReplyBean.getPeriod());
                                    auditReplyStatus.clear();

                                    //更新申请通过状态，下游可能用到其中的数据
                                    auditApprovalStatus.update(auditApprovalBean);

                                    //数据写出到下游
                                    out.collect(auditApprovalBean);

                                    // 如果新增授信的数据先到 已经存在了状态中 这里需要进行处理 将对应的数据写出到侧输出流
                                    DwdCreditAddBean creditAddBean = creditAddStatus.value();
                                    if (!Objects.isNull(creditAddBean)) {
                                        //写入申请金额和批复金额（注意：这里的前提条件是申请批复数据先于申请成功数据到达）
                                        creditAddBean.setApplyAmount(auditApprovalBean.getApplyAmount());
                                        creditAddBean.setReplyAmount(auditApprovalBean.getReplyAmount());
                                        ctx.output(creditAddTag, creditAddBean);
                                        creditAddStatus.clear();
                                    }

                                    // 如果完成授信占用的数据先到 已经存在状态中 这里需要进行处理 将对应的数据写到侧输出流
                                    DwdCreditOccupyBean creditOccupyBean = creditOccupyStatus.value();
                                    if (creditOccupyBean != null) {
                                        creditOccupyBean.setApplyAmount(auditApprovalBean.getApplyAmount());
                                        creditOccupyBean.setReplyAmount(auditApprovalBean.getReplyAmount());
                                        ctx.output(creditOccupyTag, creditOccupyBean);
                                        creditOccupyStatus.clear();
                                    }

                                    // 如果完成合同制作的数据先到 已经存在状态中 这里需要进行处理 将对应的数据写到侧输出流
                                    DwdLeaseContractProducedBean contractProducedBean = contractProducedStatus.value();
                                    if (contractProducedBean != null) {
                                        contractProducedBean.setApplyAmount(auditApprovalBean.getApplyAmount());
                                        contractProducedBean.setReplyAmount(auditApprovalBean.getReplyAmount());
                                        ctx.output(contractProducedTag, contractProducedBean);
                                        contractProducedStatus.clear();
                                    }
                                }
                                break;
                            case "20":
                                DwdAuditRejectBean auditRejectBean = value.getObject("data", DwdAuditRejectBean.class);
                                //补全信审经办信息
                                auditRejectBean.setAuditManId(auditManIdStatus.value());
                                auditManIdStatus.clear();
                                //补全其他信息
                                auditRejectBean.setRejectTime(value.getJSONObject("data").getString("update_time"));
                                auditRejectBean.setApplyAmount(value.getJSONObject("data").getBigDecimal("credit_amount"));
                                //写出到侧输出流
                                ctx.output(auditRejectTag, auditRejectBean);
                                //申请拒绝之后可能会复议，因此需要将之前的状态清空
                                auditApprovalStatus.clear();
                                break;
                            case "21":
                                DwdAuditCancelBean auditCancelBean = value.getObject("data", DwdAuditCancelBean.class);
                                //补全信审经办信息
                                auditCancelBean.setAuditManId(auditManIdStatus.value());
                                auditManIdStatus.clear();
                                //补全其他信息
                                auditCancelBean.setCancelTime(value.getJSONObject("data").getString("update_time"));
                                auditCancelBean.setApplyAmount(value.getJSONObject("data").getBigDecimal("credit_amount"));
                                //写出到侧输出流
                                ctx.output(auditCancelTag, auditCancelBean);
                                //申请取消，不需要后续状态，因此清空
                                auditApprovalStatus.clear();
                                break;
                        }
                    }

                }
                //授信批复表
                else if ("reply".equals(tableName)) {
                    DwdAuditReplyBean auditReplyBean = value.getObject("data", DwdAuditReplyBean.class);
                    //添加信息
                    auditReplyBean.setReplyAmount(value.getJSONObject("data").getBigDecimal("credit_amount"));
                    auditReplyBean.setReplyTime(value.getJSONObject("data").getString("create_time"));

                    //判断是申请通过的数据先到还是批复数据先到
                    DwdAuditApprovalBean auditApprovalBean = auditApprovalStatus.value();
                    if (Objects.isNull(auditApprovalBean)) {
                        //批复数据先到
                        auditReplyStatus.update(auditReplyBean);
                    } else {
                        //申请通过的数据先到
                        //补全批复信息
                        auditApprovalBean.setReplyId(auditReplyBean.getId());
                        auditApprovalBean.setReplyAmount(auditReplyBean.getReplyAmount());
                        auditApprovalBean.setReplyTime(auditReplyBean.getReplyTime());
                        auditApprovalBean.setIrr(auditReplyBean.getIrr());
                        auditApprovalBean.setPeriod(auditReplyBean.getPeriod());

                        // 将申请通过的数据当做主流写出
                        out.collect(auditApprovalBean);
                        auditApprovalStatus.update(auditApprovalBean);

                        // 如果新增授信的数据先到 已经存在了状态中 这里需要进行处理 将对应的数据写出到侧输出流
                        DwdCreditAddBean creditAddBean = creditAddStatus.value();
                        if (!Objects.isNull(creditAddBean)) {
                            //写入申请金额和批复金额（注意：这里的前提条件是申请批复数据先于申请成功数据到达）
                            creditAddBean.setApplyAmount(auditApprovalBean.getApplyAmount());
                            creditAddBean.setReplyAmount(auditApprovalBean.getReplyAmount());
                            ctx.output(creditAddTag, creditAddBean);
                            creditAddStatus.clear();
                        }

                        // 如果完成授信占用的数据先到 已经存在状态中 这里需要进行处理 将对应的数据写到侧输出流
                        DwdCreditOccupyBean creditOccupyBean = creditOccupyStatus.value();
                        if (creditOccupyBean != null) {
                            creditOccupyBean.setApplyAmount(auditApprovalBean.getApplyAmount());
                            creditOccupyBean.setReplyAmount(auditApprovalBean.getReplyAmount());
                            ctx.output(creditOccupyTag, creditOccupyBean);
                            creditOccupyStatus.clear();
                        }

                        // 如果完成合同制作的数据先到 已经存在状态中 这里需要进行处理 将对应的数据写到侧输出流
                        DwdLeaseContractProducedBean contractProducedBean = contractProducedStatus.value();
                        if (contractProducedBean != null) {
                            contractProducedBean.setApplyAmount(auditApprovalBean.getApplyAmount());
                            contractProducedBean.setReplyAmount(auditApprovalBean.getReplyAmount());
                            ctx.output(contractProducedTag, contractProducedBean);
                            contractProducedStatus.clear();
                        }
                    }
                }
                //授信表
                else {
                    //需要判断 新增授信 授信占用 合同制作三种状态
                    String type = value.getString("type");
                    DwdAuditApprovalBean auditApprovalBean = auditApprovalStatus.value(); //从中获取申请金额和批复金额
                    if ("insert".equals(type)) {
                        //新增授信数据
                        DwdCreditAddBean creditAddBean = value.getObject("data", DwdCreditAddBean.class);
                        //补全 新增授信时间
                        creditAddBean.setAddTime(value.getJSONObject("data").getString("create_time"));
                        if (!Objects.isNull(auditApprovalBean)) {
                            if (Objects.isNull(auditApprovalBean.getReplyAmount())) {
                                //批复数据还没到，先存入状态
                                creditAddStatus.update(creditAddBean);
                            } else {
                                //批复的数据到了，数据完整
                                //补全申请金额 | 批复金额 | 新增授信时间
                                creditAddBean.setReplyAmount(auditApprovalBean.getReplyAmount());
                                creditAddBean.setApplyAmount(auditApprovalBean.getApplyAmount());
                                ctx.output(creditAddTag, creditAddBean);
                                auditApprovalStatus.clear();
                            }
                        }
                    } else {
                        String status = value.getJSONObject("data").getString("status");
                        if ("2".equals(status)) {
                            // 此时为授信占用数据
                            DwdCreditOccupyBean creditOccupyBean = value.getObject("data", DwdCreditOccupyBean.class);
                            //补全 完成授信占用时间
                            creditOccupyBean.setOccupyTime(value.getJSONObject("data").getString("credit_occupy_time"));
                            if (!Objects.isNull(auditApprovalBean)) {
                                // 审批通过的数据先到
                                if (auditApprovalBean.getReplyAmount() == null) {
                                    // 批复的数据未到
                                    creditOccupyStatus.update(creditOccupyBean);
                                } else {
                                    // 批复的数据也到了 审批的数据完整
                                    //补全 申请金额 | 批复金额
                                    creditOccupyBean.setApplyAmount(auditApprovalBean.getApplyAmount());
                                    creditOccupyBean.setReplyAmount(auditApprovalBean.getReplyAmount());
                                    ctx.output(creditOccupyTag, creditOccupyBean);
                                    auditApprovalStatus.clear();
                                }
                            }
                        } else if ("3".equals(status)) {
                            // 此时为制作合同
                            DwdLeaseContractProducedBean contractProducedBean = value.getObject("data", DwdLeaseContractProducedBean.class);
                            contractProducedBean.setId(value.getJSONObject("data").getString("contract_id"));
                            contractProducedBean.setCreditId(value.getJSONObject("data").getString("id"));
                            contractProducedBean.setProducedTime(value.getJSONObject("data").getString("contract_produce_time"));

                            if (auditApprovalBean != null) {
                                // 审批通过的数据先到
                                if (auditApprovalBean.getReplyAmount() == null) {
                                    // 批复的数据未到
                                    contractProducedStatus.update(contractProducedBean);
                                } else {
                                    // 批复的数据也到了 审批的数据完整
                                    contractProducedBean.setApplyAmount(auditApprovalBean.getApplyAmount());
                                    contractProducedBean.setReplyAmount(auditApprovalBean.getReplyAmount());
                                    ctx.output(contractProducedTag, contractProducedBean);
                                    auditApprovalStatus.clear();
                                }
                            }
                        }
                    }
                }
            }
        });
```

**<font style="background-color:#FBDE28;">逻辑说明：</font>**

1. 定义侧输出流标签，除了审批通过的数据之外，其余的审批业务相关数据全部放入侧输出流中
2. 基于分组后的`keyedStream`调用`process`方法，进行处理
+ 在`open`方法中对`ValueState`进行初始化，之所以使用`ValueState`，是因为各种类型的数据到达的顺序是不定的，但是审批流程又是有序的，所以审批流程中后出现的数据先到达时，就需要存储到状态中，用于后到达的数据补全字段
+ 处理流程也是基于表格进行处理的，在3.2.2中提到，将授信业务相关表和合同表分成两个流进行处理，授信业务相关表对应的是主流，然后在3.2.3中我们又对主流数据进行了分组，所以这里我们需要处理的数据就是授信申请状态表（`credit_facility_status`）、授信申请表（`credit_facility`）、授信批复表（`reply`）以及授信表（`credit`）这四个表、
+ 首先是`credit_facility_status`，这个表中存储了授信申请所处的各个状态的相关信息，我们可以从中获取到<u>信审经办人的id</u>，并将其存储到`state`中，用于在授信申请相关事实表中补全字段
+ 然后是`credit_facility`，在事实表设计时我们并没有针对每一个授信申请状态创建事实表，而是只关注授信申请的最终结果状态（通过/拒绝/取消），所以这里我们也是要筛选出处于授信申请通过/拒绝/取消这三个状态的表中数据。
    - 如果是审批拒绝或者取消，我们只需要补全信审经办信息以及处理字段转换即可，但
    - 如果是审批通过，我们还需要补全批复金额信息；这里需要分两种情况：审批通过数据先到或是批复数据先到，如果是审批通过数据先到，此时批复数据还没到，无法进行字段补全，因此将审批通过数据写入到`state`中，当批复数据到达时从`state`中取出审批通过数据，进行字段补全；如果时批复数据先到，则直接进行字段补全即可，然后将数据写出到下游
    - 同理，如果新增授信/授信占用/合同制作的数据先到了，也需要根据审批通过数据和批复数据进行字段补全（申请金额和批复金额）
+ 然后是`reply`，对批复表中的数据进行处理；这里也是要对审批通过数据和批复数据进行处理，分为审批通过数据先到和批复数据先到两种情况，处理逻辑和上面完全相同
+ 最后是`credit`，对授信表进行处理，授信表通过`status`字段存储了当前授信所处的状态，有新建（新增授信）/已占用（授信占用）/已制作合同（合同制作）/已取消四个状态，所以我们需要根据不同的状态来对相应的事实表进行处理。
    - 如果数据操作类型是`insert`，则说明是**新增授信**，我们需要判断批复数据是否已到达，从而在合适的地方更新批复金额
    - 授信占用和合同制作也是同理，需要补全批复金额

**至此，审批域和授信域的数据基本处理完毕，可以写出到相应的Kafka主题中；**

**<font style="background-color:#FBDE28;">重点说明：</font>**

1. 侧输出流的使用：
+ 首先需要自定义一个侧输出流标签（OutputTag）：`OutputTag<String> outputTag = new OutputTag<String>("side-output") {};`，在标签中定义了侧输出流中的数据类型以及侧输出流的标志
+ 基于主流调用`process`，在`processElement`方法中基于`context`调用`output`方法，传入侧输出流标签以及要写入侧输出流的数据，这样就把相应的数据写到了侧输出流中
+ 如何从侧输出流中取数据：基于主流调用`.getSideOutput()`即可得到侧输出流的数据
2. 状态的使用-`ValueState`：值状态，状态中只保存一个值
+ 如何进行初始化，在`process`方法中，可以通过`getRuntimeContext().getState()`，传入一个`ValueStateDescriptor`来进行初始化，例如：`getRuntimeContext().getState(new ValueStateDescriptor<String>("audit_man_id", String.class))`
+ 通过`.value()`获取当前状态的值
+ 通过`update()`更新当前状态
+ 通过`clear()`清空当前状态

#### 3.2.5 将审批域写出到kafka对应的主题中
```java
//TODO 6 将审批域写出到kafka对应的主题中
        String approveTopic = "financial_dwd_audit_approve";
        String cancelTopic = "financial_dwd_audit_cancel";
        String rejectTopic = "financial_dwd_audit_reject";

        //将申请通过数据写入到Kafka中
        processStream.map(new MapFunction<DwdAuditApprovalBean, String>() {
            @Override
            public String map(DwdAuditApprovalBean value) throws Exception {
                return JSON.toJSONString(value);
            }
        }).sinkTo(KafkaUtil.getKafkaProducer(approveTopic, approveTopic + "_sink")).name("approve_stream_sink");
        System.out.println("申请通过数据写出成功");

        //将申请取消数据写入到Kafka中
        SideOutputDataStream<DwdAuditCancelBean> cancelStream = processStream.getSideOutput(auditCancelTag);
        cancelStream.map(new MapFunction<DwdAuditCancelBean, String>() {
            @Override
            public String map(DwdAuditCancelBean value) throws Exception {
                return JSON.toJSONString(value);
            }
        }).sinkTo(KafkaUtil.getKafkaProducer(cancelTopic, cancelTopic + "_sink")).name("cancel_stream_sink");
        System.out.println("申请取消数据写出成功");

        //将申请拒绝的数据写入到Kafka中
        SideOutputDataStream<DwdAuditRejectBean> rejectStream = processStream.getSideOutput(auditRejectTag);
        rejectStream.map(new MapFunction<DwdAuditRejectBean, String>() {
                    @Override
                    public String map(DwdAuditRejectBean value) throws Exception {
                        return JSON.toJSONString(value);
                    }
                })
                .sinkTo(KafkaUtil.getKafkaProducer(rejectTopic, rejectTopic + "_sink"))
                .name("reject_stream_sink");
        System.out.println("申请拒绝数据写出成功");
```

逻辑说明：

1. 在map方法中对数据进行了类型转换，这样做是为了统一标准
2. 自定义Kafka Sink，用于写出数据到相应的topic

工具类：`KafkaUtil`-`getKafkaProducer`方法：

```java
    /**
     * 创建Kafka生产者
     * @param topicName
     * @param transId
     * @return
     */
    public static KafkaSink<String> getKafkaProducer(String topicName, String transId)
    {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,FinancialLeaseCommon.KAFKA_TRANSACTION_TIMEOUT);
        return KafkaSink.<String>builder()
                .setBootstrapServers(FinancialLeaseCommon.KAFKA_BOOTSTRAP_SERVERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topicName)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setKafkaProducerConfig(props)
                .setTransactionalIdPrefix(transId)
                .build();
    }
```

#### 3.2.6 将授信域写出到对应的Kafka主题中
main函数：

```java
        //TODO 7 将授信域写出到Kafka对应的主题中
        String creditAddTopic = "financial_dwd_credit_add";
        String creditOccupyTopic = "financial_dwd_credit_occupy";

        // 7.1 新增授信数据
        SideOutputDataStream<DwdCreditAddBean> creditAddStream = processStream.getSideOutput(creditAddTag);
        creditAddStream.map(new MapFunction<DwdCreditAddBean, String>() {
                    @Override
                    public String map(DwdCreditAddBean value) throws Exception {
                        return JSON.toJSONString(value);
                    }
                })
                .sinkTo(KafkaUtil.getKafkaProducer(creditAddTopic, creditAddTopic + "_sink"))
                .name("credit_add_sink");
        System.out.println("新增授信数据写出成功");

        // 7.2 完成授信占用
        SideOutputDataStream<DwdCreditOccupyBean> creditOccupyStream = processStream.getSideOutput(creditOccupyTag);
        creditOccupyStream.map(new MapFunction<DwdCreditOccupyBean, String>() {
                    @Override
                    public String map(DwdCreditOccupyBean value) throws Exception {
                        return JSON.toJSONString(value);
                    }
                })
                .sinkTo(KafkaUtil.getKafkaProducer(creditOccupyTopic, creditOccupyTopic + "_sink"))
                .name("credit_occupy_sink");
        System.out.println("完成授信占用数据写出成功");
```

逻辑与审批域完全相同

#### 3.2.7 将合同制作状态的数据写出到Kafka对应的主题中
```java
        // TODO 8 将合同制作状态的数据写出到Kafka对应的主题中
        // 8.1 合同制作数据
        String leaseContractProducedTopic = "financial_dwd_lease_contract_produce";
        SideOutputDataStream<DwdLeaseContractProducedBean> contractProducedStream = processStream.getSideOutput(contractProducedTag);
        contractProducedStream.map(JSON::toJSONString)
                .sinkTo(KafkaUtil.getKafkaProducer(leaseContractProducedTopic, leaseContractProducedTopic + "_sink"))
                .name("contract_produce_sink");
        System.out.println("合同制作数据写出成功");
```

逻辑与审批域完全相同

#### 3.2.8 合并租赁域合同制作数据和合同表数据
```java
        //TODO 9 合并租赁域合同制作数据和合同表的数据
        //9.1 合同制作数据
        KeyedStream<DwdLeaseContractProducedBean, String> contractProducedKeyedStream = contractProducedStream.keyBy(new KeySelector<DwdLeaseContractProducedBean, String>() {
            @Override
            public String getKey(DwdLeaseContractProducedBean value) throws Exception {
                return value.getCreditId();
            }
        });
        //9.2 合同表数据
        KeyedStream<JSONObject, String> contractKeyedStream = contractStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getJSONObject("data").getString("credit_id");
            }
        });

        //9.3 合并
        ConnectedStreams<DwdLeaseContractProducedBean, JSONObject> connectStream = contractProducedKeyedStream.connect(contractKeyedStream);
```

合同制作数据和合同表数据都是依据授信id进行分组，然后进行合并

#### 3.2.9 处理签约和起租数据
```java
        // TODO 10 数据分流 处理签约和起租数据
        // 以签约流为主流 起租数据写入到侧输出流
        // 起租流标签
        OutputTag<DwdLeaseExecutionBean> executionStreamTag = new OutputTag<DwdLeaseExecutionBean>("execution_stream"){};
        // 对合并后的流进行处理
        SingleOutputStreamOperator<DwdLeaseSignedBean> signedBeanStream = connectStream.process(new KeyedCoProcessFunction<String, DwdLeaseContractProducedBean, JSONObject, DwdLeaseSignedBean>() {
            // 合同制作数据
            private ValueState<DwdLeaseContractProducedBean> contractProducedStatus;
            // 签约数据
            private ValueState<DwdLeaseSignedBean> signedStatus;
            // 起租数据
            private ValueState<DwdLeaseExecutionBean> executionStatus;

            @Override
            public void open(Configuration parameters) throws Exception {
                contractProducedStatus = getRuntimeContext().getState(new ValueStateDescriptor<DwdLeaseContractProducedBean>("contract_produced_state", DwdLeaseContractProducedBean.class));
                signedStatus = getRuntimeContext().getState(new ValueStateDescriptor<DwdLeaseSignedBean>("sign_state", DwdLeaseSignedBean.class));
                executionStatus = getRuntimeContext().getState(new ValueStateDescriptor<DwdLeaseExecutionBean>("execution_state", DwdLeaseExecutionBean.class));
            }

            @Override
            public void processElement1(DwdLeaseContractProducedBean value, KeyedCoProcessFunction<String, DwdLeaseContractProducedBean, JSONObject, DwdLeaseSignedBean>.Context ctx, Collector<DwdLeaseSignedBean> out) throws Exception {
                //当前为合同制作数据
                DwdLeaseSignedBean signedBean = signedStatus.value();
                DwdLeaseExecutionBean executionBean = executionStatus.value();
                if (signedBean == null || executionBean == null) {
                    //有任意一个为空 说明数据不完整 需要等待合同表到来再进行进一步的处理 此时选择将合同制作数据保存到相应状态中
                    contractProducedStatus.update(value);
                }
                // 合同表数据先到 更新过签约状态 但数据不完整
                if (signedBean != null) {
                    signedBean.setCreditFacilityId(value.getCreditFacilityId());
                    signedBean.setApplyAmount(value.getApplyAmount());
                    signedBean.setReplyAmount(value.getReplyAmount());
                    signedBean.setCreditAmount(value.getCreditAmount());
                    out.collect(signedBean);
                    signedStatus.clear();
                }
                // 合同表数据先到 更新过起租状态 但数据不完整
                if (executionBean != null) {
                    executionBean.setCreditFacilityId(value.getCreditFacilityId());
                    executionBean.setApplyAmount(value.getApplyAmount());
                    executionBean.setReplyAmount(value.getReplyAmount());
                    executionBean.setCreditAmount(value.getCreditAmount());
                    ctx.output(executionStreamTag, executionBean);
                    executionStatus.clear();
                }
            }

            @Override
            public void processElement2(JSONObject value, KeyedCoProcessFunction<String, DwdLeaseContractProducedBean, JSONObject, DwdLeaseSignedBean>.Context ctx, Collector<DwdLeaseSignedBean> out) throws Exception {
                //当前为合同表数据
                DwdLeaseContractProducedBean contractProducedBean = contractProducedStatus.value();

                String type = value.getString("type");
                if ("update".equals(type)) {
                    String status = value.getJSONObject("data").getString("status");
                    if ("2".equals(status)) {
                        // 当前为签约数据
                        DwdLeaseSignedBean leaseSignedBean = value.getObject("data", DwdLeaseSignedBean.class);
                        if (Objects.isNull(contractProducedBean)) {
                            signedStatus.update(leaseSignedBean);
                        } else {
                            // 合同制作数据先到  需要将签约数据写出到主流
                            leaseSignedBean.setCreditFacilityId(contractProducedBean.getCreditFacilityId());
                            leaseSignedBean.setApplyAmount(contractProducedBean.getApplyAmount());
                            leaseSignedBean.setReplyAmount(contractProducedBean.getReplyAmount());
                            leaseSignedBean.setCreditAmount(contractProducedBean.getCreditAmount());
                            out.collect(leaseSignedBean);
                        }

                    } else if ("3".equals(status)) {
                        // 当前为起租数据
                        DwdLeaseExecutionBean executionBean = value.getObject("data", DwdLeaseExecutionBean.class);
                        if (Objects.isNull(contractProducedBean)) {
                            executionStatus.update(executionBean);
                        } else {
                            // 合同制作数据先到  需要将起租数据写出到侧输出流
                            executionBean.setCreditFacilityId(contractProducedBean.getCreditFacilityId());
                            executionBean.setApplyAmount(contractProducedBean.getApplyAmount());
                            executionBean.setReplyAmount(contractProducedBean.getReplyAmount());
                            executionBean.setCreditAmount(contractProducedBean.getCreditAmount());
                            ctx.output(executionStreamTag, executionBean);
                        }
                    }
                }
            }
        });
```

逻辑说明：

1. 以签约流为主流 起租数据写入到侧输出流
2. 基于合同制作数据和合同表数据合并的流调用`process`方法，进行相应的处理
3. 首先我们应该关注合同表数据，合同表中通过`status`字段记录了当前合同所处的状态，分别是新建/已签约/已起租/已取消
+ 对于签约数据，首先判断合同制作数据是否到达，如果未到达，则将签约数据记录到`state`中，等待合同制作数据到达后进行处理；如果已到达，则进行字段补全
+ 对于起租数据同理
4. 然后关注合同制作数据，此时先判断签约状态和起租状态是否为空，如果有一个为空的话，说明数据还不完整，将合同制作数据记录到状态中，等待合同表数据到达后取出状态中的数据进行字段补全；接下来可以对不为空的数据进行处理

#### 3.2.10 将签约/起租数据写出到对应的Kafka主题中
main函数：

```java
        // TODO 11 提取起租流 将签约和起租数据写出到对应的kafka主题
        // 11.1 写出签约数据
        String signedTopic = "financial_dwd_lease_sign";

        signedBeanStream.map(new MapFunction<DwdLeaseSignedBean, String>() {
                    @Override
                    public String map(DwdLeaseSignedBean value) throws Exception {
                        return JSON.toJSONString(value);
                    }
                })
                .sinkTo(KafkaUtil.getKafkaProducer(signedTopic, signedTopic + "_sink"))
                .name("sign_sink");
        System.out.println("签约数据写出成功");

        // 11.2 写出起租数据
        // 获取起租流
        SideOutputDataStream<DwdLeaseExecutionBean> executionStream = signedBeanStream.getSideOutput(executionStreamTag);
        String executionTopic = "financial_dwd_lease_execution";
        executionStream.map(new MapFunction<DwdLeaseExecutionBean, String>() {
                    @Override
                    public String map(DwdLeaseExecutionBean value) throws Exception {
                        return JSON.toJSONString(value);
                    }
                })
                .sinkTo(KafkaUtil.getKafkaProducer(executionTopic, executionTopic + "_sink"))
                .name("execution_sink");
        System.out.println("起租数据写出成功");
```

逻辑与授信域完全相同

#### 3.2.11 执行
```java
//TODO 12 执行
env.execute();
```

### 3.3 测试
#### 3.3.1 环境准备
1. 启动Hadoop集群
2. 启动zookeeper
3. 启动Kafka

#### 3.3.2 Kafka历史数据处理
如果之前运行过程序，有相应的Kafka Topic，则需要先清空topic，再运行程序

查看topic：`kafka-topics.sh --bootstrap-server hadoop108:9092 --list topic`

删除topic的脚本如下：

```shell
#!/bin/bash

# Kafka broker 地址和端口
KAFKA_BROKER="hadoop108:9092"

# 获取所有 Kafka topics
topics=$(kafka-topics.sh --bootstrap-server $KAFKA_BROKER --list)

# 遍历每个 topic，找到以 financial_dwd_ 开头的，并删除它
for topic in $topics; do
    if [[ $topic == financial_dwd_* ]]; then
        echo "Deleting topic: $topic"
        kafka-topics.sh --bootstrap-server $KAFKA_BROKER --delete --topic $topic
    fi
done
```

#### 3.3.3 执行程序
执行完毕后即可在Kafka中看到相应的topic中的数据：

![](https://cdn.nlark.com/yuque/0/2024/png/2675852/1728296567393-6bb42ea8-353c-42b7-8132-61fcaead0aa4.png)

## 4.DWS层
基于指标体系构建DWS层，包括原子指标、派生指标。衍生指标等

DWS层表名的命名规范为`dws_数据域_统计粒度_业务过程_统计周期`（window）

> window 表示窗口对应的时间范围
>

其中数据域可以是审批域、授信域、租赁域

统计粒度可以是业务经办粒度、信审经办粒度等

业务过程可以是各行业、各业务方向、各部门等

统计周期通过window来设置（Flink代码中）



### 4.0 工具类说明
#### 4.0.1 DateFormatUtil
日期格式化工具类，用于时间戳和各种格式的日期之间的转换

在定义窗口起始时间和窗口结束时间的时候，我们需要将时间戳转化成想要的日期格式，因此封装此工具类

```java
public class DateFormatUtil {

    // 定义 yyyy-MM-dd 格式的日期格式化对象
    private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    // 定义 yyyy-MM-dd HH:mm:ss 格式的日期格式化对象
    private static final DateTimeFormatter dtfFull = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // 定义 yyyy-MM-dd HH:mm:ss.SSSSSS 格式的日期格式化对象
    private static final DateTimeFormatter dtfFullWithMs = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    /**
     * 将 yyyy-MM-dd HH:mm:ss.SSSSSS 格式的格式化日期字符串转换为时间戳
     * @param dtStr 格式化日期字符串
     * @return 格式化日期字符串转换得到的时间戳
     */
    public static Long toTs(String dtStr) {

        // 将 yyyy-MM-dd HH:mm:ss.SSSSSS 格式的格式化日期字符串转换为 LocalDateTime 类型的日期对象
        LocalDateTime localDateTime = LocalDateTime.parse(dtStr, dtfFullWithMs);

        return localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
    }

    /**
     * 将时间戳转换为 yyyy-MM-dd 格式的格式化日期字符串
     * @param ts 时间戳
     * @return 格式化日期字符串
     */
    public static String toDate(Long ts) {
        Date dt = new Date(ts);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(dt.toInstant(), ZoneId.systemDefault());
        return dtf.format(localDateTime);
    }

    /**
     * 将时间戳转换为 yyyy-MM-dd HH:mm:ss 格式的格式化日期字符串
     * @param ts 时间戳
     * @return 格式化日期字符串
     */
    public static String toYmdHms(Long ts) {
        Date dt = new Date(ts);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(dt.toInstant(), ZoneId.systemDefault());
        return dtfFull.format(localDateTime);
    }
}
```

#### 4.0.2 Bean2JSONUtil
将Bean转化为JSONString

为了便于进行维表Join，补全DWS层表中需要的字段，我们将数据封装成bean来进行处理，但是sink到Doris的时候需要使用String类型，所以自定义工具类用于将Bean转化为JSONString，同时配合反射机制和注解还可以筛选Bean中的属性，使得一些不必要的属性不写入数据库：

```java
public class Bean2JSONUtil {
    public static <T> String Bean2Json(T bean) throws IllegalAccessException {
        //传入的bean需要利用反射获取其成员变量
        Class<?> clazz = bean.getClass();
        Field[] fields = clazz.getDeclaredFields();
        //遍历bean的属性，将不需要的属性去除，将需要的属性的名称和值放入一个JSONObject中
        JSONObject jsonObject = new JSONObject();
        for (Field field : fields) {
            //判断成员变量有没有注解
            TransientSink annotation = field.getAnnotation(TransientSink.class);
            if (Objects.isNull(annotation))
            {
                //没有注解说明该成员变量需要保留
                field.setAccessible(true);
                String name = field.getName();
                Object value = field.get(bean);
                //由于该方法用于转化bean为JSONObject然后写入doris数据库，因此要对name的格式进行处理
                StringBuilder snakeCaseName = new StringBuilder();
                for (int i = 0; i < name.length(); i++) {
                    char curChar = name.charAt(i);
                    if (Character.isUpperCase(curChar)) {
                        snakeCaseName.append("_");
                        curChar = Character.toLowerCase(curChar);
                    }
                    snakeCaseName.append(curChar);
                }
                name = snakeCaseName.toString();
                jsonObject.put(name,value);
            }
        }
        return jsonObject.toJSONString();
    }
}
```

相应的注解：

#### 4.0.3 `DorisUtil`
定义了Doris Sink

```java
public class DorisUtil {
    public static DorisSink<String> getDorisSink(String table, String dorisLabel) {
        Properties props = new Properties();
        props.setProperty("format", "json");
        props.setProperty("read_json_by_line", "true"); // 每行一条 json 数据

        return DorisSink.<String>builder()
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisOptions(DorisOptions.builder() // 设置 doris 的连接参数
                        .setFenodes(FinancialLeaseCommon.DORIS_FE_NODES)
                        .setTableIdentifier(table)
                        .setUsername(FinancialLeaseCommon.DORIS_USER_NAME)
                        .setPassword(FinancialLeaseCommon.DORIS_PASSWD)
                        .build())
                .setDorisExecutionOptions(DorisExecutionOptions.builder() // 执行参数
                        .setLabelPrefix(dorisLabel)  // stream-load 导入的时候的 label 前缀
                        .disable2PC() // 开启两阶段提交后,labelPrefix 需要全局唯一,为了测试方便禁用两阶段提交
                        .setDeletable(false)
                        .setBufferCount(3) // 批次条数: 默认 3
                        .setBufferSize(8 * 1024) // 批次大小: 默认 1M
                        .setCheckInterval(3000) // 批次输出间隔   三个对批次的限制是或的关系
                        .setMaxRetries(3)
                        .setStreamLoadProp(props) // 设置 stream load 的数据格式 默认是 csv,需要改成 json
                        .build())
                .setSerializer(new SimpleStringSerializer())
                .build();
    }
}
```

#### 4.0.4 RedisUtil
添加异步连接以及异步读写数据的方法



```java
    /**
     * 获取一个到 redis 线程安全的异步连接, key value 都用 utf-8 进行编码
     *
     * @return
     */
    public static StatefulRedisConnection<String, String> getAsyncRedisConnection() {
        // 连接到 redis 的 0号库
        RedisClient redisClient = RedisClient.create("redis://" + FinancialLeaseCommon.REDIS_HOST + ":" + FinancialLeaseCommon.REDIS_PORT + "/0");

        return redisClient.connect();
    }


    /**
     * 异步读取数据
     * @param asyncConn
     * @param key
     * @return
     */
    public static JSONObject asyncReadDim(StatefulRedisConnection<String, String> asyncConn ,String key){
        RedisAsyncCommands<String, String> asyncCommon = asyncConn.async();
        try {
            String jsonStr = asyncCommon.get(key).get();
            if (jsonStr !=null){
                return JSONObject.parseObject(jsonStr);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 异步写入数据
     * @param asyncConn
     * @param key
     * @param dim
     * @return
     */
    public static String asyncWriteDim(StatefulRedisConnection<String, String> asyncConn, String key, JSONObject dim) {
        RedisAsyncCommands<String, String> asyncCommon = asyncConn.async();
        RedisFuture<String> setex = asyncCommon.setex(key, 2 * 24 * 60 * 60, dim.toJSONString());
        try {
            return setex.get();
        }catch (Exception e){
            throw new RuntimeException("数据写入redis失败");
        }
    }
```

#### 4.0.5 HBaseUtil
添加异步连接的方法，以及异步读取数据的方法（用于维表Join）

```java
    /**
     * 创建HBase异步连接
     *
     * @return
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static AsyncConnection getAsyncHBaseConnection() throws ExecutionException, InterruptedException {
        Configuration conf = HBaseConfiguration.create();
        //设置zookeeper服务的主机名以及端口
        conf.set(FinancialLeaseCommon.HBASE_ZOOKEEPER_QUORUM, FinancialLeaseCommon.HBASE_ZOOKEEPER_QUORUM_HOST);
        conf.set(FinancialLeaseCommon.HBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT, FinancialLeaseCommon.HBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT_VALUE);

        return ConnectionFactory.createAsyncConnection(conf).get();
    }

    /**
     * 关闭HBase异步连接
     *
     * @param asyncConnection
     */
    public static void closeAsyncHBaseConnection(AsyncConnection asyncConnection) {
        if (!Objects.isNull(asyncConnection) && !asyncConnection.isClosed()) {
            try {
                asyncConnection.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * 异步读取数据
     *
     * @param asyncConnection
     * @param namespace
     * @param tableName
     * @param rowKey
     * @return
     */
    public static JSONObject asyncReadRow(AsyncConnection asyncConnection, String namespace, String tableName, String rowKey) {
        AsyncTable<AdvancedScanResultConsumer> table = asyncConnection.getTable(TableName.valueOf(Bytes.toBytes(namespace), Bytes.toBytes(tableName)));

        Get get = new Get(Bytes.toBytes(rowKey));

        CompletableFuture<Result> resultCompletableFuture = table.get(get);

        try {
            Result result = resultCompletableFuture.get();
            JSONObject res = new JSONObject();
            for (Cell cell : result.rawCells()) {
                //获取列名
                String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
                //获取列值
                String columnValue = Bytes.toString(CellUtil.cloneValue(cell));
                res.put(columnName, columnValue);
            }
//            System.out.println("rowKey为:" + rowKey + "查询出的数据为:" + res.toJSONString());
            return res;

        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
```

#### 4.0.6 AsyncDimFunctionHBase
用于异步读取HBase中的维度信息

```java
public abstract class AsyncDimFunctionHBase<T> extends RichAsyncFunction<T, T> implements DimFunction<T> {

    //HBase异步连接
    private AsyncConnection asyncHBaseConnection = null;
    //redis异步连接
    private StatefulRedisConnection<String, String> asyncRedisConnection;

    @Override
    public void open(Configuration parameters) throws Exception {
        //创建HBase异步连接
        asyncHBaseConnection = HBaseUtil.getAsyncHBaseConnection();
        //创建redis异步连接
        asyncRedisConnection = RedisUtil.getAsyncRedisConnection();
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        CompletableFuture.supplyAsync(new Supplier<JSONObject>() {
            //添加redis缓存之后需要进行的操作流程：
            //1.判断要从HBase中读取的数据是否已经缓存过
            //2.已经缓存过则直接读取redis
            //3.没有缓存过则读取HBase，并将读出来的数据写入到redis中
            //4.补充维度表信息
            @Override
            public JSONObject get() {
                //从redis中异步读取数据
                JSONObject dim = RedisUtil.asyncReadDim(asyncRedisConnection, getTable() + ":" + getId(input));
//                System.out.println("从redis中读取的数据为:" + dim.toJSONString());
                return dim;
            }
        }).thenApplyAsync(new Function<JSONObject, JSONObject>() {
            @Override
            public JSONObject apply(JSONObject dim) {
                String tableName = getTable();
                String id = getId(input);
                //判断从redis中是否读取到数据
                if (Objects.isNull(dim))
                {
                    System.out.println(tableName + "  " + id + "  从 hbase 读取");
                    //从 HBase 中异步读取数据
                    dim = HBaseUtil.asyncReadRow(asyncHBaseConnection, FinancialLeaseCommon.HBASE_NAMESPACE, tableName, id);
                    //将数据写入到Redis中
                    if (Objects.isNull(dim))
                    {
                        Log.error("没有匹配的维度信息，表名： " + getTable() + "，rowKey： " + getId(input));
                    }
                    RedisUtil.asyncWriteDim(asyncRedisConnection,tableName + ":" + id,dim);
                }else {
                    System.out.println(tableName + "  " + id + "  从 redis 读取");
                }
                return dim;
            }
        }).thenAccept(new Consumer<JSONObject>() {
            //补充维度表信息
            @Override
            public void accept(JSONObject dim) {
                addDim(input, dim);
                // 收集结果
                resultFuture.complete(Collections.singletonList(input));
            }
        });
    }

    @Override
    public void close() throws Exception {
        HBaseUtil.closeAsyncHBaseConnection(asyncHBaseConnection);
    }

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        System.err.println("async handle timeout");
    }
}
```

#### 4.0.7 DimFunction
用于主程序和AsyncDimFunctionHBase之间的传值

主程序中根据数据流的信息提取出rowKey并传入表名，从而能在AsyncDimFunctionHBase中拿到相关信息，用于读取HBase中的维度表数据

```java
public interface DimFunction<T> {

    String getTable(); //获取表名

    String getId(T bean); //获取rowKey

    void addDim(T bean, JSONObject dim); //补充维度表信息
}
```

#### 4.0.8 KafkaTopicChecker
判断要从中读取数据的Kafka Topic是否存在，如果存在才可以读取数据

```java
public class KafkaTopicChecker {
    public static boolean topicExists(String bootstrapServers, String topicName) {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        try (AdminClient adminClient = AdminClient.create(properties)) {
            ListTopicsResult topics = adminClient.listTopics();
            return topics.names().get().contains(topicName);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            return false;
        }
    }
}
```

### 4.1 <font style="background-color:#FBDE28;">审批</font>域<font style="background-color:#FBDE28;">行业</font>业务方向<font style="background-color:#FBDE28;">业务经办</font>粒度<font style="background-color:#FBDE28;">审批通过</font>各窗口汇总表
#### 4.1.1 建表
```sql
drop table if exists financial_lease_realtime.dws_audit_industry_lease_organization_salesman_approval_win;
create table if not exists financial_lease_realtime.dws_audit_industry_lease_organization_salesman_approval_win
(
    `stt`                DATETIME comment '窗口起始时间',
    `edt`                DATETIME comment '窗口结束时间',
    `cur_date`           DATE comment '当天日期',
    `industry1_id`       INT comment '一级行业ID',
    `industry1_name`     CHAR(100) comment '一级行业名称',
    `industry2_id`       INT comment '二级行业ID',
    `industry2_name`     CHAR(100) comment '二级行业名称',
    `industry3_id`       INT comment '三级行业ID',
    `industry3_name`     CHAR(100) comment '三级行业名称',
    `lease_organization` CHAR(100) comment '业务方向',
    `salesman_id`        CHAR(100) comment '业务经办ID',
    `salesman_name`      CHAR(100) comment '业务经办姓名',
    `department1_id`     INT comment '一级部门ID',
    `department1_name`   CHAR(100) comment '一级部门名称',
    `department2_id`     INT comment '二级部门ID',
    `department2_name`   CHAR(100) comment '二级部门名称',
    `department3_id`     INT comment '三级部门ID',
    `department3_name`   CHAR(100) comment '三级部门名称',
    `apply_count`        BIGINT replace comment '申请项目数',
    `apply_amount`       DECIMAL(16, 2) replace comment '申请金额',
    `reply_amount`       DECIMAL(16, 2) replace comment '批复金额'
) engine = olap aggregate key (`stt`, `edt`,`cur_date`,`industry1_id`,`industry1_name`,`industry2_id`,`industry2_name`,`industry3_id`,`industry3_name`,`lease_organization`,`salesman_id`,`salesman_name`,`department1_id`,`department1_name`,`department2_id`,`department2_name`,`department3_id`,`department3_name`)
partition by range(`cur_date`)()
distributed by hash(`stt`) buckets 10
properties (
"replication_num" = "3",
"dynamic_partition.enable" = "true",
"dynamic_partition.time_unit" = "DAY",
"dynamic_partition.start" = "-100",
"dynamic_partition.end" = "3",
"dynamic_partition.prefix" = "par",
"dynamic_partition.buckets" = "10",
"dynamic_partition.hot_partition_num" = "1",
"dynamic_partition.create_history_partition" = "true",
"dynamic_partition.history_partition_num" = "100"
);
```

可以看到，这个表存储了审批通过的数据，并且为其添加了部门、行业、业务方向等维度信息

由此可以看出进行维表Join是我们构建DWS层的重点之一

#### 4.1.2 代码实现
整体流程如下：

1. 创建环境
2. 获取Kafka数据（DWD层数据）
3. 转换数据结构（将DWD层的bean对应的String转换为DWS层的bean）
4. 引入水位线
5. 分组（依据：业务方向、部门、行业）
6. 开窗
7. 聚合
8. 补全维度信息
9. 将数据写出到Doris
10. 执行

```java
public class DwsAuditIndLeaseOrgSalesmanApprovalWin {
    public static void main(String[] args) throws Exception {
        String appName = "dws_audit_industry_lease_organization_salesman_approval_window";
        //TODO 1 创建环境
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(8083, appName);
        env.setParallelism(1);

        //TODO 2 获取Kafka数据
        String approveTopic = "financial_dwd_audit_approve";
        KafkaSource<String> kafkaSource = KafkaUtil.getKafkaConsumer(approveTopic, appName, OffsetsInitializer.earliest());
        DataStreamSource<String> kafkaSourceStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source");

        //TODO 3 转换数据结构（从Kafka中读取的数据是DwdAuditApprovalBean格式的字符串，需要将其转换为dws层的bean结构
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanApprovalBean> dwsBeanStream = kafkaSourceStream.map(new MapFunction<String, DwsAuditIndLeaseOrgSalesmanApprovalBean>() {
            @Override
            public DwsAuditIndLeaseOrgSalesmanApprovalBean map(String value) throws Exception {

                //对应的字段会自动转化
                DwsAuditIndLeaseOrgSalesmanApprovalBean bean = JSONObject.parseObject(value, DwsAuditIndLeaseOrgSalesmanApprovalBean.class);
                //填充其余字段
                bean.setIndustry3Id(JSONObject.parseObject(value).getString("industryId"));
                bean.setApplyCount(1L);
                return bean;
            }
        });

//        dwsBeanStream.print("dws>>>");

        //TODO 4 引入水位线
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanApprovalBean> streamWithWatermark = dwsBeanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<DwsAuditIndLeaseOrgSalesmanApprovalBean>forBoundedOutOfOrderness(
                Duration.ofSeconds(5L)
        ).withTimestampAssigner(new SerializableTimestampAssigner<DwsAuditIndLeaseOrgSalesmanApprovalBean>() {
            @Override
            public long extractTimestamp(DwsAuditIndLeaseOrgSalesmanApprovalBean element, long recordTimestamp) {
                //从数据中提取字段作为水位线的时间戳
                return element.getTs();
            }
        }));

        //TODO 5 分组
        //分组依据：行业 业务方向 业务经办id
        KeyedStream<DwsAuditIndLeaseOrgSalesmanApprovalBean, String> keyedStream = streamWithWatermark.keyBy(new KeySelector<DwsAuditIndLeaseOrgSalesmanApprovalBean, String>() {
            @Override
            public String getKey(DwsAuditIndLeaseOrgSalesmanApprovalBean bean) throws Exception {
                return bean.getLeaseOrganization() + ":" + bean.getIndustry3Id() + ":" + bean.getSalesmanId();
            }
        });

        //TODO 6 开窗
        // 开窗的时间决定了最终结果的最小时间范围  越小精度越高  越大越省资源
        WindowedStream<DwsAuditIndLeaseOrgSalesmanApprovalBean, String, TimeWindow> windowStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10L)));

        //TODO 7 聚合 先聚合再进行维度关联，可以减少处理的数据
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanApprovalBean> reducedStream = windowStream.reduce(new ReduceFunction<DwsAuditIndLeaseOrgSalesmanApprovalBean>() {
            @Override
            public DwsAuditIndLeaseOrgSalesmanApprovalBean reduce(DwsAuditIndLeaseOrgSalesmanApprovalBean value1, DwsAuditIndLeaseOrgSalesmanApprovalBean value2) throws Exception {
                // 聚合逻辑
                value1.setApplyCount(value1.getApplyCount() + value2.getApplyCount());
                value1.setApplyAmount(value1.getApplyAmount().add(value2.getApplyAmount()));
                value1.setReplyAmount(value1.getReplyAmount().add(value2.getReplyAmount()));
                return value1;
            }
        }, new ProcessWindowFunction<DwsAuditIndLeaseOrgSalesmanApprovalBean, DwsAuditIndLeaseOrgSalesmanApprovalBean, String, TimeWindow>() {
            @Override
            public void process(String s, ProcessWindowFunction<DwsAuditIndLeaseOrgSalesmanApprovalBean, DwsAuditIndLeaseOrgSalesmanApprovalBean, String, TimeWindow>.Context context, Iterable<DwsAuditIndLeaseOrgSalesmanApprovalBean> elements, Collector<DwsAuditIndLeaseOrgSalesmanApprovalBean> out) throws Exception {
                String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                String curDate = DateFormatUtil.toDate(context.window().getStart());
                for (DwsAuditIndLeaseOrgSalesmanApprovalBean element : elements) {
                    element.setStt(stt);
                    element.setEdt(edt);
                    element.setCurDate(curDate);
                    out.collect(element);
                }
            }
        });

//        reducedStream.print(">>>>");

        //TODO 8 补全维度信息
        //需要用到Flink的异步流处理
        //使用RichAsyncFunction的原因是需要在open/close方法中开启/关闭HBase的异步连接
        //为了逻辑复用，自己实现一个RichAsyncFunction来应用于所有的维度信息补全操作

        //8.1 关联三级行业名称及二级行业ID
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanApprovalBean> C3NameC2IdStream = AsyncDataStream.unorderedWait(reducedStream, new AsyncDimFunctionHBase<DwsAuditIndLeaseOrgSalesmanApprovalBean>() {
            @Override
            public String getTable() {
                return "dim_industry";
            }

            @Override
            public String getId(DwsAuditIndLeaseOrgSalesmanApprovalBean bean) {
                return bean.getIndustry3Id();
            }

            @Override
            public void addDim(DwsAuditIndLeaseOrgSalesmanApprovalBean bean, JSONObject dim) {
                bean.setIndustry3Name(dim.getString("industry_name"));
                bean.setIndustry2Id(dim.getString("superior_industry_id"));
            }
        }, 60L, TimeUnit.SECONDS);

//        C3NameC2IdStream.print("C3NameC2IdStream>>>");

        //8.2 关联二级行业名称及一级行业ID
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanApprovalBean> C2NameC1IdStream = AsyncDataStream.unorderedWait(C3NameC2IdStream, new AsyncDimFunctionHBase<DwsAuditIndLeaseOrgSalesmanApprovalBean>() {
            @Override
            public String getTable() {
                return "dim_industry";
            }

            @Override
            public String getId(DwsAuditIndLeaseOrgSalesmanApprovalBean bean) {
                return bean.getIndustry2Id();
            }

            @Override
            public void addDim(DwsAuditIndLeaseOrgSalesmanApprovalBean bean, JSONObject dim) {
                bean.setIndustry2Name(dim.getString("industry_name"));
                bean.setIndustry1Id(dim.getString("superior_industry_id"));
            }
        }, 60L, TimeUnit.SECONDS);

        // 8.3 关联一级行业名称
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanApprovalBean> C1NameStream = AsyncDataStream.unorderedWait(C2NameC1IdStream, new AsyncDimFunctionHBase<DwsAuditIndLeaseOrgSalesmanApprovalBean>() {
            @Override
            public String getTable() {
                return "dim_industry";
            }

            @Override
            public String getId(DwsAuditIndLeaseOrgSalesmanApprovalBean bean) {
                return bean.getIndustry1Id();
            }

            @Override
            public void addDim(DwsAuditIndLeaseOrgSalesmanApprovalBean bean, JSONObject dim) {
                bean.setIndustry1Name(dim.getString("industry_name"));
            }
        }, 60L, TimeUnit.SECONDS);

        // 8.4 关联业务经办姓名及三级部门id
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanApprovalBean> SalesNameD1IdStream = AsyncDataStream.unorderedWait(C1NameStream, new AsyncDimFunctionHBase<DwsAuditIndLeaseOrgSalesmanApprovalBean>() {
            @Override
            public String getTable() {
                return "dim_employee";
            }

            @Override
            public String getId(DwsAuditIndLeaseOrgSalesmanApprovalBean bean) {
                return bean.getSalesmanId();
            }

            @Override
            public void addDim(DwsAuditIndLeaseOrgSalesmanApprovalBean bean, JSONObject dim) {
                bean.setSalesmanName(dim.getString("name"));
                bean.setDepartment3Id(dim.getString("department_id"));
            }
        }, 60L, TimeUnit.SECONDS);

        // 8.5 关联三级部门名称及二级部门id
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanApprovalBean> D3NameD2IdStream = AsyncDataStream.unorderedWait(SalesNameD1IdStream, new AsyncDimFunctionHBase<DwsAuditIndLeaseOrgSalesmanApprovalBean>() {
            @Override
            public String getTable() {
                return "dim_department";
            }

            @Override
            public String getId(DwsAuditIndLeaseOrgSalesmanApprovalBean bean) {
                return bean.getDepartment3Id();
            }

            @Override
            public void addDim(DwsAuditIndLeaseOrgSalesmanApprovalBean bean, JSONObject dim) {
                bean.setDepartment3Name(dim.getString("department_name"));
                bean.setDepartment2Id(dim.getString("superior_department_id"));
            }
        }, 60L, TimeUnit.SECONDS);

        // 8.6 关联二级部门名称及一级部门id
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanApprovalBean> D2NameD1IdStream = AsyncDataStream.unorderedWait(D3NameD2IdStream, new AsyncDimFunctionHBase<DwsAuditIndLeaseOrgSalesmanApprovalBean>() {
            @Override
            public String getTable() {
                return "dim_department";
            }

            @Override
            public String getId(DwsAuditIndLeaseOrgSalesmanApprovalBean bean) {
                return bean.getDepartment2Id();
            }

            @Override
            public void addDim(DwsAuditIndLeaseOrgSalesmanApprovalBean bean, JSONObject dim) {
                bean.setDepartment2Name(dim.getString("department_name"));
                bean.setDepartment1Id(dim.getString("superior_department_id"));
            }
        }, 60L, TimeUnit.SECONDS);

        // 8.7 关联一级部门名称
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanApprovalBean> D1NameStream = AsyncDataStream.unorderedWait(D2NameD1IdStream, new AsyncDimFunctionHBase<DwsAuditIndLeaseOrgSalesmanApprovalBean>() {
            @Override
            public String getTable() {
                return "dim_department";
            }

            @Override
            public String getId(DwsAuditIndLeaseOrgSalesmanApprovalBean bean) {
                return bean.getDepartment1Id();
            }

            @Override
            public void addDim(DwsAuditIndLeaseOrgSalesmanApprovalBean bean, JSONObject dim) {
                bean.setDepartment1Name(dim.getString("department_name"));
            }
        }, 60L, TimeUnit.SECONDS);

//        D1NameStream.print();
        //TODO 9 将数据写出到Doris
        //现在数据流中数据的格式是DwsAuditIndLeaseOrgSalesmanApprovalBean类型，需要将其转化为JSONString类型，再进行写入
        D1NameStream.map(new MapFunction<DwsAuditIndLeaseOrgSalesmanApprovalBean, String>() {
            @Override
            public String map(DwsAuditIndLeaseOrgSalesmanApprovalBean bean) throws Exception {
                return Bean2JSONUtil.Bean2Json(bean);
            }
        }).sinkTo(DorisUtil.getDorisSink("financial_lease_realtime.dws_audit_industry_lease_organization_salesman_approval_win","dws_audit_industry_lease_organization_salesman_approval_win"));

        //TODO 执行
        env.execute();
    }
}
```

重点说明：

1. `reduce`归约函数：基于windowStream调用`reduce`方法，可以传入两个参数，一个是`ReduceFunction`，用于定义数据之间的聚合逻辑；另一个是`ProcessWindowFunction`（窗口处理函数），用于定义对各个窗口的处理逻辑；在上面的代码中，我们在聚合逻辑中进行了申请金额、批复金额等数值的累加，在窗口处理逻辑中通过`context`获取到了窗口的起止时间，并写入到相应的bean中
2. `AsyncDataStream.unorderedWait`：用于在流处理中执行异步操作，如**查询外部数据库**、调用 REST API 等，从而避免阻塞 Flink 任务的主处理线程
+ 特点：结果乱序，不保证异步操作返回的结果顺序与输入顺序相同，这意味着当某个异步操作耗时较长时，后续较快完成的操作可以先返回结果
+ 参数说明：

![](https://cdn.nlark.com/yuque/0/2024/png/2675852/1728355863856-a01f3304-5666-4e8f-826f-5c994c6a6fea.png)

`in`：输入的数据流（即要进行异步处理的数据流）

`func`：异步处理逻辑，实现`AsyncFunction<IN, OUT>`接口来定义逻辑

`timeout`：超时时间，超过此时间未完成的操作将被终止

`timeUnit`：超时的时间单位，通常可以是秒、毫秒等

3. `RichAsyncFunction`：继承了 `RichFunction`方法，定义具体的异步处理逻辑（这里也可以看出在4.0.4和4.0.5中为什么要定义HBase和Redis的异步连接，就是为了在`RichAsyncFunction`异步处理逻辑中访问对应的数据库）
+ 参数说明：`<IN,OUT>`，分别是输入、输出数据的格式
+ 核心方法：`asyncInvoke(IN input, ResultFuture<OUT> resultFuture)`，**用于发起异步请求**。该方法不会阻塞输入的流处理，它会在后台执行异步操作，操作完成后调用提供的 `resultFuture` 对象将结果返回
+ `timeout()`：处理超时情况
4. `CompletableFuture.supplyAsync`：
+ 基本概念：`CompletableFuture.supplyAsync` 是一个异步方法，它接受一个 `Supplier`，会在默认的 ForkJoinPool 线程池或自定义的线程池中执行这个 `Supplier`，并返回一个 `**<font style="background-color:#FBDE28;">CompletableFuture</font>**` 对象。该对象表示一个未来可能完成的计算或操作  
    - 语法：`CompletableFuture.supplyAsync(Supplier<U> supplier)`
    - `**Supplier<U> supplier**`：提供一个返回值的函数接口。`supplyAsync` 执行的就是这个 `Supplier`，它会异步地返回计算结果  
    - `**CompletableFuture<U>**`：`CompletableFuture` 是一种异步计算的容器，表示某个操作将在未来完成，并可以对其执行各种回调操作  
+ 工作流程：
    - 异步执行：`supplyAsync` 会将 `Supplier` 提交到一个线程池中异步执行，不会阻塞主线程  
    -  返回 `CompletableFuture`：** 立即返回一个 **`**CompletableFuture**`** 对象**。这个对象可以通过后续的操作链，如 `thenApply()`, `thenAccept()`, 或 `thenCompose()` 来处理异步任务完成后的结果或错误  
    - 回调和链式操作： 通过 `thenApply()`、`thenAccept()` 或 `thenCompose()` 等方法，可以在异步任务完成后**立即执行回调操作处理结果**  

### 4.2 审批域行业业务方向业务经办粒度审批取消各窗口汇总表
#### 4.2.1 建表语句
```sql
drop table if exists financial_lease_realtime.dws_audit_industry_lease_organization_salesman_cancel_win;
create table if not exists financial_lease_realtime.dws_audit_industry_lease_organization_salesman_cancel_win
(
    `stt`                DATETIME comment '窗口起始时间',
    `edt`                DATETIME comment '窗口结束时间',
    `cur_date`           DATE comment '当天日期',
    `industry1_id`       INT comment '一级行业ID',
    `industry1_name`     CHAR(100) comment '一级行业名称',
    `industry2_id`       INT comment '二级行业ID',
    `industry2_name`     CHAR(100) comment '二级行业名称',
    `industry3_id`       INT comment '三级行业ID',
    `industry3_name`     CHAR(100) comment '三级行业名称',
    `lease_organization` CHAR(100) comment '业务方向',
    `salesman_id`        CHAR(100) comment '业务经办ID',
    `salesman_name`      CHAR(100) comment '业务经办姓名',
    `department1_id`     INT comment '一级部门ID',
    `department1_name`   CHAR(100) comment '一级部门名称',
    `department2_id`     INT comment '二级部门ID',
    `department2_name`   CHAR(100) comment '二级部门名称',
    `department3_id`     INT comment '三级部门ID',
    `department3_name`   CHAR(100) comment '三级部门名称',
    `apply_count`        BIGINT replace comment '申请项目数',
    `apply_amount`       DECIMAL(16, 2) replace comment '申请金额'
) engine = olap aggregate key (`stt`, `edt`,`cur_date`,`industry1_id`,`industry1_name`,`industry2_id`,`industry2_name`,`industry3_id`,`industry3_name`,`lease_organization`,`salesman_id`,`salesman_name`,`department1_id`,`department1_name`,`department2_id`,`department2_name`,`department3_id`,`department3_name`)
partition by range(`cur_date`)()
distributed by hash(`stt`) buckets 10
properties (
"replication_num" = "3",
"dynamic_partition.enable" = "true",
"dynamic_partition.time_unit" = "DAY",
"dynamic_partition.start" = "-100",
"dynamic_partition.end" = "3",
"dynamic_partition.prefix" = "par",
"dynamic_partition.buckets" = "10",
"dynamic_partition.hot_partition_num" = "1",
"dynamic_partition.create_history_partition" = "true",
"dynamic_partition.history_partition_num" = "100"
);
```

#### 4.2.2 代码实现
代码逻辑与4.1.2完全相同

### 4.3 审批域行业业务方向业务经办粒度审批拒绝各窗口汇总表
#### 4.3.1 建表语句
```sql
drop table if exists financial_lease_realtime.dws_audit_industry_lease_organization_salesman_reject_win;
create table if not exists financial_lease_realtime.dws_audit_industry_lease_organization_salesman_reject_win
(
    `stt`                DATETIME comment '窗口起始时间',
    `edt`                DATETIME comment '窗口结束时间',
    `cur_date`           DATE comment '当天日期',
    `industry1_id`       INT comment '一级行业ID',
    `industry1_name`     CHAR(100) comment '一级行业名称',
    `industry2_id`       INT comment '二级行业ID',
    `industry2_name`     CHAR(100) comment '二级行业名称',
    `industry3_id`       INT comment '三级行业ID',
    `industry3_name`     CHAR(100) comment '三级行业名称',
    `lease_organization` CHAR(100) comment '业务方向',
    `salesman_id`        CHAR(100) comment '业务经办ID',
    `salesman_name`      CHAR(100) comment '业务经办姓名',
    `department1_id`     INT comment '一级部门ID',
    `department1_name`   CHAR(100) comment '一级部门名称',
    `department2_id`     INT comment '二级部门ID',
    `department2_name`   CHAR(100) comment '二级部门名称',
    `department3_id`     INT comment '三级部门ID',
    `department3_name`   CHAR(100) comment '三级部门名称',
    `apply_count`        BIGINT replace comment '申请项目数',
    `apply_amount`       DECIMAL(16, 2) replace comment '申请金额'
) engine = olap aggregate key (`stt`, `edt`,`cur_date`,`industry1_id`,`industry1_name`,`industry2_id`,`industry2_name`,`industry3_id`,`industry3_name`,`lease_organization`,`salesman_id`,`salesman_name`,`department1_id`,`department1_name`,`department2_id`,`department2_name`,`department3_id`,`department3_name`)
partition by range(`cur_date`)()
distributed by hash(`stt`) buckets 10
properties (
"replication_num" = "3",
"dynamic_partition.enable" = "true",
"dynamic_partition.time_unit" = "DAY",
"dynamic_partition.start" = "-100",
"dynamic_partition.end" = "3",
"dynamic_partition.prefix" = "par",
"dynamic_partition.buckets" = "10",
"dynamic_partition.hot_partition_num" = "1",
"dynamic_partition.create_history_partition" = "true",
"dynamic_partition.history_partition_num" = "100"
);
```

#### 4.3.2 代码实现
逻辑与4.1.2完全相同

### 4.4 审批域信审经办粒度审批通过各窗口汇总表
#### 4.4.1 建表语句
```sql
drop table if exists financial_lease_realtime.dws_audit_audit_man_approval_win;
create table if not exists financial_lease_realtime.dws_audit_audit_man_approval_win
(
    `stt`            DATETIME comment '窗口起始时间',
    `edt`            DATETIME comment '窗口结束时间',
    `cur_date`       DATE comment '当天日期',
    `audit_man_id`   CHAR(100) comment '信审经办ID',
    `audit_man_name` CHAR(100) comment '信审经办姓名',
    `apply_count`    BIGINT replace comment '申请项目数',
    `apply_amount`   DECIMAL(16, 2) replace comment '申请金额',
    `reply_amount`   DECIMAL(16, 2) replace comment '批复金额'
) engine = olap aggregate key (`stt`, `edt`,`cur_date`,`audit_man_id`,`audit_man_name`)
partition by range(`cur_date`)()
distributed by hash(`stt`) buckets 10
properties (
"replication_num" = "3",
"dynamic_partition.enable" = "true",
"dynamic_partition.time_unit" = "DAY",
"dynamic_partition.start" = "-100",
"dynamic_partition.end" = "3",
"dynamic_partition.prefix" = "par",
"dynamic_partition.buckets" = "10",
"dynamic_partition.hot_partition_num" = "1",
"dynamic_partition.create_history_partition" = "true",
"dynamic_partition.history_partition_num" = "100"
);
```

#### 4.4.2 代码实现
逻辑与4.1.1基本一致，区别在于维度信息

### 4.5 审批域信审经办粒度审批取消各窗口汇总表
#### 4.5.1 建表语句
```sql
drop table if exists financial_lease_realtime.dws_audit_audit_man_cancel_win;
create table if not exists financial_lease_realtime.dws_audit_audit_man_cancel_win
(
    `stt`            DATETIME comment '窗口起始时间',
    `edt`            DATETIME comment '窗口结束时间',
    `cur_date`       DATE comment '当天日期',
    `audit_man_id`   CHAR(100) comment '信审经办ID',
    `audit_man_name` CHAR(100) comment '信审经办姓名',
    `apply_count`    BIGINT replace comment '申请项目数',
    `apply_amount`   DECIMAL(16, 2) replace comment '申请金额'
) engine = olap aggregate key (`stt`, `edt`,`cur_date`,`audit_man_id`,`audit_man_name`)
partition by range(`cur_date`)()
distributed by hash(`stt`) buckets 10
properties (
"replication_num" = "3",
"dynamic_partition.enable" = "true",
"dynamic_partition.time_unit" = "DAY",
"dynamic_partition.start" = "-100",
"dynamic_partition.end" = "3",
"dynamic_partition.prefix" = "par",
"dynamic_partition.buckets" = "10",
"dynamic_partition.hot_partition_num" = "1",
"dynamic_partition.create_history_partition" = "true",
"dynamic_partition.history_partition_num" = "100"
);
```

#### 4.5.2 代码实现
逻辑与4.4.2一致

### 4.6 审批域信审经办粒度审批拒绝各窗口汇总表
#### 4.6.1 建表语句
```sql
drop table if exists financial_lease_realtime.dws_audit_audit_man_reject_win;
create table if not exists financial_lease_realtime.dws_audit_audit_man_reject_win
(
    `stt`            DATETIME comment '窗口起始时间',
    `edt`            DATETIME comment '窗口结束时间',
    `cur_date`       DATE comment '当天日期',
    `audit_man_id`   CHAR(100) comment '信审经办ID',
    `audit_man_name` CHAR(100) comment '信审经办姓名',
    `apply_count`    BIGINT replace comment '申请项目数',
    `apply_amount`   DECIMAL(16, 2) replace comment '申请金额'
) engine = olap aggregate key (`stt`, `edt`,`cur_date`,`audit_man_id`,`audit_man_name`)
partition by range(`cur_date`)()
distributed by hash(`stt`) buckets 10
properties (
"replication_num" = "3",
"dynamic_partition.enable" = "true",
"dynamic_partition.time_unit" = "DAY",
"dynamic_partition.start" = "-100",
"dynamic_partition.end" = "3",
"dynamic_partition.prefix" = "par",
"dynamic_partition.buckets" = "10",
"dynamic_partition.hot_partition_num" = "1",
"dynamic_partition.create_history_partition" = "true",
"dynamic_partition.history_partition_num" = "100"
);
```

#### 4.6.2 代码实现
逻辑与4.4.2一致

### 4.7 授信域新增授信各窗口汇总表
#### 4.7.1 建表语句
```sql
drop table if exists financial_lease_realtime.dws_credit_credit_add_win;
create table if not exists financial_lease_realtime.dws_credit_credit_add_win
(
    `stt`           DATETIME comment '窗口起始时间',
    `edt`           DATETIME comment '窗口结束时间',
    `cur_date`      DATE comment '当天日期',
    `apply_count`   BIGINT replace comment '申请项目数',
    `apply_amount`  DECIMAL(16, 2) replace comment '申请金额',
    `reply_amount`  DECIMAL(16, 2) replace comment '批复金额',
    `credit_amount` DECIMAL(16, 2) replace comment '授信金额'
) engine = olap aggregate key (`stt`, `edt`,`cur_date`)
partition by range(`cur_date`)()
distributed by hash(`stt`) buckets 10
properties (
"replication_num" = "3",
"dynamic_partition.enable" = "true",
"dynamic_partition.time_unit" = "DAY",
"dynamic_partition.start" = "-100",
"dynamic_partition.end" = "3",
"dynamic_partition.prefix" = "par",
"dynamic_partition.buckets" = "10",
"dynamic_partition.hot_partition_num" = "1",
"dynamic_partition.create_history_partition" = "true",
"dynamic_partition.history_partition_num" = "100"
);
```

#### 4.7.2 代码实现
逻辑与4.1.2基本一致，不过不需要维度关联操作

### 4.8 授信域完成授信占用各窗口汇总表
#### 4.8.1 建表语句
```sql
drop table if exists financial_lease_realtime.dws_credit_credit_occupy_win;
create table if not exists financial_lease_realtime.dws_credit_credit_occupy_win
(
    `stt`           DATETIME comment '窗口起始时间',
    `edt`           DATETIME comment '窗口结束时间',
    `cur_date`      DATE comment '当天日期',
    `apply_count`   BIGINT replace comment '申请项目数',
    `apply_amount`  DECIMAL(16, 2) replace comment '申请金额',
    `reply_amount`  DECIMAL(16, 2) replace comment '批复金额',
    `credit_amount` DECIMAL(16, 2) replace comment '授信金额'
) engine = olap aggregate key (`stt`, `edt`,`cur_date`)
partition by range(`cur_date`)()
distributed by hash(`stt`) buckets 10
properties (
"replication_num" = "3",
"dynamic_partition.enable" = "true",
"dynamic_partition.time_unit" = "DAY",
"dynamic_partition.start" = "-100",
"dynamic_partition.end" = "3",
"dynamic_partition.prefix" = "par",
"dynamic_partition.buckets" = "10",
"dynamic_partition.hot_partition_num" = "1",
"dynamic_partition.create_history_partition" = "true",
"dynamic_partition.history_partition_num" = "100"
);
```

#### 4.8.2 代码实现
逻辑与4.7.2完全一致

### 4.9 租赁域完成合同制作各窗口汇总表
#### 4.9.1 建表语句
```sql
drop table if exists financial_lease_realtime.dws_lease_contract_produce_win;
create table if not exists financial_lease_realtime.dws_lease_contract_produce_win
(
    `stt`           DATETIME comment '窗口起始时间',
    `edt`           DATETIME comment '窗口结束时间',
    `cur_date`      DATE comment '当天日期',
    `apply_count`   BIGINT replace comment '申请项目数',
    `apply_amount`  DECIMAL(16, 2) replace comment '申请金额',
    `reply_amount`  DECIMAL(16, 2) replace comment '批复金额',
    `credit_amount` DECIMAL(16, 2) replace comment '授信金额'
) engine = olap aggregate key (`stt`, `edt`,`cur_date`)
partition by range(`cur_date`)()
distributed by hash(`stt`) buckets 10
properties (
"replication_num" = "3",
"dynamic_partition.enable" = "true",
"dynamic_partition.time_unit" = "DAY",
"dynamic_partition.start" = "-100",
"dynamic_partition.end" = "3",
"dynamic_partition.prefix" = "par",
"dynamic_partition.buckets" = "10",
"dynamic_partition.hot_partition_num" = "1",
"dynamic_partition.create_history_partition" = "true",
"dynamic_partition.history_partition_num" = "100"
);
```

#### 4.9.2 代码实现
代码逻辑与4.7.2完全一致

### 4.10 租赁域签约各窗口汇总表
#### 4.10.1 建表语句
```sql
drop table if exists financial_lease_realtime.dws_lease_sign_win;
create table if not exists financial_lease_realtime.dws_lease_sign_win
(
    `stt`           DATETIME comment '窗口起始时间',
    `edt`           DATETIME comment '窗口结束时间',
    `cur_date`      DATE comment '当天日期',
    `apply_count`   BIGINT replace comment '申请项目数',
    `apply_amount`  DECIMAL(16, 2) replace comment '申请金额',
    `reply_amount`  DECIMAL(16, 2) replace comment '批复金额',
    `credit_amount` DECIMAL(16, 2) replace comment '授信金额'
) engine = olap aggregate key (`stt`, `edt`,`cur_date`)
partition by range(`cur_date`)()
distributed by hash(`stt`) buckets 10
properties (
"replication_num" = "3",
"dynamic_partition.enable" = "true",
"dynamic_partition.time_unit" = "DAY",
"dynamic_partition.start" = "-100",
"dynamic_partition.end" = "3",
"dynamic_partition.prefix" = "par",
"dynamic_partition.buckets" = "10",
"dynamic_partition.hot_partition_num" = "1",
"dynamic_partition.create_history_partition" = "true",
"dynamic_partition.history_partition_num" = "100"
);
```

#### 4.10.2 代码实现
逻辑与4.7.2完全一致

### 4.11 租赁域起租各窗口汇总表
#### 4.11.1 建表语句
```sql
drop table if exists financial_lease_realtime.dws_lease_execution_win;
create table if not exists financial_lease_realtime.dws_lease_execution_win
(
    `stt`           DATETIME comment '窗口起始时间',
    `edt`           DATETIME comment '窗口结束时间',
    `cur_date`      DATE comment '当天日期',
    `apply_count`   BIGINT replace comment '申请项目数',
    `apply_amount`  DECIMAL(16, 2) replace comment '申请金额',
    `reply_amount`  DECIMAL(16, 2) replace comment '批复金额',
    `credit_amount` DECIMAL(16, 2) replace comment '授信金额'
) engine = olap aggregate key (`stt`, `edt`,`cur_date`)
partition by range(`cur_date`)()
distributed by hash(`stt`) buckets 10
properties (
"replication_num" = "3",
"dynamic_partition.enable" = "true",
"dynamic_partition.time_unit" = "DAY",
"dynamic_partition.start" = "-100",
"dynamic_partition.end" = "3",
"dynamic_partition.prefix" = "par",
"dynamic_partition.buckets" = "10",
"dynamic_partition.hot_partition_num" = "1",
"dynamic_partition.create_history_partition" = "true",
"dynamic_partition.history_partition_num" = "100"
);
```

#### 4.11.2 代码实现
代码逻辑与4.7.2完全一致

## 5.ADS层
ADS层的内容和离线数仓一致，分为已审和待审两种情况，分各行业、各部门、各业务方向进行汇总



