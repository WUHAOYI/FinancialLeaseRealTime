package com.why.util;

import com.alibaba.fastjson.JSONObject;
import com.why.common.FinancialLeaseCommon;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Created by WHY on 2024/9/5.
 * Functions:
 */
public class HBaseUtil {
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

}
