package com.why.app.func;

import com.alibaba.fastjson.JSONObject;
import com.why.bean.TableProcess;
import com.why.common.FinancialLeaseCommon;
import com.why.util.HBaseUtil;
import com.why.util.RedisUtil;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;

/**
 * Created by WHY on 2024/9/5.
 * Functions:
 */
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
