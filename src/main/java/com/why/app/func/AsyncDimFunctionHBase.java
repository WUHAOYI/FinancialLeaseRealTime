package com.why.app.func;

import com.alibaba.fastjson.JSONObject;
import com.esotericsoftware.minlog.Log;
import com.why.common.FinancialLeaseCommon;
import com.why.util.HBaseUtil;
import com.why.util.RedisUtil;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.client.AsyncConnection;


import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Created by WHY on 2024/9/7.
 * Functions:
 */
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
