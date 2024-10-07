package com.why.util;

import com.alibaba.fastjson.JSONObject;
import com.why.common.FinancialLeaseCommon;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
/**
 * Created by WHY on 2024/9/8.
 * Functions: redis工具类
 */
public class RedisUtil {
    private static final JedisPool pool;

    static {

        //配置信息
        GenericObjectPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(300);  // 最多提供 300 个连接
        config.setMaxIdle(10);  // 最多允许 10 个空闲连接
        config.setMinIdle(2); // 最少允许 2 个空闲连接
        config.setMaxWaitMillis(10 * 1000);  // 获取连接最多等到时间
        config.setTestOnCreate(true);
        config.setTestOnBorrow(true);
        config.setTestOnReturn(true);

        pool = new JedisPool(config, FinancialLeaseCommon.REDIS_HOST, FinancialLeaseCommon.REDIS_PORT);
    }

    /**
     * 创建redis客户端
     * @return
     */
    public static Jedis getRedisClient() {
        // 1. 直接 new
        // 2. 使用连接池
        Jedis jedis = pool.getResource();
        jedis.select(4);  // 选择单独的库
        return jedis;
    }

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
}
