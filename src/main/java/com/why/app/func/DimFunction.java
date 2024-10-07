package com.why.app.func;

import com.alibaba.fastjson.JSONObject;

/**
 * 该接口在 AsyncDimFunctionHBase 和主程序中都有实现，主程序中根据数据流的信息提取出rowKey并传入表名，
 * 从而能在AsyncDimFunctionHBase获取相关信息，从而读取HBase中的维度表数据
 * @param <T>
 */
public interface DimFunction<T> {

    String getTable(); //获取表名

    String getId(T bean); //获取rowKey

    void addDim(T bean, JSONObject dim); //补充维度表信息
}
