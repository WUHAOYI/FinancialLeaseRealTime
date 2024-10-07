package com.why.util;

import com.why.common.FinancialLeaseCommon;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.SimpleStringSerializer;

import java.util.Properties;
/**
 * Created by WHY on 2024/9/8.
 * Functions: 自定义 Doris Sink ，添加参数设置
 */
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
