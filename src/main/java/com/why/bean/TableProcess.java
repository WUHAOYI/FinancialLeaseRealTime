package com.why.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by WHY on 2024/9/5.
 * Functions: 实体类，对应HBase中的dim表
 */

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TableProcess {
    // 数据源表(mysql中的表)
    String sourceTable;

    // 操作类型(增删改查)
    String operateType;

    // 目标表
    String sinkTable;

    // 列族
    String sinkFamily;

    // 字段
    String sinkColumns;

    // HBASE 建表 rowKey
    String sinkRowKey;
}
