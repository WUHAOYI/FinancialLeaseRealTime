package com.why.util;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.why.common.FinancialLeaseCommon;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * Created by WHY on 2024/9/5.
 * Functions: mysql工具类
 */
public class MySQLUtil {
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

    /**
     * 创建MySQL数据库连接
     * @return
     */
    public static Connection getConnection()
    {
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(
                    FinancialLeaseCommon.MYSQL_URL,
                    FinancialLeaseCommon.MYSQL_USERNAME,
                    FinancialLeaseCommon.MYSQL_PASSWD
            );
        }catch (Exception e)
        {
            e.printStackTrace();
        }
        return connection;
    }

}
