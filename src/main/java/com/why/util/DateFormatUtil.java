package com.why.util;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
/**
 * Created by WHY on 2024/9/7.
 * Functions:
 */
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
