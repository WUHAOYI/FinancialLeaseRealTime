package com.why.util;

import com.alibaba.fastjson.JSONObject;
import com.why.bean.TransientSink;

import java.lang.reflect.Field;
import java.util.Objects;

/**
 * Created by WHY on 2024/9/8.
 * Functions: 将bean类型的数据转化为JSONString
 */
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
