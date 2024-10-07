package com.why.bean;


import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.FIELD) //定义了注解的应用范围 这里表示可以应用在成员变量上
@Retention(RetentionPolicy.RUNTIME) //定义了注解的生命周期 运行时保留
public @interface TransientSink {
}
