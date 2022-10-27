package com.eurlanda.datashire.server.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 第一阶段:用来区分新老接口
 * 第二阶段:通过该注解来绑定命令号
 * Created by zhudebin on 16/1/14.
 */
@Target({ ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface SocketApiMethod {

    /**
     * 命令号
     *
     * @return
     */
    String commandId();

    boolean sendResponse() default true;
}
