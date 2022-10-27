package com.eurlanda.datashire.server.annotation;

import com.eurlanda.datashire.socket.Agreement;
import com.eurlanda.datashire.socket.protocol.ServiceConf;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by zhudebin on 2017/6/7.
 */
public class CommandProcessor implements BeanPostProcessor {

    // 新版API的前缀
    private final static String NEW_VERSION_PREFIX = "2";

    @Override public Object postProcessBeforeInitialization(Object bean, String beanName)
            throws BeansException {
        return bean;
    }

    @Override public Object postProcessAfterInitialization(Object bean, String beanName)
            throws BeansException {
        Class cls = bean.getClass();
        Annotation annotation = cls.getAnnotation(SocketApi.class);
        if (annotation != null) {
            SocketApi sa = (SocketApi) annotation;
            String apiId = sa.value().trim();

            if (!apiId.startsWith(NEW_VERSION_PREFIX)) {
                throw new CommandProcessorException("新版接口命令号必须以2开头,异常类:" + cls.getCanonicalName());
            }
            // 判断是否已经存在该命令号
            if (Agreement.AGREEMENT_CLASS.containsKey(apiId)) {
                throw new CommandProcessorException("已经存在该命令号:" + apiId);
            }
            // 用的类全路径名称
            Agreement.AGREEMENT_CLASS.put(apiId, cls.getCanonicalName());

            Method[] methods = cls.getDeclaredMethods();
            Set<String> methodIdSet = new HashSet<>();
            for (Method m : methods) {
                SocketApiMethod sam = m.getAnnotation(SocketApiMethod.class);
                if (sam != null) {
                    String methodId = sam.commandId().trim();
                    boolean sendResponse = sam.sendResponse();
                    // 检验命令号是否重复
                    if (methodIdSet.contains(methodId)) {
                        throw new CommandProcessorException(cls.getCanonicalName()
                                + " 中存在重复的命令号方法 " + m.getName());
                    }
                    // 获取参数类型
                    Class[] parameterTypes = m.getParameterTypes();
                    Class argType;
                    if (parameterTypes.length > 0) {
                        if (parameterTypes.length > 1) {
                            throw new CommandProcessorException("api接口方法只能有一个参数");
                        }
                        argType = parameterTypes[0];
                    } else {
                        argType = Void.TYPE;
                    }

                    // 获取是否有返回值
                    Type returnType = m.getGenericReturnType();
                    if (returnType == Void.TYPE && sendResponse) {
                        throw new CommandProcessorException("接口方法"
                                + cls.getCanonicalName() + "." + m.getName()
                                + "()没有返回值,需要将@SocketApiMethod中sendResponse设置为false");
                    }

                    // 判断是否已经存在该命令号
                    if (Agreement.ServiceConfMap.containsKey(apiId + methodId)) {
                        throw new CommandProcessorException("已经存在该命令号:" + apiId + methodId);
                    }
                    Agreement.ServiceConfMap.put(apiId + methodId,
                            new ServiceConf(m.getName(), argType, null, sendResponse));
                    methodIdSet.add(methodId);
                }
            }
        }
        return bean;
    }
}
