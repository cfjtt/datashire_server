/*
 * ColumnMpping.java - 2013-10-2
 *
 * 版权声明: (c) 2013，悦岚（上海）数据服务有限公司，保留所有权利。
 *
 * 项目名称: datashire-server
 * 
 * 修改历史:
 * ===========================================
 *   修改人	     日期		     描述
 *   ---------------------------------------
 *   dang.lu  2013-10-2   create    
 * ===========================================
 */
package com.eurlanda.datashire.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

//定义注解的作用目标
//@Target(ElementType.TYPE)   //接口、类、枚举、注解
@Target(ElementType.FIELD) //字段、枚举的常量
//@Target(ElementType.METHOD) //方法
//@Target(ElementType.PARAMETER) //方法参数
//@Target(ElementType.CONSTRUCTOR)  //构造函数
//@Target(ElementType.LOCAL_VARIABLE)//局部变量
//@Target(ElementType.ANNOTATION_TYPE)//注解
//@Target(ElementType.PACKAGE) ///包   

//@Retention(RetentionPolicy.SOURCE)   //注解仅存在于源码中，在class字节码文件中不包含
//@Retention(RetentionPolicy.CLASS)     // 默认的保留策略，注解会在class字节码文件中存在，但运行时无法获得，
@Retention(RetentionPolicy.RUNTIME)  // 注解会在class字节码文件中存在，在运行时可以通过反射获取到

@Documented //说明该注解将被包含在javadoc中
@Inherited //说明子类可以继承父类中的该注解

/** ORMapping 之 column <-> field */
public @interface ColumnMpping {
	
	/** 字段名 */
	String name(); // 支持列别名映射到对象属性
	
	/** 字段描述 */
	String desc();
	
	///** 字段类型名 */
	//String typeName();
	
	/** 字段类型(SQL type from java.sql.Types) */
	int type(); //支持列类型转换 Timestamp->String; char(1)->boolean [true:'Y', false:'N']

	/** 精度(添加/更新时超出精度抛出异常) */
	int precision();
	
	/** 是否可以为空（如果是false且值为空：添加/更新时抛出异常，查询时记录warn日志） */
	boolean nullable();
	
	/** 数据校验正则 */
	String valueReg();
	
}
