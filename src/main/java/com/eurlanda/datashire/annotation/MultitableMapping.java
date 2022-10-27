/*
 * MultitableMapping.java - 2013-11-13
 *
 * 版权声明: (c) 2013，悦岚（上海）数据服务有限公司，保留所有权利。
 *
 * 项目名称: datashire-server
 * 
 * 修改历史:
 * ===========================================
 *   修改人	     日期		     描述
 *   ---------------------------------------
 *   dang.lu  2013-11-13   create    
 * ===========================================
 */
package com.eurlanda.datashire.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited

/** ORMapping 之 table name <-> class name */
public @interface MultitableMapping {
	/** 表名，支持一个实体类对应多张表情况(主表在前，从表在后，添加更新多张表一起维护，查询、删除只操作从表) */
	String[] name();
	/** 主键，支持复合主键 */
	String[] pk();
	/** 描述 */
	String desc();
}
