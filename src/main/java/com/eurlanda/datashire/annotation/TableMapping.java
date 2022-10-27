/*
 * TableMapping.java - 2013-10-2
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

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited

/** ORMapping 之 table name <-> class name */
public @interface TableMapping {
	/** 表名 */
	String name_old();
	/** 描述 */
	String desc_old();
}
