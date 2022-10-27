package com.eurlanda.datashire.common.webService;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 在属性上使用次注解。在替换量时使用此属性的propertyName来检测
**/
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Documented
public @interface StartController {
	/**annotate
	 * 这个值可以确定是否允许这个字段被修改
	 */
	public StartTypeEnum valid() default StartTypeEnum.valid;
	/**
	 * 通过这个值检测。属性名称。
	 */
	public String propertyName();
}
