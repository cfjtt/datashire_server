package com.eurlanda.datashire.utility.objectsql;

/**
 * sql 查询条件
 * @date 2014-1-10
 * @author jiwei.zhang
 */
public abstract class SQLCondition {
	String operator;		//运算符
	String fieldName;
	Object value;
	
	public static SQLCondition eq(String fieldName,Object value){
		SimpleCondation condation = new SimpleCondation();
		condation.operator="=";
		condation.fieldName=fieldName;
		condation.value=value;
		return condation;
	}
	/**
	 * >
	 * @date 2014-1-10
	 * @author jiwei.zhang
	 * @param fieldName
	 * @param value
	 * @return
	 */
	public static SQLCondition lt(String fieldName,Object value){
		SimpleCondation condation = new SimpleCondation();
		condation.operator="<";
		condation.fieldName=fieldName;
		condation.value=value;
		return condation;
	}
	/**
	 * >=
	 * @date 2014-1-10
	 * @author jiwei.zhang
	 * @param fieldName
	 * @param value
	 * @return
	 */
	public static SQLCondition lte(String fieldName,Object value){
		SimpleCondation condation = new SimpleCondation();
		condation.operator="<=";
		condation.fieldName=fieldName;
		condation.value=value;
		return condation;
	}
	/**
	 * <
	 * @date 2014-1-10
	 * @author jiwei.zhang
	 * @param fieldName
	 * @param value
	 * @return
	 */
	public static SQLCondition gt(String fieldName,Object value){
		SimpleCondation condation = new SimpleCondation();
		condation.operator=">";
		condation.fieldName=fieldName;
		condation.value=value;
		return condation;
	}
	/**
	 * <=
	 * @date 2014-1-10
	 * @author jiwei.zhang
	 * @param fieldName
	 * @param value
	 * @return
	 */
	public static SQLCondition gte(String fieldName,Object value){
		SimpleCondation condation = new SimpleCondation();
		condation.operator=">=";
		condation.fieldName=fieldName;
		condation.value=value;
		return condation;
	}
	/**
	 * like
	 * @date 2014-1-10
	 * @author jiwei.zhang
	 * @param fieldName
	 * @param value
	 * @return
	 */
	public static SQLCondition like(String fieldName,Object value,EMatchRange range){
		LikeCondation condation = new LikeCondation();
		condation.operator="like";
		condation.matchRange=range;
		condation.fieldName=fieldName;
		condation.value=value;
		return condation;
	}
	/**
	 * 全匹配
	 * @date 2014-1-11
	 * @author jiwei.zhang
	 * @param fieldName
	 * @param value
	 * @return
	 */
	public static SQLCondition like(String fieldName,Object value){
		return like(fieldName,value,EMatchRange.ALL);
	}
	public static SQLCondition and(SQLCondition left,SQLCondition right){
		LogicCondation condation = new LogicCondation();
		condation.operator="and";
		condation.left=left;
		condation.right=right;
		return condation;
	}
	public static SQLCondition or(SQLCondition left,SQLCondition right){
		LogicCondation condation = new LogicCondation();
		condation.operator="or";
		condation.left=left;
		condation.right=right;
		return condation;
	}
	public static SQLCondition in(String fieldName,Object value){
		InCondation condation = new InCondation();
		condation.fieldName=fieldName;
		condation.operator="in";
		condation.value = value;
		return condation;
	}
	public static SQLCondition between(String fieldName,Object leftValue,Object rightValue){
		BetweenCondation condation = new BetweenCondation();
		condation.fieldName=fieldName;
		condation.leftValue=leftValue;
		condation.rightValue=rightValue;
		return condation;
	}
	/**
	 * 生成查询条件的sql
	 * @date 2014-1-11
	 * @author jiwei.zhang
	 * @return
	 */
	abstract public String generateConditionSql();

}
