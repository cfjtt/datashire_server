package com.eurlanda.datashire.utility.objectsql;


/**
 * sql between
 * 
 * @date 2014-1-10
 * @author jiwei.zhang
 */
public class BetweenCondation extends SimpleCondation {
	Object leftValue;
	Object rightValue;
	
	@Override
	public String generateConditionSql() {
		String ret="("+super.fieldName +" between "+ObjectSQL.convertVal(leftValue)+" and "+ ObjectSQL.convertVal(rightValue)+")";
		return ret;
	}

	@Override
	public String toString() {
		return this.generateConditionSql();
	}
}
