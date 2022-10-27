package com.eurlanda.datashire.utility.objectsql;

/**
 * sql 查询条件
 * @date 2014-1-10
 * @author jiwei.zhang
 */
public class LogicCondation extends SQLCondition{
	SQLCondition left;
	SQLCondition right;
	
	@Override
	public String generateConditionSql() {
		String ret="(";
		ret+=left.generateConditionSql()+" "+ this.operator+" "+right.generateConditionSql()+")";
		return ret;
	}

	@Override
	public String toString() {
		return this.generateConditionSql();
	}

}
