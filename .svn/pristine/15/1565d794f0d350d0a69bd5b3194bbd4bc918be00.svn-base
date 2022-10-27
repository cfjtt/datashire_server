package com.eurlanda.datashire.utility.objectsql;

/**
 * sql 查询条件
 * @date 2014-1-10
 * @author jiwei.zhang
 */
public class SimpleCondation extends SQLCondition{
	
	@Override
	public String generateConditionSql() {
		String sql =fieldName+super.operator;
		sql+=ObjectSQL.convertVal(value);
		return sql;
	}
}
