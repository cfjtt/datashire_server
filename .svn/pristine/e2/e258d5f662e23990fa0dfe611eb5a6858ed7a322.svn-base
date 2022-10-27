package com.eurlanda.datashire.utility.objectsql;

/**
 * sql 查询条件
 * @date 2014-1-10
 * @author jiwei.zhang
 */
public class LikeCondation extends SimpleCondation{
	EMatchRange matchRange;
	
	@Override
	public String generateConditionSql() {
		switch (matchRange) {
		case RIGHT:
			return fieldName +" like '"+this.value+"%'";
		case LEFT:
			return fieldName +" like '%"+this.value+"'";
		default:
			return fieldName +" like '%"+this.value+"%'";
		}
	}
}
