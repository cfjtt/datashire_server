package com.eurlanda.datashire.utility.objectsql;

import java.lang.reflect.Array;
import java.util.Collection;

/**
 * sql 查询条件
 * @date 2014-1-10
 * @author jiwei.zhang
 */
public class InCondation extends SimpleCondation{
	
	@Override
	public String generateConditionSql() {
		String in ="";
		if(value instanceof java.util.Collection){
			Collection valueList = (Collection) value;
			for(Object val:valueList){
				in+=ObjectSQL.convertVal(val)+",";
			}
		}else if(value.getClass().isArray()){
			for(int i=0;i<Array.getLength(value);i++){
				Object val = Array.get(value, i);
				in+=ObjectSQL.convertVal(val)+",";
			}
		}else{
			in=ObjectSQL.convertVal(value);
		}
		in= in.replaceAll(",$", "");
		return fieldName+" in ("+in+")";
	}
}
