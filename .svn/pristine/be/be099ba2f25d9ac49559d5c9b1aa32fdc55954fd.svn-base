package com.eurlanda.datashire.utility.objectsql;

import cn.com.jsoft.jframe.utils.CollectionUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * sql修改
 * 
 * @date 2014-1-11
 * @author jiwei.zhang
 * 
 */
public class InsertSQL extends ObjectSQL {
	private Map<String, Object> insertFields = new HashMap<String, Object>();

	/**
	 * 查询指定的表
	 * 
	 * @param tableName
	 *            表名
	 */
	public InsertSQL(String tableName) {
		super(tableName);
	}

	public Map<String, Object> getInsertFields() {
		return insertFields;
	}

	public void setInsertFields(Map<String, Object> insertFields) {
		this.insertFields = insertFields;
	}

	/**
	 * 添加一个待插入的列。
	 * 
	 * @date 2014-1-11
	 * @author jiwei.zhang
	 * @param fieldName
	 *            列名
	 * @param value
	 *            值
	 */
	public void addField(String fieldName, Object value) {
		if (fieldName != null) {
			this.insertFields.put(fieldName, value);
		}
	}

	@Override
	public String generateSQL() {
		List<String> fieldList = new ArrayList<String>();
		List<String> valueList = new ArrayList<String>();
		for(String field : this.insertFields.keySet()){
			fieldList.add(field);
			valueList.add(convertVal(this.insertFields.get(field)));
		}
		String sql = "insert into "  + super.tableName + " (" + CollectionUtils.join(fieldList)+")";
		sql+=" values ("+CollectionUtils.join(valueList)+") ";
		sql+=super.generateConditionSQL();
		return sql;
	}

	/**
	 * 使用JPA实体类自动构造select. 暂时不受支持
	 * 
	 * @date 2014-1-11
	 * @author jiwei.zhang
	 * @param entity
	 * @return
	 */
	public static InsertSQL fromEntity(Class entity) {
		return null;
	}
}
