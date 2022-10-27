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
public class UpdateSQL extends ObjectSQL {
	private Map<String, Object> modifyFields = new HashMap<String, Object>();

	/**
	 * 查询指定的表
	 * 
	 * @param tableName
	 *            表名
	 */
	public UpdateSQL(String tableName) {
		super(tableName);
	}

	public Map<String, Object> getModifyFields() {
		return modifyFields;
	}

	/**
	 * 设置需要修改的列。
	 * 
	 * @date 2014-1-11
	 * @author jiwei.zhang
	 * @param modifyFields
	 */
	public void setModifyFields(Map<String, Object> modifyFields) {
		this.modifyFields = modifyFields;
	}

	/**
	 * 添加一个待修改的列。
	 * 
	 * @date 2014-1-11
	 * @author jiwei.zhang
	 * @param fieldName
	 *            列名
	 * @param value
	 *            值
	 */
	public void addModifyField(String fieldName, Object value) {
		if (fieldName != null) {
			this.modifyFields.put(fieldName, value);
		}
	}

	@Override
	public String generateSQL() {
		List<String> modifyList = new ArrayList<String>();
		String sql = "update " + super.tableName + " set " ;
		for(String field : this.modifyFields.keySet()){
			Object val = this.modifyFields.get(field);
			modifyList.add(field+"="+convertVal(val));
		}	
		sql+=CollectionUtils.join(modifyList) + " ";
		sql+= super.generateConditionSQL();
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
	public static UpdateSQL fromEntity(Class entity) {
		return null;
	}
}
