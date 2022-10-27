package com.eurlanda.datashire.server.utils.dbsource.datatype;

import com.eurlanda.datashire.enumeration.DataBaseType;

/**
 * 类型映射 。
 * @author Gene
 *
 */
public class TypeMapping {
	private DataBaseType dbType;		// 数据库名称。
	private String sysDataType;			// 在系统中的数据类型。
	private String javaType;		// 使用java类型去接受数据库的数据。
	private String dbDataType;			// 数据库中的数据类型
	private String finalJavaType;		// 最终转换为参与运算的java类型。
	private boolean isInMapping=false;		// 是否是inMapping，否则是outMapping
	public String getJavaType() {
		return javaType;
	}
	public void setJavaType(String javaType) {
		this.javaType = javaType;
	}
	public DataBaseType getDbType() {
		return dbType;
	}
	public void setDbType(DataBaseType dbType) {
		this.dbType = dbType;
	}
	public String getSysDataType() {
		return sysDataType;
	}
	public void setSysDataType(String sysDataType) {
		this.sysDataType = sysDataType;
	}
	public String getDbDataType() {
		return dbDataType;
	}
	public void setDbDataType(String dbDataType) {
		this.dbDataType = dbDataType;
	}
	public String getFinalJavaType() {
		return finalJavaType;
	}
	public void setFinalJavaType(String finalJavaType) {
		this.finalJavaType = finalJavaType;
	}
	public boolean isInMapping() {
		return isInMapping;
	}
	public void setInMapping(boolean isInMapping) {
		this.isInMapping = isInMapping;
	}
	
	
}
