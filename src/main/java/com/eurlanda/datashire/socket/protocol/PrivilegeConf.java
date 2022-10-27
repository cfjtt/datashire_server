package com.eurlanda.datashire.socket.protocol;

import com.eurlanda.datashire.enumeration.DMLType;
import com.eurlanda.datashire.enumeration.DSObjectType;
/**
 * 权限管理类
 * @author eurlanda01
 *
 */
public class PrivilegeConf {

	private DSObjectType objectType; // 对象类型(仅限于 DSObjectType.Entity_Type_ID )
	private DMLType dmlType; // 操作类型（增删查改）

	public DSObjectType getObjectType() {
		return objectType;
	}

	public void setObjectType(DSObjectType objectType) {
		this.objectType = objectType;
	}

	public DMLType getDmlType() {
		return dmlType;
	}

	public void setDmlType(DMLType dmlType) {
		this.dmlType = dmlType;
	}

	public PrivilegeConf(DMLType dmlType) {
		this.dmlType=dmlType;
	}

	public PrivilegeConf(DSObjectType objectType, DMLType dmlType) {
		this.objectType=objectType;
		this.dmlType=dmlType;
	}

}
