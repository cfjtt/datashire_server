package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.enumeration.DSObjectType;

/**
 * RepoValidationResponse实体类
 * 
 * <p>
 * Title : 
 * </p>
 * <p>
 * Description: 
 * </p>
 * <p>
 * Author :宾磊 2013-9-18
 * </p>
 * <p>
 * update :宾磊 2013-9-18
 * </p>
 * <p>
 * Department :  JAVA后端研发部
 * </p>
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司
 * </p>
 */
public class RepoValidationResponse extends DSBaseModel {
	{
		this.setType(DSObjectType.REPOSITORY.value());
	}
	/*
	 * 返回信息代码
	 */
	private int result_code;
	/*
	 * 表名
	 */
	private String table_name;

	public int getResult_code() {
		return result_code;
	}

	public void setResult_code(int result_code) {
		this.result_code = result_code;
	}

	public String getTable_name() {
		return table_name;
	}

	public void setTable_name(String table_name) {
		this.table_name = table_name;
	}

	@Override
	public String toString() {
		return "RepoValidationResponse[result_code='" + result_code
				+ "', table_name='" + table_name + "']";
	}
}
