package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;

import java.sql.Types;

/**
 * 
 * 
 * <p>
 * Title : 第三方链接Params参数集合
 * </p>
 * <p>
 * Description: 
 * </p>
 * <p>
 * Author :周益 2014-11-10
 * </p>
 * <p>
 * update :周益 2014-11-10
 * </p>
 * <p>
 * Department :  JAVA后端研发部
 * </p>
 * Copyright : ©2014-2015 悦岚（上海）数据服务有限公司
 * </p>
 */
@MultitableMapping(pk="", name = {"DS_THIRDPARTY_PARAMS"}, desc = "")
public class ThirdPartyParams extends DSBaseModel {
	
	@ColumnMpping(name="squid_id", desc="fk:DS_SQUID", nullable=false, precision=0, type=Types.INTEGER, valueReg=">=1")
	private int squid_id;
	@ColumnMpping(name="source_table_id", desc="pk:DS_SOURCE_TABLE", nullable=false, precision=0, type=Types.INTEGER, valueReg=">=1")
	private int source_table_id;
	@ColumnMpping(name="params_type", desc="URL/非URL", nullable=true, precision=0, type=Types.INTEGER, valueReg=">=1")
	private int params_type;//0=get,1=head,2=content,post
	@ColumnMpping(name="value_type", desc="", nullable=true, precision=0, type=Types.INTEGER, valueReg=">=1")
	private int value_type;//0=值;1=column;2=变量
	@ColumnMpping(name="val", desc="", nullable=true, precision=200, type=Types.VARCHAR, valueReg="")
	protected String val;
	@ColumnMpping(name="column_id", desc="", nullable=true, precision=0, type=Types.INTEGER, valueReg=">=1")
	private int column_id;
	@ColumnMpping(name="ref_squid_id", desc="", nullable=true, precision=0, type=Types.INTEGER, valueReg=">=1")
	private int ref_squid_id;
	
	
	public ThirdPartyParams(){
	}
	
	public ThirdPartyParams(int squid_id, int params_type, int value_type,
			String val, int column_id, int variable_id,int ref_squid_id,int source_table_id) {
		super();
		this.squid_id = squid_id;
		this.params_type = params_type;
		this.value_type = value_type;
		this.val = val;
		this.column_id = column_id;
		this.ref_squid_id=ref_squid_id;
		this.source_table_id=source_table_id;
	}
	
	public int getSource_table_id() {
		return source_table_id;
	}

	public void setSource_table_id(int source_table_id) {
		this.source_table_id = source_table_id;
	}

	public int getSquid_id() {
		return squid_id;
	}
	public void setSquid_id(int squid_id) {
		this.squid_id = squid_id;
	}
	public int getParams_type() {
		return params_type;
	}
	public void setParams_type(int params_type) {
		this.params_type = params_type;
	}
	public int getValue_type() {
		return value_type;
	}
	public void setValue_type(int value_type) {
		this.value_type = value_type;
	}
	public String getVal() {
		return val;
	}
	public void setVal(String val) {
		this.val = val;
	}
	public int getColumn_id() {
		return column_id;
	}
	public void setColumn_id(int column_id) {
		this.column_id = column_id;
	}
	public int getRef_squid_id() {
		return ref_squid_id;
	}

	public void setRef_squid_id(int ref_squid_id) {
		this.ref_squid_id = ref_squid_id;
	}
	
}
