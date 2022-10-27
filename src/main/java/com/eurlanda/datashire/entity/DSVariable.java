package com.eurlanda.datashire.entity;

/**
 * 变量对象
 */

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;

import java.io.Serializable;
import java.sql.Types;

@MultitableMapping(name = {"DS_VARIABLE"}, pk="ID", desc = "变量对象")
public class DSVariable implements Serializable {
	@ColumnMpping(name = "id", desc = "", nullable = false, precision = 0, type = Types.INTEGER, valueReg = "")
	private int id;
	
	@ColumnMpping(name = "project_id", desc = "", nullable = true, precision = 0, type = Types.INTEGER, valueReg = "")
	private int project_id;
	
	@ColumnMpping(name = "squid_flow_id", desc = "", nullable = true, precision = 0, type = Types.INTEGER, valueReg = "")
	private int squid_flow_id;
	
	@ColumnMpping(name = "squid_id", desc = "", nullable = true, precision = 0, type = Types.INTEGER, valueReg = "")
	private int squid_id;
	
	@ColumnMpping(name = "variable_scope", desc = "作用域", nullable = true, precision = 0, type = Types.INTEGER, valueReg = "")
	private int variable_scope; 	//作用域   0：project、1：squidFlow、2：squid
	
	@ColumnMpping(name = "variable_name", desc = "", nullable = true, precision = 200, type = Types.VARCHAR, valueReg = "")
	private String variable_name; 	//名称
	
	@ColumnMpping(name = "variable_type", desc = "", nullable = true, precision = 0, type = Types.INTEGER, valueReg = "")
	private int variable_type;	  	//枚举值  SystemDataType
	
	@ColumnMpping(name = "variable_length", desc = "", nullable = true, precision = 0, type = Types.INTEGER, valueReg = "")
	private int variable_length;  	//长度
	
	@ColumnMpping(name = "variable_precision", desc = "", nullable = true, precision = 0, type = Types.INTEGER, valueReg = "")
	private int variable_precision; //精度
	
	@ColumnMpping(name = "variable_scale", desc = "", nullable = true, precision = 0, type = Types.INTEGER, valueReg = "")
	private int variable_scale;     //小数位
	
	@ColumnMpping(name = "variable_value", desc = "", nullable = true, precision = 500, type = Types.VARCHAR, valueReg = "")
	private String variable_value;  //对象
	@ColumnMpping(name = "add_date", desc = "", nullable = true, precision = 0, type = Types.TIMESTAMP, valueReg = "")
	private String add_date;     //添加时间
	
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public int getProject_id() {
		return project_id;
	}
	public void setProject_id(int project_id) {
		this.project_id = project_id;
	}
	public int getSquid_flow_id() {
		return squid_flow_id;
	}
	public void setSquid_flow_id(int squid_flow_id) {
		this.squid_flow_id = squid_flow_id;
	}
	public int getSquid_id() {
		return squid_id;
	}
	public void setSquid_id(int squid_id) {
		this.squid_id = squid_id;
	}
	public int getVariable_scope() {
		return variable_scope;
	}
	public void setVariable_scope(int variable_scope) {
		this.variable_scope = variable_scope;
	}
	public String getVariable_name() {
		return variable_name;
	}
	public void setVariable_name(String variable_name) {
		this.variable_name = variable_name;
	}
	public int getVariable_type() {
		return variable_type;
	}
	public void setVariable_type(int variable_type) {
		this.variable_type = variable_type;
	}
	public int getVariable_length() {
		return variable_length;
	}
	public void setVariable_length(int variable_length) {
		this.variable_length = variable_length;
	}
	public int getVariable_precision() {
		return variable_precision;
	}
	public void setVariable_precision(int variable_precision) {
		this.variable_precision = variable_precision;
	}
	public int getVariable_scale() {
		return variable_scale;
	}
	public void setVariable_scale(int variable_scale) {
		this.variable_scale = variable_scale;
	}
	public String getVariable_value() {
		return variable_value;
	}
	public void setVariable_value(String variable_value) {
		this.variable_value = variable_value;
	}

	public String getAdd_date() {
		return add_date;
	}

	public void setAdd_date(String add_date) {
		this.add_date = add_date;
	}
}
