package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;

import java.sql.Types;

/**
 * 模版数据实体类
 * @author yi.zhou
 *
 */
@MultitableMapping(pk="", name = "DS_SYS_TEMPLATE_DATA", desc = "")
public class TemplateDataInfo extends DSBaseModel{
	@ColumnMpping(name="type_id", desc="fk:DS_SYS_TEMPLATE_TYPE", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private int type_id;
	
	private String type_name;
	
	@ColumnMpping(name="data_value", desc="", nullable=true, precision=500, type=Types.VARCHAR, valueReg="")
	private String data_value;

	public int getType_id() {
		return type_id;
	}

	public void setType_id(int typeId) {
		type_id = typeId;
	}
	
	public String getType_name() {
		return type_name;
	}

	public void setType_name(String typeName) {
		type_name = typeName;
	}

	public String getData_value() {
		return data_value;
	}

	public void setData_value(String dataValue) {
		data_value = dataValue;
	}
	
	@Override
	public String toString(){
		return "TemplateDataInfo [type_id=" + type_id + ", data_value=" + data_value + "]";
	}
}
