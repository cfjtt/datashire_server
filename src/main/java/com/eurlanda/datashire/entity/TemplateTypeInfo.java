package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;

import java.sql.Types;

/**
 * 模版数据类型实体类
 * @author yi.zhou
 *
 */
@MultitableMapping(pk="", name = "DS_SYS_TEMPLATE_TYPE", desc = "")
public class TemplateTypeInfo extends DSBaseModel{
	@ColumnMpping(name="type_name", desc="", nullable=true, precision=50, type=Types.VARCHAR, valueReg="")
	private String type_Name;
	
	@ColumnMpping(name="data_type", desc="", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private int data_Type;
	
	private String templateColumnName;
	
	public String getType_Name() {
		return type_Name;
	}

	public void setType_Name(String typeName) {
		type_Name = typeName;
	}

	public int getData_Type() {
		return data_Type;
	}

	public void setData_Type(int dataType) {
		data_Type = dataType;
	}

	public String getTemplateColumnName() {
		return templateColumnName;
	}

	public void setTemplateColumnName(String templateColumnName) {
		this.templateColumnName = templateColumnName;
	}

	@Override
	public String toString() {
		return "TemplateTypeInfo [type_name=" + type_Name + "templateColumnName=" + templateColumnName + "]";
	}
}
