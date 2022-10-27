package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;
import com.eurlanda.datashire.enumeration.DSObjectType;

import java.sql.Types;

@MultitableMapping(pk="", name = "DS_REFERENCE_COLUMN", desc = "")
public class ReferenceColumn extends Column{
	{
		super.setType(DSObjectType.COLUMN.value());
	}
	@ColumnMpping(name="column_id", desc="super.id,由程序维护主外键关系", nullable=false, precision=0, type=Types.INTEGER, valueReg="")
	private int column_id; // == column.id
	@ColumnMpping(name="reference_squid_id", desc="被谁引用", nullable=false, precision=0, type=Types.INTEGER, valueReg="")
	private int reference_squid_id; // 目标squid
	@ColumnMpping(name="host_squid_id", desc="", nullable=false, precision=0, type=Types.INTEGER, valueReg="")
	private int host_squid_id; // == column.squid_id
	@ColumnMpping(name="is_referenced", desc="创建/删除transLink时更新", nullable=true, precision=1, type=Types.VARCHAR, valueReg="")
	private boolean is_referenced; // 是否参与目标squid列变换
	@ColumnMpping(name="group_id", desc="0123...", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private int group_id;

	@ColumnMpping(name="host_column_deleted", desc="", nullable=true, precision=1, type=Types.VARCHAR, valueReg="")
	private boolean host_column_deleted; //  true表示真实列被删除，界面字体变为红色
	@ColumnMpping(name="group_order", desc="", nullable=true, precision=0, type=Types.NULL, valueReg="")
	private int group_order; // == ReferenceColumnGroup.relative_order
	@ColumnMpping(name="group_name", desc="", nullable=true, precision=0, type=Types.NULL, valueReg="")
	private String group_name; // == host_squid_id 对应的 SquidModelBase name
	
	public int getColumn_id() {
		return column_id;
	}
	public void setColumn_id(int column_id) {
		this.column_id = column_id;
	}
	public int getReference_squid_id() {
		return reference_squid_id;
	}
	public void setReference_squid_id(int reference_squid_id) {
		this.reference_squid_id = reference_squid_id;
	}
	public int getHost_squid_id() {
		return host_squid_id;
	}
	public void setHost_squid_id(int host_squid_id) {
		this.host_squid_id = host_squid_id;
	}
	public boolean isIs_referenced() {
		return is_referenced;
	}
	public void setIs_referenced(boolean is_referenced) {
		this.is_referenced = is_referenced;
	}
	public int getGroup_id() {
		return group_id;
	}
	public void setGroup_id(int group_id) {
		this.group_id = group_id;
	}

	public int getGroup_order() {
		return group_order;
	}
	public void setGroup_order(int group_order) {
		this.group_order = group_order;
	}
	public String getGroup_name() {
		return group_name;
	}
	public void setGroup_name(String groupName) {
		group_name = groupName;
	}
	
	public boolean isHost_column_deleted() {
		return host_column_deleted;
	}
	public void setHost_column_deleted(boolean host_column_deleted) {
		this.host_column_deleted = host_column_deleted;
	}
	@Override
	public String toString() {
		return "ReferenceColumn [column_id=" + column_id + ", name=" + name + ", id=" + id + ", key=" + key + "]";
	}

 
}
