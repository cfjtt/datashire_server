package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;

import java.sql.Types;
import java.util.List;


@MultitableMapping(pk="", name = "DS_REFERENCE_COLUMN_GROUP", desc = "")
public class ReferenceColumnGroup extends DSBaseModel{

	@ColumnMpping(name="reference_squid_id", desc="", nullable=false, precision=0, type=Types.INTEGER, valueReg=">=1")
	private int reference_squid_id;
	@ColumnMpping(name="relative_order", desc="0123...", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private int relative_order;
	
	private String group_name;
	
	private List<ReferenceColumn> referenceColumnList;

	public int getReference_squid_id() {
		return reference_squid_id;
	}

	public void setReference_squid_id(int reference_squid_id) {
		this.reference_squid_id = reference_squid_id;
	}

	public int getRelative_order() {
		return relative_order;
	}

	public void setRelative_order(int relative_order) {
		this.relative_order = relative_order;
	}

	public List<ReferenceColumn> getReferenceColumnList() {
		return referenceColumnList;
	}

	public void setReferenceColumnList(List<ReferenceColumn> referenceColumnList) {
		this.referenceColumnList = referenceColumnList;
	}

	public String getGroup_name() {
		return group_name;
	}

	public void setGroup_name(String group_name) {
		this.group_name = group_name;
	}

}
