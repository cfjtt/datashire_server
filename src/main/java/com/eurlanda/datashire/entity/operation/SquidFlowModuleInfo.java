package com.eurlanda.datashire.entity.operation;

import com.eurlanda.datashire.entity.DSVariable;
import com.eurlanda.datashire.entity.SquidLink;
import com.thoughtworks.xstream.annotations.XStreamAlias;

import java.util.List;

@XStreamAlias("SquidFlowModuleInfo")
public class SquidFlowModuleInfo {
	
	private int id;
	private String name;
	private String key;
	private String modification_date;
	private int project_id;
	private int squidFlow_type;
	private int compilation_status;
	//数猎场类型(DES加密)
	private String datashireFieldType;
	private List<DSVariable> variableList;
	private List<SquidModuleInfo> squidList;
	private List<SquidLink> squidLinkList;
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public String getModification_date() {
		return modification_date;
	}
	public void setModification_date(String modification_date) {
		this.modification_date = modification_date;
	}
	public int getProject_id() {
		return project_id;
	}
	public void setProject_id(int project_id) {
		this.project_id = project_id;
	}
	public int getCompilation_status() {
		return compilation_status;
	}
	public void setCompilation_status(int compilation_status) {
		this.compilation_status = compilation_status;
	}
	public List<DSVariable> getVariableList() {
		return variableList;
	}
	public void setVariableList(List<DSVariable> variableList) {
		this.variableList = variableList;
	}
	public List<SquidModuleInfo> getSquidList() {
		return squidList;
	}
	public void setSquidList(List<SquidModuleInfo> squidList) {
		this.squidList = squidList;
	}
	public List<SquidLink> getSquidLinkList() {
		return squidLinkList;
	}
	public void setSquidLinkList(List<SquidLink> squidLinkList) {
		this.squidLinkList = squidLinkList;
	}

	public int getSquidFlow_type() {
		return squidFlow_type;
	}

	public void setSquidFlow_type(int squidFlow_type) {
		this.squidFlow_type = squidFlow_type;
	}

	public String getDatashireFieldType() {
		return datashireFieldType;
	}

	public void setDatashireFieldType(String datashireFieldType) {
		this.datashireFieldType = datashireFieldType;
	}
}
