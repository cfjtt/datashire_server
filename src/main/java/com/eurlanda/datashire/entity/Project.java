package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;

import java.sql.Types;
import java.util.List;

/**
 * 
 * Project
 * <p>
 * Title : 
 * </p>
 * <p>
 * Description: 
 * </p>
 * <p>
 * Author :赵春花 2013-9-5
 * </p>
 * <p>
 * update :赵春花 2013-9-5
 * </p>
 * <p>
 * Department :  JAVA后端研发部
 * </p>
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司
 * </p>
 */
@MultitableMapping(pk="", name = "DS_PROJECT", desc = "")
public class Project extends DSBaseModel {

	@ColumnMpping(name="creator", desc="creator", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private int creator;
	
	@ColumnMpping(name="description", desc="description", nullable=true, precision=50, type=Types.VARCHAR, valueReg="")
	private String description;
	
	@ColumnMpping(name="modification_date", desc="modification_date", nullable=true, precision=50, type=Types.TIMESTAMP, valueReg="")
	private String modification_date;
	
	@ColumnMpping(name="repository_id", desc="", nullable=false, precision=0, type=Types.INTEGER, valueReg="")
	private int repository_id;
	public int getRepository_id() {
		return repository_id;
	}

	public void setRepository_id(int repositoryId) {
		repository_id = repositoryId;
	}

	/**
	 * SquidFlow集合
	 */
	private List<SquidFlow> squidFlowList;
	private List<DSVariable> variables;
	public Project(){

	}

	public void finalize() throws Throwable {
		super.finalize();
	}

	public int getCreator() {
		return creator;
	}

	public void setCreator(int creator) {
		this.creator = creator;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getModification_date() {
		return modification_date;
	}

	public void setModification_date(String modificationDate) {
		modification_date = modificationDate;
	}

	public List<SquidFlow> getSquidFlowList() {
		return squidFlowList;
	}

	public void setSquidFlowList(List<SquidFlow> squidFlowList) {
		this.squidFlowList = squidFlowList;
	}

	public List<DSVariable> getVariables() {
		return variables;
	}

	public void setVariables(List<DSVariable> variables) {
		this.variables = variables;
	}

}