package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;
import com.eurlanda.datashire.enumeration.DSObjectType;

import java.sql.Types;
import java.util.List;

/**
 * 
 * Team
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
@MultitableMapping(pk="", name = "DS_SYS_TEAM", desc = "TEAM信息表")
public class Team extends DSBaseModel {

	{
		this.setType(DSObjectType.TEAM.value());
	}
	
	@ColumnMpping(name="description", desc="", nullable=true, precision=500, type=Types.VARCHAR, valueReg="")
	private String desc;
	
	//@ColumnMpping(name="name", desc="", nullable=true, precision=500, type=Types.VARCHAR, valueReg="")
	//private transient String teamName; // transient: gson declares multiple JSON fields named
	
	private List<Group> groupList;
	
	private List<Repository> repositoryList;
	
	public String getDesc() {
		return desc;
	}

	public void setDesc(String desc) {
		this.desc = desc;
	}
	
	public List<Group> getGroupList() {
		return groupList;
	}

	public void setGroupList(List<Group> groupList) {
		this.groupList = groupList;
	}

	public List<Repository> getRepositoryList() {
		return repositoryList;
	}

	public void setRepositoryList(List<Repository> repositoryList) {
		this.repositoryList = repositoryList;
	}

	@Override
	public String toString() {
		return "Team[id='"+super.getId()+"', key='"+super.getKey()+"', name='"+super.getName()+"', desc='"+desc+"', created_date='"+super.getCreation_date()+"']";
	}
	
}