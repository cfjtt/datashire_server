package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;
import com.eurlanda.datashire.enumeration.DSObjectType;

import java.sql.Types;
import java.util.List;

/**
 * 
 * Group
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
@MultitableMapping(pk="", name = "DS_SYS_GROUP", desc = "GROUP信息表")
public class Group extends DSBaseModel{
	
	{
		this.setType(DSObjectType.GROUP.value());
	}

	@ColumnMpping(name="description", desc="", nullable=true, precision=500, type=Types.VARCHAR, valueReg="")
	private String desc;

	@ColumnMpping(name="location_x", desc="C#界面坐标X", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private int location_x;

	@ColumnMpping(name="location_y", desc="C#界面坐标Y", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private int location_y;
	
	@ColumnMpping(name="parent_group_id", desc="直接上级ID", nullable=true, precision=0, type=Types.INTEGER, valueReg=">=1")
	private int parent_group_id;
	
	@ColumnMpping(name="team_id", desc="", nullable=true, precision=0, type=Types.INTEGER, valueReg=">=1")
	private int team_id;

	private List<Role> roleList; // 当前group的role列表
	
	public Group(){
	}

	public void finalize() throws Throwable {
		super.finalize();
	}

	public String getDesc() {
		return desc;
	}

	public void setDesc(String desc) {
		this.desc = desc;
	}

	public int getLocation_x() {
		return location_x;
	}

	public void setLocation_x(int locationX) {
		location_x = locationX;
	}

	public int getLocation_y() {
		return location_y;
	}

	public void setLocation_y(int locationY) {
		location_y = locationY;
	}

	public int getParent_group_id() {
		return parent_group_id;
	}

	public void setParent_group_id(int parentGroupId) {
		parent_group_id = parentGroupId;
	}

	public int getTeam_id() {
		return team_id;
	}

	public void setTeam_id(int teamId) {
		team_id = teamId;
	}
	
	public List<Role> getRoleList() {
		return roleList;
	}

	public void setRoleList(List<Role> roleList) {
		this.roleList = roleList;
	}

	@Override
	public String toString() {
		return "Group[location_x='"+getLocation_x()+"', location_y='"+getLocation_y()+"', parent_group_id='"+getParent_group_id()+"', team_id='"+getTeam_id()+"', desc='"+getDesc()+"', name='"+getName()+"', key='"+getKey()+"', id='"+getId()+"', type='"+getType()+"']";
	}

}