package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;
import com.eurlanda.datashire.enumeration.DSObjectType;

import java.sql.Types;

/**
 * 
 *Role 
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
@MultitableMapping(pk="", name = "DS_SYS_ROLE", desc = "角色信息表")
public class Role extends DSBaseModel {
	{
		this.setType(DSObjectType.ROLE.value());
	}
	
	@ColumnMpping(name="description", desc="角色描述", nullable=true, precision=500, type=Types.VARCHAR, valueReg="")
	private String desc;
	
	@ColumnMpping(name="group_id", desc="", nullable=true, precision=0, type=Types.INTEGER, valueReg=">=1")
	private int group_id;
 
	
	public String getDesc() {
		return desc;
	}

	public void setDesc(String desc) {
		this.desc = desc;
	}

	public int getGroup_id() {
		return group_id;
	}

	public void setGroup_id(int groupId) {
		group_id = groupId;
	}
	
	@Override
	public String toString() {
		return "Role[group_id='"+getGroup_id()+"', desc='"+getDesc()+"', name='"+getName()+"', key='"+getKey()+"', id='"+getId()+"', type='"+getType()+"']";
	}

}