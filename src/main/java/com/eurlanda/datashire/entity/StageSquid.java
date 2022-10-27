package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.MultitableMapping;
import com.eurlanda.datashire.enumeration.DSObjectType;

import java.util.List;

/**
 * 
 * 
 * <p>
 * Title : 
 * </p>
 * <p>
 * Description:  stage squid
 * </p>
 * <p>
 * Author :赵春花 2013-9-9
 * </p>
 * <p>
 * update :赵春花 2013-9-9
 * </p>
 * <p>
 * Department :  JAVA后端研发部
 * </p>
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司
 * </p>
 */
@MultitableMapping(name = {"DS_SQUID"}, pk="ID", desc = "")
public class StageSquid extends DataSquid {

	{
		super.setType(DSObjectType.STAGE.value());
	}
	
	private List<SquidJoin> joins;

	{
		this.setType(DSObjectType.STAGE.value());
	}
	public List<SquidJoin> getJoins() {
		return joins;
	}

	public void setJoins(List<SquidJoin> joins) {
		this.joins = joins;
	}

}