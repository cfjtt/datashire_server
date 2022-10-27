package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.enumeration.DSObjectType;

/**
 * 
 * DBDestinationSquid
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
public class DBDestinationSquid extends DbSquid {
	{
		this.setType(DSObjectType.DBSOURCE.value());
	}
}