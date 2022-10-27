package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.MultitableMapping;
import com.eurlanda.datashire.enumeration.DSObjectType;

/**
 * 
 * 
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
@MultitableMapping(name = {"DS_SQUID"}, pk="ID", desc = "默认从sourceTableList拖拽创建")
public class MongodbExtractSquid extends DataSquid {
	
	{
		this.setType(DSObjectType.MONGODBEXTRACT.value());
	}
	
	public MongodbExtractSquid(){
	}
	
	public MongodbExtractSquid(Squid s){
		if(s!=null && s.getId()>0){
			this.setId(s.getId());
			this.setName(s.getName());
			this.setTable_name(s.getTable_name());
			this.setFilter(s.getFilter());
			this.setKey(s.getKey());
			this.setSquidflow_id(s.getSquidflow_id());
			this.setLocation_x(s.getLocation_x());
			this.setLocation_y(s.getLocation_y());
			this.setSquid_width(s.getSquid_width());
			this.setSquid_height(s.getSquid_height());
			this.setTransformation_group_x(s.getTransformation_group_x());
			this.setTransformation_group_y(s.getTransformation_group_y());
			this.setSource_transformation_group_x(s.getSource_transformation_group_x());
			this.setSource_transformation_group_y(s.getSource_transformation_group_y());
			this.setIs_show_all(s.isIs_show_all());
			this.setSource_is_show_all(s.isSource_is_show_all());
		}
	}
}