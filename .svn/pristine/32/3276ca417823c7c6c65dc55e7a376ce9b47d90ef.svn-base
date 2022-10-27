package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;
import com.eurlanda.datashire.enumeration.DSObjectType;

import java.sql.Types;

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
public class ExtractSquid extends DataSquid {
	
	{
		this.setType(DSObjectType.EXTRACT.value());
	}

	@ColumnMpping(name="is_incremental", desc="是否增量抽取", nullable=true, precision=1, type=Types.VARCHAR, valueReg="")
	private boolean is_incremental;
	@ColumnMpping(name="incremental_expression", desc="增量抽取条件", nullable=true, precision=200, type=Types.VARCHAR, valueReg="")
	private String incremental_expression;
	//@ColumnMpping(name="source_table_code", desc="fk:DS_SOURCE_TABLE.source_squid_id+'_'+table_name", nullable=false, precision=200, type=Types.VARCHAR, valueReg="")
	//private String source_table_code;
	
	public ExtractSquid(){
	}
	
	public ExtractSquid(Squid s){
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
	
	public boolean isIs_incremental() {
		return is_incremental;
	}
	public void setIs_incremental(boolean is_incremental) {
		this.is_incremental = is_incremental;
	}
	public String getIncremental_expression() {
		return incremental_expression;
	}
	public void setIncremental_expression(String incremental_expression) {
		this.incremental_expression = incremental_expression;
	}

}