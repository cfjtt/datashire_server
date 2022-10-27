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
public class TableExtractSquid extends DataSquid {
    
    public TableExtractSquid(){
        
    }
	
	{
		this.setType(DSObjectType.EXTRACT.value());
	}

	//@ColumnMpping(name="source_table_code", desc="fk:DS_SOURCE_TABLE.source_squid_id+'_'+table_name", nullable=false, precision=200, type=Types.VARCHAR, valueReg="")
	//private String source_table_code;
	// TODO
	//private List<Indexes> indexesList;
	   public TableExtractSquid(DataSquid dataSquid){
	        this.setId(dataSquid.getId());
	        this.setKey(dataSquid.getKey());
	        this.setCreation_date(dataSquid.getCreation_date());
	        this.setName(dataSquid.getName());
	        this.setLocation_x(dataSquid.getLocation_x());
	        this.setLocation_y(dataSquid.getLocation_y());
	        this.setSquid_width(dataSquid.getSquid_width());
	        this.setSquid_height(dataSquid.getSquid_height());
	        this.setSquid_type(dataSquid.getSquid_type());
	        this.setSquidflow_id(dataSquid.getSquidflow_id());
	        this.setIs_show_all(dataSquid.isIs_show_all());
	        this.setSource_is_show_all(dataSquid.isSource_is_show_all());
	        this.setSource_transformation_group_x(dataSquid.getSource_transformation_group_x());
	        this.setSource_transformation_group_y(dataSquid.getSource_transformation_group_y());
	        this.setTransformation_group_x(dataSquid.getTransformation_group_x());
	        this.setTransformation_group_y(dataSquid.getTransformation_group_y());
	        this.setTable_name(dataSquid.getTable_name());
	        this.setFilter(dataSquid.getFilter());
	        this.setEncoding(dataSquid.getEncoding());
	        this.setDescription(dataSquid.getDescription());
	        this.setIs_incremental(dataSquid.isIs_incremental());
	        this.setIncremental_expression(dataSquid.getIncremental_expression());
	        this.setIs_indexed(dataSquid.isIs_indexed());
	        this.setIs_persisted(dataSquid.isIs_indexed());
	        this.setIs_persisted(dataSquid.isIs_persisted());
	        this.setDestination_squid_id(dataSquid.getDestination_squid_id());
	        this.setTop_n(dataSquid.getTop_n());
	        this.setTruncate_existing_data_flag(dataSquid.isTruncate_existing_data_flag());
	        this.setProcess_mode(dataSquid.getProcess_mode());
	        this.setLog_format(dataSquid.getLog_format());
	        this.setPost_process(dataSquid.getPost_process());
	        this.setCdc(dataSquid.getCdc());
	        this.setEncoding(dataSquid.getEncoding());
	        this.setException_handling_flag(dataSquid.isException_handling_flag());
	        this.setSource_table_id(dataSquid.getSource_table_id());
	        this.setXsd_dtd_file(dataSquid.getXsd_dtd_file());
	        this.setXsd_dtd_path(dataSquid.getXsd_dtd_path());
	        this.setUnion_all_flag(dataSquid.getUnion_all_flag());
	        this.setIs_distinct(dataSquid.getIs_distinct());
	        this.setColumns(dataSquid.getColumns());
	        this.setSourceColumns(dataSquid.getSourceColumns());
	        this.setTransformationLinks(dataSquid.getTransformationLinks());
	        this.setTransformations(dataSquid.getTransformations());
	        this.setFromTransformations(dataSquid.getFromTransformations());
	        this.setToTransformations(dataSquid.getToTransformations());
	        this.setSquidIndexesList(dataSquid.getSquidIndexesList());
	        this.setIs_distinct(dataSquid.getIs_distinct());
	        this.setMaxExtractNumberPerTimes(dataSquid.getMaxExtractNumberPerTimes());
	        this.setIsUnionTable(dataSquid.getIsUnionTable());
	        this.setTableNameSettingType(dataSquid.getTableNameSettingType());
	        this.setTableNameSetting(dataSquid.getTableNameSetting());
	    }
	
	public TableExtractSquid(Squid s){
        if(s!=null && s.getId()>0){
            this.setId(s.getId());
            this.setKey(s.getKey());
            this.setCreation_date(s.getCreation_date());
            this.setName(s.getName());
            this.setLocation_x(s.getLocation_x());
            this.setLocation_y(s.getLocation_y());
            this.setSquid_width(s.getSquid_width());
            this.setSquid_height(s.getSquid_height());
            this.setSquid_type(s.getSquid_type());
            this.setSquidflow_id(s.getSquidflow_id());
            this.setIs_show_all(s.isIs_show_all());
            this.setSource_is_show_all(s.isSource_is_show_all());
            this.setSource_transformation_group_x(s.getSource_transformation_group_x());
            this.setSource_transformation_group_y(s.getSource_transformation_group_y());
            this.setTransformation_group_x(s.getTransformation_group_x());
            this.setTransformation_group_y(s.getTransformation_group_y());
            this.setTable_name(s.getTable_name());
            this.setFilter(s.getFilter());
            this.setEncoding(s.getEncoding());
            this.setDescription(s.getDescription());
        }
    }
	
}