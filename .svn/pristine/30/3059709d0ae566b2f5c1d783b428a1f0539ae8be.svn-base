package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;
import com.eurlanda.datashire.common.webService.StartController;
import com.eurlanda.datashire.common.webService.StartTypeEnum;

import java.sql.Types;
import java.util.List;

/**
 * 
 * SquidModelBase
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
@MultitableMapping(pk="ID", name = "DS_SQUID", desc = "")
public class Squid extends DSBaseModel {
	
	@ColumnMpping(name="location_x", desc="x", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private int location_x;
	@ColumnMpping(name="location_y", desc="y", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private int location_y;
	@ColumnMpping(name="squid_height", desc="h", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private int squid_height;
	@ColumnMpping(name="squid_width", desc="w", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private int squid_width;
	@ColumnMpping(name="squid_type_id", desc="fk:SquidTypeEnum", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private int squid_type;
	@ColumnMpping(name="squid_flow_id", desc="fk:DS_SQUID_FLOW", nullable=true, precision=0, type=Types.INTEGER, valueReg=">=1")
	private int squidflow_id;
	@ColumnMpping(name="is_show_all", desc="目标是否显示", nullable=true, precision=1, type=Types.VARCHAR, valueReg="")
	private boolean is_show_all; // target_
	@ColumnMpping(name="source_is_show_all", desc="源是否显示", nullable=true, precision=1, type=Types.VARCHAR, valueReg="")
	private boolean source_is_show_all;
	@ColumnMpping(name="reference_group_x", desc="源的位置x", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private int source_transformation_group_x;
	@ColumnMpping(name="reference_group_y", desc="源的位置 Y", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private int source_transformation_group_y;
	@ColumnMpping(name="column_group_x", desc="目标位置 x", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private int transformation_group_x;
	@ColumnMpping(name="column_group_y", desc="目标位置 y", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private int transformation_group_y;
	@ColumnMpping(name="table_name", desc="", nullable=true, precision=300, type=Types.VARCHAR, valueReg="")
	private String table_name;
	@ColumnMpping(name="filter", desc="过滤条件", nullable=true, precision=3000, type=Types.VARCHAR, valueReg="")
	@StartController(propertyName="filter",valid= StartTypeEnum.valid)
	private String filter;
	@ColumnMpping(name="encoding", desc="编码", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private int encoding;
	@ColumnMpping(name="max_travel_depth", desc="遍历深度", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private int max_travel_depth;
	@ColumnMpping(name="description", desc="描述", nullable=true, precision=300, type=Types.VARCHAR, valueReg="")
	private String description;
	@ColumnMpping(name="design_status", desc="设计期的状态,比如pending, error等", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private int design_status;


	/**
	 *断点标志 
	 */
	private boolean break_flag;
	/**
	 *查看数据 
	 */
	private boolean data_viewer_flag;
	/**
	 *运行至本squid 
	 */
	private boolean run_to_here_flag;
	/**
	 * 运行时的状态״̬
	 */
	private int runTimeStatus;
	
	//Squid下的变量集合
	private List<DSVariable> variables;

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


	public int getRunTimeStatus() {
		return runTimeStatus;
	}

	public void setRunTimeStatus(int runTimeStatus) {
		this.runTimeStatus = runTimeStatus;
	}

	public int getSquid_height() {
		return squid_height;
	}

	public void setSquid_height(int squidHeight) {
		squid_height = squidHeight;
	}

	public int getSquid_width() {
		return squid_width;
	}

	public void setSquid_width(int squidWidth) {
		squid_width = squidWidth;
	}
    
	/**
	 * @return squid_type
	 */
	public int getSquid_type() {
		return squid_type;
	}

	/**
	 * @param squid_type
	 */
	public void setSquid_type(int squid_type) {
		this.squid_type = squid_type;
	}

	public int getSquidflow_id() {
		return squidflow_id;
	}

	public void setSquidflow_id(int squidflowId) {
		squidflow_id = squidflowId;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public boolean isIs_show_all() {
		return is_show_all;
	}

	public void setIs_show_all(boolean is_show_all) {
		this.is_show_all = is_show_all;
	}

	public boolean isSource_is_show_all() {
		return source_is_show_all;
	}

	public void setSource_is_show_all(boolean source_is_show_all) {
		this.source_is_show_all = source_is_show_all;
	}

	public int getSource_transformation_group_x() {
		return source_transformation_group_x;
	}

	public void setSource_transformation_group_x(int source_transformation_group_x) {
		this.source_transformation_group_x = source_transformation_group_x;
	}

	public int getSource_transformation_group_y() {
		return source_transformation_group_y;
	}

	public void setSource_transformation_group_y(int source_transformation_group_y) {
		this.source_transformation_group_y = source_transformation_group_y;
	}

	public int getTransformation_group_x() {
		return transformation_group_x;
	}

	public void setTransformation_group_x(int transformation_group_x) {
		this.transformation_group_x = transformation_group_x;
	}

	public int getTransformation_group_y() {
		return transformation_group_y;
	}

	public void setTransformation_group_y(int transformation_group_y) {
		this.transformation_group_y = transformation_group_y;
	}

	public String getTable_name() {
		return table_name;
	}

	public void setTable_name(String table_name) {
		this.table_name = table_name;
	}

	public String getFilter() {
		return filter;
	}

	public void setFilter(String filter) {
		this.filter = filter;
	}

	public boolean isBreak_flag() {
		return break_flag;
	}

	public void setBreak_flag(boolean break_flag) {
		this.break_flag = break_flag;
	}

	public boolean isData_viewer_flag() {
		return data_viewer_flag;
	}

	public void setData_viewer_flag(boolean data_viewer_flag) {
		this.data_viewer_flag = data_viewer_flag;
	}

	public boolean isRun_to_here_flag() {
		return run_to_here_flag;
	}

	public void setRun_to_here_flag(boolean run_to_here_flag) {
		this.run_to_here_flag = run_to_here_flag;
	}

	public int getEncoding() {
		return encoding;
	}

	public void setEncoding(int encoding) {
		this.encoding = encoding;
	}

	public int getDesign_status() {
		return design_status;
	}

	public void setDesign_status(int design_status) {
		this.design_status = design_status;
	}

	public int getMax_travel_depth() {
		return max_travel_depth;
	}

	public void setMax_travel_depth(int max_travel_depth) {
		this.max_travel_depth = max_travel_depth;
	}

	public List<DSVariable> getVariables() {
		return variables;
	}

	public void setVariables(List<DSVariable> variables) {
		this.variables = variables;
	}


	@Override
	public String toString() {
		return "SquidModelBase [id=" + id + ", key=" + key + ", name=" + name + ", table_name=" + table_name + ", squid_type=" + squid_type + "]";
	}

}