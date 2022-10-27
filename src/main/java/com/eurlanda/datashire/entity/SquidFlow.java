package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;

import java.sql.Types;
import java.util.List;

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
@MultitableMapping(pk="", name = "DS_SQUID_FLOW", desc = "")
public class SquidFlow extends DSBaseModel {

	@ColumnMpping(name="creator", desc="创始人", nullable=true, precision=50, type=Types.VARCHAR, valueReg="")
	private String creator;
	
	@ColumnMpping(name="description", desc="", nullable=true, precision=200, type=Types.VARCHAR, valueReg="")
	private String description;
	
	@ColumnMpping(name="modification_date", desc="修改时间", nullable=false, precision=0, type=Types.TIMESTAMP, valueReg="")
	private String modification_date;
	
	@ColumnMpping(name="project_id", desc="", nullable=false, precision=0, type=Types.INTEGER, valueReg=">=1") // "^[0-9]*[1-9][0-9]*$"　//正整数 
	private int project_id;

	@ColumnMpping(name="compilation_status", desc="是否编译通过", nullable=false, precision=0, type=Types.INTEGER, valueReg="") // "^[0-9]*[1-9][0-9]*$"　//正整数 
	private int compilation_status;

    @ColumnMpping(name = "squidflow_type", desc="squidflow 状态, 0或者空为批处理squidflow, 1为流式squidflow", nullable=false, precision=0, type=Types.INTEGER, valueReg="")
    private int squidflow_type;

	@ColumnMpping(name="field_type",desc="所属数猎场类型,0:本地场,1:公有数猎场 2:私有数猎场 3:实训数猎场",nullable = false,precision = 0,type=Types.INTEGER,valueReg = "")
	private int dataShireFieldType;

	private boolean isPush;
	private String project_name;
	private List<SquidLink> squidLinkList;
	private List<Squid> squidList;
    private int squid_flow_status;
	public SquidFlow(){

	}

	public String getProject_name() {
		return project_name;
	}

	public void setProject_name(String project_name) {
		this.project_name = project_name;
	}

	public String getCreator() {
		return creator;
	}

	public void setCreator(String creator) {
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

	public int getProject_id() {
		return project_id;
	}

	public void setProject_id(int projectId) {
		project_id = projectId;
	}

	public List<SquidLink> getSquidLinkList() {
		return squidLinkList;
	}

	public void setSquidLinkList(List<SquidLink> squidLinkList) {
		this.squidLinkList = squidLinkList;
	}

	public List<Squid> getSquidList() {
		return squidList;
	}

	public void setSquidList(List<Squid> squidList) {
		this.squidList = squidList;
	}

	/**
	 * @return isPush
	 */
	public boolean isPush() {
		return isPush;
	}

	/**
	 * @param isPush
	 */
	public void setPush(boolean isPush) {
		this.isPush = isPush;
	}

	public int getSquid_flow_status() {
		return squid_flow_status;
	}

	public void setSquid_flow_status(int squid_flow_status) {
		this.squid_flow_status = squid_flow_status;
	}

    public int getCompilation_status() {
        return compilation_status;
    }

    public void setCompilation_status(int compilation_status) {
        this.compilation_status = compilation_status;
    }

    public int getSquidflow_type() {
        return squidflow_type;
    }

    public void setSquidflow_type(int squidflow_type) {
        this.squidflow_type = squidflow_type;
    }

	public int getDataShireFieldType() {
		return dataShireFieldType;
	}
	public void setDataShireFieldType(int dataShireFieldType) {
		this.dataShireFieldType = dataShireFieldType;
	}
}