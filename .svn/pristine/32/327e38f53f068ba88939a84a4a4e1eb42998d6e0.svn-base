package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;

import java.sql.Types;

@MultitableMapping(pk="id", name = "DS_SYS_REPORT_FOLDER", desc = "")
public class ReportFolder{
	// 节点ID
	@ColumnMpping(name="id", desc="", nullable=false, precision=0, type=Types.INTEGER, valueReg="")
	private Integer id;
	// 文件名称
	@ColumnMpping(name="folder_name", desc="", nullable=false, precision=100, type=Types.VARCHAR, valueReg="")
	private String folder_name;
	// 创建日期
	@ColumnMpping(name="creation_date", desc="", nullable=false, precision=0, type=Types.TIMESTAMP, valueReg="")
	private String creation_date;
	// 父节点ID
	@ColumnMpping(name="pid", desc="", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private Integer pid;
	// 是否显示
	@ColumnMpping(name="is_display", desc="", nullable=false, precision=0, type=Types.BOOLEAN, valueReg="")
	private Boolean is_display;

	/**
	 * @return the pid
	 */
	public Integer getPid() {
		return pid;
	}

	/**
	 * @param pid the pid to set
	 */
	public void setPid(Integer pid) {
		this.pid = pid;
	}

	/**
	 * @return the id
	 */
	public Integer getId() {
		return id;
	}

	/**
	 * @param id the id to set
	 */
	public void setId(Integer id) {
		this.id = id;
	}

	/**
	 * @return the folder_name
	 */
	public String getFolder_name() {
		return folder_name;
	}

	/**
	 * @param folder_name the folder_name to set
	 */
	public void setFolder_name(String folder_name) {
		this.folder_name = folder_name;
	}

	/**
	 * @return the creation_date
	 */
	public String getCreation_date() {
		return creation_date;
	}

	/**
	 * @param creation_date the creation_date to set
	 */
	public void setCreation_date(String creation_date) {
		this.creation_date = creation_date;
	}

	/**
	 * @return the is_display
	 */
	public Boolean getIs_display() {
		return is_display;
	}

	/**
	 * @param is_display the is_display to set
	 */
	public void setIs_display(Boolean is_display) {
		this.is_display = is_display;
	}


}
