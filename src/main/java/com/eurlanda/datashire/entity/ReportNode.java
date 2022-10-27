package com.eurlanda.datashire.entity;

public class ReportNode {
   
	private Integer id;
	private String nodeName;
	private String createdTime;
	private Integer pid;
	private boolean display_flag;
	private ReportNodeType reportNodeType;
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
	 * @return the nodeName
	 */
	public String getNodeName() {
		return nodeName;
	}
	/**
	 * @param nodeName the nodeName to set
	 */
	public void setNodeName(String nodeName) {
		this.nodeName = nodeName;
	}
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
	 * @return the display_flag
	 */
	public boolean isDisplay_flag() {
		return display_flag;
	}
	/**
	 * @param display_flag the display_flag to set
	 */
	public void setDisplay_flag(boolean display_flag) {
		this.display_flag = display_flag;
	}
	/**
	 * @return the createdTime
	 */
	public String getCreatedTime() {
		return createdTime;
	}
	/**
	 * @param createdTime the createdTime to set
	 */
	public void setCreatedTime(String createdTime) {
		this.createdTime = createdTime;
	}
	/**
	 * @return the reportNodeType
	 */
	public ReportNodeType getReportNodeType() {
		return reportNodeType;
	}
	/**
	 * @param reportNodeType the reportNodeType to set
	 */
	public void setReportNodeType(ReportNodeType reportNodeType) {
		this.reportNodeType = reportNodeType;
	}
}
