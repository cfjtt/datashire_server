package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;

import java.sql.Types;

/**
 * 
 * @author yi.zhou
 *
 */
@MultitableMapping(name = {"DS_REPORT_VERSION"},pk="ID", desc = "")
public class ReportVersion {
	// id
	@ColumnMpping(name="id", desc="", nullable=false, precision=0, type=Types.INTEGER, valueReg="")
	private int id;
	// squid_id
	@ColumnMpping(name="squid_id", desc="", nullable=false, precision=0, type=Types.INTEGER, valueReg="")
	private int squid_id;
	// 版本号
	@ColumnMpping(name="version", desc="", nullable=false, precision=0, type=Types.INTEGER, valueReg="")
	private int version;
	// 报表模板定义
	@ColumnMpping(name="template", desc="", nullable=true, precision=100, type=Types.CLOB, valueReg="")
	private String template;
	// 创建日期
	@ColumnMpping(name="add_date", desc="", nullable=false, precision=0, type=Types.TIMESTAMP, valueReg="")
	private String add_date;
	
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public int getSquid_id() {
		return squid_id;
	}
	public void setSquid_id(int squid_id) {
		this.squid_id = squid_id;
	}
	public int getVersion() {
		return version;
	}
	public void setVersion(int version) {
		this.version = version;
	}
	public String getTemplate() {
		return template;
	}
	public void setTemplate(String template) {
		this.template = template;
	}
	public String getAdd_date() {
		return add_date;
	}
	public void setAdd_date(String add_date) {
		this.add_date = add_date;
	}
}
