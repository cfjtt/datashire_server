package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;

import java.sql.Types;
import java.util.List;

/**
 * 微博连接实体类
 * @author lei.bin
 *
 */
@MultitableMapping(pk="ID", name ={"DS_SQUID","DS_WEIBO_CONNECTION"}, desc = "")
public class WeiboSquid extends Squid {
	@ColumnMpping(name = "user_name", desc = "登录用户名", nullable = true, precision = 100, type = Types.VARCHAR, valueReg = "")
	private String user_name;
	@ColumnMpping(name = "password", desc = "登录密码", nullable = true, precision = 100, type = Types.VARCHAR, valueReg = "")
	private String password;
	@ColumnMpping(name = "service_provider", desc = "枚举下拉框【新浪，腾讯】", nullable = true, precision = 0, type = Types.INTEGER, valueReg = "")
	private int service_provider;
	@ColumnMpping(name="start_data_date", desc="起始时间", nullable=true, precision=0, type=Types.DATE, valueReg="")
	private String start_data_date;
	@ColumnMpping(name="end_data_date", desc="结束时间", nullable=true, precision=0, type=Types.DATE, valueReg="")
	private String end_data_date;
	private List<SourceTable> sourceTableList;
	private JobSchedule jobSchedule;
	public List<SourceTable> getSourceTableList() {
		return sourceTableList;
	}

	public void setSourceTableList(List<SourceTable> sourceTableList) {
		this.sourceTableList = sourceTableList;
	}
	
	public JobSchedule getJobSchedule() {
		return jobSchedule;
	}

	public void setJobSchedule(JobSchedule jobSchedule) {
		this.jobSchedule = jobSchedule;
	}

	public String getUser_name() {
		return user_name;
	}

	public void setUser_name(String user_name) {
		this.user_name = user_name;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public int getService_provider() {
		return service_provider;
	}

	public void setService_provider(int service_provider) {
		this.service_provider = service_provider;
	}

	public String getStart_data_date() {
		return start_data_date;
	}

	public void setStart_data_date(String start_data_date) {
		this.start_data_date = start_data_date;
	}

	public String getEnd_data_date() {
		return end_data_date;
	}

	public void setEnd_data_date(String end_data_date) {
		this.end_data_date = end_data_date;
	}

	

}
