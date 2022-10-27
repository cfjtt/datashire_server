package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;

import java.sql.Types;
import java.util.List;

/**
 * 网页连接实体类
 * @author lei.bin
 *
 */
@MultitableMapping(pk="ID", name ={"DS_SQUID","DS_WEB_CONNECTION"}, desc = "")
public class WebSquid extends Squid {
	@ColumnMpping(name = "max_threads", desc = "最大并发抓取线程数", nullable = true, precision = 0, type = Types.INTEGER, valueReg = "")
	private int max_threads;
	@ColumnMpping(name = "max_fetch_depth", desc = "最大抓取深度", nullable = true, precision = 0, type = Types.INTEGER, valueReg = "")
	private int max_fetch_depth;
	@ColumnMpping(name = "domain_limit_flag", desc = "允许跳转到外部域", nullable = true, precision = 0, type = Types.INTEGER, valueReg = "")
	private int domain_limit_flag;
	@ColumnMpping(name="start_data_date", desc="起始时间", nullable=true, precision=0, type=Types.DATE, valueReg="")
	private String start_data_date;
	@ColumnMpping(name="end_data_date", desc="结束时间", nullable=true, precision=0, type=Types.DATE, valueReg="")
	private String end_data_date;
	private List<SourceTable> sourceTableList;
    private List<Url> urlList;
    private JobSchedule jobSchedule;
	public List<SourceTable> getSourceTableList() {
		return sourceTableList;
	}

	public void setSourceTableList(List<SourceTable> sourceTableList) {
		this.sourceTableList = sourceTableList;
	}
	
	public List<Url> getUrlList() {
		return urlList;
	}

	public void setUrlList(List<Url> urlList) {
		this.urlList = urlList;
	}

	public JobSchedule getJobSchedule() {
		return jobSchedule;
	}

	public void setJobSchedule(JobSchedule jobSchedule) {
		this.jobSchedule = jobSchedule;
	}

	public int getMax_threads() {
		return max_threads;
	}

	public void setMax_threads(int max_threads) {
		this.max_threads = max_threads;
	}

	public int getMax_fetch_depth() {
		return max_fetch_depth;
	}

	public void setMax_fetch_depth(int max_fetch_depth) {
		this.max_fetch_depth = max_fetch_depth;
	}

	public int getDomain_limit_flag() {
		return domain_limit_flag;
	}

	public void setDomain_limit_flag(int domain_limit_flag) {
		this.domain_limit_flag = domain_limit_flag;
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
