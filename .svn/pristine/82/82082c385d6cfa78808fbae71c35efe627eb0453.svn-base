package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;

import java.sql.Types;

@MultitableMapping(pk="ID", name = "DS_URL", desc = "")
public class Url {
	@ColumnMpping(name="id", desc="", nullable=false, precision=0, type=Types.INTEGER, valueReg=">=1")
	   private int id;
	@ColumnMpping(name="squid_id", desc="", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	   private int squid_id;
	@ColumnMpping(name="url", desc="", nullable=true, precision=500, type=Types.VARCHAR, valueReg="")
	   private String url;
	@ColumnMpping(name="user_name", desc="", nullable=true, precision=50, type=Types.VARCHAR, valueReg="")
	   private String user_name;
	@ColumnMpping(name="password", desc="", nullable=true, precision=50, type=Types.VARCHAR, valueReg="")
	   private String password;
	@ColumnMpping(name="max_fetch_depth", desc="", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	   private int max_fetch_depth;
	@ColumnMpping(name="filter", desc="", nullable=true, precision=200, type=Types.VARCHAR, valueReg="")
	   private String filter;
	@ColumnMpping(name="domain", desc="", nullable=true, precision=50, type=Types.VARCHAR, valueReg="")
	   private String domain;
	@ColumnMpping(name="domain_limit_flag", desc="", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	   private int domain_limit_flag;
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
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
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
	public int getMax_fetch_depth() {
		return max_fetch_depth;
	}
	public void setMax_fetch_depth(int max_fetch_depth) {
		this.max_fetch_depth = max_fetch_depth;
	}
	public String getFilter() {
		return filter;
	}
	public void setFilter(String filter) {
		this.filter = filter;
	}
	public String getDomain() {
		return domain;
	}
	public void setDomain(String domain) {
		this.domain = domain;
	}
	public int getDomain_limit_flag() {
		return domain_limit_flag;
	}
	public void setDomain_limit_flag(int domain_limit_flag) {
		this.domain_limit_flag = domain_limit_flag;
	}
	
}
