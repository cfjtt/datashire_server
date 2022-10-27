package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;

import java.sql.Types;
import java.util.List;

/**
 * 
 * 
 * <p>
 * Title : 第三方链接Squid
 * </p>
 * <p>
 * Description: 
 * </p>
 * <p>
 * Author :周益 2014-11-10
 * </p>
 * <p>
 * update :周益 2014-11-10
 * </p>
 * <p>
 * Department :  JAVA后端研发部
 * </p>
 * Copyright : ©2014-2015 悦岚（上海）数据服务有限公司
 * </p>
 */
@MultitableMapping(pk="", name = {"DS_SQUID"}, desc = "")
public class HttpSquid extends Squid {
	
	@ColumnMpping(name="host", desc="主机名或者IP地址", nullable=true, precision=100, type=Types.VARCHAR, valueReg="")
	private String host;
	@ColumnMpping(name="port", desc="端口号", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private int port;
	
	private List<SourceTable> sourceTableList;
	
	public List<SourceTable> getSourceTableList() {
		return sourceTableList;
	}

	public void setSourceTableList(List<SourceTable> sourceTableList) {
		this.sourceTableList = sourceTableList;
	}

	public HttpSquid(){
	}
	
	public HttpSquid(String host, int port) {
		super();
		this.host = host;
		this.port = port;
	}
	public String getHost() {
		return host;
	}
	public void setHost(String host) {
		this.host = host;
	}
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
}