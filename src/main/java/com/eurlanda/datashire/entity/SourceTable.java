package com.eurlanda.datashire.entity;

import java.util.Map;

/**
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
public class SourceTable {
	private int id;// 表ＩＤ

	private Map<String,Column> columnInfo; // sprint 6 ( what will be in next sprint ? )
	//private Map<String,String> columnInfo; // sprint 5
	private String tableName;//表名
	private String databaseName;//数据库名称
	private String serverName;//实例名称
	private String userName;//用户名
	private String createDate;//创建日期
	private String schema;//概要
	private String rowCount;//列数
	private boolean is_extracted;//是否被抽取
	private boolean is_directory;//是否文件夹
	private int source_squid_id;
	
	//addr yi.zhou 新ThridPartyConnection
	//wadl or http
	private String url;
	private String post_params;
	private String encoded;
	//wsdl
	private String header_xml;
	private String params_xml;
	private String templet_xml;
	private Map<String, String> returnParam;
	private int method;


	public Map<String, String> getReturnParam() {
		return returnParam;
	}
	

	public String getEncoded() {
		return encoded;
	}


	public void setEncoded(String encoded) {
		this.encoded = encoded;
	}


	public void setReturnParam(Map<String, String> returnParam) {
		this.returnParam = returnParam;
	}

	public String getWsdlRequest() {
		if (templet_xml != null) {
			String requestString = templet_xml.replace("@header;",
					this.getHeader_xml() == null ? "" : this.getHeader_xml())
					.replace(
							"@body;",
							this.getParams_xml() == null ? "" : this
									.getParams_xml());
			return requestString;
		} else {
			return null;
		}
	}

	public int getMethod() {
		return method;
	}

	public void setMethod(int method) {
		this.method = method;
	}

	public int getSource_squid_id() {
		return source_squid_id;
	}

	public void setSource_squid_id(int source_squid_id) {
		this.source_squid_id = source_squid_id;
	}

	public Map<String, Column> getColumnInfo() {
		return columnInfo;
	}

	public void setColumnInfo(Map<String, Column> columnInfo) {
		this.columnInfo = columnInfo;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public boolean isIs_extracted() {
		return is_extracted;
	}

	public void setIs_extracted(boolean is_extracted) {
		this.is_extracted = is_extracted;
	}

	public String getDatabaseName() {
		return databaseName;
	}

	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}

	public String getServerName() {
		return serverName;
	}

	public void setServerName(String serverName) {
		this.serverName = serverName;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getCreateDate() {
		return createDate;
	}

	public void setCreateDate(String createDate) {
		this.createDate = createDate;
	}

	public String getSchema() {
		return schema;
	}

	public void setSchema(String schema) {
		this.schema = schema;
	}

	public String getRowCount() {
		return rowCount;
	}

	public void setRowCount(String rowCount) {
		this.rowCount = rowCount;
	}

	public boolean isIs_directory() {
		return is_directory;
	}

	public void setIs_directory(boolean is_directory) {
		this.is_directory = is_directory;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getPost_params() {
		return post_params;
	}

	public void setPost_params(String post_params) {
		this.post_params = post_params;
	}

	public String getHeader_xml() {
		return header_xml;
	}

	public void setHeader_xml(String header_xml) {
		this.header_xml = header_xml;
	}

	public String getParams_xml() {
		return params_xml;
	}

	public void setParams_xml(String params_xml) {
		this.params_xml = params_xml;
	}

	public String getTemplet_xml() {
		return templet_xml;
	}

	public void setTemplet_xml(String templet_xml) {
		this.templet_xml = templet_xml;
	}
}