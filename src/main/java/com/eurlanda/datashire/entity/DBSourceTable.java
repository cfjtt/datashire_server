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
 * Author :赵春花 2013-9-10
 * </p>
 * <p>
 * update :赵春花 2013-9-10
 * </p>
 * <p>
 * Department :  JAVA后端研发部
 * </p>
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司
 * </p>
 */
@MultitableMapping(pk="", name = "DS_SOURCE_TABLE", desc = "")
public class DBSourceTable extends DSBaseModel{
	
	@ColumnMpping(name="table_name", desc="", nullable=true, precision=2000, type=Types.VARCHAR, valueReg="")
	private String table_name;
	@ColumnMpping(name="source_squid_id", desc="fk:DS_SQUID", nullable=true, precision=0, type=Types.INTEGER, valueReg=">=1")
	private int source_squid_id;
	@ColumnMpping(name="is_extracted", desc="是否已抽取", nullable=true, precision=1, type=Types.VARCHAR, valueReg="")
	private boolean is_extracted; // (缓存表保存)，创建时更新为true/删除时判断如果没有任何extract引用则改为false
	@ColumnMpping(name="url", desc="", nullable=true, precision=300, type=Types.VARCHAR, valueReg="")
	private String url;
	@ColumnMpping(name="method", desc="post/get/put/delete", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private int method;
	@ColumnMpping(name="templet_xml", desc="", nullable=true, precision=3000, type=Types.VARCHAR, valueReg="")
	private String templet_xml;
	@ColumnMpping(name="params_xml", desc="", nullable=true, precision=3000, type=Types.VARCHAR, valueReg="")
	private String params_xml;
	@ColumnMpping(name="header_xml", desc="", nullable=true, precision=3000, type=Types.VARCHAR, valueReg="")
	private String header_xml;
	@ColumnMpping(name="post_params", desc="", nullable=true, precision=3000, type=Types.VARCHAR, valueReg="")
	private String post_params;
	@ColumnMpping(name="encoded", desc="", nullable=true, precision=3000, type=Types.VARCHAR, valueReg="")
	private String encoded;
	@ColumnMpping(name="is_directory", desc="是否为directory", nullable=true, precision=3000, type=Types.VARCHAR, valueReg="")
	private boolean is_directory;
	//@ColumnMpping(name="keyspace", desc="", nullable=true, precision=100, type=Types.VARCHAR, valueReg="")
	//private String keyspace;
	//@ColumnMpping(name="cluster", desc="", nullable=true, precision=100, type=Types.VARCHAR, valueReg="")
	//private String cluster;

	/*public String getKeyspace() {
		return keyspace;
	}

	public void setKeyspace(String keyspace) {
		this.keyspace = keyspace;
	}

	public String getCluster() {
		return cluster;
	}

	public void setCluster(String cluster) {
		this.cluster = cluster;
	}*/

	private List<SourceColumn> sourceColumns;
	
	public String getTemplet_xml() {
		return templet_xml;
	}

	public void setTemplet_xml(String templet_xml) {
		this.templet_xml = templet_xml;
	}

	public String getParams_xml() {
		return params_xml;
	}

	public String getEncoded() {
		return encoded;
	}

	public void setEncoded(String encoded) {
		this.encoded = encoded;
	}

	public void setParams_xml(String params_xml) {
		this.params_xml = params_xml;
	}

	public String getHeader_xml() {
		return header_xml;
	}

	public void setHeader_xml(String header_xml) {
		this.header_xml = header_xml;
	}

	public String getPost_params() {
		return post_params;
	}

	public void setPost_params(String post_params) {
		this.post_params = post_params;
	}

	public DBSourceTable() {
	}
	
	public DBSourceTable(int id, String tableName, int source_squid_id) {
		this.id = id;
		this.table_name = tableName;
		this.source_squid_id = source_squid_id;
	}

	public String getTable_name() {
		return table_name;
	}
	public void setTable_name(String tableName) {
		table_name = tableName;
	}

	public int getSource_squid_id() {
		return source_squid_id;
	}

	public void setSource_squid_id(int source_squid_id) {
		this.source_squid_id = source_squid_id;
	}

	public boolean isIs_extracted() {
		return is_extracted;
	}

	public void setIs_extracted(boolean is_extracted) {
		this.is_extracted = is_extracted;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public int getMethod() {
		return method;
	}

	public void setMethod(int method) {
		this.method = method;
	}

	public List<SourceColumn> getSourceColumns() {
		return sourceColumns;
	}

	public void setSourceColumns(List<SourceColumn> sourceColumns) {
		this.sourceColumns = sourceColumns;
	}

	@Override
	public String toString() {
		return "DBSourceTable [table_name=" + table_name + ", source_squid_id="
				+ source_squid_id + ", id=" + id + "]";
	}

	public boolean isIs_directory() {
		return is_directory;
	}

	public void setIs_directory(boolean is_directory) {
		this.is_directory = is_directory;
	}
}
