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
@MultitableMapping(pk="", name = {"DS_SQUID"}, desc = "")
public class NOSQLConnectionSquid extends Squid {
	
	@ColumnMpping(name="database_name", desc="", nullable=true, precision=100, type=Types.VARCHAR, valueReg="")
	private String db_name;
	@ColumnMpping(name="host", desc="", nullable=true, precision=100, type=Types.VARCHAR, valueReg="")
	private String host;
	@ColumnMpping(name="password", desc="", nullable=true, precision=100, type=Types.VARCHAR, valueReg="")
	private String password;
	@ColumnMpping(name="user_name", desc="", nullable=true, precision=100, type=Types.VARCHAR, valueReg="")
	private String user_name;
	@ColumnMpping(name="db_type_id", desc="", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private int  db_type;
	@ColumnMpping(name="port", desc="", nullable=true, precision=100, type=Types.INTEGER, valueReg="")
	private int port;
	@ColumnMpping(name="CONNECTION_STRING", desc="", nullable=true, precision=100, type=Types.VARCHAR, valueReg="")
	private String connectionString;
	@ColumnMpping(name="STATE", desc="", nullable=true, precision=100, type=Types.INTEGER, valueReg="1")
	private int state;
	
	/**
	 * NOSQLConnectionSquid的原数据
	 */
	private List<SourceTable> sourceTableList;

	public List<SourceTable> getSourceTableList() {
		return sourceTableList;
	}

	public void setSourceTableList(List<SourceTable> sourceTableList) {
		this.sourceTableList = sourceTableList;
	}

	public String getDb_name() {
		return db_name;
	}

	public void setDb_name(String dbName) {
		db_name = dbName;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getUser_name() {
		return user_name;
	}

	public void setUser_name(String userName) {
		user_name = userName;
	}


	public int getDb_type() {
		return db_type;
	}

	public void setDb_type(int dbType) {
		db_type = dbType;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getConnectionString() {
		return connectionString;
	}

	public void setConnectionString(String connectionString) {
		this.connectionString = connectionString;
	}

	public int getState() {
		return state;
	}

	public void setState(int state) {
		this.state = state;
	}

	@Override
	public String toString() {
		return "NOSQLConnectionSquid [db_name=" + db_name + ", host=" + host + ", password="
				+ password + ", user_name=" + user_name + ", db_type="
				+ db_type + ", port=" + port + ", sourceTableList="
				+ sourceTableList + "]";
	}

}