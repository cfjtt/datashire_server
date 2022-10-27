package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.enumeration.DataBaseType;
import com.eurlanda.datashire.enumeration.NoSQLDataBaseType;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.utility.SysConf;

/**
 * 
 * @date 2014-1-10
 * @author jiwei.zhang
 *
 */
public class DBConnectionInfo {

	private String dbName;
	private String host;
	private String password;
	private String userName;
	private DataBaseType dbType;
	private NoSQLDataBaseType noSQLDataBaseType;
	private String connectionString;
	private Boolean isNoSql;
	private int port;
	private String keyspace;
	private String cluster;
	private int verification;

	public String getKeyspace() {
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
	}

	public NoSQLDataBaseType getNoSQLDataBaseType() {
		return noSQLDataBaseType;
	}

	public void setNoSQLDataBaseType(NoSQLDataBaseType noSQLDataBaseType) {
		this.noSQLDataBaseType = noSQLDataBaseType;
	}

	public String getDbName() {
		return dbName;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
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

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public DataBaseType getDbType() {
		return dbType;
	}

	public void setDbType(DataBaseType dbType) {
		this.dbType = dbType;
	}

	public int getPort() {
		return port;
	}

	public Boolean getIsNoSql() {
		return isNoSql;
	}

	public void setIsNoSql(Boolean isNoSql) {
		this.isNoSql = isNoSql;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public int getVerification() {
		return verification;
	}

	public void setVerification(int verification) {
		this.verification = verification;
	}

	/**
	 * 根据DBSquid生成一个连接信息。
	 * @date 2014-1-10
	 * @author jiwei.zhang
	 * @param ds
	 * @return
	 */
	public static DBConnectionInfo fromDBSquid(DbSquid ds){
		DBConnectionInfo info = new DBConnectionInfo();
		info.setDbName(ds.getDb_name());
		info.setDbType(DataBaseType.parse(ds.getDb_type()));
		if(ds.getSquid_type()== SquidTypeEnum.TRAININGDBSQUID.value()){
			if(SysConf.getValue("train_db_host").equalsIgnoreCase(ds.getHost())){
				ds.setHost(SysConf.getValue("train_db_real_host"));
			}
		}
		info.setHost(ds.getHost());
		info.setPassword(ds.getPassword());
		info.setPort(ds.getPort());
		info.setUserName(ds.getUser_name());
		info.setIsNoSql(false);
		return info;
	}

	/**
	 * 根据DBSquid生成一个连接信息。
	 * @date 2014-1-10
	 * @author jiwei.zhang
	 * @param ds
	 * @return
	 */
	public static DBConnectionInfo fromNOSQLSquid(NOSQLConnectionSquid ds){
		DBConnectionInfo info = new DBConnectionInfo();
		info.setDbName(ds.getDb_name());
		info.setNoSQLDataBaseType(NoSQLDataBaseType.parse(ds.getDb_type()));
		info.setHost(ds.getHost());
		info.setPassword(ds.getPassword());
		info.setPort(ds.getPort());
		info.setUserName(ds.getUser_name());
		info.setIsNoSql(true);
		return info;
	}
	/**
	 * 将连接信息转换为DbSquid
	 * @date 2014-1-10
	 * @author jiwei.zhang
	 * @return
	 */
	public DbSquid toDBSquid(){
		DbSquid info = new DbSquid();
		info.setDb_name(this.getDbName());
		info.setDb_type(this.getDbType().value());
		info.setHost(this.getHost());
		info.setPassword(this.getPassword());
		info.setPort(this.getPort());
		info.setUser_name(this.getUserName());
		return info;
	}

	public String getConnectionString() {
		return connectionString;
	}

	public void setConnectionString(String connectionString) {
		this.connectionString = connectionString;
	}
	
}
