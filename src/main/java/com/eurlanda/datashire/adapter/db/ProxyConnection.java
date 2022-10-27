package com.eurlanda.datashire.adapter.db;

import cn.com.jsoft.jframe.utils.dbcp.ConnectionImpl;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Executor;

/**
 * 连接池connection
 * 
 * @date 2014-1-17
 * @author jiwei.zhang
 * 
 */
public class ProxyConnection extends ConnectionImpl {
	private boolean keepOpen = false;
	private Logger log = Logger.getLogger(ProxyConnection.class);

	public ProxyConnection(Connection arg0) {
		super(arg0);
	}
	/**
	 * 创建一个代理connection
	 * @param conn connection
	 * @param keepOpen 是否保持不关闭，如果设置为true，连接始终会被保持，除非调用realClose方法。
	 */
	public ProxyConnection(Connection conn,boolean keepOpen){
		super(conn);
		this.keepOpen=keepOpen;
	}

	@Override
	public void commit() throws SQLException {
		super.commit();
	}

	@Override
	public void rollback() throws SQLException {
		super.rollback();
	}

	@Override
	public void setAutoCommit(boolean arg0) throws SQLException {
		super.setAutoCommit(arg0);
	}

	@Override
	public void close() throws SQLException {
		if (this.keepOpen==false) {
			realClose();
		} else {
			log.debug("连接被设置为keepOpen,跳过关闭");
		}
	}

	/**
	 * 真正关闭一个连接
	 * 
	 * @date 2014-1-17
	 * @author jiwei.zhang
	 * @throws SQLException
	 */
	public void realClose() throws SQLException {
		super.close();
	}

	@Override
	public void setSchema(String schema) throws SQLException {
		this.realConn.setSchema(schema);
	}

	@Override
	public String getSchema() throws SQLException {
		return this.realConn.getSchema();
	}

	@Override
	public void abort(Executor executor) throws SQLException {
		this.realConn.abort(executor);
	}

	@Override
	public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
		this.realConn.setNetworkTimeout(executor, milliseconds);
	}

	@Override
	public int getNetworkTimeout() throws SQLException {
		return this.realConn.getNetworkTimeout();
	}

}
