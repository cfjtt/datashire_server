/*
 * DataManagerInterface.java - 2013-10-2
 *
 * 版权声明: (c) 2013，悦岚（上海）数据服务有限公司，保留所有权利。
 *
 * 项目名称: datashire-server
 * 
 * 修改历史:
 * ===========================================
 *   修改人	      日期		      描述
 *   ---------------------------------------
 *   dang.lu  2013-10-2   create    
 * ===========================================
 */
package com.eurlanda.datashire.adapter;

import com.eurlanda.datashire.entity.Column;
import com.eurlanda.datashire.utility.DatabaseException;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * Datashire-server传统关系型数据操作接口
 * 
 * 	1. 实体类增删查改(通过注解实现对实体类属性和数据库表字段的自动关系映射)
 *  2. 通用SQL查询、更新(支持批量更新)操作
 *  3. 查询分页支持
 *  
 * @author dang.lu 2013-10-2
 *
 */
public interface IRelationalDataManager {
	
	/** The load factor used when none specified in constructor. */
	static final float DEFAULT_LOAD_FACTOR = 0.75f;
	
//////////////////// << 以下接口为数据库连接及事务控制 >> /////////////////////////
	/**
	 * 开启数据库连接
	 * 需要手工关闭连接，仅供更新操作显式使用事务
	 * (因为查询类操作不需要使用事务)
	 */
	void openSession();
	// 获取Connection
	public Connection getConnection();
	/**如果开启事务，则数据库连接打开、关闭和事务提交回滚需要调用函数自己处理;否则每一步操作都将是一个单独的事务*/
	void openSession(boolean withSession);
	
	/** 关闭数据库连接，并提交事务 Quietly */
	void closeSession();
	
	/** 提交事务 */
	void commit() throws SQLException;
	
	/** 回滚事务 */
	void rollback() throws SQLException;
	
	
//////////////////// << 以下接口为通用DML数据更新操作 >> /////////////////////////	
	/**
	 * 数据更新/删除
	 * @param sql
	 * @return
	 * @throws SQLException
	 */
	int execute(String sql) throws DatabaseException;
	
	/**
	 * 数据更新/删除
	 * @param sql
	 * @param params
	 * @return
	 * @throws SQLException
	 */
	int execute(String sql, List<Object> params) throws SQLException;
	
	/**
	 * 数据批量更新/删除
	 * @param sql		这是一条SQL
	 * @param params	这是一批参数
	 * @return rows updated
	 * @throws SQLException
	 */
	int executeBatch(String sql, List<List<Object>> params) throws SQLException;

	/**
	 * 查询多条信息
	 * @param sql
	 * @param params
	 * @return
	 * @throws SQLException
	 */
	public List<Map<String, Object>> query2List(String sql, List<Object> params) throws SQLException;
	
	/**
	 * 查询一条信息
	 * @param sql
	 * @param params
	 * @return
	 * @throws SQLException
	 */
	public Map<String, Object> query2Object(String sql, List<Object> params) throws SQLException;
	public Map<String, Object> query2Object(boolean inSession, String sql, List<Object> params) throws SQLException;
	public Map<String, Object> query2Object4(boolean inSession, String sql, List<Object> params) throws SQLException;
	public List<Map<String, Object>> query2List(boolean inSession, String sql, List<Object> params) throws SQLException;
	public List<Map<String, Object>> query2List4(boolean inSession, String sql, List<Object> params) throws SQLException;
	
	/**
	 * 删除
	 * @param params
	 * @param c
	 * @return rows removed
	 * @throws SQLException
	 */
	int delete(Map<String, String> params, String tableName) throws SQLException;
	
	
//////////////////// 特别说明：以下接口仅适用于内部系统元数据  /////////////////////////
	/**
	 * 查询(单)对象列表
	 * 	1.支持列类型转换 Timestamp->String; char(1)->boolean [true:'Y', false:'N']
	 * 	2.支持列别名映射到对象属性
	 * 	3.支持JOIN等复杂语句（自己写）
	 * @param sql
	 * @param params
	 * @param c
	 * @return List<T>
	 * @throws SQLException
	 */
	<T>List<T> query2List(String sql, List<String> params, Class<T> c);
	<T>List<T> query2List(boolean inSession, String sql, List<String> params, Class<T> c);
	
	/**
	 * 单表集合查询
	 * 	1. inSession 是否在当前事务中，否则可能会锁表;（true：数据库连接开启和关闭需要由调用函数自行处理）
	 *  2. 支持单值、集合参数 (column=?, column in a list)
	 * @param params
	 * @param c
	 * @return
	 * @throws SQLException
	 */
	<T>List<T> query2List(Map<String, String> params, Class<T> c);
	@Deprecated
	<T>List<T> query2List(boolean inSession, Map<String, String> params, Class<T> c);
	<T>List<T> query2List2(boolean inSession, Map<String, Object> params, Class<T> c);
	<T>List<T> query2List3(boolean inSession, Map<String, String> params, Class<T> c);
	
	/**
	 * 单表单对象查询
	 * @param params
	 * @param c
	 * @return
	 * @throws SQLException
	 */
	public <T> T query2Object(Map<String, String> params, Class<T> c);
	public <T> T query2Object(boolean inSession, Map<String, Object> params, Class<T> c);
	public <T> T query2Object2(boolean inSession, Map<String, String> params, Class<T> c);
	
	/**
	 * 新增(支持实体类和表对应关系一对多)
	 * 		数字类型值是0(且要求数据是大于0)的则新增时忽略
	 *  
	 * @param obj 实体类（需要JDK1.4以上注解支持）
	 * @return 新增对象ID(根据key获得，如果key有重复则返回MAX(id)); 0/-1表示新增失败或异常
	 * @author dang.lu 2013.9.30
	 */
	public int insert2(Object obj) throws SQLException;
	/**
	 * 新增(支持实体类和表对应关系一对多)
	 * @param obj
	 * @param isParms 在key为空的情况下，是否需要根据对象属性来拼接
	 * @return
	 * @throws SQLException
	 */
	public int insert2(Object obj, boolean isParms) throws SQLException;
	/**
	 * 更新
	 * 		根据ID更新对象
	 * 		数字类型值是0(且要求数据是大于0)的则更新时置空
	 * @param obj
	 * @return true|false
	 */
	public boolean update2(Object obj) throws SQLException; // V2.0 支持多表更新
	
	/**
	 * 更新
	 * 		根据ID更新对象
	 * 		数字类型值是0(且要求数据是大于0)的则更新时置空
	 * @param obj
	 * @return true|false
	 */
	public boolean update2(Object obj, List<String> otherColumn) throws SQLException; // V2.0 支持多表更新
	/**
	 * 更新
	 * 		根据ID更新对象，更新数据库表中一个多个对象
	 * @param hashmap
	 * @return true|false
	 */
	public boolean updateSomeColumn(Map<String,Object> map) throws SQLException, ClassNotFoundException, NoSuchFieldException;
	/**
	 * 删除
	 * @param params
	 * @param c
	 * @return rows removed
	 * @throws SQLException
	 */
	<T> int delete(Map<String, String> params, Class<T> c) throws SQLException;
	/**
	 * 根据表名删除
	 * @param params
	 * @param c
	 * @return
	 * @throws SQLException
	 */
	int deleteByTableName(Map<String, String> params,String tableName) throws SQLException;

	/**
	 * 查看指定表的所有列信息: 
	 * 		列名、类型、是否自增等
	 * @param tableName
	 * @return
	 * @throws SQLException
	 */
	public Map<String, Column> getColumnMetaData(String tableName) throws SQLException;
	/**
	 * 创建数据库基础信息,创建新的数据仓库.
	 * @return 结果
	 * @throws Exception 异常抛出
	 */
	public boolean createRepository();
	
    /**
     * 创建索引
     * @param indexName
     * @param tableName
     * @param indexType
     * @param columnList
     * @return
     * @throws SQLException
     * @author bo.dang
     * @date 2014年5月20日
     */
	public int insertIndex(String indexName, String tableName, int indexType, List<Column> columnList) throws SQLException;
	
	/**
	 * 删除索引
	 * @param indexName
	 * @param tableName
	 * @return
	 * @throws SQLException
	 * @author bo.dang
	 * @date 2014年5月20日
	 */
	public int dropIndex(String indexName, String tableName) throws SQLException;
	
	/**
	 * 取得对象
	 * @param inSession
	 * @param sql
	 * @param params
	 * @param c
	 * @return
	 * @throws SQLException
	 * @author bo.dang
	 * @date 2014年5月23日
	 */
    public <T> T query2Object(boolean inSession, String sql, List<String> params, Class<T> c) throws SQLException;
    
    /**
     * 调用存储过程
     * @param inSession
     * @param str
     * @return
     * @author bo.dang
     * @date 2014年6月5日
     */
    public CallableStatement prepareCall(boolean inSession, String str);
    
    /**
     * 调用存储过程
     * @param conn
     * @param str
     * @return
     * @author bo.dang
     * @date 2014年6月5日
     */
    public CallableStatement prepareCall(Connection conn, String str);
}
