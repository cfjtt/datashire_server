package com.eurlanda.datashire.adapter;

import com.eurlanda.datashire.entity.Column;
import com.eurlanda.datashire.entity.SourceTable;
import com.eurlanda.datashire.entity.operation.ColumnInfo;
import com.eurlanda.datashire.entity.operation.DataEntity;
import com.eurlanda.datashire.entity.operation.ResultSets;
import com.eurlanda.datashire.entity.operation.WhereCondition;
import com.eurlanda.datashire.utility.ReturnValue;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * 外部数据库统一操作接口。用来获取外部数据库metadata
 * 
 * @author eurlanda
 * @version 1.0
 * @created 16-八月-2013 21:34:07
 */
public interface IDBAdapter{
	/**
	 * <p>
	 * 作用描述：创建表
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param tableName
	 *@param columns
	 *@param out
	 *@return
	 */
	@Deprecated
	public boolean createTable(String tableName,List<Column> columns,ReturnValue out);
	
	
	
	/**
	 * <p>
	 * 作用描述：删除表
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param name
	 *@param out
	 */
	@Deprecated
	public void dropTable(String name, ReturnValue out);
	
	
	/**
	 * <p>
	 * 作用描述：插入记录
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param tableName
	 *@param paramValue
	 *@param out
	 *@return
	 */
	@Deprecated
	public boolean insert(String tableName, List<DataEntity> paramValue, ReturnValue out);
	
	/**
	 * <p>
	 * 作用描述：修改记录
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param tableName
	 *@param paramValue
	 *@param whereCondList
	 *@param out
	 *@return
	 */
	@Deprecated
	public boolean update(String tableName, List<DataEntity> paramValue, List<WhereCondition> whereCondList, ReturnValue out);
	
	
	/**
	 * <p>
	 * 作用描述：删除记录
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param tableName
	 *@param whereCondList
	 *@param out
	 *@return
	 */
	@Deprecated
	public boolean delete(String tableName, List<WhereCondition> whereCondList, ReturnValue out);

	
	/**
     * <p>
     * 作用描述：批量创建
     * </p>
     * <p>
     * 修改说明：
     * </p>
     *@param tableName
     *@param paramList
     *@param out
     *@return
     */
    public boolean createBatch(String tableName,List<List<DataEntity>> paramList, ReturnValue out);
	
	/**
	 * <p>
	 * 作用描述：批量修改
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param tableNames
	 *@param whereCondList
	 *@param out
	 *@return
	 */
	//public boolean deleteBatch(List<String> tableNames,List<List<WhereCondition>> whereCondList, ReturnValue out);
	
	/**
	 * <p>
	 * 作用描述：批量删除
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param tableName
	 *@param paramValue
	 *@param whereCondList
	 *@param out
	 *@return
	 */
    @Deprecated
	public boolean updatBatch(String tableName,List<List<DataEntity>> paramValue, List<List<WhereCondition>> whereCondList, ReturnValue out);
	

	/**
	 * <p>
	 * 作用描述：提交事务，关闭Adapter
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 */
	public void commitAdapter();
	
	/**
	 * 释放资源，关闭连接
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 */
	public void closeAdapter();
	/**
	 * 事务回滚
	 * @throws SQLException
	 */
	public void rollback() throws SQLException;
	/**
	 * <p>
	 * 作用描述：重置连接
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 */
	
	@Deprecated
	public boolean resetAdapter();
	
	/**
	 * Returns true if the connection has not been closed and is still valid.  
	 * The driver shall submit a query on the connection or use some other 
	 * mechanism that positively verifies the connection is still valid when 
	 * this method is called.
	 * <p>
	 * The query submitted by the driver to validate the connection shall be 
	 * executed in the context of the current transaction.
	 * 
	 * @param timeout -		The time in seconds to wait for the database operation 
	 * 						used to validate the connection to complete.  If 
	 * 						the timeout period expires before the operation 
	 * 						completes, this method returns false.  A value of 
	 * 						0 indicates a timeout is not applied to the 
	 * 						database operation.
	 * <p>
	 * @return true if the connection is valid, false otherwise
         * @exception SQLException if the value supplied for <code>timeout</code> 
         * is less then 0
         * @since 1.6
	 * <p>
	 * @see java.sql.DatabaseMetaData#getClientInfoProperties
	 */
	@Deprecated
	boolean isValid(int timeout) throws SQLException;
	
	/**
	 * <p>
	 * 作用描述：查询表记录
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param sql sql字符串
	 *@param out
	 *@return
	 */
	@Deprecated
	public List<Map<String, Object>> query(String sql, ReturnValue out);
	
	
	/**
	 *  查询表记录  
	 * @param columns 列集合
	 * @param tableName 表名
	 * @param whereCond 条件
	 * @param out 异常结果返回
	 * @return List集合返回
	 */
	@Deprecated
	public List<Map<String, Object>> query(List<String> columns,
			String tableName, List<WhereCondition> whereCond, ReturnValue out);
	
	
	/**
	 *  查询表记录  
	 * @param columns 列集合
	 * @param tableName 表名
	 * @param whereCond 条件
	 * @param out 异常结果返回
	 * @return List集合返回
	 */
	@Deprecated
   public Map<String, Object> queryRecord(List<String> columns,
			String tableName, List<WhereCondition> whereCond, ReturnValue out);	
   
   
	/**
	 * 查询前面某条记录
	 * 
	 * @param columns列的集合
	 * @param name  表名
	 * @param value 值
	 * @return Map<String, Object>多条数据
	 
	public Map<String, Object> queryOneRecord(List<String> columns,
			String name, ReturnValue out);
*/
	
	/**
	 * <p>
	 * 作用描述：通用执行SQL串，可用于操作插入数据，修改，删除
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param sql
	 *@param value
	 *@return
	 */
	public int executeSQL(String sql, ReturnValue value);
	
	/**
	 * 获得列信息
	 * @param name
	 * @param out
	 * @return
	 */
	@Deprecated
	public List<ColumnInfo> getColums(String name, ReturnValue out);
	
	
	/**
	 * <p>
	 * 作用描述：获取所有表明
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param out
	 *@return
	 */
	@Deprecated
	public SourceTable[] getTables(ReturnValue out,String filter);
	
	
	/**
	 * 
	
	public ResultSets queryResultSets(List<String> columns,
			String tableName, List<WhereCondition> whereCond, ReturnValue out);
 */
	//public ResultSets selectResultSets(String sql, ReturnValue out);
	
	//public ResultSets selectResultSets(String sql,Object[] objects, ReturnValue out);
	/**
	 * 获得所有列信息
	 */
	//public ColumnInfo[] getColums(String name, ReturnValue out);


	/**
	 * 查询表
	 */
	//public BasicTableInfo[] getTables(ReturnValue out);
	/**
	 * 根据schema查询表
	 * @param schema 架构
	 * @param value
	 * @return
	 */
	//public BasicTableInfo[] getTables(String schema, ReturnValue out);

	/**
	 * 查询视图
	 */
	//public BasicTableInfo[] getViews(ReturnValue out);
	
	/**
	 * 根据Schema查询视图
	 * @param dataBase 数据库名
	 * @param schema架构
	 * @param value
	 * @return
	 */
	//public BasicTableInfo[] getViews(String schema, ReturnValue out);
	
	/**
	 * 是否有效
	 */
	//public boolean isValid(ReturnValue value);
	


	/**
	 * 查询一条记录
	 * 
	 * @param columns列的集合
	 * @param name  表名
	 * @param value 值
	 * @return Map<String, Object>多条数据
	 */
	//public Map<String, Object> queryOneRecord(List<String> columns,
	//		String name, ReturnValue out);

	/**
	 * 查询多条数据记录
	 * @param num 数值
	 * @param name  表名
	 * @param out 异常结果返回
	 * @return
	 */
	//public List<Map<String, Object>> queryTopNRecord(int recordCount,
	//		String name, ReturnValue out);

	/**
	 * 从一个表中，查询记录
	 * 
	 * @param columns 列集合
	 * @param tableName 表名
	 * @param whereCond 条件
	 * @param out 异常结果返回
	 */
	//public Map<String, Object> queryOneRecord(List<String> columns,
	//		String tableName, List<WhereCondition> whereCond, ReturnValue out);

	/**
	 * 查询多少条
	 * @param recordCount 数量
	 * @param tableName 表名
	 * @param whereCond 条件
	 * @param out 异常结果返回
	 * @return
	 */
	//public List<Map<String, Object>> queryTopNRecord(int recordCount,
	//		String tableName, List<WhereCondition> whereCond, ReturnValue value) ;

	/**
	 * 查询指定数据表数据
	 * @param recordCount  数据值
	 * @param tableName  表名
	 * @param sort  排序值
	 * @param whereCond 条件
	 * @param out 异常结果返回
	 * @return
	 */
	//public List<Map<String, Object>> queryTopNRecord(int recordCount,
	//		String tableName, String sort, List<WhereCondition> whereCond,
	//		ReturnValue out);
	
	/**
	 * 查询指定记录表数据
	 * @param recordCount 数值
	 * @param tableName 表名
	 * @param sort 排序值
	 * @param sortType 排序类型
	 * @param whereCond 条件
	 * @param  out 异常结果返回
	 * @return
	 */
	//public List<Map<String, Object>> queryTopNRecord(int recordCount,List<String> columns,
	//		String tableName, String sort, int sortType,
	//		List<WhereCondition> whereCond, ReturnValue out) ;
	
	/**
	 * 查询指定记录表数据
	 * @param recordCount 数值
	 * @param tableName 表名
	 * @param sort 排序值
	 * @param sortType 排序类型
	 * @param whereCond 条件
	 * @param  out 异常结果返回
	 * @return
	 */
	//public List<Map<String, Object>> queryTopNRecord(int recordCount,
	//		String tableName, String sort, int sortType,
	//		List<WhereCondition> whereCond, ReturnValue out);
	/**
	 * 列数据处理方法
	 * @param tableName表名
	 * @param out 异常结果返回
	 * @param datas 数据集合
	 * @param result 结果
	 * @param columns列集合
	 */
	//public void columnsData(String tableName, ReturnValue out,
			//		List<String> datas,  List<String> columns);
	

	
	
	/**
	 * 语句查询
	 * @param columns 列集合
	 * @param tableName 表名
	 * @param whereCond 条件
	 * @param out 异常结果返回
	 * @return List集合返回
	 */
	//public ResultSets queryResultSets(List<String> columns,
	//		String tableName, List<WhereCondition> whereCond, ReturnValue out);
	
	/**
	 * 语句查询
	 * @param columns 列集合
	 * @param tableName 表名
	 * @param whereCond 条件
	 * @param out 异常结果返回
	 * @return List集合返回
	 */
	//public ResultSets queryResultSets(List<String> columns,
			//		String tableName, List<WhereCondition> whereCond, Integer page,ReturnValue out);
	
	/**
	 * 查询
	 * @param columns
	 * @param tableName
	 * @param whereCond
	 * @param out
	 * @return
	 */
	//public ResultSets select(List<String> columns,
	//		String tableName, List<WhereCondition> whereCond, ReturnValue out);

	/**
	 * 查询数据库名
	 * @param out 异常结果返回
	 * @return String数组
	 */
	//public String[] queryDataBase(ReturnValue out);

	/**
	 * 转换成列实体类
	 * @param map 集合
	 * @return ColumnInfo
	 */
	//public ColumnInfo toColumnInfo(Map<String, Object> map);
	
	/**
	 * 创建数据库基础信息,创建新的数据仓库，移植到{@link IRelationalDataManager}
	 * @return 结果
	 * @throws Exception 异常抛出
	 */
	@Deprecated
	public boolean createRepository(ReturnValue value);

	/**
	 * 获取指定表数据
	 * @param currPage
	 * @param pageSize
	 * @param tableName
	 * @return
	 */
	@Deprecated
	ResultSets queryAll(int currPage, int pageSize, String tableName);



	
	
}