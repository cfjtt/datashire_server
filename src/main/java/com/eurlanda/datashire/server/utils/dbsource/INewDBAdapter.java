package com.eurlanda.datashire.server.utils.dbsource;

import com.eurlanda.datashire.entity.SquidIndexes;
import com.eurlanda.datashire.entity.operation.*;
import com.eurlanda.datashire.server.utils.entity.operation.BasicTableInfo;
import com.eurlanda.datashire.server.utils.entity.operation.TableForeignKey;
import com.eurlanda.datashire.server.utils.entity.operation.TableIndex;
import com.eurlanda.datashire.server.utils.entity.operation.TablePrimaryKey;
import com.eurlanda.datashire.server.utils.entity.operation.*;
import com.eurlanda.datashire.server.utils.entity.operation.ColumnInfo;
import com.eurlanda.datashire.utility.ReturnValue;
import com.eurlanda.datashire.utility.objectsql.ObjectSQL;
import org.apache.thrift.TException;

import java.util.List;
import java.util.Map;

/**
 * DBAdapter新接口,实现数据库元数据获取和基本数据操作
 * 
读取表定义
读取列定义
设置表存储分区
设置表空间/文件组
设置表索引
设置表外键
设置表主键
基本数据操作（增删改查）
 * @date 2014-1-10
 * @author jiwei.zhang
 *
 */
public interface INewDBAdapter {
	/**
	 * 查询一个列表
	 * @date 2014-1-10
	 * @author jiwei.zhang
	 * @param sql
	 * @return
	 */
	List<Map<String,Object>> queryForList(ObjectSQL sql, Map<String, String> map);
	List<Map<String,Object>> queryForList(String sql);
	/**
	 * 查询一条记录
	 * @date 2014-1-10
	 * @author jiwei.zhang
	 * @param sql
	 * @return
	 */
	Map<String,Object> queryForMap(ObjectSQL sql, Map<String, String> map);
	Map<String,Object> queryForMap(String sql, Map<String, String> map);
	/**
	 * 查询一个数字
	 * @date 2014-1-10
	 * @author jiwei.zhang
	 * @param sql
	 * @return
	 */
	Integer queryForInt(ObjectSQL sql);

	/**
	 * 删除一张表
	 * @date 2014-1-17
	 * @author jiwei.zhang
	 * @param tableName 表明
	 */
	void  deleteTable(String tableName);
	/**
	 * 删除一张表
	 */
	boolean deleteMongoTable(String tableName);
	/**
	 * 取表中数据数量。
	 * @date 2014-1-23
	 * @author jiwei.zhang
	 * @return
	 */
	Integer getRecordCount(String tableName);
	/**
	 * 查询数据库的所有表
	 * @date 2014-1-10
	 * @author jiwei.zhang
	 * @return 数据库所有的表名
	 */
	List<BasicTableInfo> getAllTables(String filter);
	/**
	 * 查询数据库的所有表
	 * @date 2014-1-10
	 * @author jiwei.zhang
	 * @return 数据库所有的表名
	 */
	public List<BasicTableInfo> getAllTables(String filter, String schema) throws Exception;

	/**
	 * 取得数据库所有的列定义
	 * @date 2014-1-10
	 * @author jiwei.zhang
	 * @param tableName 表明
	 * @return 当前表的所有列
	 */
	List<ColumnInfo> getTableColumns(String tableName, String databaseName) throws TException;
	/**
	 * 创建一个表。
	 * @date 2014-1-15
	 * @author jiwei.zhang
	 * @param table
	 */
	void createTable(BasicTableInfo table);

	/**
	 * 作用描述:通过sql语句创建落地表
	 * @param sql
	 * @param out
	 * @date 2016_04_19
	 */
	String createTable(String sql, ReturnValue out);


	/**
	 * 创建一个表。
	 * @date 2014-1-15
	 * @author jiwei.zhang
	 * @param table
	 */
	void createTable(BasicTableInfo table, ReturnValue out);
	/**
	 * 删除一个表下的列
	 * @date 2014-1-17
	 * @author jiwei.zhang
	 * @param tableName 表名
	 * @param columnName 列名
	 */
	void deleteTableColumn(String tableName, String columnName);
	/**
	 * 添加一个表列
	 * @date 2014-1-17
	 * @author jiwei.zhang
	 * @param tableName 表名
	 * @param col 列定义
	 */
	void addTableColumn(String tableName, ColumnInfo col);
	/**
	 * 更改列定义
	 * @date 2014-1-17
	 * @author jiwei.zhang
	 * @param tableName 表名
	 * @param oldCol 待修改的列名
	 * @param newCol 新列定义
	 */
	void modifyTableColumn(String tableName, String oldCol, ColumnInfo newCol);
	/**
	 * 添加一个外键
	 * @date 2014-1-18
	 * @author jiwei.zhang
	 * @param foreignKey 外键。
	 */
	void addForeignKey(String tableName, TableForeignKey foreignKey);
	/**
	 * 添加主键
	 * @date 2014-1-20
	 * @author jiwei.zhang
	 * @param tableName 表名
	 * @param primaryKey 主键
	 */
	void addPrimaryKey(String tableName, TablePrimaryKey primaryKey);
	/**
	 * 添加一个索引。
	 * @date 2014-1-20
	 * @author jiwei.zhang
	 * @param tableName 表名
	 * @param index 索引名
	 */
	void addIndex(String tableName, TableIndex index);

	List<SquidIndexes> selectTableIndex(String tableName);
	/**
	 * 删除一个外键
	 * @date 2014-1-18
	 * @author jiwei.zhang
	 * @param tableName 表名
	 * @param keyName 外键名
	 */
	void deleteForeignKey(String tableName, String keyName);
	/**
	 * 删除一个主键
	 * @date 2014-1-20
	 * @author jiwei.zhang
	 * @param tableName 表名
	 * @param keyName 主键名称
	 */
	void deletePrimaryKey(String tableName, String keyName);
	/**
	 * 删除一个索引。
	 * @date 2014-1-20
	 * @author jiwei.zhang
	 * @param tableName 表名
	 * @param indexName 索引名称
	 */
	void deleteIndex(String tableName, String indexName);
	/**
	 * 删除一个索引。
	 * @date 2014-1-20
	 * @author jiwei.zhang
	 * @param tableName 表名
	 * @param indexName 索引名称
	 */
	void deleteIndex(String tableName, TableIndex index);
	/**
	 * 执行操作
	 */
	int executeUpdate(String sql);
	/**
	 * 执行操作
	 */
	int executeUpdate(String sql, Object... sqlArgs);
	/**
	 * 执行sql语句返回Map
	 * @param sql
	 * @return
	 */
	public Map<String, Object> queryForMap2(String sql, Map<String, String> map);
	/**
	 * 开始事务
	 * @date 2014-1-23
	 * @author jiwei.zhang
	 */
	void beginTransaction();
	/**
	 * 提交事务
	 * @date 2014-1-23
	 * @author jiwei.zhang
	 */
	void commit();
	/**
	 * 回滚事务
	 * @date 2014-1-23
	 * @author jiwei.zhang
	 */
	void rollback();
	/**
	 * 关闭adapter,创建的connection会自动关闭。
	 * @date 2014-1-23
	 * @author jiwei.zhang
	 */
	void close();

	List<SquidIndexes> getTableIndexes(String tableName, String dataBaseName);
	/**
	 *
	 * 返回buildColumnSql方法 ，方便其他给其他方法调用   todo---
	 * @date 2016_04_14
	 * @author dzp
	 */
	String getSQLForTable(BasicTableInfo table, ReturnValue out);
}
