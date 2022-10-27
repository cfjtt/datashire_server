package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.adapter.IDBAdapter;
import com.eurlanda.datashire.entity.DBSourceTable;
import com.eurlanda.datashire.entity.DbSquid;
import com.eurlanda.datashire.entity.SourceColumn;
import com.eurlanda.datashire.entity.SourceTable;
import com.eurlanda.datashire.sprint7.plug.ConnectPlug;
import com.eurlanda.datashire.utility.ReturnValue;

import java.util.List;

/**
 * connect业务逻辑实现层
 * 根据连接信息去获取数据源,然后回填DS_SQL_CONNECTION,DS_SOURCE_TABLE_CACHE,DS_COLUMN_CACHE
 * (如果输入的数据源已经存在,还需要从数据源获取到的表数据进行对比,以便判断哪些表是否被抽取)
 * @author binlei
 *
 */
public interface IConnectProcess{
/**
	 * 根据数据源，获取SourceTable
	 * 
	 * @param info
	 * @param out
	 * @return
 * @throws Exception 
	 */
	public List<SourceTable> getConnect(String info, ReturnValue out) throws Exception;
	/**
	 * 根据dbsquid获取adpter
	 * 
	 * @param dbSquid
	 * @return
	 */
	public IDBAdapter getAdapter(DbSquid dbSquid);
	/**
	 * 根据adapter获取数据源的表数据
	 * 
	 * @param dbSquid
	 * @return
	 */
	public List<DBSourceTable> getTableNames(DbSquid dbSquid,IDBAdapter adaoterSource, ReturnValue out,String filter);
	/**
	 * 根据dbsquid,表名去查询列的信息
	 * 
	 * @param dbSquid
	 * @param tableName
	 * @param out
	 * @param executeResult
	 * @return
	 */
	public List<SourceColumn> getColumn(IDBAdapter adaoterSource, String tableName,
			ReturnValue out, int sourece_table_id);
	/**
	 * 对DBSourceTable表的是否抽取字段赋值
	 * 
	 * @param dbSourceTables
	 * @param out
	 * @return
	 */
	public int updateDBSourceTable(List<DBSourceTable> dbSourceTables,
			ReturnValue out, ConnectPlug connectPlug);
	/**
	 * 封装返回的数据
	 */
	public void setSourceTable(DBSourceTable dbSourceTable,
			List<SourceColumn> sourceColumns, List<SourceTable> sourceTables);
	/**
	 * 根据source_squid_id获取DBSourceTable表信息
	 * 
	 * @param id
	 * @param out
	 * @return
	 */
	public List<DBSourceTable> getDBSourceTable(int id, ReturnValue out);
	/**
	 * 根据source_table_id获取SourceColumn表记录
	 * 
	 * @param id
	 * @param out
	 * @return
	 */
	public List<SourceColumn> getSourceColumn(int id, ReturnValue out);
	/**
	 * 根据tableName,source_squid_id查询列
	 * @param id
	 * @param out
	 * @return
	 */
	public List<SourceColumn> getSourceColumn2(String tableName,int source_squid_id, ReturnValue out);
	/**
	 * 抽取list2中有，而list1中没有的元素
	 * 
	 * @param list1
	 * @param list2
	 * @return
	 */
	public List<DBSourceTable> compareTables(List<DBSourceTable> list1,
			List<DBSourceTable> list2);
	/**
	 * 抽取list2中有，而list1中没有的元素
	 * 
	 * @param list1
	 * @param list2
	 * @return
	 */
	public List<SourceColumn> compareColumns(List<SourceColumn> list1,
			List<SourceColumn> list2);
	
}