package com.eurlanda.datashire.sprint7.plug;

import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.entity.DBSourceTable;
import com.eurlanda.datashire.entity.DbSquid;
import com.eurlanda.datashire.entity.SourceColumn;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.utility.DatabaseException;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.util.List;

/**
 * connect数据交互处理类
 * @author binlei
 *
 */
public class ConnectPlug {
	/**
	 * 对ConnectPlug记录日志
	 */
	static Logger logger = Logger.getLogger(ConnectPlug.class);
	protected IRelationalDataManager adapter;

	public ConnectPlug(IRelationalDataManager adapter) {
		this.adapter = adapter;
	}
	/**
	 * 根据id获取DbSquid信息
	 * @param id
	 * @return
	 */
	public List<DbSquid> getDbSquidById(int id)
	{
		return adapter.query2List("SELECT * FROM DS_SQUID  WHERE id="+id + " AND SQUID_TYPE_ID" + SquidTypeEnum.DBSOURCE.value(), null, DbSquid.class);
	}
	/**
	 * 根据id查询DBSourceTable
	 * @param id
	 * @return
	 */
	public List<DBSourceTable> getDbSourceTableById(int id)
	{
		//实现DB重新连接后 被抽取的放前面
		return adapter.query2List(true,"SELECT * FROM DS_SOURCE_TABLE  WHERE source_squid_id="+id+" order  by is_extracted desc", null, DBSourceTable.class);
	}
	/**
	 * 根据id查询Column
	 * @param id
	 * @return
	 */
	public List<SourceColumn> getColumnById(boolean inSession, int id)
	{
		return adapter.query2List(inSession,"SELECT * FROM DS_SOURCE_COLUMN  WHERE source_table_id="+id, null, SourceColumn.class);
	}
	/**
	 * 根据table_name,source_squid_id查询对应的列
	 * @param tableName
	 * @param id
	 * @return
	 */
	public List<SourceColumn> getColumn(String tableName,int id)
	{
		return adapter.query2List("SELECT * FROM DS_SOURCE_COLUMN  WHERE source_table_id=(SELECT id FORM DS_SOURCE_TABLE WHERE table_name='"+tableName+"' AND source_squid_id="+id+")", null, SourceColumn.class);
	}
	/**
	 * 根据source_squid_id和table_Name更新DBSourceTable表
	 * @param table_Name
	 * @param id
	 * @return
	 */
	public int updateDBSourceTable(String table_Name, int id) {
		int cnt = 0;
		try {
			cnt = adapter
					.execute("update DS_SOURCE_TABLE set is_extracted = 'Y' where table_name='"
							+ table_Name + "' and source_squid_id=" + id + "",null);
		} catch (SQLException e) {
			logger.error("updateDBSourceTable is error", e);
		}
		return cnt;
	}
	/**
	 * 根据id查询DBSourceTable
	 * @param id
	 * @return
	 */
	public List<DBSourceTable> getDbSourceTable(boolean inSession,int id)
	{
		return adapter.query2List(inSession,"SELECT * FROM DS_SOURCE_TABLE  WHERE source_squid_id="+id+" order  by is_extracted desc, id asc", null, DBSourceTable.class);
	}
	
	/**
	 * 删除某dbSquid下的所有未抽取的SourceTable
	 * @throws DatabaseException 
	 */
	public void deleteDbSourceTableBySquid(int id) throws DatabaseException{
		String sql = "delete from ds_source_table where source_squid_id="+id+" and is_extracted='Y'";
		adapter.execute(sql);
		sql="delete from ds_source_table where source_squid_id="+id+" and is_extracted='1'";
		adapter.execute(sql);
		sql = "delete from ds_source_table where source_squid_id="+id+" and is_extracted='N'";
		adapter.execute(sql);
	}


	/**
	 * 删除某dbSquid下的所有未抽取的SourceTable
	 * @throws DatabaseException
	 */
	public void deleteDbSourceTablesBySquid(int id) throws DatabaseException{
		String sql = "delete from ds_source_table where source_squid_id="+id;
		adapter.execute(sql);
	}
	
	/**
	 * 根据id查询DBSourceTable
	 * @param id
	 * @return
	 */
	public List<DBSourceTable> getDbSourceTableForExtracted(boolean inSession,int id){
		return adapter.query2List(inSession,"SELECT * FROM DS_SOURCE_TABLE  WHERE source_squid_id="+id+" and is_extracted='Y' order  by is_extracted desc, id asc", null, DBSourceTable.class);
	}

	/**
	 * 根据id查询DBSourceTable
	 * @param id
	 * @return
	 */
	public List<DBSourceTable> getDbSourceTablesForExtracted(boolean inSession,int id){
		return adapter.query2List(inSession,"SELECT * FROM DS_SOURCE_TABLE  WHERE source_squid_id="+id+"  order  by id asc", null, DBSourceTable.class);
	}
}
