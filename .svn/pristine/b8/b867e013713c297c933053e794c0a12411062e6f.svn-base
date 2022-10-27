package com.eurlanda.datashire.dao;

import com.eurlanda.datashire.entity.DBSourceTable;
import com.eurlanda.datashire.entity.SourceTable;

import java.sql.SQLException;
import java.util.List;

public interface ISourceTableDao extends IBaseDao {
	/**
	 * 根据squidId获取当前抽取的SourceTable集合，集合中嵌入SourceColumn
	 * @param source_squid_id
	 * @return
	 */
	public List<SourceTable> getSourceTableBySquidId(int source_squid_id);
	
	/**
	 * 根据squidId获取当前抽取的DBSourceTable的实体对象
	 * @param source_squid_id
	 * @return
	 */
	public List<DBSourceTable> getDbSourceTableBySquidId(int source_squid_id);
	
	/**
	 * 根据squidId和tableName获取DBSourceTable
	 * @param squidId
	 * @param tableName
	 * @return
	 * @throws SQLException
	 */
	public DBSourceTable getDBSourceTableByParams(int squidId, String tableName) 
			throws SQLException;
	/**
	 * 插入sourceTable
	 */
	public int InsertSourceTableAndSourceColumn(int newSquidId,SourceTable sourceTable) throws Exception;
}
