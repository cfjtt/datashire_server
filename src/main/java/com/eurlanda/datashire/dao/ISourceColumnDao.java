package com.eurlanda.datashire.dao;

import com.eurlanda.datashire.entity.SourceColumn;
import com.eurlanda.datashire.utility.DatabaseException;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface ISourceColumnDao extends IBaseDao {
	/**
	 * 查询sourcetable下的sourceColumn信息
	 * @param source_table_id
	 * @return
	 */
	public List<SourceColumn> getSourceColumnByTableId(int source_table_id);
	
	/**
	 * 查询squid下的sourceColumn集合
	 * @param source_squid_id
	 * @return
	 */
	public List<SourceColumn> getSourceColumnBySquidId(int source_squid_id);
	
	/**
	 * 删除某sourceTable下面的所有的sourceColumn
	 * @param source_table_id
	 * @return
	 */
	public int deleteSourceColumnByTableId(int source_table_id) throws SQLException ;
	
	/**
	 * 根据squid类型和squidId来进行查询到的数据库表名
	 * @param squidId
	 * @param squidType
	 * @return
	 */
	public String getTableNameBySquidId(int squidId, int squidType) throws SQLException;
	
	/**
	 * 根据oldSquidId和复制之后生成的id，获取sourceColumn的依赖
	 * @param fromSquidId
	 * @param newFromSquidId
	 * @return
	 */
	public List<Map<String, Object>> getCopyedSourceParms(int fromSquidId,
			int newFromSquidId, String tableName) throws SQLException;
	
	/**
	 * 复制SourceTable数据
	 * @param squidId
	 * @param newSquidId
	 * @throws DatabaseException
	 */
	public void copyDbSourceForData(int squidId, int newSquidId) throws DatabaseException;
}
