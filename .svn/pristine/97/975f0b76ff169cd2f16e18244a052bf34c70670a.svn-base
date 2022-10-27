package com.eurlanda.datashire.dao.impl;

import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.adapter.datatype.DataTypeConvertor;
import com.eurlanda.datashire.dao.IColumnDao;
import com.eurlanda.datashire.dao.ISourceColumnDao;
import com.eurlanda.datashire.dao.ISourceTableDao;
import com.eurlanda.datashire.entity.Column;
import com.eurlanda.datashire.entity.DBSourceTable;
import com.eurlanda.datashire.entity.SourceColumn;
import com.eurlanda.datashire.entity.SourceTable;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SourceTableDaoImpl extends BaseDaoImpl implements ISourceTableDao {

	public SourceTableDaoImpl() {
	}

	public SourceTableDaoImpl(IRelationalDataManager adapter) {
		this.adapter = adapter;
	}

	@Override
	public List<SourceTable> getSourceTableBySquidId(int source_squid_id) {
		// TODO Auto-generated method stub
		List<SourceTable> sourceTables = null;
		List<DBSourceTable> dbSourceTables = this.getDbSourceTableBySquidId(source_squid_id);
		IColumnDao columnDao = new ColumnDaoImpl(adapter);
		Map<Integer, Map<String, Column>> map = columnDao.managerSourceColumnForMap(source_squid_id);
		if (dbSourceTables != null && dbSourceTables.size() > 0) {
			sourceTables = new ArrayList<SourceTable>();
			/*
			 * IColumnDao columnDao = new ColumnDaoImpl(adapter); Map<Integer,
			 * Map<String, Column>> map =
			 * columnDao.managerSourceColumnForMap(source_squid_id);
			 */
			for (DBSourceTable dbSourceTable : dbSourceTables) {
				SourceTable sourceTable = new SourceTable();
				sourceTable.setId(dbSourceTable.getId());
				sourceTable.setTableName(dbSourceTable.getTable_name());
				sourceTable.setIs_extracted(dbSourceTable.isIs_extracted());
				// sourceTable.setUrl_params(dbSourceTable.getUrl_params());
				// sourceTable.setHeader_params(dbSourceTable.getHeader_params());
				sourceTable.setIs_directory(dbSourceTable.isIs_directory());
				sourceTable.setHeader_xml(dbSourceTable.getHeader_xml());
				sourceTable.setParams_xml(dbSourceTable.getParams_xml());
				sourceTable.setPost_params(dbSourceTable.getPost_params());
				sourceTable.setEncoded(dbSourceTable.getEncoded());
				sourceTable.setTemplet_xml(dbSourceTable.getTemplet_xml());
				sourceTable.setSource_squid_id(dbSourceTable.getSource_squid_id());
				sourceTable.setMethod(dbSourceTable.getMethod());
				sourceTable.setUrl(dbSourceTable.getUrl());
				if (map != null && map.size() > 0) {
					sourceTable.setColumnInfo(map.get(dbSourceTable.getId()));
				}
				sourceTables.add(sourceTable);
			}
		}
		return sourceTables;
	}

	@Override
	public List<DBSourceTable> getDbSourceTableBySquidId(int source_squid_id) {
		// String sql="select * from DS_SOURCE_TABLE where source_squid_id
		// ="+source_squid_id+" order by is_extracted desc";
		String sql = "select * from DS_SOURCE_TABLE where  source_squid_id =" + source_squid_id;
		return adapter.query2List(true, sql, null, DBSourceTable.class);
	}

	@Override
	public DBSourceTable getDBSourceTableByParams(int squidId, String tableName) throws SQLException {
		String sql = "select * from ds_source_table where source_squid_id=" + squidId + " and table_name='" + tableName
				+ "'";
		return adapter.query2Object(true, sql, null, DBSourceTable.class);
	}

	@Override
	public int InsertSourceTableAndSourceColumn(int newSquidId, SourceTable sourceTable) throws Exception {
		int id=0;
		try {
			ISourceColumnDao sourceColumnDao = new SourceColumnDaoImpl(adapter);
			String sql = "insert into DS_SOURCE_TABLE (TABLE_NAME,SOURCE_SQUID_ID,IS_EXTRACTED) values (?,?,?)";
			List<Object> params = new ArrayList<>();
			params.add(sourceTable.getTableName());
			params.add(newSquidId);
			params.add(sourceTable.isIs_extracted());
			int cnt = adapter.execute(sql,params);
			if (cnt > 0) {
				sql = "select max(id) as id from ds_source_table where source_squid_id =" + newSquidId;
				Map<String, Object> idMap = adapter.query2Object(true,sql,null);
				id=Integer.parseInt(idMap.get("ID")+"");
				// 插入sourceColumn信息
				Map<String, Column> map = sourceTable.getColumnInfo();
				if(map!=null) {
					for (String key : map.keySet()) {
						SourceColumn sourceColumn = DataTypeConvertor.getSourceColumnByReferenceColumns(map.get(key));
						sourceColumn.setSource_table_id(Integer.parseInt(idMap.get("ID") + ""));
						sourceColumnDao.insert2(sourceColumn);

					}
				}

			}
		} catch (Exception e) {
			throw e;
		}
		return id;

	}
}
