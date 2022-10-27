package com.eurlanda.datashire.dao.impl;

import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.adapter.datatype.DataTypeConvertor;
import com.eurlanda.datashire.dao.IColumnDao;
import com.eurlanda.datashire.dao.ISourceColumnDao;
import com.eurlanda.datashire.entity.Column;
import com.eurlanda.datashire.entity.SourceColumn;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ColumnDaoImpl extends BaseDaoImpl implements IColumnDao {

	public ColumnDaoImpl(){
	}
	
	public ColumnDaoImpl(IRelationalDataManager adapter){
		this.adapter = adapter;
	}

	@Override
	public Column getColumnByKey(String key) {
		String sql = "select * from ds_column where key =?";
		List<String> params = new ArrayList<>();
		params.add(key);
		List<Column> columnList = adapter.query2List(true, sql, params, Column.class);
		if(columnList != null && columnList.size() ==1) {
			return columnList.get(0);
		}
		return null;
	}

	@Override
	public List<Column> getColumnListBySquidId(int squidId) {
		String sql = "select * from ds_column " +
				" where squid_id="+squidId+
				" order by relative_order asc,id asc";
		List<Column> columnList = adapter.query2List(true, sql, null, Column.class);
		return columnList;
	}
	
	@Override
	public Integer delColumnListBySquidId(int squidId) throws Exception{
		String sql = "delete from ds_column where squid_id="+squidId;
		return adapter.execute(sql);
	}
	
	@Override
	public Map<Integer, Map<String, Column>> managerSourceColumnForMap(int source_squid_id){
		ISourceColumnDao sourceColumnDao = new SourceColumnDaoImpl(adapter);
		List<SourceColumn> list = sourceColumnDao.getSourceColumnBySquidId(source_squid_id);
		Map<Integer,Map<String, Column>> outputMap = new HashMap<Integer, Map<String, Column>>();
		if (list!=null&&list.size()>0){
			for (SourceColumn sourceColumn : list) {
				if (outputMap.containsKey(sourceColumn.getSource_table_id())){
					Map<String, Column> tempMap = outputMap.get(sourceColumn.getSource_table_id());
					Column column = DataTypeConvertor.getColumnBySourceColumn(sourceColumn);
					tempMap.put(column.getName(), column);
					outputMap.put(sourceColumn.getSource_table_id(), tempMap);
				} else {
					Map<String, Column> tempMap = new HashMap<String, Column>();
					Column column = DataTypeConvertor.getColumnBySourceColumn(sourceColumn);
					tempMap.put(column.getName(), column);
					outputMap.put(sourceColumn.getSource_table_id(), tempMap);
				}
			}
		}
		return outputMap;
	}
	
	/**
	 * 根据主键id取得column
	 * @param id
	 * @return
	 * @author bo.dang
	 */
	public Column selectColumnByPK(Boolean inSession, Integer id){
		Map<String, String> paramsMap = new HashMap<String, String>();
		paramsMap.put("id", Integer.toString(id, 10));
		Column column = adapter.query2Object2(inSession, paramsMap, Column.class);
		return column;
	}

	@Override
	public List<Map<String, Object>> getColumnTransBySquidId(int squidId) throws SQLException {
		String sql = "select dt.id, dt.column_id from ds_transformation dt " +
				"inner join ds_column dc " +
				"on dt.column_id=dc.id and dc.squid_id=dt.squid_id " +
				"where dt.squid_id="+squidId+" order by dc.relative_order asc";
		List<Map<String, Object>> objList = adapter.query2List(true, sql, null);
		return objList;
	}

	@Override
	public List<Column> getColumnByTransAndSquidId(int squidId) {
		String sql = "select dc.* from ds_transformation dt " +
				"inner join ds_column dc " +
				"on dt.column_id=dc.id " +
				"where dt.squid_id="+squidId+" order by dc.relative_order asc";
		List<Column> columnList = adapter.query2List(true, sql, null,Column.class);
		return columnList;

	}

	@Override
	public List<Column> getColumnListForCdc(int squidId){
		String sql = "select * from ds_column where squid_id="+squidId+""
				+ " and name in ('start_date','end_date','active_flag','version')";
		List<Column> listColumns = adapter.query2List(true, sql, null, Column.class);
		return listColumns;
	}

	@Override
	public Integer getColumnCountBySquidId(int squidId) throws SQLException {
		int order = 0;
		String sql = "select count(id) CNT from ds_column where squid_id="+squidId;
		Map<String, Object> tempMap = adapter.query2Object(true, sql, null);
		if (tempMap!=null&&tempMap.containsKey("CNT")){
			order = Integer.parseInt(tempMap.get("CNT")+"");
		}
		return order;
	}

	@Override
	public List<SourceColumn> findSourceColumnBySourceTableId(int sourceTableId) {
		Map<String,String> params = new HashMap<String,String>();
		params.put("source_table_id",sourceTableId+"");
		return adapter.query2List(true, params, SourceColumn.class);
	}
	
	@Override
	public Column getColumnByParams(int squidId, String name){
		Map<String,String> params = new HashMap<String,String>();
		params.put("squid_id",squidId+"");
		params.put("name", name);
		List<Column> list = adapter.query2List(true, params, Column.class);
		if (list!=null&&list.size()>0){
			return list.get(0);
		}
		return null;
	}


    /**
     * 判断datamingsquid里面的key是否引用
	 * @param squidId
	 * @return
	 * @throws SQLException
	 */
	@Override
	public Column getColumnKeyForDataMingSquid(int squidId) throws SQLException{
		String sql = "select dc.* from ds_column dc inner join ds_transformation dr on dc.id=dr.column_id inner join ds_transformation_link drl on "
				+ "dr.id=drl.to_transformation_id where dc.squid_id="+squidId+" and dc.name='key'";
		List<Column> lists = adapter.query2List(true, sql, null, Column.class);
		if (lists!=null&&lists.size()>0){
			return lists.get(0);
		}
		return null;
	}

	@Override
	public List<Column> getUnlinkedColumnBySquidId(int squidId) {
		String sql = "select dc.* from ds_column dc left join ds_transformation dr on dc.id=dr.column_id " +
				" left join ds_transformation_link drl on dr.id=drl.to_transformation_id where drl.id is null and dr.squid_id=" + squidId + " and dc.squid_id=" + squidId;
		List<Column> list = adapter.query2List(true, sql, null, Column.class);
		return list;
	}

	@Override
	public List<Column> getColumnListByIds(List<Integer> ids) {

		StringBuilder sb = new StringBuilder();
		sb.append("select * from ds_column where id in (");
		boolean isFirst = true;
		for(Integer id : ids) {
			if(!isFirst) {
				sb.append(",");
			} else {
				isFirst = false;
			}
			sb.append(id);
		}
		sb.append(")");

		String sql = sb.toString();
		List<Column> list = adapter.query2List(true, sql, null, Column.class);
		return list;
	}
}
