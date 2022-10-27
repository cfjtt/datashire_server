package com.eurlanda.datashire.dao.impl;

import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.dao.IReferenceColumnDao;
import com.eurlanda.datashire.entity.ReferenceColumn;
import com.eurlanda.datashire.entity.ReferenceColumnGroup;
import com.eurlanda.datashire.utility.DatabaseException;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReferenceColumnDaoImpl extends BaseDaoImpl implements IReferenceColumnDao{

	public ReferenceColumnDaoImpl(){
	}
	
	public ReferenceColumnDaoImpl(IRelationalDataManager adapter){
		this.adapter = adapter;
	}
	
	@Override
	public ReferenceColumn getReferenceColumnById(int refSquidId, int columnId) {
		Map<String, Object> paramMap = new HashMap<String, Object>();
		paramMap.put("column_id", columnId);
		paramMap.put("reference_squid_id", refSquidId);
		ReferenceColumn refColumn = adapter.query2Object(true, paramMap, ReferenceColumn.class);
		if (refColumn!=null){
			refColumn.setId(refColumn.getColumn_id());
		}
		return refColumn;
	}

	@Override
	public List<ReferenceColumn> getRefColumnListBySquid(int hostSquidId,
			int refSquidId) {
		String sql = "select * from ds_reference_column " +
				" where host_squid_id="+hostSquidId+" and reference_squid_id="+refSquidId+
				" order by relative_order asc,column_id asc";
		List<ReferenceColumn> refColumnList = adapter.query2List(true, sql, null, ReferenceColumn.class);
		if (refColumnList!=null&&refColumnList.size()>0){
			for (ReferenceColumn referenceColumn : refColumnList) {
				referenceColumn.setId(referenceColumn.getColumn_id());
			}
		}
		return refColumnList;
	}

	@Override
	public ReferenceColumnGroup getRefColumnGroupBySquidId(int refSquidId, int hostSquidId) throws SQLException{
		String sql = "select t1.*, group_id from ds_reference_column_group t1 inner join ds_reference_column t2 on t1.id=t2.group_id " +
				" where host_squid_id="+hostSquidId+" and t1.reference_squid_id="+refSquidId;
		ReferenceColumnGroup refColumnList = adapter.query2Object(true, sql, null, ReferenceColumnGroup.class);
		return refColumnList;
	}
	
	@Override
	public List<ReferenceColumnGroup> getRefColumnGroupListBySquidId(
			int refSquidId) {
		String sql ="select distinct refGroup.*  " +
				"from ds_reference_column_group refGroup  " +
				"inner join ds_reference_column refColumn on  " +
				"refColumn.REFERENCE_SQUID_ID = refGroup.REFERENCE_SQUID_ID  " +
				"and  " +
				"refColumn.GROUP_ID=refGroup.ID  " +
				"where  " +
				"refGroup.REFERENCE_SQUID_ID="+refSquidId;
		List<ReferenceColumnGroup> groupList = adapter.query2List(sql,null,ReferenceColumnGroup.class);
		return groupList;
	}

	@Override
	public List<ReferenceColumn> getRefColumnListByGroupId(int groupId) {
		String sql = "select * from ds_reference_column " +
				" where group_id="+groupId+
				" order by relative_order asc,column_id asc";
		List<ReferenceColumn> refColumnList = adapter.query2List(true, sql, null, ReferenceColumn.class);
		if (refColumnList!=null&&refColumnList.size()>0){
			for (ReferenceColumn referenceColumn : refColumnList) {
				referenceColumn.setId(referenceColumn.getColumn_id());
				referenceColumn.setSquid_id(referenceColumn.getHost_squid_id());
			}
		}
		return refColumnList;
	}

	@Override
	public boolean delReferenceColumnByColumnId(int columnId, int toSquidId) throws SQLException {
		Map<String, String> paramMap = new HashMap<String, String>();
		paramMap.put("column_id", columnId+"");
		paramMap.put("reference_squid_id", toSquidId+"");
		return adapter.delete(paramMap, ReferenceColumn.class)>=0?true:false;
	}
	
	public boolean deleReferenceColumnBySquidId(int hostSquidId, int refSquidId) throws SQLException{
		Map<String, String> paramMap = new HashMap<String, String>();
		paramMap.put("host_squid_id", hostSquidId+"");
		paramMap.put("reference_squid_id", refSquidId+"");
		return adapter.delete(paramMap, ReferenceColumn.class)>=0?true:false;
	}

	@Override
	public String getRefColumnNameForTrans(int transSquidId, int transColumnId) throws SQLException {
		String tempSql = "select ds.name+'_'+drc.name as name from ds_reference_column drc " +
				"left join ds_squid ds on drc.host_squid_id=ds.id " +
				"where drc.column_id="+transColumnId+" and drc.reference_squid_id="+transSquidId;
		Map<String, Object> ref = adapter.query2Object(true, tempSql, null);
		String sourceName = ref==null||!ref.containsKey("NAME")?"":ref.get("NAME")+"";
		return sourceName;
	}

	@Override
	public List<ReferenceColumn> getRefColumnListByRefSquidId(int ref_squid_id) {
		List<ReferenceColumn> list = adapter.query2List(true, "select c.id,r.host_squid_id squid_id,r.*, if(c.id, 'N','Y') host_column_deleted,"
				+ " g.relative_order group_order from"
				+ " DS_SOURCE_COLUMN c right join DS_REFERENCE_COLUMN r on r.column_id=c.id left join  "
				+ " DS_REFERENCE_COLUMN_GROUP g on r.group_id = g.id "
				+ " where r.reference_squid_id="+ref_squid_id + " order by g.relative_order asc,r.relative_order asc,r.column_id asc", null, ReferenceColumn.class);
		if (list!=null&&list.size()>0){
			for (ReferenceColumn referenceColumn : list) {
				referenceColumn.setId(referenceColumn.getColumn_id());
			}
		}
		return list;
	}
	
	@Override
	public Integer getRefGroupCountForSquidId(int squidId) throws SQLException{
		String sql = "select count(*) CNT from ds_reference_column_group "
				+ "where reference_squid_id="+squidId;
		Map<String, Object> tMap = adapter.query2Object(true, sql, null);
		int count = 0;
		if (tMap!=null&&tMap.containsKey("CNT")){
			count = Integer.parseInt(tMap.get("CNT")+"");
		}
		return count;
	}
	
	@Override
	public boolean updateRefColumnForLink(int from_transformation_id,
            boolean is_referenced) throws DatabaseException {
        String sql = "UPDATE DS_REFERENCE_COLUMN SET is_referenced='"
                + (is_referenced ? 'Y' : 'N') + "'";
        sql += " WHERE column_id IN (SELECT column_id FROM DS_TRANSFORMATION WHERE ID="
                + from_transformation_id + " AND column_id IS NOT NULL)";
        return adapter.execute(sql) > 0 ? true : false;
    }

	@Override
	public boolean updateRefColumnByObj(ReferenceColumn refColumn)
			throws SQLException {
		List<String> objList = new ArrayList<String>();
		objList.add("column_id");
		objList.add("reference_squid_id");
		objList.add("host_squid_id");
		return adapter.update2(refColumn, objList);
	}
	
	@Override
	public boolean updateRefColumnForOrder(ReferenceColumn refColumn) throws Exception {
        String sql = "UPDATE DS_REFERENCE_COLUMN SET relative_order=? where column_id=? and "
        		+ "reference_squid_id=? and host_squid_id=?";
        List<Object> objs = new ArrayList<Object>();
        objs.add(refColumn.getRelative_order());
        objs.add(refColumn.getColumn_id());
        objs.add(refColumn.getReference_squid_id());
        objs.add(refColumn.getHost_squid_id());
        return adapter.execute(sql, objs) > 0 ? true : false;
    }
	
	@Override
	public List<ReferenceColumn> getInitRefColumnListByRefSquid(int squid){
		String sql2 = "select drc.column_id id, drc.*, dss.name group_name, drcg.relative_order group_order,drc.host_squid_id as squid_id from ds_squid dss "
				+ " left join ds_reference_column drc on dss.id = drc.host_squid_id "
				+ " inner join ds_reference_column_group drcg on drc.group_id = drcg.id "
				+ " where drc.reference_squid_id =" + squid + "";
        List<ReferenceColumn> referenceColumnList = adapter.query2List(true, sql2, null, ReferenceColumn.class);
        return referenceColumnList;
	}

	@Override
	public List<ReferenceColumn> getInitRefColumnListByCurrentSquid(int squid){
		String sql2 = "select drc.column_id id, drc.*, dss.name group_name, drcg.relative_order group_order,drc.host_squid_id as squid_id from ds_squid dss "
				+ " left join ds_reference_column drc on dss.id = drc.host_squid_id "
				+ " inner join ds_reference_column_group drcg on drc.group_id = drcg.id "
				+ " where drc.reference_squid_id =" + squid + " order by drc.column_id asc";
		List<ReferenceColumn> referenceColumnList = adapter.query2List(true, sql2, null, ReferenceColumn.class);
		return referenceColumnList;
	}
}
