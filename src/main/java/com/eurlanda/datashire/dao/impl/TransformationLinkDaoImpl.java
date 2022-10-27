package com.eurlanda.datashire.dao.impl;

import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.dao.ITransformationLinkDao;
import com.eurlanda.datashire.entity.TransformationLink;
import com.eurlanda.datashire.utility.DatabaseException;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TransformationLinkDaoImpl extends BaseDaoImpl implements ITransformationLinkDao {

	public TransformationLinkDaoImpl(){
	}
	
	public TransformationLinkDaoImpl(IRelationalDataManager adapter){
		this.adapter = adapter;
	}

	@Override
	public TransformationLink getTransformationLinkByKey(String key) {
		String sql = "select * from ds_transformation_link where key=?";
		List<String> params = new ArrayList<>();
		params.add(key);
		List<TransformationLink> links = adapter.query2List(true, sql, params, TransformationLink.class);
		if(links != null && links.size()==1) {
			return links.get(0);
		}
		return null;
	}

	@Override
	public List<TransformationLink> getTransLinkByTransId(int transId) {
		String linkSql = "select * from ds_transformation_link where from_transformation_id="+transId
				+ " or to_transformation_id="+transId;
		List<TransformationLink> links = adapter.query2List(true, linkSql, null, TransformationLink.class);
		return links;
	}


	
	public List<TransformationLink> getTransLinkByFromTrans(int from_trans_id){
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("from_transformation_id", from_trans_id);
		return adapter.query2List2(true, params, 
				TransformationLink.class);
	}

	@Override
	public boolean delTransLinkByTranId(int transId) throws SQLException, DatabaseException {
		if (transId<=0){
			return false;
		}
		String linkSql = "delete from ds_transformation_link where from_transformation_id="+transId
				+ " or to_transformation_id="+transId;
		return adapter.execute(linkSql)>=0?true:false;
	}
	
	@Override
	public List<TransformationLink> getTransLinksByFromSquid(int fromSquidId, int toSquidId) {
		String queryTranSql = "select drl.* from ds_transformation_link drl " + 
				"inner join ds_transformation dt on " +
				"drl.from_transformation_id=dt.id or drl.to_transformation_id=dt.id " +
				"inner join (select * from ds_column ds where ds.squid_id="+fromSquidId+")t1 " +
				"on t1.id = dt.column_id where dt.squid_id="+toSquidId;
		List<TransformationLink> transformationLinks = adapter.query2List(
				true, queryTranSql, null, TransformationLink.class);
		return transformationLinks;
	}
	
	@Override
	public List<TransformationLink> getTransLinkByToTransId(int toTransId){
		String sql = "select dtl.* from ds_transformation_link dtl " +
    			"inner join ds_transformation dt on dtl.from_transformation_id=dt.id and (dt.column_id is null or dt.column_id=0) " +
    			"where dtl.to_transformation_id="+toTransId+" order by dtl.in_order";
		//修改bug5255
		/*String sql = "select dtl.* from ds_transformation_link dtl " +
				"inner join ds_transformation dt on dtl.from_transformation_id=dt.id " +
				"where dtl.to_transformation_id="+toTransId;*/
    	List<TransformationLink> transLinkList = adapter.query2List(true, sql, null, TransformationLink.class);
    	return transLinkList;
	}

	@Override
	public List<TransformationLink> getTransLink2ByToTransId(int toTransId) {
		String sql = "select dtl.* from ds_transformation_link dtl " +
				"inner join ds_transformation dt on dtl.from_transformation_id=dt.id " +
				"where dtl.to_transformation_id="+toTransId;
		List<TransformationLink> transLinkList = adapter.query2List(true, sql, null, TransformationLink.class);
		return transLinkList;
	}

	@Override
	public List<TransformationLink> getTransLinkListBySquidId(int squidId) {
		String sql = "select l.* from ds_transformation_link l inner join ds_transformation t on t.id = l.to_transformation_id where t.squid_id = " + squidId;
		return adapter.query2List(true, sql, null, TransformationLink.class);
	}


	
	public TransformationLink getTransLinkByTrans(int fromTransId, int toTransId){
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("from_transformation_id", fromTransId);
		params.put("to_transformation_id", toTransId);
		List<TransformationLink> list = adapter.query2List2(true, params, 
				TransformationLink.class);
		if (list!=null&&list.size()>0){
			return list.get(0);
		}
		return null;
	}
}
