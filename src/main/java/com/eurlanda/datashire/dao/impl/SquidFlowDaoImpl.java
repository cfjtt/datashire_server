package com.eurlanda.datashire.dao.impl;

import cn.com.jsoft.jframe.utils.ValidateUtils;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.dao.ISquidFlowDao;
import com.eurlanda.datashire.entity.SquidFlow;
import com.eurlanda.datashire.entity.SquidFlowStatus;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SquidFlowDaoImpl extends BaseDaoImpl implements ISquidFlowDao {

	public SquidFlowDaoImpl(){
	}
	
	public SquidFlowDaoImpl(IRelationalDataManager adapter){
		this.adapter = adapter;
	}
	
	@Override
	public int getSquidCountBySquidFlow(int squidFlowId) throws SQLException {
		// TODO Auto-generated method stub
		int count = 0;
		String sql = "select count(id) AS CNTID from DS_SQUID where squid_flow_id=" + squidFlowId;
		Map<String, Object> map = adapter.query2Object(true, sql, null);
		if (map!=null&&map.size()>0&&map.containsKey("CNTID")){
			String cntId = map.get("CNTID")+"";
			if (ValidateUtils.isNumeric(cntId)){
				count = Integer.parseInt(cntId);
			}
		}
		return count;
	}

	@Override
	public boolean getLockOnSquidFlow(int squid_flow_id, int project_id,
			int repository_id, int squid_flow_status, String token) throws Exception {
		Map<String, String> params = new HashMap<String, String>(); // 查询参数
		params.put("repository_id", String.valueOf(repository_id));
		params.put("squid_flow_id", String.valueOf(squid_flow_id));
		SquidFlowStatus status = adapter.query2Object2(true,
				params, SquidFlowStatus.class);
		if(null!=status && 0!=status.getId()){
			status.setSquid_flow_status(squid_flow_status);
		    return adapter.update2(status);
		} else {
			SquidFlowStatus flowStatus = new SquidFlowStatus();
			flowStatus.setProject_id(project_id);
			flowStatus.setRepository_id(repository_id);
			flowStatus.setSquid_flow_id(squid_flow_id);
			flowStatus.setSquid_flow_status(squid_flow_status);
			flowStatus.setOwner_client_token(token);
			return adapter.insert2(flowStatus)>=0?true:false;
		}
	}

	@Override
	public List<Map<String, Object>> getSquidTypeBySquidFlow(int squidFlowId)
			throws Exception {
		String sql = "select distinct squid_type_id from DS_SQUID "
				+ " where squid_flow_id="+squidFlowId;
		List<Map<String, Object>> typeObj = adapter.query2List(true, sql, null);
		return typeObj;
	}
	
	@Override
	public List<Map<String, Object>> getSquidTypeBySquidids(int squidFlowId, String ids)
			throws Exception{
		List<Map<String, Object>> typeObj = adapter.query2List(true, 
				"select distinct squid_type_id from ds_squid " +
				"where squid_flow_id=" + squidFlowId + " and id in ("+ids+")", null);
		return typeObj;
	}

	@Override
	public SquidFlow getSquidFlowById(int squidFlowId,int respositoryId) throws Exception{
		String sql ="select df.* from ds_squid_flow df inner join ds_project dp on df.project_id = dp.id where df.id ="+squidFlowId+" and dp.repository_id="+respositoryId;
		SquidFlow squidFlow = adapter.query2Object(true,sql,null,SquidFlow.class);
		return squidFlow;
	}

	@Override
	public List<Map<String, Object>> getSomeSquidFlow(String value) throws Exception {
		// TODO Auto-generated method stub
		List<Map<String, Object>> flowName=adapter.query2List(true, "select id,name from DS_SQUID_FLOW where id in"+value, null);
		return flowName;
	}

	@Override
	public List<SquidFlow> getAllSquidFlows(int projectId) throws Exception {
		// TODO Auto-generated method stub
		Map<String, Object> paramMap = new HashMap<String, Object>();
		paramMap.put("project_id", projectId);
		List<SquidFlow> squidFlows=adapter.query2List2(true, paramMap,SquidFlow.class);
		return squidFlows;
	}
}
