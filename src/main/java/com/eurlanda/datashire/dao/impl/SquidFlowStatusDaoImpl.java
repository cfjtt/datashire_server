package com.eurlanda.datashire.dao.impl;

import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.dao.ISquidFlowStatusDao;
import com.eurlanda.datashire.entity.SquidFlowStatus;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SquidFlowStatusDaoImpl extends BaseDaoImpl implements ISquidFlowStatusDao {
	public SquidFlowStatusDaoImpl(){
	}
	
	public SquidFlowStatusDaoImpl(IRelationalDataManager adapter){
		this.adapter = adapter;
	}
	
	@Override
	public List<SquidFlowStatus> getSquidFlowStatusBySquidFlow(int repositoryId,
			int squidflowId) throws Exception {
		Map<String, Object> params = new HashMap<String, Object>(); // 查询参数
		params.put("repository_id", repositoryId);
		params.put("squid_flow_id", squidflowId);
		return adapter.query2List2(true, params, SquidFlowStatus.class);
	}
	
	public List<SquidFlowStatus> getSquidFlowStatusByProject(int repositoryId,
			int projectId) throws Exception{
		Map<String, Object> params = new HashMap<String, Object>(); // 查询参数
		params.put("repository_id", repositoryId);
		params.put("project_id", projectId);
		return adapter.query2List2(true, params, SquidFlowStatus.class);
	}

	@Override
	public int updateSquidFlowStatus(int repositoryId, int squidflowId)
			throws Exception {
		String sql="update DS_SYS_SQUID_FLOW_STATUS set status=0 where repository_id="+repositoryId+" and squid_flow_id="+squidflowId+"";
		return adapter.execute(sql);
	}

	@Override
	public SquidFlowStatus getOneSquidFlowStatus(int repositoryId,
			int squidflowId) throws Exception {
		// TODO Auto-generated method stub
		Map<String, Object> params = new HashMap<String, Object>(); // 查询参数
		params.put("repository_id", repositoryId);
		params.put("squid_flow_id", squidflowId);
		return adapter.query2Object(true, params, SquidFlowStatus.class);
	}
}
