package com.eurlanda.datashire.dao;

import com.eurlanda.datashire.entity.SquidFlow;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;


public interface ISquidFlowDao extends IBaseDao {
	/**
	 * 根据squidFlowId获取改squidflow下的squid个数
	 * @param squidFlowId
	 * @return
	 */
	public int getSquidCountBySquidFlow(int squidFlowId) throws SQLException;
	
	/**
	 * 修改当前squidflow状态
	 * @param squid_flow_id
	 * @param project_id
	 * @param repository_id
	 * @param squid_flow_status
	 * @return
	 * @throws Exception
	 */
	public boolean getLockOnSquidFlow(int squid_flow_id, int project_id, 
			int repository_id, int squid_flow_status, String token) throws Exception;
	
	/**
	 * 获取当前squidflow下的squid类型
	 * @param squidFlowId
	 * @return
	 * @throws Exception
	 */
	public List<Map<String, Object>> getSquidTypeBySquidFlow(int squidFlowId) throws Exception;
	/**
	 * 获取部分字段的值
	 * @return
	 * @throws Exception
	 */
	public List<Map<String, Object>> getSomeSquidFlow(String value)throws Exception;

	/**
	 * 根据projectId获取所有的squidflow集合
	 * @param projectId
	 * @return
	 * @throws Exception
	 */
	public List<SquidFlow> getAllSquidFlows(int projectId)throws Exception;

	
	/**
	 * 获取当前squidFlow下的squidIds
	 * @param squidFlowId
	 * @param ids
	 * @return
	 * @throws Exception
	 */
	public List<Map<String, Object>> getSquidTypeBySquidids(int squidFlowId, String ids)
			throws Exception;
	/**
	 * 获取squidFlow
	 */
	public SquidFlow getSquidFlowById(int squidFlowId,int respositoryId) throws Exception;

}
