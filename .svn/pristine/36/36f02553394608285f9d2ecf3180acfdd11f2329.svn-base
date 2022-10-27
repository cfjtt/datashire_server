package com.eurlanda.datashire.dao;

import com.eurlanda.datashire.entity.SquidFlowStatus;

import java.util.List;

public interface ISquidFlowStatusDao extends IBaseDao {
	/**
	 * 根据repositoryId和squidflowId获取所有squidflowstatus集合
	 * @param repositoryId
	 * @param squidflowId
	 * @return
	 * @throws Exception
	 */
	public List<SquidFlowStatus> getSquidFlowStatusBySquidFlow(int repositoryId,int squidflowId) throws Exception;
	/**
	 * 根据根据repositoryId和squidflowId获取squidflowstatus对象
	 * @param repositoryId
	 * @param squidflowId
	 * @return
	 * @throws Exception
	 */
	public SquidFlowStatus getOneSquidFlowStatus(int repositoryId,int squidflowId)throws Exception;
	
	/**
	 * 根据repositoryId和projectId获取所有squidflowstatus集合
	 * @param repositoryId
	 * @param projectId
	 * @return
	 * @throws Exception
	 */
	public List<SquidFlowStatus> getSquidFlowStatusByProject(int repositoryId,
			int projectId) throws Exception;
	/**
	 * 根据repositoryId和projectId更新表状态
	 * @return
	 */
	public int updateSquidFlowStatus(int repositoryId,int squidflowId)throws Exception;
}
