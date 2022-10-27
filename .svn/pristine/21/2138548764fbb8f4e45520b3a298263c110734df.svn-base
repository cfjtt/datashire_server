package com.eurlanda.datashire.sprint7.service.squidflow.subservice;

import com.eurlanda.datashire.entity.SquidFlow;
import com.eurlanda.datashire.utility.ReturnValue;

/**
 * SquidFlow处理类
 * @author lei.bin
 *
 */
public interface ISquidFlowService{
/**
	 * 根据squidFlowId删除squidflow以及关联子类
	 * 
	 * @param squidFlowId
	 * @param out
	 * @return
	 */
	public boolean deleteSquidFlow(int squidFlowId,int repositoryId, ReturnValue out);
	/**
	 * 根据squid_flow_id获取当前squid flow详细信息
	 * 运行SquidFlow时调用，将SquidFlow传送至变换引擎
	 * @param squid_flow_id
	 * @return
	 */
	public SquidFlow getSquidFlow(int squid_flow_id);
	
}