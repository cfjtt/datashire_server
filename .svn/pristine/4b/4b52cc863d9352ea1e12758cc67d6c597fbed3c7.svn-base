package com.eurlanda.datashire.sprint7.service.squidflow;


public interface IRepositoryService {
	/**
	 * 删除SquidLink
	 * @param squidLink
	 * @param out
	 * @return
	 */
	public String deleteSquidLink(String info);
	
	/**
	 * 创建squidJoin
	 */
	public String createJoin(String info);
	
	/**
	 * 删除transGroup
	 * @param info
	 * @param out
	 * @return
	 */
	public String deleteTransGroup(String info);
	
	/**
	 * 修改transGroup的排序
	 * @param info
	 * @param out
	 * @return
	 */
	public String updateTransGroupOrder(String info);
	
	/**
	 * 创建从Extract到Stage的Link
	 * @param info
	 * @return
	 * @author bo.dang
	 * @date 2014-4-2
	 */
	public String createExtract2StageLink(String info);
	

	/**
	 * 根据join_id删除join
	 * @param join_id
	 * @param out
	 * @return
	 */
	public String deleteSquidJoin(String info);
	
	/**
	 * 更新JoinSquid
	 * @param squidJoin
	 * @return
	 * @author bo.dang
	 * @date 2014-4-9
	 */
	public String updateSquidJoin(String info);
	

    /**
	 * 更新join的顺序
	 * @param info
	 * @return
	 * @author bo.dang
	 * @date 2014-4-9
	 */
	public String updateJoinOrder(String info);
	
	/**
	 * 所有样板数据的类型 返回 List<Object>
	 * @param info
	 * @return
	 */
	public String getTemplateDataByTypes(String info);
	
	/**
	 * 根据DataTemplateTypes生成的List<Map<String,String>>
	 * @param info
	 * @return
	 */
	public String getAllTemplateDataTypes(String info);
	
	/**
	 * 删除多个join
	 * @param info
	 * @return
	 */
	public String deleteJoins(String info);
	
	/**
	 * 修改join类型或者Condition，为了简化业务流程（该接口与UpdateSquidJoin分开调用)
	 * @param info
	 * @return
	 */
	public String upDateJoinForTypeCond(String info);
}
