package com.eurlanda.datashire.dao;

import com.eurlanda.datashire.entity.SquidLink;

import java.sql.SQLException;
import java.util.List;

/**
 * SquidLink的数据处理接口
 * @author yi.zhou
 *
 */
public interface ISquidLinkDao extends IBaseDao {
	/**
	 * 根据上游squidId和下游squidId获取SquidLink对象
	 * @param fromSquidId 上游squidId
	 * @param toSquidId 下游squidId
	 * @return
	 */
	public SquidLink getSquidLinkByParams(int fromSquidId, int toSquidId);
	
	/**
	 * 根据squidId获取link集合
	 * @param fromSquidId
	 * @return
	 */
	public List<SquidLink> getSquidLinkListByFromSquid(int fromSquidId);

	/**
	 * 根据squidId获取link集合
	 * @param toSquidId
	 * @return
	 */
	public List<SquidLink> getSquidLinkListByToSquid(int toSquidId);


	/**
	 * 获取某squidFlow下的所有的squidlink集合
	 * @param squidFlowId
	 * @return
	 */
	public List<SquidLink> getSquidLinkListBySquidFlow(int squidFlowId);
	
	/**
	 * 获取某squidFlow下的squidId在ids中的squidlink集合
	 * @param squidFlowId
	 * @param ids
	 * @return
	 */
	public List<SquidLink> getSquidLinkBySquidIds(int squidFlowId, String ids);
	
	/**
	 * 删除SquidLink
	 * @param fromSquidId 上游squidId
	 * @param toSquidId 下游squidId
	 * @return
	 */
	public boolean delSquidLinkByParams(int fromSquidId, int toSquidId) throws SQLException;
	
	/**
	 * 根据参入的squidLink集合，进行依赖关联排序
	 * @param squidLinkIds
	 * @return
	 * @throws SQLException
	 */
	public List<Integer> orderSquidLinkIds(List<Integer> squidLinkIds) throws SQLException;
	
	/**
	 * 根据参入的squidFlow，进行依赖关联排序
	 * @param squidFlowId
	 * @return
	 * @throws SQLException
	 */
	public List<Integer> orderSquidLinkIdsForSquidFlowId(int squidFlowId) throws SQLException;
	
	/**
	 * 获取squid中cdc属性相关的squidlink集合
	 * @param squidId
	 * @return
	 * @throws SQLException
	 */
	public List<SquidLink> getSquidLinkListByCdc(int squidId) throws SQLException;

	/**
	 * 根据squidId，查询出上游的squidLink
	 * @param squidId
	 * @return
	 * @throws SQLException
     */
	public List<SquidLink> getFormSquidLinkBySquidId(int squidId) throws SQLException;
}
