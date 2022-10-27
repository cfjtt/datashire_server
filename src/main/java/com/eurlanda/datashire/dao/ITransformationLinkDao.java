package com.eurlanda.datashire.dao;

import com.eurlanda.datashire.entity.TransformationLink;
import com.eurlanda.datashire.utility.DatabaseException;

import java.sql.SQLException;
import java.util.List;

public interface ITransformationLinkDao extends IBaseDao {

	public TransformationLink getTransformationLinkByKey(String key);

	/**
	 * 获取某trans下面的所有link
	 * @param transId
	 * @return
	 */
	public List<TransformationLink> getTransLinkByTransId(int transId);
	
	/**
	 * 查询fromtrans的link集合
	 * @param from_trans_id
	 * @return
	 */
	public List<TransformationLink> getTransLinkByFromTrans(int from_trans_id);
	/**
	 * 删除某Trans下的links
	 * @return
	 * @throws SQLException
	 */
	public boolean delTransLinkByTranId(int transId) throws SQLException, DatabaseException;
	
	/**
	 * 获取某上游squid中refColumn上链接的TransLink
	 * @param fromSquidId
	 * @param toSquidId
	 * @return
	 */
	public List<TransformationLink> getTransLinksByFromSquid(int fromSquidId, int toSquidId);
	
	/**
	 * 获取某squid中translink集合
	 * @param squidId
	 * @return
	 */
	public List<TransformationLink> getTransLinkListBySquidId(int squidId);



	
	/**
	 * 获取某toTransId的transLink集合,只获取transformation存在的link
	 * @param toTransId
	 * @return
	 */
	public List<TransformationLink> getTransLinkByToTransId(int toTransId);

	/**
	 * 获取某toTransId的transLink集合
	 * @param toTransId
	 * @return
	 */
	public List<TransformationLink> getTransLink2ByToTransId(int toTransId);

	/**
	 * 获取trans链接信息
	 * @param fromTransId
	 * @param toTransId
	 * @return
	 */
	public TransformationLink getTransLinkByTrans(int fromTransId, int toTransId);


}
