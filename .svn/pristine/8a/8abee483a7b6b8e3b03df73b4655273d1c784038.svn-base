package com.eurlanda.datashire.dao;

import com.eurlanda.datashire.entity.Transformation;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface ITransformationDao extends IBaseDao{

	/**
	 * 通过 key 获取 transformation
	 * @param key
	 * @return
	 */
	public Transformation getTransformationByKey(String key);

	/**
	 * 获取Transformation，根据squid和columnId
	 * @param squidId
	 * @param columnId
	 * @return
	 */
	public Transformation getTransformationById(int squidId, int columnId);
	
	/**
	 * 获取某squid下的Transformation列表
	 * @param squidId
	 * @return
	 */
	public List<Transformation> getTransListBySquidId(int squidId) throws SQLException;
	
	/**
	 * 根据trans的类型获取output_data_type
	 * @param transType
	 * @return
	 * @throws SQLException
	 */
	public Integer getOutputDataTypeByTransType(int transType) throws SQLException;
	
	/**
	 * 根据transId获取trans的最大链接数
	 * @param transId
	 * @return
	 */
	public Map<String, Object> getTransParamsByTransId(int transId) throws SQLException;
	
	/**
	 * 获取squid中的 虚拟transformation
	 * @return
	 * @throws SQLException
	 */
	public List<Transformation> getVTransBySquidId(int groupId, int squidId)
			throws SQLException;
	
	/**
	 * 获取squid中的实体Transformation
	 * @param squidId
	 * @return
	 */
	public List<Transformation> getEntityTransBySquidId(int squidId);
	
	/**
	 * 修改transformation的属性字段的squidId
	 * @param transId
	 * @param squidId
	 */
	public void modifyRefSquidForTransId(int transId, int squidId, int type) throws SQLException;
	
	public void initTransformationInputDataType(Transformation transformation) throws SQLException;

	/**
	 * 通过column id, 获取虚拟transformation
	 * @param columnId
	 * @return
	 */
	public Transformation getVTransformationByColumnId(int columnId, int squidId);

	/**
	 * 通过reference column id, 获取虚拟transformation
	 * @param refColumnId
	 * @return
	 */
	public Transformation getVTransformationByReferenceColumnId(int refColumnId, int squidId);
}
