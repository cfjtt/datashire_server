package com.eurlanda.datashire.dao;

import com.eurlanda.datashire.entity.Transformation;
import com.eurlanda.datashire.entity.TransformationInputs;
import com.eurlanda.datashire.entity.TransformationLink;
import com.eurlanda.datashire.utility.DatabaseException;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface ITransformationInputsDao extends IBaseDao {
	/**
	 * 获取某trans下的TransformationInputs列表
	 * @param transId
	 * @return
	 */
	public List<TransformationInputs> getTransInputListByTransId(int transId);
	
	/**
	 * 删除某trans下的inputs集合
	 * @param transId
	 */
	public boolean delTransInputsByTransId(int transId) throws SQLException;
	
	/**
	 * 重置某trans下的inputs
	 * @param from_transformation_id
	 * @param to_transformation_id
	 * @param indexMap
	 * @throws SQLException
	 */
	public void resetTransformationInput(int from_transformation_id, int to_transformation_id, 
			Map<String, Integer> indexMap) throws SQLException;
	
	/**
	 * 根据trans查询inputs集合，同时把相关信息查询出来并填充到列表中，特殊处理时使用
	 * @param transId
	 * @return
	 * @throws SQLException
	 */
	public List<TransformationInputs> getTransInputsForColumnByTransId(int transId) throws SQLException;
	
	/**
	 * 根据 ds_tran_inputs_definiton 记录生成 input
	 * @param trans
	 * @param dataType 虚拟Transformation时，复制column的datatType， 其他类型的时候传入0，进行数据库匹配
	 * @return
	 * @throws Exception
	 */
	public List<TransformationInputs> initTransInputs(Transformation trans, int dataType) throws Exception;

	/**
	 * 根据 ds_tran_inputs_definiton 记录生成 input  复制时使用
	 * @param trans
	 * @param dataType 虚拟Transformation时，复制column的datatType， 其他类型的时候传入0，进行数据库匹配
	 * @return
	 * @throws Exception
	 */
	public List<TransformationInputs> initTransInputs1(Transformation trans, int dataType) throws Exception;
	
	/**
	 * 根据 ds_tran_inputs_definiton 记录生成 input
	 * @param order
	 * @param dataType 虚拟Transformation时，复制column的datatType， 其他类型的时候传入0，进行数据库匹配
	 * @param source_trans_id 上游链接的transid
	 * @param sourceTransName 上游链接的transName
	 * @return
	 * @throws Exception
	 */
	public List<TransformationInputs> initTransInputs(Transformation trans, int dataType, int source_trans_id, String sourceTransName, int order, int source_input_index) throws Exception;
	
	/**
	 * 更新trans下的input的source
	 * @param link
	 * @param trans
	 * @return
	 * @throws Exception
	 */
	public boolean updTransInputs(TransformationLink link, Transformation trans) throws Exception;

	/**
	 * 更新trans中已选择的input
	 * @param currentInput
	 * @return
	 * @throws Exception
	 */
	public boolean updTransSelect(TransformationInputs currentInput,Transformation toTrans) throws Exception;
	
	/**
	 * 根据squid中的trans分组，绑定inputs
	 * @param squidId
	 * @return
	 * @throws Exception
	 */
	public Map<Integer, List<TransformationInputs>> getTransInputsBySquidId(int squidId) throws Exception;
	
	/**
	 * 复制某trans的inputs
	 * @param newToTransId
	 * @param transMap
	 * @param oldToTransId
	 * @return
	 * @throws DatabaseException
	 * @throws SQLException
	 */
	public boolean copyTransInputByLink(int newToTransId, Map<Integer, Integer> transMap, 
			int oldToTransId) throws DatabaseException, SQLException;

}
