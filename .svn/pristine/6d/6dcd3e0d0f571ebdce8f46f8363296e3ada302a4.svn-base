package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.entity.Transformation;
import com.eurlanda.datashire.sprint7.packet.InfoPacket;
import com.eurlanda.datashire.utility.ReturnValue;

import java.util.List;

/**
 * 
 * Transformation业务处理类
 * Title : 
 * Description: 
 * Author :赵春花 2013-10-28
 * update :赵春花 2013-10-28
 * Department :  JAVA后端研发部
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司
 */
public interface ITransformationService{
/**
	 * 创建Transformation对象集合
	 * 作用描述：
	 * Transformation对象集合不能为空且必须有数据
	 * Transformation对象集合里的每个Key必须有值
	 * 根据传入的Transformation对象集合创建SquidFLow对象
	 * 创建成功——根据Transformation集合对象里的Key查询相对应的ID
	 * 修改说明：
	 *@param transformations transformation集合对象
	 *@param out 异常处理
	 *@return
	 */
	public List<InfoPacket> createTransformations(
			List<Transformation> transformations, ReturnValue out);
	/**
	 * 
	 * 作用描述：
	 * 修改Transformation对象集合
	 * 调用方法前确保Transformation对象集合不为空，并且Transformation对象集合里的每个Transformation对象的Id不为空
	 * 修改说明：
	 *@param transformations transformation集合对象
	 *@param out 异常处理
	 *@return
	 */
	public List<InfoPacket> updateTransformations(List<Transformation> transformations,ReturnValue out);
	
}