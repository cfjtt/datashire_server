package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.entity.TransformationLink;
import com.eurlanda.datashire.sprint7.packet.InfoPacket;
import com.eurlanda.datashire.utility.ReturnValue;

import java.util.List;

/**
 * TransformationLink业务处理类
 * 
 * Title : 
 * Description: 
 * Author :赵春花 2013-10-28
 * update :赵春花 2013-10-28
 * Department :  JAVA后端研发部
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司
 */
public interface ITransformationLinkService{
/**
	 * 创建TransformationLink对象集合
	 * 作用描述：
	 * 根据传入的TransformationLink对象集合创建TransformationLink对象
	 * 创建成功——根据TransformationLink集合对象里的Key查询相对应的ID
	 * 验证：
	 * 1、 TransformationLink对象集合不能为空且必须有数据
	 * 2、 TransformationLink对象集合里的每个Key必须有值
	 * 修改说明：
	 *@param transformationLinks transformationLink集合对象
	 *@param out 异常返回
	 *@return
	 */
	public List<InfoPacket> createTransformationLinks(
			List<TransformationLink> transformationLinks, ReturnValue out);
	/**
	 * 
	 * 作用描述：
	 * 修改TransformationLink对象集合
	 * 调用方法前确保TransformationLink对象集合不为空，并且TransformationLink对象集合里的每个TransformationLink对象的Id不为空
	 * 修改说明：
	 *@param transformationLinks transformationLink集合对象
	 *@param out 异常返回
	 *@return
	 */
	public List<InfoPacket> updateTransformationLinks(List<TransformationLink> transformationLinks,ReturnValue out);
	
}