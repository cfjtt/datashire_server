package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.sprint7.packet.InfoPacket;
import com.eurlanda.datashire.utility.ReturnValue;

import java.util.List;

public interface ITransformationProcess{
/**
	 * 创建Transformation集合对象处理类
	 * 作用描述：
	 * 创建SquidFlow对象集合
	 * 成功根据Transformation对象集合里的key查询相对应的Id封装成InfoPacket对象集合返回
	 * 修改说明：
	 *@param info json字符串
	 *@param out 异常处理
	 *@return
	 */
	public List<InfoPacket> createTransformations(String info,ReturnValue out);
	/**
	 * 修改Transformation对象集合
	 * 作用描述：
	 * 根据Transformation对象集合里Transformation的ID修改数据
	 * 验证：
	 * 1、验证数据序列化后是否有值
	 * 2、检验Transformation对象必要字段值的正确性
	 * 修改说明：
	 *@param info json字符串
	 *@param out 异常处理
	 *@return
	 */
	public List<InfoPacket> updateTransformations(String info,ReturnValue out);
	
}