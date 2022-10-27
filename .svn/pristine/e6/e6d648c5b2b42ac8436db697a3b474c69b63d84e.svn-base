package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.entity.operation.ExtractSquidAndSquidLink;
import com.eurlanda.datashire.sprint7.packet.InfoPacket;
import com.eurlanda.datashire.utility.ReturnValue;

import java.util.List;

public interface ISquidProcess{
/**
	 * 创建StageSquid
	 * 作用描述：
	 * 序列化TransformationAndCloumn集合对象并验证TransformationAndCloumn是否为空，为空的情况下返回null；
	 * 修改说明：
	 *@param info
	 *@param out
	 *@return
	 */
	public List<InfoPacket> createMoreStageSquid(String info , ReturnValue out);
	/**
	 * 创建DBSourceSquid集合对象处理类
	 * 作用描述：
	 * 序列化DBSourceSquid对象集合
	 * 创建DBSourceSquid对象集合
	 * 成功根据DBSourceSquid对象集合里的key查询相对应的Id封装成InfoPacket对象集合返回
	 * 修改说明：
	 *@param info
	 *@param out
	 *@return
	 */
	public List<InfoPacket> createDBSourceSquids(String info,ReturnValue out);
	/**
	 * 修改Squid对象集合
	 * 作用描述：
	 * 根据Squid对象集合里Squid的ID进行修改
	 * 修改说明：
	 *@param info json字符串
	 *@param out 异常处理
	 *@return
	 */
	public void  updateDbSourceSquid(String info,ReturnValue out);
	/**
	 * 修改Squid
	 * 作用描述：
	 * 根据Squid对象集合里Squid的ID进行修改
	 * 修改说明：
	 *@param info json字符串
	 *@param out 异常处理
	 *@return
	 */
	public boolean  updateSquid(String info,ReturnValue out);
	/**
	 * 修改ExtractSquid对象集合
	 * 作用描述：
	 * 根据ExtractSquid对象集合里ExtractSquid的ID进行修改
	 * 修改说明：
	 *@param info
	 *@param out
	 *@return
	 */
	public void updateExtractSquids(String info,ReturnValue out);
	/**
	 * 修改stageSquid对象集合
	 * 作用描述：
	 * 根据Squid对象集合里stageSquid的ID进行修改
	 * 修改说明：
	 *@param info json字符串
	 *@param out 异常处理
	 *@return
	 */
	public void updateStageSquids(String info,ReturnValue out);
	/**
	 * 修改DestinationSquid对象集合
	 * 作用描述：
	 * 根据DestinationSquid对象集合里DestinationSquid的ID进行修改
	 * 修改说明：
	 *@param info json字符串
	 *@param out 异常处理
	 *@return
	 */
	public List<InfoPacket> updateDestinationSquids(String info,ReturnValue out);
	/**
	 * 创建ExtractSquids集合对象处理类
	 * 作用描述：
	 * 创建ExtractSquids对象集合
	 * 成功根据ExtractSquids对象集合里的key查询相对应的Id封装成InfoPacket对象集合返回
	 * 修改说明：
	 *@param info json字符串
	 *@param out 异常处理
	 *@return
	 */
	public List<InfoPacket> createExtractSquids(String info,ReturnValue out);
	/**
	 * 创建DestinationSquid集合对象处理类
	 
	 * 作用描述：
	 * 创建DestinationSquid对象集合
	 * 成功根据DestinationSquid对象集合里的key查询相对应的Id封装成InfoPacket对象集合返回
	
	 
	 * 修改说明：
	
	 *@param info json字符串
	 *@param out 异常处理
	 *@return
	 */
	public List<InfoPacket> createDestinationSquids(String info,ReturnValue out);
	/**
	 *创建StageSquid
	 * 作用描述：
	 * 序列化StageSquidAndSquidLink对象
	 * ExtractSquidAndSquidLink对象里包含ExtractSquid对象和SquidLink对象
	 * 验证：
	 * ExtractSquidAndSquidLink对象集合不为空
	 * 修改说明：
	 *@param info json字符串
	 *@param out 异常处理
	 */
	public List<ExtractSquidAndSquidLink> createMoreExtractsquid(String info, ReturnValue out);
	
}