package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.sprint7.packet.InfoPacket;
import com.eurlanda.datashire.utility.ReturnValue;

import java.util.List;

public interface ISquidLinkProcess{
/**
	 * 创建SquidLink集合对象处理类
	 * 作用描述：
	 *将json对象解析成SquidLink对象调用SquidLinkService处理业务逻辑
	 *验证：
	 *1、验证数据是否为空
	 *2、验证SquidLink的必要字段值是否正确
	 * 修改说明：
	 *@param info json字符串
	 *@param out 异常处理
	 *@return
	 */
	public List<InfoPacket> createSquidLinks(String info,ReturnValue out);
	/**
	 * 修改SquidLink对象集合
	 * 作用描述：
	 * 根据Project对象集合里SquidLink的ID进行修改
	 * 修改说明：
	 *@param info json字符串
	 *@param out 异常处理
	 *@return
	 */
	public List<InfoPacket> updateSquidLinks(String info,ReturnValue out);
	/**
	 * 创建SquidLink，同时添加SquidJoin
	 * 作用描述：
	 * 序列化SquidLink集合对象
	 * 验证：
	 * 验证SquidLink集合对象是否有值
	 * 修改说明：
	 *@param info Json串
	 *@param out 异常输出
	 *@return
	 */
	public List<InfoPacket> createSquidLinksJoin(String info,ReturnValue out);
	
}