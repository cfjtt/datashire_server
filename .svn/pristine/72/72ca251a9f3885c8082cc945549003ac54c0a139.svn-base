package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.sprint7.packet.InfoPacket;
import com.eurlanda.datashire.utility.ReturnValue;

import java.util.List;

public interface IColumnProcess{
/**
	 * 创建Column集合对象处理类
	 * 作用描述：
	 * 创建Column对象集合
	 * 成功根据Column对象集合里的key查询相对应的Id封装成InfoPacket对象集合返回
	 * 验证：
	 * 1、序列化结果集数据是否为空
	 * 2、Column对象集合里的Column对象的必要字段的值是否正确
	 * 修改说明：
	 *@param info json字符串
	 *@param out 异常处理
	 *@return
	 */
	public List<InfoPacket> createColumns(String info,ReturnValue out);
	/**
	 * 修改Column对象集合
	 * 作用描述：
	 * 根据Column对象集合里Column的ID进行修改
	 * 修改说明：
	 *@param info json字符串
	 *@param out 异常处理
	 *@return
	 */
	public void updateColumns(String token,String info,ReturnValue out);
	
}