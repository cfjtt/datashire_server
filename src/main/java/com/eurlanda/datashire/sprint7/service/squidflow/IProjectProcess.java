package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.sprint7.packet.InfoPacket;
import com.eurlanda.datashire.utility.ReturnValue;

import java.util.List;
import java.util.Map;

public interface IProjectProcess{
/**
	 * 创建Project集合对象处理类
	 * 作用描述：
	 * 将Json包解析成Project对象集合调用DataShirBusiness的新增Project对象集合的方法
	 * 验证：
	 * 1、验证数据是否为空
	 * 2、验证Project对象集合里Project对象必要对象的值是否正确
	 * 修改说明：
	 *@param info json字符串
	 *@param out 异常处理
	 *@return
	 */
	public Map<String, Object> createProjects(String info,ReturnValue out);
	/**
	 * 修改Project对象集合
	 * 作用描述：
	 * 根据Project对象集合里Project的ID进行修改
	 * 验证：
	 * 1、Project集合对象序列化后结果是否有值
	 * 2、Project集合对象中Project对象的必要字段是否有值
	 * 修改说明：
	 *@param info json字符串
	 *@param out 异常处理
	 *@return
	 */
	public List<InfoPacket> updateProjects(String info,ReturnValue out);
	/**
	 * 查询所有的Project对象集合
	 * 作用描述：
	 * 序列化Repository对象调用ProjectService业务处理
	 * 修改说明：
	 *@param info json字符串
	 *@param out 异常处理
	 *@return
	 */
	public Map<String, Object> queryAllProjects(String info,ReturnValue out);
	
}