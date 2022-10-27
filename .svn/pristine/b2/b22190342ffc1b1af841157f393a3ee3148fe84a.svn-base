package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.sprint7.packet.InfoPacket;
import com.eurlanda.datashire.utility.ReturnValue;

import java.util.List;
import java.util.Map;

public interface ISquidFlowProcess{
/**
	 * 创建SquidFlow集合对象处理类
	 * 作用描述：
	 * 1、将Json对象序列化成SquidFlow对象集合
	 * 2、调用DataShirBusiness业务处理类新增squidFlow 对象集合并查询新增ID封装返回Infopacket对象集合
	 * 验证：
	 * 1、判断序列化的数据是否有值
	 * 2、判断SquidFlow的必要字段的值是否正确
	 * 修改说明：
	 *@param info
	 *@param out
	 *@return
	 */
	public Map<String, Object> createSquidFlows(String info,ReturnValue out);
	/**
	 * 修改SquidFlow对象集合
	 * 作用描述：
	 * 根据SquidFlow对象集合里SquidFlow的ID进行修改
	 * 验证：
	 * 	1、序列化数据是否正确
	 *  2、SquidFlow的必要字段值是否正确
	 * 修改说明：
	 *@param info
	 *@param out
	 *@return
	 */
	public List<InfoPacket> updateSquidFlows(String info,ReturnValue out);
	/**
	 * 根据ProjectId查询SquidFlows对象
	 * 作用描述：
	 * 修改说明：
	 *@param info json字符串
	 *@param out 异常处理
	 *@return
	 */
	public Map<String, Object> queryAllSquidFlows(String info, ReturnValue out);
	
	
}