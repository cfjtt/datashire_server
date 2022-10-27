package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.entity.SquidFlow;
import com.eurlanda.datashire.sprint7.packet.InfoPacket;
import com.eurlanda.datashire.utility.ReturnValue;

import java.util.List;

/**
 * SquidFlow业务处理类
 * 
 * Description: 
 * Author :赵春花 2013-10-28
 * Department :  JAVA后端研发部
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司
 */
public interface ISquidFlowService{
/**
	 * 创建SquidFLow对象集合
	 * 作用描述：
	 * 1、根据传入的SquidFLow对象集合创建SquidFLow对象
	 * 2、创建成功——根据SquidFLow集合对象里的Key查询相对应的ID组装返回infopacket对象集合返回
	 * 验证：
	 * 1、SquidFLow对象集合不能为空且必须有数据
	 * 2、SquidFLow对象集合里的每个Key必须有值
	 * 修改说明：
	 *@param squidFlows squidFlow对象集合
	 *@param out 异常返回
	 *@return
	 */
	public List<InfoPacket> createSquidFLows(List<SquidFlow> squidFlows,ReturnValue out);
	/**
	 * 修改SquidFlow
	 * 作用描述：
	 * 修改squidFlows对象集合
	 * 调用方法前确保squidFlows对象集合不为空，并且squidFlows对象集合里的每个squidFlows对象的Id不为空
	 * 修改说明：
	 *@param squidFlows squidFlow集合对象
	 *@param out 异常返回
	 *@return
	 */
	public List<InfoPacket> updateSquidFlows(List<SquidFlow> squidFlows,ReturnValue out);
	/**
	 * 根据ProjectID查询SquidFlow集合
	 * 作用描述：
	 * 根据ProjectId查询相应下的SquidFlow信息
	 * 修改说明：
	 *@param id projectID
	 *@param out 异常返回
	 */
	public List<SquidFlow> querySquidFlows(int projectId,int repositoryId, ReturnValue out);
	
}