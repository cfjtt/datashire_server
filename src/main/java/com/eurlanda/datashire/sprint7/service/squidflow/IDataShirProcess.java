package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.entity.operation.SquidDataSet;
import com.eurlanda.datashire.entity.operation.StageSquidAndSquidLink;
import com.eurlanda.datashire.utility.ReturnValue;

import java.util.Map;

/**
 * DataShirProcess预处理类
 * DataShirProcess预处理类：对数据进行验证验证数据的完整性和正确性；并调用下层业务处理方法
 * Title : 
 * Description: 
 * Author :赵春花 2013-9-4
 * update :赵春花 2013-9-4
 * Department :  JAVA后端研发部
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司
 */
public interface IDataShirProcess{
/**
	 * 根据类型删除数据
	 * 作用描述：
	 * 修改说明：
	 *@param info
	 *@param out
	 *@return
	 */
	public boolean deleteRepositoryObject(String info,ReturnValue out);
	/**
	 * 查询SquidAndSquidLink信息
	 * 作用描述：
	 *根据SquidFlowId查询Squid信息和SquidLink信息
	 *序列化传入InfoPacket信息获得SquidFlowID
	 * 修改说明：
	 *@param info
	 *@param out
	 */
	public int queryAllSquidAndSquidLink(String info, ReturnValue out);
	/**
	 * 
	 * 作用描述：
	 * 修改说明：
	 *@param info json字符串
	 *@param out 异常处理
	 *@param key 
	 *@return
	 */
	public StageSquidAndSquidLink drag2StageSquid(String info, ReturnValue out);
	/**
	 * 获得变换序列中的数据
	 * 作用描述：
	 * 序列化前端传入Json对象
	 * 修改说明：
	 *@param info json字符串
	 *@param out 异常处理
	 *@return
	 */
	public Map<String, Object> queryDatas(String info,ReturnValue out);
	/**
	 * 
	 * 作用描述：
	 * 修改说明：
	 *@param info json字符串
	 *@param out 异常处理
	 *@return
	 */
	public SquidDataSet queryRuntimeData(String info,ReturnValue out);

	/**
	 * 删除一个Squidflow内所有的Squid
	 * @param info
     * @return
     */
	public String deleteAllSquidsInSquidflow(String info);
	
}