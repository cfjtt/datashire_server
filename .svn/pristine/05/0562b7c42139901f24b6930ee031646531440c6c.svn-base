package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.entity.operation.StageSquidAndSquidLink;
import com.eurlanda.datashire.sprint7.packet.InfoNewPacket;
import com.eurlanda.datashire.sprint7.packet.InfoPacket;
import com.eurlanda.datashire.utility.ReturnValue;

import java.util.List;

/**
 * DataShirService处理
 * DataShirService业务处理类：处理组合业务
 * Title : 
 * Description: 
 * Author :赵春花 2013-9-4
 * update :赵春花 2013-9-4
 * Department :  JAVA后端研发部
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司
 */
public interface IDataShirServiceplug{
/**
	 * 删除的通用方法
	 * 作用描述：
	 * 修改说明：
	 *@param infoPackets 对象
	 *@param out
	 *@return
	 */
	public boolean deleteRepositoryObject(List<InfoNewPacket> infoPackets,
			ReturnValue out);
	/**
	 * 创建StageSquid和SquidLink
	 * 作用描述：
	 * 前端传入StageSquidAndSquidLink对象包含StageSquid和SquidLink两个对象
	 * 1、根据SquidLink创建连接信息
	 * 2、根据SquidLink对象的来源SquidId查询出来源的Column信息(stageSquidID)
	 * 3、将来源Column信息新增到Column表
	 * 4、新增Transformation信息
	 * 5、查询sourceColumn信息和目标Column信息
	 * 6、查询SourceTransformation信息和目标Transformation信息
	 * 
	 * 验证：
	 * 调用方法前：
	 * StageSquidAndSquidLink对象不能为空 
	 * StageSquid id 不能为空
	 * SquidLink 对象不为空且sourceSquidID和目标SquidID不能为空
	 * 修改说明：
	 *@param squidAndSquidLink squidAndSquidLink对象
	 *@param out 异常返回
	 *@return
	 */
	public StageSquidAndSquidLink createMoreStageSquidAndSquidLink_old(
			StageSquidAndSquidLink squidAndSquidLink, ReturnValue out);
	/**
	 * 创建COlumnm列信息和Transformation信息
	 * 作用描述：
	 * 根据拖入的列创建相应的列和Transformation信息并返回StageSquidAndSquidLink对象
	 * StageSquidAndSquidLink对象包含相关信息
	 * 业务描述：
	 * 1、根据来源SquidId获得SquidLink
	 * 	判断SquidLink是否存在不存在的情况下创建新的数据存在则无需创建
	 * 2、获得sourceColumns来源列的信息
	 * 　根据SquidLink得到sourceId来源ID根据来源ＩＤ查询ｓｑｕｉｄ获得来源表名根据ｓｑｕｉｄＩＤ和表名获得代表SourceTableID
	 * 	 根据SourceTableId获得来源列信息
	 * 3、根据来源列的信息新增当前Squid的列信息
	 * 	根据SquidＩＤ和Ｔａｂｌｅｎａｍｅ获得SourceTableID组装列信息进行批量新增
	 * 4、根据新增列的信息创建Transformation信息
	 * 	 列信息创建成功后根据前台算法创建Transformation信息
	 * 5、组装StageSquidAndSquidLink完整信息返回
	 * 	   stageSquid对象及相关联的信息
	 * 		SquidLink信息
	 * 修改说明：
	 *@param squidAndSquidLink squidAndSquidLink对象
	 *@param out 异常返回
	 *@return
	 */
	public void drag2StageSquid(StageSquidAndSquidLink squidAndSquidLink, ReturnValue out);
	/**
	
	 * 作用描述：根据suqidFlowId 执行一个suqidFlow
	 * @param squidFlowId squidFlow的ID
	 * @param out
	 */
	public void executeFlow(int id, boolean isPush, ReturnValue out);
	/**
	 *处理外部提供的删除方法的返回值转换
	 * @return
	 */
	public boolean getCommons(InfoPacket packet);
	
}