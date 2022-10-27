package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.entity.SquidLink;
import com.eurlanda.datashire.sprint7.packet.InfoPacket;
import com.eurlanda.datashire.utility.ReturnValue;

import java.util.List;

/**
 * 
 * SquidLink业务处理类
 * Title : 
 * Description: 
 * Author :赵春花 2013-10-28
 * update :赵春花 2013-10-28
 * Department :  JAVA后端研发部
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司
 */
public interface ISquidLinkService{
/**
	 * 创建SquidLink对象集合业务处理
	 * 作用描述：
	 * 根据传入的SquidLink对象集合调用SquidLinkPlug的创建SquidLink对象方法
	 * 创建成功的情况下判断该SquidLink是否是DBDESTINATION类型如果是该类型则执行落地操作
	 * 创建成功——根据SquidLink集合对象里的Key查询相对应的ID
	 * 验证：
	 * 1、SquidLink对象集合不能为空且必须有数据
	 * 2、SquidLink对象集合里的每个Key必须有值
	 * 修改说明：
	 *@param squidLinks squidLink集合对象
	 *@param out 异常处理
	 *@return
	 */
	public List<InfoPacket> createSquidLinks(List<SquidLink> squidLinks,ReturnValue out);
	/**
	 * 根据SquidLinkID修改SquidLink信息
	 * 作用描述：
	 * 修改SquidLink对象集合
	 * 调用方法前确保SquidLink对象集合不为空，并且SquidLink对象集合里的每个SquidLink对象的Id不为空
	 * 修改说明：
	 *@param squidLinks squidLink集合对象
	 *@param out 异常处理
	 *@return
	 */
	public List<InfoPacket> updateSquidLinks(List<SquidLink> squidLinks,ReturnValue out);
	/**
	 * 新增SquidLink并同时增加Join信息
	 * 作用描述：
	 * 1、从源dataSquid 创建一个到目标dataSquid的squidLink
	 * 2、在目标dataSquid 上自动创建一个join
	 * 	业务描述：
	 * 		a、如果目标dataSquid 上还没有任何进入的squidLink，
	 * 		新创建的join 类型为Base，否则按顺序追加，
	 * 		默认join 类型为INNER JOIN。
	 * 		b、Condition 自动配对。
	 * 		 	自动配对的规则:
	 * 			假设源dataSquid 的名字为s_customer，主键column 为id。
	 * 			在目标dataSquid 的join 中已经存在的所有其他源dataSquid 的column 中搜索名为customer_id，
	 * 			customer_key，customer_fk,CustomerId 的column，
	 * 			如果发现匹配，则自动生成新join 的condition。比如，假设在名为e_sales 的源dataSquid 中存在CustomerId，
	 * 			则join condition 为s_customer.id =e_sales.CustomerId。
	 * 修改说明：
	 *@param squidLinks
	 *@param out
	 *@return
	 */
	public List<InfoPacket> createSquidLinksAndJoin(List<SquidLink> squidLinks,ReturnValue out);
	
}