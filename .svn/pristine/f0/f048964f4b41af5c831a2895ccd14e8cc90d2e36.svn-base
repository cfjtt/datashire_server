package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.entity.MessageBubble;
import com.eurlanda.datashire.entity.Squid;
import com.eurlanda.datashire.entity.SquidLink;
import com.eurlanda.datashire.entity.operation.DataEntity;
import com.eurlanda.datashire.entity.operation.WhereCondition;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.enumeration.MatchTypeEnum;
import com.eurlanda.datashire.enumeration.MessageBubbleCode;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.enumeration.SystemTableEnum;
import com.eurlanda.datashire.server.utils.TokenUtil;
import com.eurlanda.datashire.sprint7.packet.InfoPacket;
import com.eurlanda.datashire.sprint7.packet.PushMessagePacket;
import com.eurlanda.datashire.sprint7.plug.DataPlug;
import com.eurlanda.datashire.sprint7.plug.GetParam;
import com.eurlanda.datashire.sprint7.plug.SquidLinkPlug;
import com.eurlanda.datashire.sprint7.plug.SquidPlug;
import com.eurlanda.datashire.sprint7.plug.SupportPlug;
import com.eurlanda.datashire.sprint7.service.squidflow.subservice.MessageBubbleService;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.util.ArrayList;
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
public class SquidLinkService  extends SupportPlug{
	static Logger logger = Logger.getLogger(SquidLinkService.class);// 记录日志
	private String token;
	public SquidLinkService(String token) {
		super(token);
		this.token = token;
	}
	SquidLinkPlug squidLinkPlug = new SquidLinkPlug(adapter);

	
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
	public List<InfoPacket> createSquidLinks(List<SquidLink> squidLinks,ReturnValue out){
		logger.debug(String.format("createProject-projectList.size()=%s", squidLinks.size()));
		//定义接收返回结果集
		List<InfoPacket> infoPackets = new ArrayList<InfoPacket>();
		boolean create = false;
		try {
			create = squidLinkPlug.createSquidLinks(squidLinks, out);
			//新增成功
			if(create){
				//根据Key查询出新增的ID
				SquidPlug squidPlug = new SquidPlug(adapter);
				DataPlug dataPlug = new DataPlug(TokenUtil.getToken());
				for (SquidLink squidLink : squidLinks) {
					//查询根据ID查询Squid
					Squid squid = squidPlug.getSquid(squidLink.getTo_squid_id(), out);
					if(squid != null){
						if(squid.getSquid_type()== SquidTypeEnum.DBDESTINATION.value()){
							//落地
							create = dataPlug.createTable(squidLink.getFrom_squid_id(), squid.getId(), out);
						}
					}	
					//获得Key
					String key = squidLink.getKey();
					int id = squidLinkPlug.getSquidLinkId(key, out);
					InfoPacket infoPacket = new InfoPacket();
					infoPacket.setId(id);
					infoPacket.setKey(key);
					infoPackets.add(infoPacket);
				}
			}
			logger.debug(String.format("createUsers-return=%s",infoPackets ));
		} catch (Exception e) {
			out.setMessageCode(MessageCode.ERR_ARRAYS);
		}finally{
			
			//释放连接
			adapter.commitAdapter();
		}
	
		return infoPackets;
	}
	
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
	public List<InfoPacket> updateSquidLinks(List<SquidLink> squidLinks,ReturnValue out){
		//boolean create = false;
		//查询返回结果
		//List<InfoPacket> infoPackets = new ArrayList<InfoPacket>();
		try {
			//封装批量新增数据集
			List<List<DataEntity>>  paramList = new ArrayList<List<DataEntity>>(squidLinks.size());
			//封装条件集合
			List<List<WhereCondition>>  whereCondList = new ArrayList<List<WhereCondition>>(squidLinks.size());
			//调用转换类
			GetParam getParam = new GetParam();
			//for (SquidLink squidLink : squidLinks) {
			for (int i=0; i<squidLinks.size(); i++) {
				// 调试：看前台到底一次发送了多了link
				logger.debug(i+"/"+squidLinks.size()+"\t[更新squid-link]\t"+squidLinks.get(i).getId()
						+"\tfr:"+squidLinks.get(i).getFrom_squid_id()
						+"\tto:"+squidLinks.get(i).getTo_squid_id()
						);
				if(squidLinks.get(i).getFrom_squid_id()<=0 || squidLinks.get(i).getTo_squid_id()<=0){
					continue; // 这些应该是创建失败的squid，数据库里没有的，所以更新没意义
				}
				//参数
				List<DataEntity> dataEntitys =  new ArrayList<DataEntity>();
				getParam.getSquidLink(squidLinks.get(i), dataEntitys);
				paramList.add(dataEntitys);
				//条件
				List<WhereCondition> whereConditions = new ArrayList<WhereCondition>(1);
				whereConditions.add(new WhereCondition("ID", MatchTypeEnum.EQ, squidLinks.get(i).getId()));
				whereCondList.add(whereConditions);
			}
			//调用批量修改
			adapter.updatBatch(SystemTableEnum.DS_SQUID_LINK.toString(), paramList, whereCondList, out);
			/*logger.debug(String.format("updateSquidLinks-return=%s",create ));
			if(create){
				SquidLinkPlug squidLinkPlug = new SquidLinkPlug(adapter);
				//定义接收返回结果集
				infoPackets = new ArrayList<InfoPacket>();
				//根据Key查询出新增的ID
				for (SquidLink squidLink : squidLinks) {
					//获得Key
					String key = squidLink.getKey();
					int id = squidLinkPlug.getSquidLinkId(key, out);
					InfoPacket infoPacket = new InfoPacket();
					infoPacket.setCode(1);
					infoPacket.setId(id);
					infoPacket.setType(DSObjectType.SQUIDLINK);
					infoPacket.setKey(key);
					infoPackets.add(infoPacket);
				}
			}*/
		} catch (Exception e) {
			logger.error("updateSquidLinks-return=%s", e);
			out.setMessageCode(MessageCode.ERR_ARRAYS);
		} finally { //释放
			if(adapter!=null)adapter.commitAdapter();
		}
		return null;
	}
	
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
	public List<InfoPacket> createSquidLinksAndJoin(List<SquidLink> squidLinks,ReturnValue out){
		logger.debug(String.format("createSquidLinksAndJoin-squidLinkList.size()=%s", squidLinks.size()));	
		List<InfoPacket> infoPackets = new ArrayList<InfoPacket>();
		List<MessageBubble> list=new ArrayList<MessageBubble>();
		boolean create = false;
		try {
			//1、从源dataSquid 创建一个到目标dataSquid的squidLink
			create = squidLinkPlug.createSquidLinks(squidLinks, out);
			//创建成功的情况下
			if(create){
				//JoinPlug joinPlug = new JoinPlug(adapter);
				DataPlug dataPlug = new DataPlug(TokenUtil.getToken());
				SquidPlug squidPlug = new SquidPlug(adapter);
				//List<SquidJoin> joins = new ArrayList<SquidJoin>();
				for (SquidLink squidLink : squidLinks) {
					int toSquidId = squidLink.getTo_squid_id();
					//查询根据ID查询Squid
					Squid squidFrom=squidPlug.getSquid(squidLink.getFrom_squid_id(), out);
					Squid squidTo = squidPlug.getSquid(toSquidId, out);
					if(toSquidId<=0||squidTo.getId()<=0){
						continue;
					}
					if(squidTo.getSquid_type()== SquidTypeEnum.DBDESTINATION.value()&&squidFrom.getSquid_type()==SquidTypeEnum.STAGE.value()){
						//落地
						create = dataPlug.createTable(squidLink.getFrom_squid_id(), squidTo.getId(), out);
						if(!create)
						{
							logger.debug("落地失败======");
						}
					}
				/*	else{
						//查询目标DataSquid是否有进入的SquidLink
						List<SquidLink> sourceSquidLinks = squidLinkPlug.getFromSquidLinks(toSquidId, out);
						SquidJoin join = new SquidJoin();
						join.setJoined_squid_id(squidLink.getFrom_squid_id());
						//TODO 
						//int prior_join_id = joinPlug.getJoinByPriorJoinId(toSquidId, out);
						int prior_join_id = sourceSquidLinks==null?0:sourceSquidLinks.size();
						join.setPrior_join_id(prior_join_id);
						join.setTarget_squid_id(toSquidId);
						join.setKey(StringUtils.generateGUID());
						join.setJoinType(prior_join_id<=1?JoinType.BaseTable.value():JoinType.InnerJoin.value());
						join.setJoin_Condition("");
						joins.add(join);
					}*/
					//取消squidlink 的from_squid 和to_squid的消息泡
					list.add(new MessageBubble(squidFrom.getKey(), squidFrom.getKey(), MessageBubbleCode.WARN_SQUID_NO_LINK.value(),true));
					list.add(new MessageBubble(squidTo.getKey(), squidTo.getKey(), MessageBubbleCode.WARN_SQUID_NO_LINK.value(),true));
					// 推送校验表名的消息泡(从stage到dest)无法调用通用方法
					if (org.apache.commons.lang.StringUtils
							.isNotBlank(squidFrom.getTable_name())) {
						MessageBubbleService service = new MessageBubbleService(
								TokenUtil.getToken());
						if (!service.checkTableName(squidFrom.getTable_name())) {
							list.add(new MessageBubble(squidFrom.getKey(),
									squidFrom.getKey(),
									MessageBubbleCode.ERROR_DB_TABLE_NAME
											.value(), false));
						}
					} else {
						list.add(new MessageBubble(squidFrom.getKey(),
								squidFrom.getKey(),
								MessageBubbleCode.ERROR_DB_TABLE_NAME.value(),
								false));
					}
				}
				/*//新增join信息
				create = joinPlug.createJoin(joins, out);*/
				if(create){
					//查询SquidLink新增信息的Id信息
					for (SquidLink link : squidLinks) {
						InfoPacket infoPacket = new InfoPacket();
						 int id = squidLinkPlug.getSquidLinkId(link.getKey(), out);
						 infoPacket.setId(id);
						 infoPacket.setKey(link.getKey());
						 infoPacket.setType(DSObjectType.SQUIDLINK);
						 infoPackets.add(infoPacket);
					}
				}
				PushMessagePacket.push(list, TokenUtil.getToken());//推送消息泡
			}
		} catch (Exception e) {
			logger.error("createSquidLinksAndJoin-return=%s", e);
			try {
				adapter.rollback();
			} catch (SQLException e1) {
				logger.error("rollbackException", e1);
			}
			out.setMessageCode(MessageCode.SQL_ERROR);
		} finally { //释放
			if(!create)
			{
				try {
					//事务回滚
					adapter.rollback();
					//关闭连接
					adapter.closeAdapter();
				} catch (SQLException e) {
					logger.error("rollbackException",e);
				}
			}else{
				adapter.commitAdapter();
			}
		}
		return infoPackets;
	}
	
}
