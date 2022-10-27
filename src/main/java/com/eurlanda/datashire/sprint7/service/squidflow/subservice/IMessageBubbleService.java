package com.eurlanda.datashire.sprint7.service.squidflow.subservice;

import com.eurlanda.datashire.entity.DbSquid;
import com.eurlanda.datashire.entity.MessageBubble;
import com.eurlanda.datashire.entity.StageSquid;
import com.eurlanda.datashire.entity.Transformation;
import com.eurlanda.datashire.sprint7.packet.InfoNewPacket;
import com.eurlanda.datashire.sprint7.packet.InfoPacket;

import java.util.List;

/**
 * 消息泡业务处理类
 * @author lei.bin
 *
 */
public interface IMessageBubbleService{
/**
	 * 校验squid是否孤立 新增squid,删除squid,删除link,面板加载时触发
	 * 
	 * @param operateType 操作类型
	 * @param type 枚举类型
	 * @param squidLinks集合
	 * @param infoPackets
	 * @return
	 *//*
	public void isolateSquid(String operateType, DSObjectType type,
			List<SquidLink> squidLinks, List<InfoNewPacket> infoNewPackets,
			List<InfoPacket> infoPackets);*/
	/**
	 * 校验transformation是否孤立
	 * 
	 * @return
	 */
	public List<MessageBubble> isolateTransformation(List<Transformation> transformations,List<InfoPacket> infoPackets,List<InfoNewPacket> infoNewPackets);
	/**
	 * 校验Connection连接信息是否完整、正确
	 * 
	 * @return
	 */
	public void connectionValidation(List<DbSquid> dbSquids);
	/**
	 * 有destination squid link,表名不符合一般关系型数据库表名规范
	 * 
	 * @return
	 */
	public void destinationValidation(
			List<StageSquid> stageSquids);
	/**
	 * 校验表名的正则表达式
	 * 
	 * @param tableName
	 * @return
	 */
	public boolean checkTableName(String tableName);
	/**
	 * 封装消息泡返回值
	 */
	public void setMessageBubble(MessageBubble messageBubble, String squidKey,
			String key, int code, boolean status, int level);
	
}