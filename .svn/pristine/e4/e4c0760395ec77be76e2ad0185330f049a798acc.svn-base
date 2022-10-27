
package com.eurlanda.datashire.sprint7.packet;

import com.alibaba.fastjson.JSON;
import com.eurlanda.datashire.entity.MessageBubble;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.server.utils.SocketUtil;
import com.eurlanda.datashire.socket.MessagePacket;
import com.eurlanda.datashire.utility.CommonConsts;
import com.eurlanda.datashire.utility.StringUtils;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 数据推送
 * 
 * <p>
 * Title : 
 * </p>
 * <p>
 * Description: 
 * </p>
 * <p>
 * Author :赵春花 2013-10-17
 * </p>
 * <p>
 * update :赵春花 2013-10-17
 * </p>
 * <p>
 * Department :  JAVA后端研发部
 * </p>
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司
 * </p>
 */
public class PushMessagePacket {
	static Logger logger = Logger.getLogger(PushMessagePacket.class);
	/**
	 * 推送信息
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param token
	 *@param key
	 *@param dataList
	 *@param type
	 *@param commandId
	 *@param childCommandId
	 */
	public void pushInformation(final String token,final String key,String dataList,DSObjectType type,final String commandId,final String childCommandId){
		/*ProtocolData protocolData = new ProtocolData();
		protocolData.setData(dataList);
		protocolData.setType(type);*/
	 
		MessagePacket packet = new MessagePacket();
		//packet.setId(MessagePacket.geturrPacketID());
		packet.setCommandId(commandId);
		packet.setChildCommandId(childCommandId);
		packet.setData(StringUtils.str2Bytes(JSON.toJSONString(dataList)));
		packet.setGuid(StringUtils.str2Bytes(key));
		packet.setToken(StringUtils.str2Bytes(token));
		//zjweii: 新的发送方法。
		SocketUtil.sendMessage(packet);
		//new SendMessageProtocol().sendMessage(packet);
	}
	
	/**
	 * 消息气泡推送
	 * @param list
	 * @param token
	 */
	public static final void push(List<MessageBubble> list, final String token){
		logger.debug("[消息气泡推送]\ttoken="+token+"\t"+list);
		// SquidFlow打开的时候，如果消息泡的状态是true,那么不向客户端发送请求
/*		if(!CommonConsts.executePushMessageBubbleFlag){
			if(StringUtils.isNotNull(list) && !list.isEmpty()){
				MessageBubble messageBubble = list.get(0);
				// 如果消息泡的状态是true, 那么不向客户端发送请求
				if(StringUtils.isNotNull(messageBubble)){
					if(!messageBubble.isStatus()){
						// warn error 推送拦截
						push(list,DSObjectType.UNKNOWN,
								CommonConsts.CMD_ID_MESSAGE_BUBBLE,CommonConsts.CMD_ID_MESSAGE_BUBBLE,
								CommonConsts.DEFAULT_EMPTY_KEY,token);
						
					}
				}
			}
		}
		else {
			// warn error 推送拦截
			push(list,DSObjectType.UNKNOWN,
					CommonConsts.CMD_ID_MESSAGE_BUBBLE,CommonConsts.CMD_ID_MESSAGE_BUBBLE,
					CommonConsts.DEFAULT_EMPTY_KEY,token);
		}*/
		
		// warn error 推送拦截
		push(list,DSObjectType.UNKNOWN,
				CommonConsts.CMD_ID_MESSAGE_BUBBLE,CommonConsts.CMD_ID_MESSAGE_BUBBLE,
				CommonConsts.DEFAULT_EMPTY_KEY,token);
	}
	
	public static final void push(MessageBubble msg, final String token){
		logger.debug("[消息气泡推送]\ttoken="+token+"\t"+msg);
		if(msg!=null){
			List<MessageBubble> list =new ArrayList<MessageBubble>(1);
			list.add(msg);
			// warn error 推送拦截
			push(list,DSObjectType.UNKNOWN,
					CommonConsts.CMD_ID_MESSAGE_BUBBLE,CommonConsts.CMD_ID_MESSAGE_BUBBLE,
					CommonConsts.DEFAULT_EMPTY_KEY,token);
		}
	}
	
	/**
	 * 推送集合消息
	 * @param list
	 * @param t
	 * @param cmdId
	 * @param cmdId2
	 * @param key
	 * @param token
	 */
	public static final void push(List<?> list, DSObjectType t, final String cmdId, final String cmdId2, final String key, final String token){
		logger.debug("[业务推送-begin]\tcmd="+cmdId+cmdId2+"\ttype="+t+"\tkey="+key+"\ttoken="+token);
		if(list==null || list.isEmpty() || !CommonConsts.isValidToken(token)){
			return;
		}
		//logger.debug("[业务推送]\tcmd="+cmdId+cmdId2+"\ttype="+t+"\tkey="+key+"\ttoken="+token+"\t"+list);
		ListMessagePacket listMessagePacket = new ListMessagePacket();
		listMessagePacket.setCode(1);
		listMessagePacket.setDataList(list);
		listMessagePacket.setType(t);
 
		/*ProtocolData protocolData = new ProtocolData();
		protocolData.setData(JSON.toJSONString(listMessagePacket));
		if(StringUtils.isNotNull(JSON.toJSONString(listMessagePacket))){
		    logger.info("消息泡 包大小：" + (JSON.toJSONString(listMessagePacket).length() + 162));
		}
		else {
		    logger.info("消息泡 包大小：" + -1);
		}
//		protocolData.setData(JsonUtil.toGsonString(listMessagePacket));
		protocolData.setType(t);*/
		if(StringUtils.isNotNull(JSON.toJSONString(listMessagePacket))){
		    logger.info("消息泡 包大小：" + (JSON.toJSONString(listMessagePacket).length() + 162));
		}
		else {
		    logger.info("消息泡 包大小：" + -1);
		}

		MessagePacket packet = new MessagePacket();
		//packet.setId(MessagePacket.geturrPacketID());
		packet.setCommandId(cmdId);
		packet.setChildCommandId(cmdId2);
		packet.setData(StringUtils.str2Bytes(JSON.toJSONString(listMessagePacket)));
		packet.setGuid(StringUtils.str2Bytes(key));
		packet.setToken(StringUtils.str2Bytes(token));
		//new SendMessageProtocol().sendMessage(packet);

        SocketUtil.sendMessage(packet);
		//logger.debug("[业务推送-end]\tcmd="+cmdId+cmdId2+"\ttype="+t+"\tkey="+key+"\ttoken="+token+"\t"+new String(packet.getData()));
	}

	public static final void pushMap(Map<String,Object> map, DSObjectType t, final String cmdId, final String cmdId2, final String key, final String token){
		logger.debug("[业务推送-begin]\tcmd="+cmdId+cmdId2+"\ttype="+t+"\tkey="+key+"\ttoken="+token);
		if(map==null || map.isEmpty() || !CommonConsts.isValidToken(token)){
			return;
		}
		//logger.debug("[业务推送]\tcmd="+cmdId+cmdId2+"\ttype="+t+"\tkey="+key+"\ttoken="+token+"\t"+list);
		InfoNewMessagePacket listMessagePacket = new InfoNewMessagePacket();
		listMessagePacket.setCode(1);
		//String objStr = JsonUtil.toGsonString(map);
		listMessagePacket.setInfo(map);
		listMessagePacket.setType(t);
 
		// update by yi.zhou 去壳，减少解析步骤
		/*ProtocolData protocolData = new ProtocolData();
		protocolData.setData(JSON.toJSONString(listMessagePacket));
		protocolData.setType(t);*/
	 
		MessagePacket packet = new MessagePacket();
		//packet.setId(MessagePacket.geturrPacketID());
		packet.setDsObjectType(t.value());
		packet.setCommandId(cmdId);
		packet.setChildCommandId(cmdId2);
		String result=JSON.toJSONString(listMessagePacket);
       /* if(logger.isDebugEnabled()) {
            logger.debug("发送数据============" + result);
        }*/
		packet.setData(StringUtils.str2Bytes(result));
		packet.setGuid(StringUtils.str2Bytes(key));
		packet.setToken(StringUtils.str2Bytes(token));
		//new SendMessageProtocol().sendMessage(packet);

		SocketUtil.sendMessage(packet);
		//logger.debug("[业务推送-end]\tcmd="+cmdId+cmdId2+"\ttype="+t+"\tkey="+key+"\ttoken="+token+"\t"+new String(packet.getData()));
	}
	
	public static final void pushMap(Map<String,Object> map, DSObjectType t, final String cmdId, final String cmdId2, final String key, final String token, int messageCode){
		logger.debug("[业务推送-begin]\tcmd="+cmdId+cmdId2+"\ttype="+t+"\tkey="+key+"\ttoken="+token);
		if(map==null || map.isEmpty() || !CommonConsts.isValidToken(token)){
			return;
		}
		//logger.debug("[业务推送]\tcmd="+cmdId+cmdId2+"\ttype="+t+"\tkey="+key+"\ttoken="+token+"\t"+list);
		InfoNewMessagePacket listMessagePacket = new InfoNewMessagePacket();
		listMessagePacket.setCode(messageCode);
		//String objStr = JsonUtil.toGsonString(map);
		listMessagePacket.setInfo(map);
		listMessagePacket.setType(t);
 
		// update by yi.zhou 去壳，减少解析步骤
		/*ProtocolData protocolData = new ProtocolData();
		protocolData.setData(JSON.toJSONString(listMessagePacket));
		protocolData.setType(t);*/
	 
		MessagePacket packet = new MessagePacket();
		//packet.setId(MessagePacket.geturrPacketID());
		packet.setDsObjectType(t.value());
		packet.setCommandId(cmdId);
		packet.setChildCommandId(cmdId2);
		packet.setData(StringUtils.str2Bytes(JSON.toJSONString(listMessagePacket)));
		packet.setGuid(StringUtils.str2Bytes(key));
		packet.setToken(StringUtils.str2Bytes(token));
		//new SendMessageProtocol().sendMessage(packet);

        SocketUtil.sendMessage(packet);
		//logger.debug("[业务推送-end]\tcmd="+cmdId+cmdId2+"\ttype="+t+"\tkey="+key+"\ttoken="+token+"\t"+new String(packet.getData()));
	}
}
