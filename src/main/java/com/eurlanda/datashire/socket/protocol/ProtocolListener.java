package com.eurlanda.datashire.socket.protocol;

import com.eurlanda.datashire.socket.ChannelExpand;
import com.eurlanda.datashire.socket.ChannelManager;
import com.eurlanda.datashire.socket.MessagePacket;
import com.eurlanda.datashire.socket.ServerHandler;
import com.eurlanda.datashire.utility.CommonConsts;
import com.eurlanda.datashire.utility.StringUtils;
import org.apache.log4j.Logger;

import java.util.Vector;

/**
 * 
 * <h3>文件名称：</h3>
 * ProtocolListener.java
 * <h3>内容摘要：</h3>
 * 实现ProtocolCallback接口，用于具体的协议执行监听者
 * <h3>其他说明：</h3>
 * <h4>调用关系：</h4>
 * 被具体协议的 {@code beginInvoke} 及 {@code endInvoke} 方法调用
 * <h4>应用场景：</h4>
 * 被 {@link ServerHandler} 的 {@code messageReceived} 方法中被调用，作为回调传输到具体协议的 {@code beginInvoke} 方法中
 */
public class ProtocolListener implements ProtocolCallback {

	static Logger logger = Logger.getLogger(ProtocolListener.class);
	
	/**
	 * 创建这个监听者的源Channel，用于调用完成时，对Channel进行回写
	 */
	private ChannelExpand channelExpand;
	
	/**
	 * 保存提交过来的数据包，用于回调的时候，构造数据需要
	 */
	private MessagePacket lastPacket;

	public ProtocolListener(){

	}

	/**
	 * 构造方法
	 * @param chl    Socket对象
	 * @param packet    Socket数据包
	 */
	public ProtocolListener(ChannelExpand chl, MessagePacket packet){
		this.channelExpand = chl;
		this.lastPacket = packet;
	}


	
	/**
	 * 操作完成方法，回调方法
	 * 
	 * @param packet    下发协议数据包
	 * @param protocol    下发协议
	 */
	public void operationComplete(MessagePacket packet, Protocol protocol){
		logger.trace("operationComplete start");
//		if("0001".equals(packet.getCommandId()) &&
//				("0016".equals(packet.getChildCommandId()) ||
//				 "0017".equals(packet.getChildCommandId()) ||
//				 "0045".equals(packet.getChildCommandId()))){
//			return;
//		}
		if(channelExpand == null || !channelExpand.getChannle().isConnected()/* || !channelExpand.getChannle().isWritable()*/){ // isWritable=false, may be channel is in writing data
			logger.warn("channel 为空 失去连接 或不可写！"+channelExpand);
			return;
		}
		//ChannelPacket channelPacket = AbstractProtocol.createDownPacket(packet, protocol);
		//logger.debug("CommandId:"+packet.getCommandId() + ",ChildCommandId:" + packet.getChildCommandId()+",Data:" + new String(channelPacket.getData()));
		//获取token
		String token=new String(packet.getToken(), 0, CommonConsts.PACKET_TOKEN_LENGTH);
		Vector<ChannelExpand> v = ChannelManager.getChannelInfo(token);
		//if(token.replace("0", "").equals("")){ // 空间换时间
		if(CommonConsts.DEFAULT_EMPTY_TOKEN.equals(token) || // 登录前token为空
				v==null || v.isEmpty()){ // 有token，但是服务器重启丢失channel
			//首次访问不能通过token查找，必须通过客户端地址进行查找
			token=StringUtils.supplyNumber(
					StringUtils.getIp(channelExpand.getChannle().getRemoteAddress()), 
					CommonConsts.PACKET_TOKEN_LENGTH);
			packet.setToken(token.getBytes());
		}
		//数据包入队
	    new SendMessageProtocol().sendMessage(packet);
	    logger.trace("operationComplete end");
	}
	
	/**
	 * @return channelExpand
	 */
	public ChannelExpand getChannelExpand() {
		return channelExpand;
	}

	/**
	 * 得到Listner关联的上行数据包
	 * */
	public MessagePacket getChannelPacket() {
		return this.lastPacket;
	}

}