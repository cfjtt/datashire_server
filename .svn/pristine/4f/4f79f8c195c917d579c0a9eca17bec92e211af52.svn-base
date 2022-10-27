/*
 * ChannelManager.java - 2013.10.26
 *
 * 版权声明: (c) 2013，悦岚（上海）数据服务有限公司，保留所有权利。
 *
 * 项目名称: datashire-server
 * 
 * 修改历史:
 * ===========================================
 *   修改人	     日期		     描述
 *   ---------------------------------------
 *   dang.lu  2013.10.26   create    
 * ===========================================
 */
package com.eurlanda.datashire.socket;

import com.eurlanda.datashire.utility.CommonConsts;
import com.eurlanda.datashire.utility.StringUtils;
import org.apache.log4j.Logger;
import org.jboss.netty.channel.Channel;

import java.util.Hashtable;
import java.util.List;
import java.util.Vector;

/**
 * 维护socket连接通道
 * 
 * @author dang.lu 2013.10.26
 *
 */
public class ChannelManager {
	private static Logger logger = Logger.getLogger(ChannelManager.class);

	/**
	 * client socket >>- ChannelExpand
	 * key: ip(32位，前面不足补零)/token(32位，md5(时间戳)), value: Vector<channel>
	 */
	private static Hashtable<String, Vector<ChannelExpand>> channelInfo = new Hashtable<String, Vector<ChannelExpand>>();
	
	public static void channelDisconnected(Channel channel) {
		logger.info("[失去连接] channel disconnected: " + channel);
		channel.close();
	}
	public static void exceptionCaught(Channel channel, Throwable e) {
		logger.info("[通道连接异常] "+channel+"\t"+e.getMessage()); // 这种异常一般是客户端主动断连接
		if(e.getMessage()==null)logger.error("Unexpected exception from downstream.", e); // 没有异常消息描述？是什么情况？
	}
	
	
	public static void channelConnected(Channel channel) {
		logger.info("[建立连接] ChannelConnected: " + channel);
		//获取remoteAddress
		String remoteAddress=StringUtils.supplyNumber(
				StringUtils.getIp(channel.getRemoteAddress()), 
				CommonConsts.PACKET_TOKEN_LENGTH);
		if(channelInfo.containsKey(remoteAddress)){
			channelInfo.get(remoteAddress).add(new ChannelExpand(channel));
		}else{
			//新创建一个队列
			Vector<ChannelExpand> vector=new Vector<ChannelExpand>(1,3);
			//放入队列中
			vector.add(new ChannelExpand(channel));
			channelInfo.put(remoteAddress, vector);
		}
	}
	
	/**
	 * <p>
	 * 作用描述：获取等待的数据包channel
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param token
	 *@return
	 */
	public static synchronized ChannelExpand peek(String token,int i){
		Vector<ChannelExpand> vector=channelInfo.get(token);
		int channelSize = vector==null?-1:vector.size();
//		if(channelSize>=1&&i>=channelSize){
//			--i; // 如果通道数大于1，但是递归的index跳过了，则回退一个
//			try {
//				Thread.sleep(20); // 如果只有一个channel可用的话，等人家用完了在说 （先休眠10毫秒吧）
//			} catch (InterruptedException e) {
//				logger.error("等待空闲channel被打扰！", e);
//			}
//		}
		ChannelExpand channelExpand=vector.get(i);
		if(channelExpand.isLockFlag()||!isCheckChannel(token,channelExpand)){
//			if(MessageQueueManager.peek(token,0)==null){
//				logger.info("在获取通道时发现消息已经全部发送完毕！");
//				return null;
//			}
			for(int k=0; k<channelSize; k++){
				logger.debug(k+" [连接信息] token="+token+"  "+vector.get(k));
			}
			logger.warn(i+"/"+channelSize+"\t[该通道正忙] "+channelExpand.getChannle().getRemoteAddress());
			if(++i>=channelSize){
				logger.warn(i+"/"+channelSize+" [通道全忙/无连接] "+token); //TODO 如果客户端所有连接都断开，则不再发送！
				return null;
			}
			try {
				Thread.sleep(10); // 如果只有一个channel可用的话，等人家用完了在说 （先休眠10毫秒吧）
			} catch (InterruptedException e) {
				logger.error("等待空闲channel被打扰！", e);
			}
			return peek(token, i);
		}
		channelExpand.setLockFlag(true, Thread.currentThread().getName());
		logger.debug(i+"/"+vector.size()+" [通道可用] "+channelExpand.getChannle().getRemoteAddress());
		return channelExpand;
	}
	
	/**
	 * <p>
	 * 作用描述：验证通道是否可用等情况
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 * 
	 * @return
	 */
	private static  boolean isCheckChannel(String token,ChannelExpand channelExpand) {
		Channel channel=channelExpand.getChannle();
		// 判断是否为空
		if (channel == null) {
			logger.error(String.format("token:(%s) channle Object is null!", token));
			return false;
		}
		// 判断是否可读可写
		if (!channel.isConnected() || !channel.isOpen() || !channel.isReadable() || !channel.isWritable()) {
			logger.warn("[通道正忙或连接已失效] "+token+"\t"+channelExpand);
			return false;
		}
		return true;
	}
	
	/**
	 * <p>
	 * 作用描述：替换客户端IP为token,在登录成功后
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 * 
	 * @param token
	 */
	public static void replaceChannelInfo(String token, Channel channel) {
		logger.debug("begin replaceChannelInfo!");
		// 判断token、channel是否为空，如果为空，则不做替换；
		if (token == null || channel == null) {
			logger.warn("token or channel is null!");
			return;
		}
		// 获取客户端
		String remoteAddress = StringUtils.supplyNumber(
				StringUtils.getIp(channel.getRemoteAddress()), 
				CommonConsts.PACKET_TOKEN_LENGTH);
		// 获取服务器Channel集合
		//Hashtable<String, Vector<ChannelExpand>> channelInfo = ServerHandler.getChannelInfo();

		// 判断是否存在远程的客户端socket
		if (channelInfo.containsKey(remoteAddress)) {
			// 加入新的socken信息，并绑定token
			channelInfo.put(token, channelInfo.remove(remoteAddress));
		}
		//设置socket队列
		//ServerHandler.setChannelInfo(channelInfo);
		logger.debug("end replaceChannelInfo!");
	}
	
	/**
	 * 获得channelInfo队列
	 * @return channelInfo
	 */
	public static Hashtable<String, Vector<ChannelExpand>> getChannelInfo() {
		//logger.debug("getChannelInfo size"+channelInfo.size());
		return channelInfo;
	}
	
	/**
	 * 得到指定Channel的value (token)
	 * */
	public static Vector<ChannelExpand> getChannelInfo(String token) {
		return channelInfo.get(token);
	}

	/**
	 * 删除指定的token
	 * 
	 */
	public static void removeChannelInfo(String token) {
		logger.warn("[删除全部连接] RemoveChannelInfo, token="+token+"\tchannel size=" + channelInfo.size());
		channelInfo.remove(token);
	}
	
	public static void removeQueueChannel(String token,ChannelExpand channel) {
		logger.warn("[删除连接] RemoveChannelInfo, token="+token +"\t" + channel);
		int i = channelInfo.get(token).indexOf(channel);
		if (i >= 0) {
			channelInfo.get(token).removeElementAt(i);
		}
		//channelInfo.get(token).remove(channel);
	}
	
	public static void removeQueueChannel(String token, List<ChannelExpand> channel) {
		if(channel!=null){
			for(int i=0; i<channel.size(); i++){
				logger.warn(i+" [删除连接] token="+token +" " + channel.get(i));
				channelInfo.get(token).remove(channel.get(i));
			}
		}
	}
	
}