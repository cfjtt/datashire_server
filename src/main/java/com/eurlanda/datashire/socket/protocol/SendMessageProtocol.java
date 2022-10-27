package com.eurlanda.datashire.socket.protocol;

import com.eurlanda.datashire.server.utils.TokenUtil;
import com.eurlanda.datashire.socket.ChannelExpand;
import com.eurlanda.datashire.socket.ChannelManager;
import com.eurlanda.datashire.socket.MessagePacket;
import com.eurlanda.datashire.socket.MessageQueueManager;
import com.eurlanda.datashire.utility.StringUtils;
import org.apache.log4j.Logger;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;

/**
 * 
 * <p>
 * Title : 消息发送处理逻辑
 * </p>
 * <p>
 * Description:
 * </p>
 * <p>
 * Author :何科敏 Sep 12, 2013
 * </p>
 * <p>
 * update :何科敏 Sep 12, 2013
 * </p>
 * <p>
 * Department : JAVA后端研发部
 * </p>
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司 </p>
 */
public class SendMessageProtocol{

	static Logger logger = Logger.getLogger(SendMessageProtocol.class);

	/**
	 *指定单个客户端消息发送
	 */
	public void sendMessage(MessagePacket packet) {
		String token = new String(packet.getToken(), 0, 32);
		if(StringUtils.isNotNull(token)){
			MessageQueueManager.push(token, packet);
			new SendMessageThread(token).start();
		}else{
			logger.warn("token is null ! "+packet);
		}
	}
	
	private static class SendMessageThread extends Thread{

		/**
		 * 当前发送的token
		 */
		private String token;

		/**
		 * 计算频率发生次数
		 */
		//private static int frequency = 0;

		/**
		 * 当前应用的channle
		 */
		//private ChannelExpand channelExpand;
		
		/**
		 * 当前发送的数据包
		 */
		//private ChannelPacket packet;

		/**
		 * 带token参数构造函数
		 * 
		 * @param token
		 */
		public SendMessageThread(String token) {
			this.token = token;
		}

		public void run() {
			check();
		}
		
		private void check() {
				//获取指定通道
				ChannelExpand channelExpand=ChannelManager.peek(TokenUtil.getToken(),0);
				//判断是否可用
				if (channelExpand!=null) {
					//获得等待的数据包
					MessagePacket packet=MessageQueueManager.peek(TokenUtil.getToken(),0);
					if(packet!=null){
						sendMessage2(channelExpand, packet);
					}else{ // TODO 通道还回 （释放连接，很好很强大） 
						channelExpand.setLockFlag(false, Thread.currentThread().getName());
					}
				}
		}
		
		private synchronized void sendMessage2(final ChannelExpand channelExpand, final MessagePacket packet) {
			final String cmdId = packet.getCommandId()+packet.getChildCommandId();
			logger.debug("[发送消息] token="+token+", 编号="+packet.getId()+", 命令号="+cmdId);
			// 往客户端发生数据包 (准备进入消息编码阶段)
			ChannelFuture channelFuture=channelExpand.getChannle().write(packet);
			// 监听客户端写包事件
			channelFuture.addListener(new ChannelFutureListener(){
				public void operationComplete(ChannelFuture arg0) throws Exception {
					channelExpand.setLockFlag(false, Thread.currentThread().getName());
					logger.debug("[发送完成] 释放通道连接 "+channelExpand); // TODO 如果发送异常或其他原因导致通道长时间未释放呢？
					if(arg0.isSuccess()){ // 发送成功 移出消息
						if(MessageQueueManager.remove(TokenUtil.getToken(), packet)>0){
							logger.warn("[消息积压] 继续发送。。。");
							try { Thread.sleep(80); } catch (InterruptedException e) { }
							check();
						}
					}else{
						packet.setLockFlag(false); // 发送失败，将消息解锁 期待重新发送
						logger.warn("[发送消息失败] cmd="+cmdId+", token="+token+", packet="+packet);
						check();
					}
				}
			});
		}
	}
	
}