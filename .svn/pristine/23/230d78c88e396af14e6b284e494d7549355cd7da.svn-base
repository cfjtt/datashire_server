package com.eurlanda.datashire.socket;

import com.eurlanda.datashire.socket.protocol.Protocol;
import com.eurlanda.datashire.socket.protocol.ProtocolListener;
import com.eurlanda.datashire.utility.CommonConsts;
import com.eurlanda.datashire.utility.StringUtils;
import org.apache.log4j.Logger;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.WriteCompletionEvent;

/**
 * <h3>文件名称：</h3> ServerHandler.java <h3>内容摘要：</h3> 主数据处理句柄，核心维护Channel集合，Channel信息集合，包括连接创建，断开，当然，
 * 最重要的就是对上行消息的处理在messageReceived中 <h3>其他说明：</h3> <h4>调用关系：</h4> {@link ServerPipelineFactory} 的 {@code getPipeline()}
 * 方法内，添加这个handler到Pipeline中 <h4>应用场景：</h4> {@code SocketServer} 接收到外部网络的请求，分析数据包，提取
 */
public class ServerHandler extends SimpleChannelUpstreamHandler {

	static Logger logger = Logger.getLogger(ServerHandler.class);

	/**
	 * client socket >>- ChannelExpand
	 * key: ip(32位，前面不足补零)/token(32位，md5(时间戳)), value: Vector<channel>
	 */
	//private static Hashtable<String, Vector<ChannelExpand>> channelInfo = new Hashtable<String, Vector<ChannelExpand>>();
	

	/**
	 * token >>- 客户端sockent
	 */
	//private static Hashtable<String, String> tokenClient = new Hashtable<String, String>();

	/**
	 * 维护需要推送的Channel对应的扩展信息，key-squidFlowId,value-<token，Channel>
	 */
	//private static Map<Integer, Map<String, Channel>> channelPushMessage = new HashMap<Integer, Map<String, Channel>>();

	/**
	 * 服务器维护的Channel(通道)列表
	 */
	//private static ChannelGroup channels = new DefaultChannelGroup();

	/**
	 * 检查指定的Channel是否存在value (token)是否有这个令牌存在
	 * */
//	public static boolean checkChannelInfo_old(String token) {
//		logger.debug("checkChannelInfo start");
//		logger.debug("source code:" + token);
//		logger.debug("checkChannelInfo end");
//		return channelInfo.containsKey(token);
//	}

	/**
	 * <p>
	 * 作用描述：添加一个指定的token>>vector
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param value
	 *@param chl
	 */
//	public static void addChannelInfo_old(String token, Vector<ChannelExpand> vector) {
//		logger.debug("addChannelInfo start");
//		logger.debug("source code:" + token);
//		channelInfo.put(token, vector);
//		logger.debug("addChannelInfo end");
//	}

	/**
	 * 设置channelInfo队列
	 * @param channelInfo
	 */
//	public static void setChannelInfo(Hashtable<String, Vector<ChannelExpand>> channelInfo) {
//		logger.debug("setChannelInfo size"+channelInfo.size());
//		ServerHandler.channelInfo = channelInfo;
//	}


	/**
	 * 添加一个Socket到集合
	 * 
	 * @param channel
	 *            Socket对象
	 * */
//	public static void addChannel_old(Channel channel) {
//		logger.debug("Add Channel:\t" + channel);
//		channels.add(channel);
//		logger.debug("current channel count:\t" + channels.size());
//	}
	
	/**
	 * 返回Channel列表
	 * */
//	public static ChannelGroup getChannelGroup_old() {
//		logger.trace("getChannelGroup");
//		return channels;
//	}

	/**
	 * 删除一个Socket对象
	 * 
	 * @param channel
	 *            Socket对象
	 * */
//	public static void removeChannel(Channel channel) {
//		//logger.debug("removeChannel start");
//		logger.debug("source code:" + channel);
//		//channels.remove(channel);
//		//logger.debug("removeChannel:" + channels.size());
//		//logger.debug("removeChannel end");
//	}

	/**
	 * 构造器
	 */
	public ServerHandler() {

	}

	/**
	 * channel连接成功事件
	 * 
	 * @param e
	 * @param ctx
	 */
	@Override
	public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
		ChannelManager.channelConnected(e.getChannel());
	}

	/**
	 * channel断开连接事件
	 * 
	 * @param e
	 * @param ctx
	 */
	@Override
	public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
		ChannelManager.channelDisconnected(e.getChannel());
	}

	/**
	 * 异常捕获事件
	 * 
	 * @param e
	 * @param ctx
	 */
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
		ChannelManager.exceptionCaught(e.getChannel(), e.getCause());
	}

	/**
	 * 上行流处理方法
	 * 
	 * @param e
	 * @param ctx
	 */
//	@Override
//	public void handleUpstream(ChannelHandlerContext e, ChannelEvent ctx) throws Exception {
//		super.handleUpstream(e, ctx);
//	}

	/**
	 * 消息处理，对接收到的数据包
	 * 
	 * @param e
	 * @param ctx
	 */
	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
		logger.trace("messageReceived start");
		Channel channel=e.getChannel();
		Heartbeat.updateHearbeat(channel); // TODO 心跳处理可以单独线程 收到消息，标识该通道正常通讯中
		MessagePacket packet = (MessagePacket) e.getMessage();
		if (packet == null) {
			logger.error("packet is null");
			return;
		}
		if (CommonConsts.CMD_ID_HEARTBEAT.equals(packet.getCommandId()) &&
				CommonConsts.CMD_ID_HEARTBEAT.equals(packet.getChildCommandId())) {
			logger.trace("心跳包（已收到）... "+channel.getRemoteAddress());
			return;
		}
		if (StringUtils.isNull(packet.getCommandId()) || StringUtils.isNull(packet.getChildCommandId())) {
			logger.warn("unknow cmd: "+packet.getCommandId()+packet.getChildCommandId());
			return;
		}
		// 根据CommandID得到协议
		Protocol prol = ServerEndPoint.getMainServer().findProtocol(packet.getCommandId());
		if (prol == null) {
			logger.error("prol is null! channel:"+channel.getRemoteAddress()
					+", cmd="+packet.getCommandId()+packet.getChildCommandId());
			return;
		}
		prol.beginInvoke(new ProtocolListener(new ChannelExpand(channel), packet));
		logger.trace("messageReceived end");
	}

	@Override
	public void writeComplete(ChannelHandlerContext ctx, WriteCompletionEvent e) throws Exception {
		logger.debug("[本次发送结束] WriteComplete: " + e.getChannel().getRemoteAddress());
		super.writeComplete(ctx,e);
	}

	/**
	 * 添加一个推送消息的Channel的value (token)
	 * 
	 * @param value
	 *            token
	 * @param id
	 *            Socket id
	 * */
	/*public static void addChannelPushMessage(Integer squidFlowId, String token, Channel channel) {
		logger.debug("addChannelPushMessage start");
		logger.debug("source squidFlowId:" + squidFlowId + ", token:" + token);
		logger.debug("addChannelPushMessage-end:" + getChannels());
		Map<String, Channel> tokenChannelMap = null;
		if (channelPushMessage.containsKey(squidFlowId)) {
			tokenChannelMap = channelPushMessage.get(squidFlowId);
			tokenChannelMap.put(token, channel);
		} else {
			tokenChannelMap = new HashMap<String, Channel>();
			tokenChannelMap.put(token, channel);
		}
		channelPushMessage.put(squidFlowId, tokenChannelMap);
		logger.debug("addChannelPushMessage-end:" + channelPushMessage.get(squidFlowId).size());
		logger.debug("addChannelPushMessage end");
	}*/
	
	/**
	 * 根据token找到对应推送的Channel
	 * */
//	public static Map<String, Channel> getChannelPushMessage(Integer squidFlowId) {
//		logger.debug("getChannelInfo start");
//		logger.debug("source code:" + squidFlowId);
//		logger.debug("getChannelInfo end");
//		return channelPushMessage.get(squidFlowId);
//	}

	//public void writeRequested(ChannelHandlerContext ctx, MessageEvent evt) {
		//Object message = evt.getMessage();
		// Do something with the message to be written.
		// And forward the event to the next handler.
		//ctx.sendDownstream(evt);
	//}
	 
}
