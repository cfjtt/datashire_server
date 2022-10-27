package com.eurlanda.datashire.sprint7.socket;

import com.eurlanda.datashire.socket.MessagePacket;
import com.eurlanda.datashire.socket.ServerPipelineFactory;
import com.eurlanda.datashire.socket.protocol.ResponseStatus;
import com.eurlanda.datashire.utility.CommonConsts;
import com.eurlanda.datashire.utility.StringUtils;
import net.sf.json.JSONObject;
import org.apache.log4j.Logger;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * <h3>文件名称：</h3> ServerHandler.java
 * <h3>内容摘要：</h3> 主数据处理句柄，核心维护Channel集合，Channel信息集合，包括连接创建，断开，当然，
 * 最重要的就是对上行消息的处理在messageReceived中
 * <h3>其他说明：</h3>
 * <h4>调用关系：</h4> {@link ServerPipelineFactory} 的 {@code getPipeline()}
 * 方法内，添加这个handler到Pipeline中
 * <h4>应用场景：</h4> {@code SocketServer} 接收到外部网络的请求，分析数据包，提取
 */
public class SocketMessageHandler extends SimpleChannelUpstreamHandler {
    private ISocketMessageProcessor processor = new DefaultMessageProcessor();
    private IInvokeInterceptor interceptor = new LoginInceptor();
    static Logger logger = Logger.getLogger(SocketMessageHandler.class);
    private static LinkedBlockingQueue<MessagePacket> taskQueue = new LinkedBlockingQueue<MessagePacket>(1);

    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
//        logger.info("Clinet Channel count:" + ChannelService.getTokenChannels().size() + ", Client connected:" + ctx.getChannel().getRemoteAddress());
//        ChannelService.addChannel(ctx.getChannel());
    }

    /**
     * channel断开连接事件
     *
     * @param e
     * @param ctx
     */
    @Override
    public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
        logger.info("Client disconnected:" + ctx.getChannel().getRemoteAddress());
//        ChannelService.deleteChannel(ctx.getChannel());
    }

    /**
     * 异常捕获事件
     *
     * @param e
     * @param ctx
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
        logger.error("Channel error:" + ctx.getChannel().getRemoteAddress(), e.getCause());
        // deleted by bo.dang
//        ChannelService.deleteChannel(ctx.getChannel());
    }

    /**
     * 消息处理，对接收到的数据包
     *
     * @param e
     * @param ctx
     */
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
        logger.info("接收到发送过来的消息");
        MessagePacket packet = (MessagePacket) e.getMessage();
        MessagePacket packet1 = packet;
        String cmd = packet.getCommandId() + packet.getChildCommandId();

        SocketRequestContext rc = new SocketRequestContext(packet);// 注册线程级别的对象容器。
        //String token =StringUtils.bytes2Str(packet.getToken());
        // 绑定token到通道上。推送消息时用得到。
//        ChannelService.bindTokenToChannel(rc);
        //心跳信息
        if (CommonConsts.CMD_ID_HEARTBEAT.equals(packet.getCommandId()) &&
                CommonConsts.CMD_ID_HEARTBEAT.equals(packet.getChildCommandId())) {
            //logger.info("心跳包（已收到）... "+ctx.getChannel());
            return;
        } else {
            logger.info("start request Channel" + rc.getChannel().toString() + ",cmd[" + cmd + "]," +
                    "guid[" + new String(packet.getGuid(), 0, 36) + "]," +
                    "data[" + (packet.getData() == null ? -1 : (packet.getData().length) + 84) + "]");
        }
        boolean ret = interceptor.beforeInvoke(rc);
        if (ret) {
            packet = processor.process(rc);
        } else {
            ResponseStatus status = ResponseStatus.WARN_OPERATION_ILLEGAL;
            Object retobj = "{\"code\":-9999,\"desc\":\"" + e.getMessage() + "\"}";
            /*desc= ExceptionUtils.getMessage(e);*/
            packet.setData(StringUtils.str2Bytes(JSONObject.fromObject(retobj == null ? "" : retobj.toString()).toString()));
            logger.error("[调用失败，通道验证不通过，CMD：" + cmd + "]");
        }
        if (packet != null)
            interceptor.afterInvoke(rc);

        //注销客户端不接收消息
        if (!cmd.equals("00000002") && packet != null) {
//            ChannelService.sendMessage(packet);
        }
		/*if(cmd.equals("10370001")){
			logger.info("检查结束，正在从通道中移除");
			taskQueue.poll();
			logger.info("移除成功");
		}*/
    }

}
