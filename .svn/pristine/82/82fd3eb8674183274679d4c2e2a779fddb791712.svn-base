package com.eurlanda.datashire.socket;

import com.eurlanda.datashire.socket.protocol.ProtocolThreadPoolTask;
import com.eurlanda.datashire.utility.CommonConsts;
import com.eurlanda.datashire.utility.DebugUtil;
import com.eurlanda.datashire.utility.IntUtils;
import com.eurlanda.datashire.utility.StringUtils;
import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import util.ZipStrUtil;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * <h3>文件名称：</h3>
 * ProtocolDecoder.java
 * <h3>内容摘要：</h3>
 * 解码协议对象，实现对上行数据包的数据封转
 * <h3>其他说明：</h3>
 * <h4>调用关系：</h4>
 * 添加到 {@link ChannelPipleline} 中，作为Handler元素，{@link ServerPipelineFactory#getPipeline()} 方法内部使用此类
 * <h4>应用场景：</h4>
 * {@code ProtocolDecoder} 作用在数据包处理器第一层，主要用于数据包的解码分包
 * 
 * v0.1 2013.10.19 彻底解决消息包丢包、截包、粘包可能
 *  	1. 一个buffer块，可能包含一个完整的数据包，这个是理想情况
 *  	2. 或多个完整的数据包，这个应该叫“组团忽悠（组包）”（这种情况应该不多见，纯属巧合;后台已支持！）
 *  	3. 或者1个不完整的包（这种情况应该发生在数据包较大情况）
 *  	4. 或者若干个完整包+1个不完整的包（这种情况应该发生在通讯量较大情况）
 *  
 *  针对buffer块中可能有不完整数据包情况:
 *  	缺失数据可能在上一个buffer中（头数据缺失）
 *  	也有可能在下一个buffer中（尾数据缺失）
 *  
 */
public class ProtocolDecoder extends FrameDecoder {

	static Logger logger = Logger.getLogger(ProtocolDecoder.class);


	// 最小数据包长度 = 分组数据+包头长度
	private static final int minPacketSize = CommonConsts.PACKET_DATA_LENGTH+CommonConsts.PACKET_HEADER_LENGTH;
	
	//public ProtocolDecoder(){
		//super(true); // unfold是针对读到的循环数据要不要打开的意思
	//}
	
	/**
	 * 解码方法，对原始数据分析，并封包
	 * @param ctx
	 * @param channel
	 * @param buffer
	 */
	@Override
	protected Object decode(ChannelHandlerContext ctx, Channel channel,
			ChannelBuffer buffer) throws Exception {
		int packetSize = buffer.readableBytes();//数据长度
		int readIndex = buffer.readerIndex();
		byte[] b = new byte[packetSize];
		buffer.readBytes(b);
		
		//解压字节
		/*if (null != b && b.length >= 0){
			b = ZipStrUtil.unCompress(b);
			packetSize = b.length;
		}else{
			logger.trace("[丢包] try to read data length failed, readableBytes is not enough! readIndex="+readIndex);
			buffer.readerIndex(readIndex);
			return null;
		}*/
		
		if(packetSize!=103){ // 记录前台发送数据
			logger.trace("[解码开始] decoding start PacketSize="+packetSize+", DataSize="+(packetSize-CommonConsts.PACKET_DATA_LENGTH)+", Data="+new String(b, 0, packetSize));
		}
		
		// 丢包(buffer可读取数据不足一个包，剩余数据应该在下一个buffer中,也有可能部分残余数据还在上一个buffer中)
		if(packetSize<minPacketSize){ //数据包长度小于长度
			logger.trace("[丢包] try to read data length failed, readableBytes is not enough! readIndex="+readIndex);
			buffer.readerIndex(readIndex);
			return null;
		}
		
		int dataLength=0;
		int pzwLength=0;
		try {
			byte[] f1 = StringUtils.copyOfRange(b, 0, 8);
			byte[] len = StringUtils.copyOfRange2(b, 8, 12);
			// 小端反转
			dataLength = IntUtils.bytesToInt(len); //update 2014-12-11 大小端转换
			// 标志位  最后一位：表示是否压缩
			byte[] pzw = StringUtils.copyOfRange(b, 12, 16);
			pzwLength = IntUtils.byte2int(pzw);
			byte[] f2 = StringUtils.copyOfRange(b, 16, 24);
		} catch (NumberFormatException e) { 
			buffer.readerIndex(readIndex);
			logger.error("[decoding err] parse data length error", e);
			return null;
		}
		
		// 空数据包，过滤掉， yi.zhou
		if (dataLength==0){
			buffer.readerIndex(readIndex);
			logger.error("[decoding err] parse data length 0");
			return null;
		}
		
		// 截包(buffer可读取数据不足一个包，剩余数据应该在下一个buffer中)
		if(packetSize < dataLength+CommonConsts.PACKET_DATA_LENGTH){
			logger.trace("[截包] Incomplete packet data, waiting to read the next buffer bytes! readIndex="+readIndex);
			buffer.readerIndex(readIndex);
			return null;
		}
		
		// 粘包(buffer内容有多余的数据)，下一次再读取剩余数据
		if(packetSize > dataLength+CommonConsts.PACKET_DATA_LENGTH){
			logger.trace("[粘包] readerIndex "+buffer.readerIndex()+", new "+(readIndex+dataLength+CommonConsts.PACKET_DATA_LENGTH));
			buffer.readerIndex(readIndex+dataLength+CommonConsts.PACKET_DATA_LENGTH);
		}
		
		//Data被压缩过，解压缩
		byte[] dataBytes = StringUtils.copyOfRange2(b, CommonConsts.PACKET_DATA_LENGTH, 
				dataLength);
		if ((pzwLength & 0x1) == 1){
			dataBytes = ZipStrUtil.unCompress(dataBytes);
		}
		
		//(内蒙古)包头（市）
		byte[] header = StringUtils.copyOfRange2(dataBytes, 0, 
				CommonConsts.PACKET_HEADER_LENGTH); //new byte[PACKET_HEADER_LENGTH];
		
		if(DebugUtil.isDebugenabled() && "sf48WMS2-H9Jn-8Qc9-Okw2-16oC0kO9Qvkb".equals(new String(StringUtils.copyOfRange2(header,8,36),0,36))){
			ProtocolThreadPoolTask.sysRunStatus(); // TODO show system running status...
			return null;
		}
		
		//封装数据包
		MessagePacket packet = new MessagePacket();
		//packet.setId(MessagePacket.geturrPacketID());
		packet.setGuid(StringUtils.copyOfRange2(header,0,36));
		int type = IntUtils.byte2int(StringUtils.copyOfRange2(header,36,4));
		packet.setDsObjectType(type);
		packet.setCommandId(new String(header,40,4));
		packet.setChildCommandId(new String(header,44,4));
		packet.setToken(StringUtils.copyOfRange2(header, 48, 32));
		//获取Data字节
		byte[] datas = StringUtils.copyOfRange2(dataBytes, CommonConsts.PACKET_HEADER_LENGTH, dataBytes.length-CommonConsts.PACKET_HEADER_LENGTH);
		packet.setData(datas);
		packet.setChannel(channel);
		String cmd = packet.getCommandId()+packet.getChildCommandId();
		if(!Agreement.ServiceConfMap.containsKey(cmd) && !cmd.equals("99999999")){
			logger.error(new RuntimeException(">>>>>>不存在的命令号["+cmd+"],header["+header+"]！服务器将清空通道缓存！"));
			buffer.clear();
		}
/*		// 忽略心跳日志
		if (!CommonConsts.CMD_ID_HEARTBEAT.equals(packet.getCommandId()) &&
				!CommonConsts.CMD_ID_HEARTBEAT.equals(packet.getChildCommandId())) {
			logger.debug("[解码完成] " + packet);
			if(logger.isTraceEnabled()) logger.trace("[消息内容] " + new String(packet.getData()));
			String cmd = packet.getCommandId()+packet.getChildCommandId();
			List<Integer> list = MsgInLen.get(cmd);
			if(list!=null){
				list.add(packet.getData().length);
			}else{
				list = new ArrayList<Integer>();
				list.add(packet.getData().length);
				MsgInLen.put(cmd, list);
			}
		}*/
		
		return packet;
	}
	
	// (后台接收到的)完整消息包长度
	public static Map<String, List<Integer>> MsgInLen = new TreeMap<String, List<Integer>>();

	//  test synchronized 果然好强大
	public static void main(String[] args) {
		final long startTime = System.currentTimeMillis();
		final int testTimes=-50000;
		for(int i=0; i<=testTimes; i++){
			final int currIndex=i;
			new Thread(){
				@Override
				public void run() {
					//MessagePacket.geturrPacketID();
					if(currIndex==testTimes){
						long endTime = System.currentTimeMillis();
						try {
							Thread.sleep(200);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						//System.out.println("currPacketIndex: "+MessagePacket.currPacketIndex);
						System.out.println("time used: "+(endTime-startTime));
					}
				}
			}.start();
		}
	}
	
}