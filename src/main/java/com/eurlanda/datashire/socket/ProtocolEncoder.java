package com.eurlanda.datashire.socket;

import com.eurlanda.datashire.utility.CommonConsts;
import com.eurlanda.datashire.utility.IntUtils;
import com.eurlanda.datashire.utility.StringUtils;
import com.eurlanda.datashire.utility.SysConf;
import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.DynamicChannelBuffer;
import org.jboss.netty.buffer.HeapChannelBufferFactory;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;
import util.ZipStrUtil;

import java.nio.ByteOrder;

/**
 * <h3>文件名称：</h3>
 * ProtocolEncoder.java
 * <h3>内容摘要：</h3>
 * 编码协议对象，实现对下行数据包的解析输出
 * <h3>其他说明：</h3>
 * <h4>调用关系：</h4>
 * 添加到 {@link ChannelPipleline} 中，作为Handler元素，
 * {@link ServerPipelineFactory#getPipeline()} 方法内部使用此类
 * <h4>应用场景：</h4>
 * {@code ProtocolEncoder} 作用在数据包处理器最后一层，主要用于下行协议包的编码打包
 */
public class ProtocolEncoder extends OneToOneEncoder {

	static Logger logger = Logger.getLogger(ProtocolEncoder.class);

	/**
	 * 编码方法
	 * 
	 * @param ctx
	 * @param channel
	 *            Socket对象
	 * @param msg
	 *            下行消息对象
	 * @Override
	 */
	protected Object encode(ChannelHandlerContext ctx, Channel channel,
			Object msg) throws Exception {
		logger.trace("Encode start");
		MessagePacket packet = null;
		if (msg instanceof MessagePacket){
			packet = (MessagePacket) msg;
		}else if(msg instanceof ChannelBuffer){
			return msg;
		}

        /**
         * 协议
         *
         * 8位占位符 + 4位data长度 + 4位压缩标志位 + 8位占位符 + data字节数组
         *
         * data字节数组
         *  36bytes + 4bytes  + 4bytes      + 4bytes            + 32bytes + (N)bytes
         *  guid    + objType + commondId   + childCommandId    + token   + jsonData
         *
         */

        StringBuffer strbuf = new StringBuffer();
		// 数据包长度
		// strbuf.append(StringUtils.supplyNumber(buffSize-8,
		// CommonConsts.PACKET_DATA_LENGTH));
		// 数据包顺序ＩＤ
//		strbuf.append(StringUtils.supplyNumber(packet.getSequenceId(),
//				CommonConsts.PACKET_DATA_LENGTH));
		// 数据包的唯一标识符 key  36位
		strbuf.append(StringUtils.bytes2Str(packet.getGuid()));
		// 数据包的辅助参数  添加4位占位符  4位
		strbuf.append("0000"); 
		// 数据包的命令号  4位 + 4位
		strbuf.append(packet.getCommandId());
		strbuf.append(packet.getChildCommandId());
		// 数据包的令牌 32位
		strbuf.append(StringUtils.bytes2Str(packet.getToken()));
		// 数据
		strbuf.append(StringUtils.bytes2Str(packet.getData()));
		// 数据转换
		byte[] strs = StringUtils.str2Bytes(strbuf.toString());
		byte[] type = IntUtils.int2byte(packet.getDsObjectType());
        // 返回对象的类型,替换原来的0000 TODO 判断是否有用
		strs[36] = type[0];
		strs[37] = type[1];
		strs[38] = type[2];
		strs[39] = type[3];
		// 为写入的数据压缩
		int pzwValue = Integer.parseInt(SysConf.getValue("socket.compress"));
		byte[] pzw = IntUtils.int2byte(pzwValue);
		if (pzwValue==1){
			strs = ZipStrUtil.compress(strs);
		}
		// 占位符 8位
		byte[] separator = new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 
				(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF};
		// 写入压缩过的数据
		// update by yi.zhou 14/10/10
//		String headerStrs = StringUtils.supplyNumber(strs.length,
//				CommonConsts.PACKET_DATA_LENGTH);
		// 小端转换
		byte[] headers = IntUtils.intToBytes(strs.length);
		int buffSize = strs.length + CommonConsts.PACKET_DATA_LENGTH;
		// 放入缓冲区
		ChannelBuffer buf = new DynamicChannelBuffer(ByteOrder.BIG_ENDIAN,
				buffSize,
				HeapChannelBufferFactory.getInstance(ByteOrder.BIG_ENDIAN));
		// 占位符
		buf.writeBytes(separator);
		buf.writeBytes(headers);
		buf.writeBytes(pzw);
		// 占位符
		buf.writeBytes(separator);
		buf.writeBytes(strs);
		logger.info("编码消息: header length [" + strs.length + "], data [" + strbuf.length() + "]");
		return buf;
	}
}