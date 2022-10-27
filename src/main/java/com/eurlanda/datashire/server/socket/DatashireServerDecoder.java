package com.eurlanda.datashire.server.socket;

import com.eurlanda.datashire.socket.MessagePacket;
import com.eurlanda.datashire.utility.CommonConsts;
import com.eurlanda.datashire.utility.IntUtils;
import com.eurlanda.datashire.utility.StringUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.ZipStrUtil;

import java.util.List;

/**
 * Created by zhudebin on 2017/6/9.
 */
public class DatashireServerDecoder extends ByteToMessageDecoder {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatashireServerEncoder.class);

    // 最小数据包长度 = 分组数据+包头长度
    private static final int minPacketSize = CommonConsts.PACKET_DATA_LENGTH+CommonConsts.PACKET_HEADER_LENGTH;

    @Override protected void decode(
            ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
            throws Exception {
        int packetSize = in.readableBytes();//数据长度
        int readIndex = in.readerIndex();
        byte[] b = new byte[packetSize];
        in.readBytes(b);

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
            LOGGER.trace("[解码开始] decoding start PacketSize={}, DataSize={}, Data={}",
                    packetSize, packetSize-CommonConsts.PACKET_DATA_LENGTH,new String(b, 0, packetSize));
        }

        // 丢包(buffer可读取数据不足一个包，剩余数据应该在下一个buffer中,也有可能部分残余数据还在上一个buffer中)
        if(packetSize<minPacketSize){ //数据包长度小于长度
            LOGGER.trace("[丢包] try to read data length failed, readableBytes is not enough! readIndex={}", readIndex);
            in.readerIndex(readIndex);
            return;
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
            in.readerIndex(readIndex);
            LOGGER.error("[decoding err] parse data length error", e);
            return;
        }

        // 空数据包，过滤掉， yi.zhou
        if (dataLength==0){
            in.readerIndex(readIndex);
            LOGGER.error("[decoding err] parse data length 0");
            return;
        }

        // 截包(buffer可读取数据不足一个包，剩余数据应该在下一个buffer中)
        if(packetSize < dataLength+CommonConsts.PACKET_DATA_LENGTH){
            LOGGER.trace("[截包] Incomplete packet data, waiting to read the next buffer bytes! readIndex={}", readIndex);
            in.readerIndex(readIndex);
            return;
        }

        // 粘包(buffer内容有多余的数据)，下一次再读取剩余数据
        if(packetSize > dataLength+CommonConsts.PACKET_DATA_LENGTH){
            LOGGER.trace("[粘包] readerIndex {}, new {}", in.readerIndex(), readIndex + dataLength+CommonConsts.PACKET_DATA_LENGTH);
            in.readerIndex(readIndex+dataLength+CommonConsts.PACKET_DATA_LENGTH);
        }

        //Data被压缩过，解压缩
        byte[] dataBytes = StringUtils.copyOfRange2(b, CommonConsts.PACKET_DATA_LENGTH,
                dataLength);
        if ((pzwLength & 0x1) == 1){
            dataBytes = ZipStrUtil.unCompress(dataBytes);
        }

        // 包头
        byte[] header = StringUtils.copyOfRange2(dataBytes, 0,
                CommonConsts.PACKET_HEADER_LENGTH); //new byte[PACKET_HEADER_LENGTH];

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
        out.add(packet);
    }
}
