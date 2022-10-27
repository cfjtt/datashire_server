package com.eurlanda.datashire.server.socket;

import com.eurlanda.datashire.socket.MessagePacket;
import com.eurlanda.datashire.utility.CommonConsts;
import com.eurlanda.datashire.utility.IntUtils;
import com.eurlanda.datashire.utility.StringUtils;
import com.eurlanda.datashire.utility.SysConf;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.ZipStrUtil;

/**
 * Created by zhudebin on 2017/6/9.
 */
public class DatashireServerEncoder extends MessageToByteEncoder<MessagePacket> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatashireServerEncoder.class);

    @Override protected void encode(
            ChannelHandlerContext ctx, MessagePacket packet, ByteBuf out)
            throws Exception {
        /**
         * 协议 如果有调整,必须修改此注释
         *
         * 8位占位符 + 4位data长度 + 4位压缩标志位 + 8位占位符 + data字节数组
         *
         * data字节数组
         *  36bytes + 4bytes  + 4bytes      + 4bytes            + 32bytes + (N)bytes
         *  guid    + objType + commondId   + childCommandId    + token   + jsonData
         *
         */
        StringBuffer strbuf = new StringBuffer();

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

        // 小端转换
        byte[] headers = IntUtils.intToBytes(strs.length);
        int buffSize = strs.length + CommonConsts.PACKET_DATA_LENGTH;
        // 放入缓冲区
        // 占位符
        out.writeBytes(separator);
        out.writeBytes(headers);
        out.writeBytes(pzw);
        // 占位符
        out.writeBytes(separator);
        out.writeBytes(strs);
        LOGGER.debug("编码消息: header length [{}], data [{}]",strs.length, strbuf.length());
    }

    @Override protected ByteBuf allocateBuffer(ChannelHandlerContext ctx, MessagePacket msg,
            boolean preferDirect) throws Exception {
        return super.allocateBuffer(ctx, msg, preferDirect);
    }
}
