package com.eurlanda.datashire.server.socket;

import com.eurlanda.datashire.socket.MessagePacket;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 *
 * 调试使用, 防止有一些类型的数据无法编码
 *
 * Created by zhudebin on 2017/6/9.
 */
public class DatashireServerDebugEncoder extends MessageToMessageEncoder<Object> {

    private final static Logger LOGGER = LoggerFactory.getLogger(DatashireServerDebugEncoder.class);

    @Override protected void encode(ChannelHandlerContext ctx, Object msg, List<Object> out)
            throws Exception {
        if (msg instanceof MessagePacket){
            out.add(msg);
        }else {
            String errorMsg = "!!!!!!------" + DatashireServerDebugEncoder.class.getCanonicalName()
                    + "--------- 数据类型不匹配 ---------" + msg.getClass().getCanonicalName();
            System.out.println(errorMsg);
            LOGGER.error(errorMsg);
//            throw new RuntimeException("存在不能匹配的数据类型:" + errorMsg);
            out.add(msg);
        }
    }
}
