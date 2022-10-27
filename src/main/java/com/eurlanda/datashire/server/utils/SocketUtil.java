package com.eurlanda.datashire.server.utils;

import com.eurlanda.datashire.server.model.User;
import com.eurlanda.datashire.socket.MessagePacket;
import com.eurlanda.datashire.utility.IntUtils;
import com.eurlanda.datashire.utility.VersionUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.util.AttributeKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 保存登录信息
 * Created by zhudebin on 2017/6/9.
 */
public class SocketUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(SocketUtil.class);

    // channel中保存的attribute key
    public static final AttributeKey LOGIN_TOKEN = AttributeKey.valueOf("LOGIN_TOKEN");

    // 用户登录的session
    public static final ConcurrentHashMap<String, User> SESSION = new ConcurrentHashMap<>();

    // token与通道的映射, 只有登录后才保存
    public static final ConcurrentHashMap<String, Channel> TOKEN2CHANNEL = new ConcurrentHashMap<>();

    // 通道的集合,登录和未登录的都保存
    public static final CopyOnWriteArrayList<Channel> CHANNELS = new CopyOnWriteArrayList<>();

    /**
     * 获取当前后台版本号
     * @return
     */
    public static Object getServerVersion(){
        try {
            String line = VersionUtils.getServerVersion();
            String[] strs = line.split("\\.");
            // 占位符
            byte[] separator = new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
                    (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF};
           /* byte[] separator = new byte[]{(byte) 'a', (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
                    (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 'a'};*/
            // 写入压缩过的数据
            ByteBuf buf = UnpooledByteBufAllocator.DEFAULT.buffer();
            buf.writeBytes(separator);
            buf.writeBytes(IntUtils.intToBytes(12));
            buf.writeBytes(IntUtils.int2byte(0));
            buf.writeBytes(separator);
            for (String s : strs) {
                buf.writeBytes(IntUtils.int2byte(Integer.parseInt(s)));
            }
            return buf;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void sendMessage(MessagePacket packet) {
        String token = new String(packet.getToken(), 0, 32);
        Channel channel = TOKEN2CHANNEL.get(token);
        if(channel != null) {
            //LOGGER.debug("发送消息:{},token:{}", new String(packet.getData()), new String(packet.getToken()));
            channel.writeAndFlush(packet);
        } else {
            LOGGER.error("SYS_ERROR>>>>>>>>>>>>>>>>找不到对应token:{}的通道,系统异常<<<<<<<<<<<<<<<<<<", token);
            LOGGER.error("SYS_ERROR>>>>>>>>>>>>>>>>找不到对应token:{}的通道,系统异常<<<<<<<<<<<<<<<<<<", token);
            LOGGER.error("SYS_ERROR>>>>>>>>>>>>>>>>找不到对应token:{}的通道,系统异常<<<<<<<<<<<<<<<<<<", token);
            LOGGER.error("SYS_ERROR>>>>>>>>>>>>>>>>找不到对应token:{}的通道,系统异常<<<<<<<<<<<<<<<<<<", token);
//            System.exit(-1);    // 现阶段直接退出了,正式部署的时候,应该没有这个异常了
        }
    }
}
