package com.eurlanda.datashire.server.socket;

import com.eurlanda.datashire.socket.MessagePacket;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

/**
 * Created by zhudebin on 2017/6/12.
 */
public class DatashireClient {
    public static void main(String[] args) {
        int port = 9999;
        String host = "192.168.137.194";
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            Bootstrap b = new Bootstrap(); // (1)
            b.group(workerGroup); // (2)
            b.channel(NioSocketChannel.class); // (3)
            b.option(ChannelOption.SO_KEEPALIVE, true); // (4)
            b.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ch.pipeline().addLast(new DatashireServerDecoder())  // byte -> message
                            .addLast(new DatashireServerEncoder()) // message -> byte
                            .addLast(new DatashireServerDebugEncoder()) // message -> message
                            .addLast(new DatashireClientHandler());
                }
            });

            // Start the client.
            ChannelFuture f = b.connect(host, port).sync(); // (5)

            // 发送登录
            MessagePacket packet = new MessagePacket();
            //packet.setId(MessagePacket.geturrPacketID());
            packet.setGuid("123456789012345678901234567890123456".getBytes());
            packet.setDsObjectType(1);
            packet.setCommandId("2001");
            packet.setChildCommandId("0001");
            packet.setToken("12345678901234567890123456789012".getBytes());
            //获取Data字节
            packet.setData("{\"User\":{\"user_name\":\"superuser\",\"password\":\"7066a4f427769cc43347aa96b72931a\"}}".getBytes());
            //        packet.setChannel(ctx.channel());     //TODO 切换

            f.channel().writeAndFlush(packet);


            // Wait until the connection is closed.
            f.channel().closeFuture().sync();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            workerGroup.shutdownGracefully();
        }
    }
}
