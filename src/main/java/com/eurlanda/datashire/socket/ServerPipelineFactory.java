package com.eurlanda.datashire.socket;

import com.eurlanda.datashire.sprint7.socket.SocketMessageHandler;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.handler.execution.MemoryAwareThreadPoolExecutor;
import org.jboss.netty.handler.logging.LoggingHandler;
import org.jboss.netty.logging.InternalLogLevel;
import org.jboss.netty.util.DefaultObjectSizeEstimator;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * <h3>文件名称：</h3>
 * ServerPipelineFactory.java
 * <h3>内容摘要：</h3>
 * RassGateways的管道工厂，实现 {@link ChannelPipelineFactory} 接口的，实现接口 {@link ChannelPipelineFactory#getPipeline()}，用以返回内部配置构造的 {@link ChannelPipeline}
 * <h3>其他说明：</h3>
 * <h4>调用关系：</h4>
 * 非直接调用，提供给ServerBootStrap的setFactory方法，设置 {@code 服务端启动实例} 的管道工厂
 * <h4>应用场景：</h4>
 * 主要提供返回管道实例 {@link ChannelPipepline} 方法，{@link HttpServerPipelineFactory#getPipeline()} 方法内部添加了 
 * {@link ProtocolDecoder}、{@link ProtocolEncoder}、{@link ServerHandler} 及 {@link LoggingHandler} 这四个Handler
 */
public class ServerPipelineFactory implements ChannelPipelineFactory {
	/**
	 * 构造器
	 */
	public ServerPipelineFactory(){
	}
	/**
	 * 对象销毁
	 */
	
	public void finalize() throws Throwable {
	}

	/**
	 * 获得一个Socket管道
	 */
    @Override
	public ChannelPipeline getPipeline(){
		/*ChannelPipeline pipeline = Channels.pipeline();
		//解码
		pipeline.addLast("decoder",new ProtocolDecoder());
		//编译
		pipeline.addLast("encoder", new ProtocolEncoder());
		//管理
		//pipeline.addLast("handler", new ServerHandler());
		pipeline.addLast("handler", new SocketMessageHandler());	// 新的消息映射。
		//日志
		pipeline.addLast("logger", new LoggingHandler(InternalLogLevel.DEBUG));
		return pipeline;*/
		
		/*// 添加异步处理过程
		ExecutionHandler executionHandler = new ExecutionHandler(
	             new OrderedMemoryAwareThreadPoolExecutor(16, 1048576, 1048576));
		return Channels.pipeline(
                new ProtocolEncoder(),
                new ProtocolDecoder(),
                executionHandler, // Must be shared
                new SocketMessageHandler(),
                new LoggingHandler(InternalLogLevel.DEBUG));*/
		// 添加异步处理过程
		ThreadPoolExecutor pool = new MemoryAwareThreadPoolExecutor(16, 65536, 1048576, 200, 
				TimeUnit.SECONDS,
				new DefaultObjectSizeEstimator(),
				Executors.defaultThreadFactory());
		ExecutionHandler executionHandler = new ExecutionHandler(pool);
		return Channels.pipeline(
                new ProtocolEncoder(),
                new ProtocolDecoder(),
                executionHandler, // Must be shared
                new SocketMessageHandler(),
                new LoggingHandler(InternalLogLevel.DEBUG));
	}

}