package com.eurlanda.datashire.socket.protocol;

import com.eurlanda.datashire.socket.ChannelExpand;
import com.eurlanda.datashire.socket.MessagePacket;
import com.eurlanda.datashire.socket.ServerHandler;

/**
 * <h3>文件名称：</h3>
 * ProtocolCallback.java
 * <h3>内容摘要：</h3>
 * 回调接口定义，处理对协议请求的异步需要
 * <h3>其他说明：</h3>
 * <h4>调用关系：</h4>
 * 被具体协议的 {@code beginInvoke} 及 {@code endInvoke} 方法调用
 * <h4>应用场景：</h4>
 * 被 {@link ServerHandler} 的 {@code messageReceived} 方法中被调用，作为回调传输到具体协议的 {@code beginInvoke} 方法中
 */
public interface ProtocolCallback {

	/**
	 * 操作成功接口
	 * 
	 * @param packet    协议数据包
	 * @param protocol   协议接口对象，用于表示哪个协议进行了异步调用，内部需要呼叫这个协议的endInvoke结束
	 */
	public void operationComplete(MessagePacket packet, Protocol protocol);
	
	/**
	 * 得到Listner关联的Socket
	 * */
	public ChannelExpand getChannelExpand();
	
	/**
	 * 得到Listner关联的上行数据包
	 * */
	public MessagePacket getChannelPacket();
}

