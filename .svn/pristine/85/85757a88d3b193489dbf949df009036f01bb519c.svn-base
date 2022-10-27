package com.eurlanda.datashire.socket.protocol;

import com.eurlanda.datashire.socket.MessagePacket;

/**
 * 协议请求数据包
 * 
 * @author Fumin
 * 
 */
public class ProtocolCallPacket {

	//协议数据包
	private MessagePacket packet;

	//回调接口定义，处理对协议请求的异步需要
	private ProtocolCallback proCallback;

	/**
	 * 构造器
	 */
	public ProtocolCallPacket() {

	}

	/**
	 * 构造器
	 * @param packet 协议数据包
	 * @param proCallback 回调接口定义，处理对协议请求的异步需要
	 */
	public ProtocolCallPacket(MessagePacket packet,
			ProtocolCallback proCallback) {
		this.packet = packet;
		this.proCallback = proCallback;
	}

	public MessagePacket getPacket() {
		return packet;
	}

	public void setPacket(MessagePacket packet) {
		this.packet = packet;
	}

	public ProtocolCallback getProCallback() {
		return proCallback;
	}

	public void setProCallback(ProtocolCallback proCallback) {
		this.proCallback = proCallback;
	}
}
