package com.eurlanda.datashire.socket.protocol;

/**
 * <h3>文件名称：</h3>
 * Protocol.java
 * <h3>内容摘要：</h3>
 * 协议接口，所有协议都要实现的共同接口
 * <h3>其他说明：</h3>
 * <h4>调用关系：</h4>
 * 非直接调用，此类为接口，提供所有协议约定的接口集合
 * <h4>应用场景：</h4>
 * 所有具体协议的实现接口
 */
public interface Protocol {

	/**
	 * 开始调用方法（异步）
	 * 
	 * @param packet    协议数据包
	 * @param proCallback    协议回调
	 */
	public void beginInvoke(ProtocolCallback proCallback);

	/**
	 * 结束异步调用方法
	 * 
	 * @param packet   协议数据包
	 * @param proCallback    协议回调
	 */
	public void endInvoke(ProtocolCallback proCallback);
 
}

