package com.eurlanda.datashire.sprint7.socket;

import com.eurlanda.datashire.socket.MessagePacket;

public interface ISocketMessageProcessor {
	/**
	 * 调用业务对象，返回任意字符。
	 * @date 2014-4-18
	 * @author jiwei.zhang
	 * @return
	 */
	MessagePacket process(SocketRequestContext sc);
}
