package com.eurlanda.datashire.socket.protocol;

import com.eurlanda.datashire.socket.MessagePacket;
import net.sf.json.JSONObject;
import org.apache.log4j.Logger;

/**
 * <h3>文件名称：</h3> SimpleMessageProtocol.java <h3>内容摘要：</h3> 简单对数据进行消息转发，并回调 <h3>其他说明：</h3> <h4>调用关系：</h4> 非直接调用，由
 * {@link ServerHandler} 统一派发客户端请求到具体协议处理 <h4>应用场景：</h4> 简单的消息协议，处理客户端基础业务消息
 */
public class SimpleMessageProtocol extends AbstractProtocol {

	static Logger logger = Logger.getLogger(SimpleMessageProtocol.class);

	/**
	 * 调用异步方法
	 * 
	 * @param packet
	 * @param proCallback
	 */
	public void beginInvoke(ProtocolCallback proCallback) {
		// 验证token、操作权限拦截
		if (beginInvokeFilter(proCallback)) {
			// TODO 对于部分业务处理时间可能较长接口可以单独线程处理（squid flow加载，更新/刷新database squid缓存等）
			ProtocolThreadPoolTask pt = new ProtocolThreadPoolTask(this, proCallback);
			pt.start();
		}else{
			logger.error("beginInvokeFilter is failed, identify is illegal");
			//proCallback.getChannelExpand().close();
			MessagePacket packet = proCallback.getChannelPacket();
			ProtocolData protocolData = new ProtocolData();
			protocolData.setStatus(ResponseStatus.WARN_OPERATION_ILLEGAL.toString());
			protocolData.setData("");
			String result = JSONObject.fromObject(protocolData).toString();
			byte[] data = result.getBytes();
			packet.setData(data);
			this.endInvoke(proCallback);
		}
	}

	/**
	 * 结束异步调用方法
	 * 
	 * @param packet
	 * @param proCallback
	 */
	public void endInvoke(ProtocolCallback proCallback) {
		proCallback.operationComplete(proCallback.getChannelPacket(), this);
	}

}