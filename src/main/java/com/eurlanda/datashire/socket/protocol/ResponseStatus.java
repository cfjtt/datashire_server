package com.eurlanda.datashire.socket.protocol;

/**
 * <h3>文件名称：</h3>
 * ServerAnswerStatus_DownstreamMessage.java
 * <h3>内容摘要：</h3>
 * 针对下行流消息的服务器回应状态
 * <h3>其他说明：</h3>
 * <h4>数据描述</h4>
 * <table border=1>
 * <tr>
 * <td>枚举值</td><td>说明</td>
 * </tr>
 * <tr>
 * <td>SERVER_ALREADY_ACCEPTED_REQUEST</td><td>服务器已接受请求</td>
 * </tr>
 * <tr>
 * <td>SUCCESS_CALL</td><td>呼叫成功</td>
 * </tr>
 * <tr>
 * <td>ERROR_CLIENTDATA</td><td>数据格式错误</td>
 * </tr>
 * </table>
 * <h4>调用关系：</h4>
 * 在所有通过与业务交互的协议中，下行协议包内的数据状态填充
 * <h4>应用场景：</h4>
 * 提供服务端下行消息的状态值，标示服务器对客户端每次请求的状态返回值
 */
public enum ResponseStatus {
	/**
	 * 服务器已接受请求
	 */
	SERVER_ALREADY_ACCEPTED_REQUEST,
	/**
	 * 呼叫成功
	 */
	SUCCESS_CALL,
	/**
	 * 数据错误
	 */
	ERROR_CLIENTDATA,
	/**
	 * 系统错误
	 */
	ERROR_SYSTEM,
	
	/** 无操作权限（增删查改） */
	WARN_OPERATION_ILLEGAL
}
