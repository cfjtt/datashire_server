package com.eurlanda.datashire.socket.protocol;

/**
 * <h3>文件名称：</h3>
 * ServerProtocolCommand_AuthenticationProtocol.java
 * <h3>内容摘要：</h3>
 * 下载文件协议的命令枚举
 * <h3>其他说明：</h3>
 * <h4>数据描述：</h4>
 * <table border=1>
 * <tr>
 * <td colspan="2">
 * 认证协议（0x0080）128个协议范围（从0x0080 ~ 0x00FF）<br>
 * 身份认证协议 0x0080：65535个子命令范围0x0080 0000-0x0080 FFFF
 * </td>
 * </tr>
 * <tr>
 * <td>命令</td><td>说明</td>
 * </tr>
 * <tr>
 * <td>0x0080 0001</td><td>登陆命令</td>
 * </tr>
 * <tr>
 * <td>0x0080 0002</td><td>注销命令</td>
 * </tr>
 * <tr>
 * <td>0x0080 0003</td><td>断开某些客户端连接</td>
 * </tr>
 * <tr>
 * <td>0x0080 0004</td><td>断开所有客户端连接</td>
 * </tr>
 * <tr>
 * <td>0x0080 0005</td><td>给某些客户端发送通知</td>
 * </tr>
 * <tr>
 * <td>0x0080 0006</td><td>给所有客户端广播通知</td>
 * </tr>
 * </table>
 * <h4>调用关系：</h4>
 * 被 {@link AuthenticationProtocol} 内部调用
 * <h4>应用场景：</h4>
 * 应用在认证协议中各个子命令映射
 */
public enum ServerProtocolCommand_AuthenticationProtocol {
	
	/**
	 * 心跳包
	 */
	CLIENT_HEARTBEAT_PACKAGE(0),

	/**
	 * 登陆命令
	 */
	CLIENT_LOGIN_REQUEST(1),
	
	/**
	 * 注销命令
	 */
	CLIENT_LOGOUT_REQUEST(2),
	
	/**
	 * 断开某些客户端连接
	 */
	DISCONNECT_CLIENT(0x0003),
	/**
	 * 断开所有客户端连接
	 */
	DISCONNECT_ALL_CLIENT(0x0004),
	/**
	 * 给某些客户端发送通知
	 */
	SEND_MESSAGE_TO_CLIENT(0x0005),
	/**
	 * 给所有客户端广播通知
	 */
	SEND_MESSAGE_TO_ALL(0x0006);
	
	private final int value;
	
	public int getValue(){
		return value;
	}
	
	/**
	 * 构造器
	 * @param value
	 */
	ServerProtocolCommand_AuthenticationProtocol(int value){
		this.value = value;
	}
	
	/**
	 * @param value
	 * @return
	 */
	public static ServerProtocolCommand_AuthenticationProtocol getProtocolCommandEnum(String valueStr) {
		int value=Integer.parseInt(valueStr, 10);
		switch(value){
		    case 0:
			    return CLIENT_HEARTBEAT_PACKAGE;
			case 1:
				return CLIENT_LOGIN_REQUEST;
			case 2:
				return CLIENT_LOGOUT_REQUEST;
			case 0x0003:
				return DISCONNECT_CLIENT;
			case 0x0004:
				return DISCONNECT_ALL_CLIENT;
			case 0x0005:
				return SEND_MESSAGE_TO_CLIENT;
			case 0x0006:
				return SEND_MESSAGE_TO_ALL;
			default:
				return null;
		}
	}
}

