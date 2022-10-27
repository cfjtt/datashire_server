package com.eurlanda.datashire.socket.protocol;

import com.eurlanda.datashire.server.model.User;
import com.eurlanda.datashire.server.utils.SocketUtil;
import com.eurlanda.datashire.socket.ChannelManager;
import com.eurlanda.datashire.socket.MessagePacket;
import com.eurlanda.datashire.socket.ServerHandler;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MD5;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.StringUtils;
import net.sf.json.JSONObject;
import org.apache.log4j.Logger;
import org.jboss.netty.channel.Channel;

/**
 * <h3>文件名称：</h3>
 * AuthenticationProtocol.java
 * <h3>内容摘要：</h3>
 * 与认证服务器交互的协议<br>
 * <em>认证协议需要单独配置运行在一个gateway实例，不能与其他协议共用，因为认证协议写入ServerHandler用户列表</em>
 * <h3>其他说明：</h3>
 * <h4>调用关系：</h4>
 * 非直接调用，由 {@link ServerHandler} 统一派发客户端请求到具体协议处理
 * <em>暂时负责下行推送消息，由 {@link HttpServerHandler} 派发后台服务请求到此协议 </em>
 * <h4>应用场景：</h4>
 * 客户端登陆服务器，必须经过的协议，通过认证协议，获得合法会话Token，才可以请求其他数据协议或文件协议
 */

public class AuthenticationProtocol extends AbstractProtocol {
	
	static Logger logger = Logger.getLogger(AuthenticationProtocol.class);
	
	/**
	 * 在Invoke之后处理Client Login Request请求的结果
	 * @param proCallback 回调
	 * 主要提取下行包的token，在认证服务上注册这条连接，添加到ChannelInfo表中
	 * */
	public void afterLogin(ProtocolCallback proCallback){
		
		MessagePacket packet = proCallback.getChannelPacket();
		
		logger.debug("clientLoginRequestHandleAfterInvoke start");
		
		JSONObject jsonResult;
		String strToken;
		
		// 拦截颁发token下行包，取得token写入到handler队列
//		if(packet.getData().length==0){
//			logger.warn("packet data length is 0, maybe back webservice is down, so Auth Prol do nothing and return.");
//			return;
//		}
		jsonResult = JSONObject.fromObject(StringUtils.bytes2Str(packet.getData()));
		logger.debug("Packet Data: " + jsonResult);
		
		String status = jsonResult.getString("status");
		if(ResponseStatus.valueOf(status)==ResponseStatus.SUCCESS_CALL){
			String data = jsonResult.getString("data");
			jsonResult = JSONObject.fromObject(data);
			int code = jsonResult.getInt("code");
			MessageCode messageCode = null;
			messageCode = MessageCode.parse(code);
			if(messageCode==MessageCode.SUCCESS){
				strToken=MD5.encrypt(Long.toString(System.currentTimeMillis()));//getToken(jsonResult.getString("info"));
				//写入handler, channelInfo
				ChannelManager.replaceChannelInfo(strToken, proCallback.getChannelExpand().getChannle());
				packet.setToken(strToken.getBytes());
				logger.trace("data========================"+data);
				User user = JsonUtil.json2Object(jsonResult.getString("info") , User.class);
				logger.trace("user========================"+user);
				User u = new User();
				logger.trace("setUser_name========================"+user.getUser_name());
				u.setUser_name(user.getUser_name());
				u.setId(user.getId());
//				u.setKey(packet.getChannel().getRemoteAddress()+"");
				logger.trace("strToken========================"+strToken);
				SocketUtil.SESSION.put(strToken, u);//用户和token绑定

			}else{
				logger.warn("登陆失败: code="+code);
			}
		}else{
			logger.warn("登陆失败: status="+status);
		}
		logger.debug("clientLoginRequestHandleAfterInvoke end");
	}

	
	/**
	 * 在Invoke之后处理Client Logout Request请求的结果
	 * @param proCallback
	 * 主要提取下行包的token，在认证服务上注销这条连接，从ChannelInfo表中删除
	 * */
	public void afterLogout(ProtocolCallback proCallback){
		MessagePacket packet = proCallback.getChannelPacket();
		logger.debug("clientLogoutRequestHandleAfterInvoke start");
		logger.debug("source code:" + packet);
		
		JSONObject jsonResult;
		String strToken = "";		
		jsonResult = JSONObject.fromObject(StringUtils.bytes2Str(packet.getData()));
		
		String status = jsonResult.getString("status");
		if(ResponseStatus.valueOf(status)==ResponseStatus.SUCCESS_CALL){
			String data = jsonResult.getString("data");
			jsonResult = JSONObject.fromObject(data);
			int code = jsonResult.getInt("code");
			MessageCode messageCode = MessageCode.parse(code);
			if(messageCode==MessageCode.SUCCESS){
				strToken = jsonResult.getString("info");
				//客户端登出，删除认证服务器回话
				//ServerHandler.removeChannelInfo(strToken);
			}else{
				logger.debug("用户退出失败!");
			}
		}
		logger.debug("clientLogoutRequestHandleAfterInvoke end");
	}
	
	/**
	 * 开始调用异步处理
	 * @param proCallback
	 */
	public void beginInvoke(ProtocolCallback proCallback){
		/*logger.debug("message:"+proCallback.getChannelPacket().getCommandId());
		switch(ServerProtocolCommand_AuthenticationProtocol.getProtocolCommandEnum(proCallback.getChannelPacket().getChildCommandId())){
		        //心跳包
		   case CLIENT_HEARTBEAT_PACKAGE:
		        return;
		   case CLIENT_LOGIN_REQUEST: 
				//用户登陆
				break;
			case CLIENT_LOGOUT_REQUEST: 
				//用户退出
				break;
		}*/
		new ProtocolThreadPoolTask(this, proCallback).start();
	}

	/**
	 * 结束异步处理
	 * @param proCallback
	 */
	public void endInvoke(ProtocolCallback proCallback){
		MessagePacket packet = proCallback.getChannelPacket();
		//String result = new String(packet.getData()); //编码转换？
		//byte[] data=packet.getData();
		//data = result.getBytes(ServerEndPoint.getMainServer().getDataEncoding());
		//packet.setData(data);
		//packet.setLength(packet.getData().length);
		switch(ServerProtocolCommand_AuthenticationProtocol.getProtocolCommandEnum(packet.getChildCommandId())){
			case CLIENT_LOGIN_REQUEST: 
				//用户登录成功，加入token与channel关系
				afterLogin(proCallback);
				break;
			case CLIENT_LOGOUT_REQUEST: 
				//用户注销成功，解除token与channel关系
				afterLogout(proCallback);
				break;
		}
		proCallback.operationComplete(proCallback.getChannelPacket(), this);
	}
	
	/**
	 * 发送消息给客户端
	 * @param packet 下发协议数据包
	 * @deprecated
	 * */
	public boolean sendMessage_old(Channel client, MessagePacket packet){
		
		logger.debug("sendMessage start");
		logger.debug("source code:" + packet);
		
		if(packet==null){
			logger.error("packet is null");
			return false;
		}
		
		if(!this.isChannelAvailable_old(client)){
			logger.error("this client is not available");
			return false;
		}
		
		//ChannelPacket cPacket = AbstractProtocol.createDownPacket(packet, this);
		//client.write(cPacket);
		
		logger.debug("sendMessage end");
		return true;
	}
	
	/**
	 * 判断Socket是否可用
	 * @param chl 要判断的Socket连接
	 * */
	public boolean isChannelAvailable_old(Channel chl){
		return chl!=null && chl.isConnected() && chl.isWritable();
	}
	
	
	/*private String getToken(String jsonStr) {
		User user=JsonUtil.toGsonBean(jsonStr,User.class);
		StringBuffer token = new StringBuffer();
		token.append(user.getUser_name());
		token.append("-");
		token.append(user.getPassword());
		token.append("-");
		token.append(user.getRole_id());
		token.append("-");
		//token.append(user.getKey());
		token.append(System.currentTimeMillis());
		// 对TOKEN进行MD5加密
		return MD5.encrypt(token.toString());
	}*/
	
}