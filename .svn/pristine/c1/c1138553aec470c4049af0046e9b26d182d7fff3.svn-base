package com.eurlanda.datashire.sprint7.socket;

import com.eurlanda.datashire.server.model.User;
import com.eurlanda.datashire.server.utils.SocketUtil;
import com.eurlanda.datashire.socket.ChannelManager;
import com.eurlanda.datashire.socket.MessagePacket;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MD5;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.StopSendLog;
import com.eurlanda.datashire.utility.StringUtils;
import net.sf.json.JSONObject;
import org.apache.log4j.Logger;

import java.util.Map;

public class LoginInceptor  implements IInvokeInterceptor{
	static Logger logger = Logger.getLogger(LoginInceptor.class);

	@Override
	public boolean beforeInvoke(SocketRequestContext sc) {
		String token = sc.getToken();
		MessagePacket packet= sc.getMessagePacket();
		String cmd = packet.getCommandId()+packet.getChildCommandId();
		User user = SocketUtil.SESSION.get(token);
		if(user!=null){
			return true;
		}else{
			if(cmd.equals("10180001")
					|| cmd.equals("00000002")
					|| cmd.equals("20010001")   // 重构后新增的接口
					|| cmd.equals("10180020")
					|| cmd.equals("10180022")
					|| cmd.equals("10180021")
					|| cmd.equals("10400001")
					|| cmd.equals("10400002")
					|| cmd.equals("00011000")){	//用户登录成功，加入token与channel关系
				return true;
			}
		}
		logger.error("[调用失败！接口信息(token："+token+", cmd："+cmd+")]");
		return false;
	}

	@Override
	public void afterInvoke(SocketRequestContext sc) {
		MessagePacket packet= sc.getMessagePacket();
		String cmd = packet.getCommandId()+packet.getChildCommandId();
		if(cmd.equals("10180001")
                || cmd.equals("20010001")   // 重构后新的接口
                ){	//用户登录成功，加入token与channel关系
				afterLogin(packet);
		} else if(cmd.equals("10400002")){

			String info = StringUtils.bytes2Str(packet.getData());
			Map<String,Object> map = JsonUtil.toHashMap(info);
			if(Integer.parseInt(map.get("code")+"")==MessageCode.SUCCESS.value()){
				afterLogin(packet);
			}

		} else if(cmd.equals("00000002")){  //用户注销成功，解除token与channel关系
			afterLogout(packet);
		}
		
	}
	
	/**
	 * 在Invoke之后处理Client Login Request请求的结果
	 * @param packet 返回的结果
	 * 主要提取下行包的token，在认证服务上注册这条连接，添加到ChannelInfo表中
	 * */
	public void afterLogin(MessagePacket packet){
		
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
		
		int code = jsonResult.getInt("code");
		MessageCode messageCode = null;
		messageCode = MessageCode.parse(code);
		if(messageCode==MessageCode.SUCCESS||messageCode==MessageCode.WARN_LICENSE){
			strToken=MD5.encrypt(Long.toString(System.currentTimeMillis()));//getToken(jsonResult.getString("info"));
			//写入handler, channelInfo
			ChannelManager.replaceChannelInfo(strToken, packet.getChannel());
			packet.setToken(strToken.getBytes());
			Map<String, Object> map=JsonUtil.toHashMap(jsonResult.getString("info"));
			User user =JsonUtil.toGsonBean(map.get("User").toString(), User.class); 
			//JsonUtil.json2Object(jsonResult.getString("User") , User.class);
			logger.trace("user========================"+user);
			User u = new User();
			logger.trace("setUser_name========================"+user.getUser_name());
			u.setUser_name(user.getUser_name());
			u.setId(user.getId());
			u.setSpaceId(user.getSpaceId());
//			u.setKey(packet.getChannel().getRemoteAddress()+"");
			logger.trace("strToken========================"+strToken);

			SocketUtil.SESSION.put(strToken, u);//用户和token绑定
			//绑定ip和token的信息(发生异常时，根据ip->token->u,删除UserSession信息)
			
			//ChannelService.bindTokenAndAddress(packet);
		}else{
			logger.error("===============登陆失败: code="+code);
			//throw new SystemErrorException(MessageCode.ERR,"登陆失败");
		}
		
	}

	
	/**
	 * 在Invoke之后处理Client Logout Request请求的结果
	 * @param packet 返回的结果
	 * 主要提取下行包的token，在认证服务上注销这条连接，从ChannelInfo表中删除
	 * */
	public void afterLogout(MessagePacket packet){
		
		JSONObject jsonResult;
		String strToken = "";	
		strToken = new String(packet.getToken());
		jsonResult = JSONObject.fromObject(StringUtils.bytes2Str(packet.getData()));
		int code = jsonResult.getInt("code");
		MessageCode messageCode = MessageCode.parse(code);
		if(messageCode==MessageCode.SUCCESS){
			//strToken = jsonResult.getString("info");
			//客户端登出，删除认证服务器回话
            SocketUtil.TOKEN2CHANNEL.remove(strToken);
			SocketUtil.SESSION.remove(strToken);
			//CommonConsts.tokenAndAddress.remove(strToken);
			//停止日志推送
		    new StopSendLog().stopSendLog(strToken);
		}else{
			logger.debug("用户退出失败!");
		}
		logger.debug("clientLogoutRequestHandleAfterInvoke end");
	}
}
