package com.eurlanda.datashire.socket.protocol;

import com.eurlanda.datashire.entity.Privilege;
import com.eurlanda.datashire.enumeration.DMLType;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.server.model.User;
import com.eurlanda.datashire.server.utils.SocketUtil;
import com.eurlanda.datashire.socket.Agreement;
import com.eurlanda.datashire.socket.MessagePacket;
import com.eurlanda.datashire.sprint7.packet.InfoNewPacket;
import com.eurlanda.datashire.sprint7.service.user.subservice.PrivilegeService;
import com.eurlanda.datashire.utility.CommonConsts;
import com.eurlanda.datashire.utility.JsonUtil;
import org.apache.log4j.Logger;

import java.util.List;

/**
 * <h3>文件名称：</h3>
 * AbstractProtocol.java
 * <h3>内容摘要：</h3>
 * 抽象协议，定义协议的部分实现
 * <h3>其他说明：</h3>
 * <h4>调用关系：</h4>
 * 非直接调用，此类为抽象类，主要编写一些可复用的代码，为其子类共用
 * <h4>应用场景：</h4>
 * 具体派生协议的父类
 */
public abstract class AbstractProtocol implements Protocol {

	static Logger logger = Logger.getLogger(AbstractProtocol.class);
	
	public static String AUTH_ERRORCODE_KEY = "code";
	
	public static String WEBSERVICE_DATA = "data";
	
	public static String WEBSERVICE_STATUS = "status";
	
	public static String WEBSERVICE_SUCCESS_STATUS = "SUCCESS_CALL";
	
	public static long AUTH_ERRORCODE_VALUE = 1;
	
	public boolean beginInvokeFilter(ProtocolCallback proCallback) {
		boolean filterResult = false;
		if (proCallback == null) {
			logger.debug("ProtocolCallback is null");
			return filterResult;
		}

		MessagePacket packet = proCallback.getChannelPacket();

		if (packet == null) {
			logger.debug("ProtocolPacket is null");
			return filterResult;
		}
		// 获取命令号
		String childCommandId = packet.getCommandId() + packet.getChildCommandId();
		// 获取token
		String token = new String(packet.getToken());
		// 用户id
		int userId;
		// 前端传送的数据
		String info = "";
		// 获取用户信息
		try {
			ServiceConf serviceConf = Agreement.ServiceConfMap
					.get(childCommandId);
			if (null != serviceConf) {
				// 如果不需要权限验证直接返回true
				if (null == serviceConf.getP()) {
					return true;
				}
			} else {
				logger.error("serviceConf is null, cmd = "+childCommandId);
				return false;
			}
			logger.trace("获取到的命令号为===" + childCommandId + " " + "获取到的token为="
					+ token);
			User user = SocketUtil.SESSION.get(token);
			if (null != user) { // 超级用户登录直接返回ture
				logger.trace("username============"+user.getUser_name());
				if (CommonConsts.SuperUserName.equals(user.getUser_name())) {
					return  true;
				}
				// 获取用户id
				userId = user.getId();
				// 调用操作权限业务类
				PrivilegeService privilegeService = new PrivilegeService();
				// 获取对应用户的权限
				List<Privilege> userPrivilegeList = privilegeService
						.getPrivileges(userId, "U");
				// 如果调用删除方法,解析前端传输的信息获取要操作的对象
				if ("00010025".equals(childCommandId)) {
					info = new String(packet.getData());
					this.getInfo(info, serviceConf);
				}
				// 用前端请求的权限和查询出来的权限做对比
				filterResult = this.compare(serviceConf, userPrivilegeList);
				logger.debug("返回的值为===" + filterResult);
			}else{
				return true; // 有可能服务器重启
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("error", e);
		}
		return filterResult;
	}
		
//		Server mainServer = ServerEndPoint.getMainServer();
//		if(mainServer==null){
//			logger.debug("MainServer is null");
//			return false;
//		}
//		ThreadPoolExecutor tpe = mainServer.getThreadPool();
//		if(tpe==null){
//			logger.debug("ThreadPool is null");
//			return false;
//		}
		
		//分解客户端上行协议数据
		//byte[] clientJSONBytes = packet.getData();
		//String strClientJson = ServerEndPoint.convertProtocolDataBytesToString(clientJSONBytes);
		
		
		
		//logger.debug("strClientJson=" + strClientJson);
		
//		JSONObject clientJson = JSONObject.fromObject(strClientJson);
		
//		logger.debug("modify this packet data, earse token field");
//		//修改协议包数据到下行，去掉token字段
//		String strData = clientJson.getString("data");
//		byte[] data = ServerEndPoint.convertStringToProtocolDataBytes(strData);
		//packet.setData(clientJSONBytes);
		//packet.setLength(clientJSONBytes.length);
		
		
//		String strToken = clientJson.getString("token");//客户端Token
		//String strToken = new String(packet.getToken());
		//logger.debug("strToken=" + strToken);
		
//		if(!ServerHandler.checkChannelInfo(strToken)){
//			//校验连接Token合法性，到认证服务
//			logger.debug("check channelInfo not exist this token, invoke checkChannelIsVaild to connect Auth Server");
////			if(!this.checkChannelIsValid(proCallback.getChannel(), strToken)){
//				logger.debug("checkChannelIsVaild failed, this channel is illegal, close it");
//				if(proCallback.getChannel()!=null){
//					proCallback.getChannel().close();
//				}
//				return false;
////			}
//		}
		
//		logger.debug("beginInvokeFilter end");
		
	

	/**
	 * 解析前端传送的信息,并获取操作对象
	 * 
	 */
	public void getInfo(String info, ServiceConf serviceConf) {

		List<InfoNewPacket> infoPackets = JsonUtil.toGsonList(info,
				InfoNewPacket.class);
		for (InfoNewPacket infoPacket : infoPackets) {
			serviceConf.getP().setObjectType(
					DSObjectType.parse(infoPacket.getType()));
		}

	}

	/**
	 * 操作类型值进行转换
	 * 
	 * @param serviceConf
	 * @param userPrivilegeList
	 * @return
	 */
	private boolean compare(ServiceConf serviceConf,
			List<Privilege> userPrivilegeList) {
		boolean compareResult = false;
		for (int i = 0; i < userPrivilegeList.size(); i++) {
			if (serviceConf.getP().getObjectType().value() == userPrivilegeList
					.get(i).getEntity_type_id()) {
				logger.debug("==== 前端传送的类型=="+serviceConf.getP().getObjectType().value());
				if (DMLType.SELECT == serviceConf.getP().getDmlType()) {
					logger.debug("====请求查询权限==");
					if (userPrivilegeList.get(i).isCan_view()) {
						compareResult = true;
						break;
					}
					logger.debug("====没有查询权限====");
				} else if (DMLType.UPDATE == serviceConf.getP().getDmlType()) {
					logger.debug("====请求更新权限==");
					if (userPrivilegeList.get(i).isCan_update()) {
						compareResult = true;
						break;
					}
					logger.debug("====没有更新权限==");
				} else if (DMLType.INSERT == serviceConf.getP().getDmlType()) {
					logger.debug("====请求创建权限==");
					if (userPrivilegeList.get(i).isCan_create()) {
						compareResult = true;
						break;
					}
					logger.debug("====没有创建权限==");
				} else if (DMLType.DELETE == serviceConf.getP().getDmlType()) {
					logger.debug("====请求删除权限==");
					if (userPrivilegeList.get(i).isCan_delete()) {
						compareResult = true;
						break;
					}
					logger.debug("====没有删除权限==");
				}
			}
		}
		logger.debug("拦截的结果为compareResult=="+compareResult);
		return compareResult;

	}
	
   
	/**
	 * 
	 * @param proCallback
	 */
	public abstract void beginInvoke(ProtocolCallback proCallback);

	/**
	 * 
	 * @param proCallback
	 */
	public abstract void endInvoke(ProtocolCallback proCallback);
	
}