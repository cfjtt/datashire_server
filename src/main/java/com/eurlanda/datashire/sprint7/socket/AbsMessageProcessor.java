package com.eurlanda.datashire.sprint7.socket;

import com.eurlanda.datashire.entity.Privilege;
import com.eurlanda.datashire.enumeration.DMLType;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.server.model.User;
import com.eurlanda.datashire.server.utils.SocketUtil;
import com.eurlanda.datashire.socket.Agreement;
import com.eurlanda.datashire.socket.MessagePacket;
import com.eurlanda.datashire.socket.protocol.ServiceConf;
import com.eurlanda.datashire.sprint7.packet.InfoNewPacket;
import com.eurlanda.datashire.sprint7.service.user.subservice.PrivilegeService;
import com.eurlanda.datashire.utility.CommonConsts;
import com.eurlanda.datashire.utility.JsonUtil;
import org.apache.log4j.Logger;

import java.util.List;

/**
 * 默认消息处理器。用来过滤权限
 * 
 * @date 2014-4-23
 * @author jiwei.zhang
 * 
 */
public abstract class AbsMessageProcessor implements ISocketMessageProcessor {
	static Logger logger = Logger.getLogger(AbsMessageProcessor.class);

	public static String AUTH_ERRORCODE_KEY = "code";

	public static String WEBSERVICE_DATA = "data";

	public static String WEBSERVICE_STATUS = "status";

	public static String WEBSERVICE_SUCCESS_STATUS = "SUCCESS_CALL";

	public static long AUTH_ERRORCODE_VALUE = 1;

	@Override
	public MessagePacket process(SocketRequestContext sc) {
		// 检验权限
		MessagePacket packet = sc.getMessagePacket();
		/*if (validate(packet)) {*///目前不需要校验权限
			return this.doProcess(sc);
		/*} else {		// 无权限　。
			ProtocolData pdata = new ProtocolData();
			pdata.setStatus(ResponseStatus.WARN_OPERATION_ILLEGAL.toString());
			//pdata.setDesc("操作无权限，可能用户登录信息已经过期");
			String result = JSONObject.fromObject(pdata).toString();
			byte[] data = result.getBytes();
			packet.setData(data);
			logger.warn("token:"+sc.getToken()+",操作无权限，可能用户登录信息已经过期！");
			return packet;
		}*/
	}

	public boolean validate(MessagePacket packet) {
		boolean filterResult = false;

		if (packet == null) {
			logger.debug("ProtocolPacket is null");
			return false;
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
			ServiceConf serviceConf = Agreement.ServiceConfMap.get(childCommandId);
			if (null != serviceConf) {
				// 如果不需要权限验证直接返回true
				if (null == serviceConf.getP()) {
					return true;
				}
			} else {
				logger.error("serviceConf is null, cmd = " + childCommandId);
				return false;
			}
			logger.trace("获取到的命令号为===" + childCommandId + " " + "获取到的token为=" + token);
			User user = SocketUtil.SESSION.get(token);
			if (null != user) { // 超级用户登录直接返回ture
				logger.trace("username============" + user.getUser_name());
				if (CommonConsts.SuperUserName.equals(user.getUser_name())) {
					return true;
				}
				// 获取用户id
				userId = user.getId();
				// 调用操作权限业务类
				PrivilegeService privilegeService = new PrivilegeService();
				// 获取对应用户的权限
				List<Privilege> userPrivilegeList = privilegeService.getPrivileges(userId, "U");
				// 如果调用删除方法,解析前端传输的信息获取要操作的对象
				if ("00010025".equals(childCommandId)) {
					info = new String(packet.getData());
					this.getInfo(info, serviceConf);
				}
				// 用前端请求的权限和查询出来的权限做对比
				filterResult = this.compare(serviceConf, userPrivilegeList);
				logger.debug("返回的值为===" + filterResult);
			} else {
				return true; // 有可能服务器重启
			}
		} catch (Exception e) {
			logger.error("error", e);
		}
		return filterResult;
	}

	/**
	 * 解析前端传送的信息,并获取操作对象
	 * 
	 */
	public void getInfo(String info, ServiceConf serviceConf) {

		List<InfoNewPacket> infoPackets = JsonUtil.toGsonList(info, InfoNewPacket.class);
		for (InfoNewPacket infoPacket : infoPackets) {
			serviceConf.getP().setObjectType(DSObjectType.parse(infoPacket.getType()));
		}

	}

	/**
	 * 操作类型值进行转换
	 * 
	 * @param serviceConf
	 * @param userPrivilegeList
	 * @return
	 */
	private boolean compare(ServiceConf serviceConf, List<Privilege> userPrivilegeList) {
		boolean compareResult = false;
		for (int i = 0; i < userPrivilegeList.size(); i++) {
			if (serviceConf.getP().getObjectType().value() == userPrivilegeList.get(i).getEntity_type_id()) {
				logger.debug("==== 前端传送的类型==" + serviceConf.getP().getObjectType().value());
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
		logger.debug("拦截的结果为compareResult==" + compareResult);
		return compareResult;

	}
	/**
	 * 调用根据消息调用具体的业务，并返回值。
	 * @date 2014-4-23
	 * @author jiwei.zhang
	 * @param sc
	 * @return
	 */
	abstract public MessagePacket doProcess(SocketRequestContext sc);

}
