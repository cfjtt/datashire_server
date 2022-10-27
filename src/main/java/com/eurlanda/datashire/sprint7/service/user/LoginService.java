/*
 * LoginService.java - 2013-9-14
 *
 * 版权声明: (c) 2013，悦岚（上海）数据服务有限公司，保留所有权利。
 *
 * 项目名称: datashire-server
 * 
 * 修改历史:
 * ===========================================
 *   修改人	     日期		     描述
 *   ---------------------------------------
 *   dang.lu  2013.9.14   create    
 * ===========================================
 */
package com.eurlanda.datashire.sprint7.service.user;

import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.server.model.User;
import com.eurlanda.datashire.sprint7.packet.InfoMessagePacket;
import com.eurlanda.datashire.sprint7.service.user.subservice.UserService;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.google.gson.FieldNamingPolicy;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

import java.text.MessageFormat;

/**
 * 用户登录、登出业务处理
 * 
 * @author dang.lu 2013.09.12
 *
 */
@Service
public class LoginService implements ILoginService{
	private static Logger logger = Logger.getLogger(LoginService.class);
	private String token;
	private String key; // UnusedPrivateField
	
	/**
	 * 登出
	 * @return
	 */
	public String logout() {
		long s = System.currentTimeMillis();
		//logger.info(MessageFormat.format("service begin: {0}", info));
		String ret = "";
		InfoMessagePacket<Boolean> packet = new InfoMessagePacket<Boolean>();
		// 修改用户状态[已退出]？
		packet.setCode(MessageCode.SUCCESS.value());
		//packet.setInfo(process.logout(info, out));
		ret = JsonUtil.object2Json(packet);
//		CommonConsts.UserSession.remove(strToken);
		logger.info(MessageFormat.format("\r\n\r\n【{2}ms. {1}bytes.】service end: {0}", ret, ret.length(), System.currentTimeMillis()-s));
		return ret;
	}
	
	/**
	 * 登录（普通用户、超级用户）
	 * @param info
	 * @return
	 */
	public String login(String info) {
		InfoMessagePacket<User> packet = new InfoMessagePacket<User>();
		packet.setType(DSObjectType.USER);
		int teamId=Integer.MIN_VALUE;
		User user = JsonUtil.json2Object(info , User.class, FieldNamingPolicy.UPPER_CAMEL_CASE);
		if(user!=null && user.getUser_name()!=null && user.getPassword()!=null){
			user.setUser_name(user.getUser_name().trim()); // 用户名可以有前后空格

//			boolean isSuperUser =  CommonConsts.SuperUserName.equalsIgnoreCase(user.getUser_name())
//					&& CommonConsts.SuperUserPwd.equals(user.getPassword());
			boolean isSuperUser = false;
			/*if(user.getTeam()!=null)teamId = user.getTeam().getId();*/
			if(!isSuperUser){
				UserService userService = new UserService();
				user = userService.getUser(user.getUser_name(), user.getPassword());
				if(null==user)
				{
					packet.setCode(MessageCode.ERR_USER.value());//用户不存在
				}else{
					// bind user token
					
				}
			}
			/*// 验证用户信息是否存在（根据用户名、密码）
			if(user!=null || isSuperUser){
				// 将请求信息返回
				if(user==null)user=JsonUtil.json2Object(info , User.class, FieldNamingPolicy.UPPER_CAMEL_CASE);
				if(isSuperUser){
					user.setKey(StringUtils.generateGUID());
				}
				if(true||user.isIs_active()){ // 暂不校验是否激活
					// TODO 验证用户是否有team权限
					if(teamId>0){
						TeamService teamService = new TeamService();
						Team team = teamService.getDetail(teamId);
						if(team!=null){
							team.setRepositoryList(
									new RepositoryServiceImpl().getRepositoryByTeamId(team.getId()));
							user.setTeam(team);
						}
						// 修改(非超级)用户状态[已登录]？
					}
					packet.setCode((!isSuperUser&&teamId<=0)? //非超级用户又没有选择team的
							MessageCode.ERR_ID_NULL.value():MessageCode.SUCCESS.value());
				}else{ // 账号未激活(欠费停机或资产冻结待清算...)
					packet.setCode(MessageCode.ERR_USER_LOCKED.value());
				}
			}else{ // 用户信息不存在
				packet.setCode(MessageCode.ERR_USERNAME_OR_PASSWORD.value());
			}*/
		}else{ // 请求参数不合法（用户名或密码为空）
			packet.setCode(MessageCode.ERR_LONGIN_DATA_ISNULL.value());
		}
		/*if(user==null)user=JsonUtil.json2Object(info , User.class, FieldNamingPolicy.UPPER_CAMEL_CASE);
		if(user!=null)user.setType(DSObjectType.USER.value());
		if(user.getTeam()==null){
			user.setTeam(new Team());
		}else{
			user.getTeam().setType(DSObjectType.TEAM.value());
		}*/
		if(user.getId()<=0)user.setId(1); // 超级用户id=0，前台会有问题
		packet.setInfo(user);
		//logger.debug((MessageCode.SUCCESS.value()==packet.getCode()?"登录成功！":"登录失败！"));
		// 登录失败次数限制？
		return JsonUtil.object2Json(packet);
	}
	
	public String getToken() {
		return token;
	}
	public void setToken(String token) {
		this.token = token;
	}
}
