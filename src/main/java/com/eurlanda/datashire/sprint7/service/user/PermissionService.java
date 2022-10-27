/*
 * PermissionService.java - 2013-9-12
 *
 * 版权声明: (c) 2013，悦岚（上海）数据服务有限公司，保留所有权利。
 *
 * 项目名称: datashire-server
 * 
 * 修改历史:
 * ===========================================
 *   修改人	     日期		     描述
 *   ---------------------------------------
 *   dang.lu  2013.9.12   create    
 * ===========================================
 */
package com.eurlanda.datashire.sprint7.service.user;

import com.eurlanda.datashire.entity.Group;
import com.eurlanda.datashire.entity.Privilege;
import com.eurlanda.datashire.entity.Role;
import com.eurlanda.datashire.entity.Team;
import com.eurlanda.datashire.enumeration.DMLType;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.server.model.User;
import com.eurlanda.datashire.sprint7.packet.InfoPacket;
import com.eurlanda.datashire.sprint7.packet.ListMessagePacket;
import com.eurlanda.datashire.sprint7.service.user.subservice.AbstractService;
import com.eurlanda.datashire.sprint7.service.user.subservice.GroupService;
import com.eurlanda.datashire.sprint7.service.user.subservice.PrivilegeService;
import com.eurlanda.datashire.sprint7.service.user.subservice.RoleService;
import com.eurlanda.datashire.sprint7.service.user.subservice.TeamService;
import com.eurlanda.datashire.sprint7.service.user.subservice.UserService;
import com.eurlanda.datashire.utility.CommonConsts;
import com.eurlanda.datashire.utility.FastJson;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.StringUtils;
import com.google.gson.FieldNamingPolicy;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

import java.text.MessageFormat;
import java.util.List;

/**
 * 权限操作相关业务组合
 * 	1. team group role user 增删查改；
 * 	2. 权限(Privilege)设置、查询
 * 
 * @author dang.lu 2013.09.12
 *
 */

@Service
public class PermissionService implements IPermissionService{
	private static Logger logger = Logger.getLogger(PermissionService.class);

	/**
	 * 获取所有team列表（登录前加载）
	 * @return
	 */
	public String getAllTeams() {
		List teamList =  new TeamService().getAll(Team.class);
		return FastJson.toString(teamList,
				DSObjectType.TEAM, 
				teamList==null||teamList.isEmpty()?MessageCode.NODATA:MessageCode.SUCCESS);
	}
	
	/**
	 * 添加team
	 * @param info List<Team>
	 * @return
	 */
	public String addTeam(String info) {
		return saveTeam(info, DMLType.INSERT.value());
	}
	/**
	 * 更新team
	 * @param info List<Team>
	 * @return
	 */
	public String updateTeam(String info) {
		return saveTeam(info, DMLType.UPDATE.value());
	}
	/** 添加/更新Team */
	private String saveTeam(String info, int dmlType) {
		ListMessagePacket<InfoPacket> packet = new ListMessagePacket<InfoPacket>();	
		AbstractService teamService = new TeamService();
		//Team team = JsonUtil.json2Object(info, Team.class, FieldNamingPolicy.UPPER_CAMEL_CASE);
		List<Team> teamList = JsonUtil.toGsonList(info, Team.class);	
		if(teamList!=null){
			logger.debug(MessageFormat.format("team: {0}", teamList));
			packet.setDataList(teamService.save(teamList, dmlType));// 具体操作结果
			packet.setCode(MessageCode.SUCCESS.value()); // 仅表示程序执行正常，不代表数据添加成功
		}else{
			// 反序列化失败
			packet.setCode(MessageCode.DESERIALIZATION_FAILED.value());
		}
		packet.setType(DSObjectType.TEAM);
		return JsonUtil.object2Json(packet);
	}
	
	
	/**
	 * 添加Group
	 * @param info List<Team>
	 * @return
	 */
	public String addGroup(String info) {
		return saveGroup(info, DMLType.INSERT.value());
	}
	
	/**
	 * 更新Group
	 * @param info List<Team>
	 * @return
	 */
	public String updateGroup(String info) {
		return saveGroup(info, DMLType.UPDATE.value());
	}
	/** 添加/更新Group */
	private String saveGroup(String info, int dmlType) {
		ListMessagePacket<InfoPacket> packet = new ListMessagePacket<InfoPacket>();	
		AbstractService service = new GroupService();
		List<Group> list = JsonUtil.toGsonList(info, Group.class);	
		if(list!=null){
			logger.debug(MessageFormat.format("list: {0}", list));
			packet.setDataList(service.save(list, dmlType));// 具体操作结果
			packet.setCode(MessageCode.SUCCESS.value()); // 仅表示程序执行正常，不代表数据添加成功
		}else{
			// 反序列化失败
			packet.setCode(MessageCode.DESERIALIZATION_FAILED.value());
		}
		packet.setType(DSObjectType.GROUP);
		return JsonUtil.object2Json(packet);
	}
	
	/**
	 * 添加Role
	 * @param info List<Team>
	 * @return
	 */
	public String addRole(String info) {
		return saveRole(info, DMLType.INSERT.value());
	}
	
	/**
	 * 更新Role
	 * @param info List<Team>
	 * @return
	 */
	public String updateRole(String info) {
		return saveRole(info, DMLType.UPDATE.value());
	}
	/** 添加/更新Role */
	private String saveRole(String info, int dmlType) {
		ListMessagePacket<InfoPacket> packet = new ListMessagePacket<InfoPacket>();	
		AbstractService service = new RoleService();
		List<Role> list = JsonUtil.toGsonList(info, Role.class);	
		if(list!=null){
			logger.debug(MessageFormat.format("list: {0}", list));
			packet.setDataList(service.save(list, dmlType));// 具体操作结果
			packet.setCode(MessageCode.SUCCESS.value()); // 仅表示程序执行正常，不代表数据添加成功
		}else{
			// 反序列化失败
			packet.setCode(MessageCode.DESERIALIZATION_FAILED.value());
		}
		packet.setType(DSObjectType.ROLE);
		return JsonUtil.object2Json(packet);
	}
	
	
	/**
	 * 添加User
	 * @param info List<Team>
	 * @return
	 */
	public String addUser(String info) {
		return saveUser(info, DMLType.INSERT.value());
	}
	
	/**
	 * 更新User
	 * @param info List<Team>
	 * @return
	 */
	public String updateUser(String info) {
		return saveUser(info, DMLType.UPDATE.value());
	}
	/** 添加/更新user */
	private String saveUser(String info, int dmlType) {
		ListMessagePacket<InfoPacket> packet = new ListMessagePacket<InfoPacket>();	
		AbstractService service = new UserService();
		List<User> list = JsonUtil.toGsonList(info, User.class);	
		if(list!=null){
			for(int i=0; i<list.size(); i++){
				// 新增用户时，如果密码为空则保存为默认密码
				if(dmlType==DMLType.INSERT.value() && StringUtils.isNull(list.get(i).getPassword())){
					list.get(i).setPassword(CommonConsts.DEFAULT_USER_PASSWORD);
				}
				// 新增/更新时，可不分配或取消角色权限
				if(list.get(i).getRole_id()<=0){
					list.get(i).setRole_id(0);
				}
			}
			logger.debug(MessageFormat.format("list: {0}", list));
			packet.setDataList(service.saveBaseObjectForUser(list, dmlType));// 具体操作结果
			packet.setCode(MessageCode.SUCCESS.value()); // 仅表示程序执行正常，不代表数据添加成功
		}else{
			// 反序列化失败
			packet.setCode(MessageCode.DESERIALIZATION_FAILED.value());
		}
		packet.setType(DSObjectType.USER);
		return JsonUtil.object2Json(packet);
	}
	
	/**
	 * 获取team下所有用户列表
	 * @param info
	 * @return
	 */
	public String getUsersByTeamId(String info) {
		Team team = JsonUtil.json2Object(info , Team.class, FieldNamingPolicy.UPPER_CAMEL_CASE);
		ListMessagePacket<User> packet = new ListMessagePacket<User>();
		packet.setType(DSObjectType.USER);
		if(team!=null){
			UserService service = new UserService();
			List<User> userList =  service.getAllByTeamId(team.getId());
			packet.setCode((userList==null||userList.size()==0)?MessageCode.NODATA.value():MessageCode.SUCCESS.value());
			packet.setDataList(userList);
		}else{
			packet.setCode(MessageCode.DESERIALIZATION_FAILED.value());
		}
		return JsonUtil.object2Json(packet);
	}
	
	/**
	 * 获取所有user
	 * @return
	 */
	public String getAllUsers() {
		ListMessagePacket<User> packet = new ListMessagePacket<User>();
		packet.setType(DSObjectType.USER);
		UserService service = new UserService();
		List<User> userList =  service.getAll();
		packet.setCode((userList==null||userList.size()==0)?MessageCode.NODATA.value():MessageCode.SUCCESS.value());
		packet.setDataList(userList);
		return JsonUtil.object2Json(packet);
	}
	
	/**
	 * 查询user操作权限
	 * @param info User
	 * @return ListMessagePacket<Privilege>
	 */
	public String getUserPrivilege(String info) {
		User user = JsonUtil.json2Object(info , User.class, FieldNamingPolicy.UPPER_CAMEL_CASE);
		ListMessagePacket<Privilege> packet = new ListMessagePacket<Privilege>();
		packet.setType(DSObjectType.PRIVILEGE);
		if(user!=null){
			PrivilegeService service = new PrivilegeService();
			packet.setDataList(service.getPrivilegeList(service.getPrivileges(user.getId(), "U"), 
					service.getPrivileges(user.getRole_id(), "R"), user.getId(), user.getRole_id(), 'U'));
			packet.setCode(MessageCode.SUCCESS.value());
		}else{
			packet.setCode(MessageCode.DESERIALIZATION_FAILED.value());
		}
		return JsonUtil.object2Json(packet);
	}
	
	/**
	 * 查询Role操作权限
	 * @param info Role
	 * @return ListMessagePacket<Privilege>
	 */
	public String getRolePrivilege(String info) {
		Role user = JsonUtil.json2Object(info , Role.class, FieldNamingPolicy.UPPER_CAMEL_CASE);
		ListMessagePacket<Privilege> packet = new ListMessagePacket<Privilege>();
		packet.setType(DSObjectType.PRIVILEGE);
		if(user!=null){
			PrivilegeService service = new PrivilegeService();
			packet.setDataList(service.getPrivilegeList(service.getPrivileges(user.getId(), "R"), 
					service.getPrivileges(user.getGroup_id(), "G"), user.getId(), user.getGroup_id(), 'R'));
			packet.setCode(MessageCode.SUCCESS.value());
		}else{
			packet.setCode(MessageCode.DESERIALIZATION_FAILED.value());
		}
		return JsonUtil.object2Json(packet);
	}
	
	/**
	 * 查询Group操作权限
	 * @param info Group
	 * @return ListMessagePacket<Privilege>
	 */
	public String getGroupPrivilege(String info) {
		Group user = JsonUtil.json2Object(info , Group.class, FieldNamingPolicy.UPPER_CAMEL_CASE);
		ListMessagePacket<Privilege> packet = new ListMessagePacket<Privilege>();
		packet.setType(DSObjectType.PRIVILEGE);
		if(user!=null){
			PrivilegeService service = new PrivilegeService();
			packet.setDataList(service.getPrivilegeList(service.getPrivileges(user.getId(), "G"), 
					service.getPrivileges(user.getParent_group_id(), "G"), user.getId(), user.getParent_group_id(), 'G'));
			packet.setCode(MessageCode.SUCCESS.value());
		}else{
			packet.setCode(MessageCode.DESERIALIZATION_FAILED.value());
		}
		return JsonUtil.object2Json(packet);
	}

	/**
	 * 设置（操作）权限，已设置update，未设置insert
	 * @param info List<Privilege>
	 * @return ListMessagePacket<InfoPacket>
	 */
	public String setPrivilege(String info) {
		List<Privilege> beans = JsonUtil.toGsonList(info , Privilege.class);
		ListMessagePacket<InfoPacket> packet = new ListMessagePacket<InfoPacket>();
		packet.setType(DSObjectType.PRIVILEGE);
		if(beans!=null){
			PrivilegeService service = new PrivilegeService();
			packet.setDataList(service.setPrivilege(beans));
			packet.setCode(MessageCode.SUCCESS.value());
		}else{
			packet.setCode(MessageCode.DESERIALIZATION_FAILED.value());
		}
		return JsonUtil.object2Json(packet);
	}
	
	private String token;
	private String key;
}
