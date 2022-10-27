package com.eurlanda.datashire.sprint7.service.user;

/**
 * 权限操作相关业务组合
 * 	1. team group role user 增删查改；
 * 	2. 权限(Privilege)设置、查询
 * 
 * @author dang.lu 2013.09.12
 *
 */

public interface IPermissionService{
	
	/**
	 * 获取所有team列表（登录前加载）
	 * @return
	 */
	public String getAllTeams();
	/**
	 * 添加team
	 * @param info List<Team>
	 * @return
	 */
	public String addTeam(String info);
	/**
	 * 更新team
	 * @param info List<Team>
	 * @return
	 */
	public String updateTeam(String info);
	/**
	 * 添加Group
	 * @param info List<Team>
	 * @return
	 */
	public String addGroup(String info);
	/**
	 * 更新Group
	 * @param info List<Team>
	 * @return
	 */
	public String updateGroup(String info);
	/**
	 * 添加Role
	 * @param info List<Team>
	 * @return
	 */
	public String addRole(String info);
	/**
	 * 更新Role
	 * @param info List<Team>
	 * @return
	 */
	public String updateRole(String info);
	/**
	 * 添加User
	 * @param info List<Team>
	 * @return
	 */
	public String addUser(String info);
	/**
	 * 更新User
	 * @param info List<Team>
	 * @return
	 */
	public String updateUser(String info);
	/**
	 * 获取team下所有用户列表
	 * @param info
	 * @return
	 */
	public String getUsersByTeamId(String info);
	/**
	 * 获取所有user
	 * @param info
	 * @return
	 */
	public String getAllUsers();
	/**
	 * 查询user操作权限
	 * @param info User
	 * @return ListMessagePacket<Privilege>
	 */
	public String getUserPrivilege(String info);
	/**
	 * 查询Role操作权限
	 * @param info Role
	 * @return ListMessagePacket<Privilege>
	 */
	public String getRolePrivilege(String info);
	/**
	 * 查询Group操作权限
	 * @param info Group
	 * @return ListMessagePacket<Privilege>
	 */
	public String getGroupPrivilege(String info);
	/**
	 * 设置（操作）权限，已设置update，未设置insert
	 * @param info List<Privilege>
	 * @return ListMessagePacket<InfoPacket>
	 */
	public String setPrivilege(String info);
	
}