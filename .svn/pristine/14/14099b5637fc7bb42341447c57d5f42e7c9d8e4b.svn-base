/*
 * UserService.java - 2013-10-1
 *
 * 版权声明: (c) 2013，悦岚（上海）数据服务有限公司，保留所有权利。
 *
 * 项目名称: datashire-server
 * 
 * 修改历史:
 * ===========================================
 *   修改人	      日期		      描述
 *   ---------------------------------------
 *   dang.lu  2013-10-1   create    
 * ===========================================
 */
package com.eurlanda.datashire.sprint7.service.user.subservice;

import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.server.model.User;
import com.eurlanda.datashire.server.utils.Constants;
import com.eurlanda.datashire.sprint7.packet.InfoPacket;
import com.eurlanda.datashire.utility.MessageCode;

import java.util.List;

/**
 * 用户增删查改
 * 
 * @author dang.lu 2013-10-1
 *
 */
public class UserService extends AbstractService{

	/**
	 * 删除用户，同时删除分配的权限
	 * @param user_id
	 * @return
	 */
	public InfoPacket remove(int user_id) {
        InfoPacket info = new InfoPacket();
        info.setId(user_id);
        info.setType(DSObjectType.USER);
        com.eurlanda.datashire.server.service.UserService userService = Constants.context.getBean(com.eurlanda.datashire.server.service.UserService.class);
        int cnt = userService.deleteById(user_id);
        if(cnt > 0) {
            // TODO 暂时没有权限,先不用删除,等权限重构后再删除
            info.setCode(MessageCode.SUCCESS.value());
        } else {
            info.setCode(MessageCode.DELETE_NOTEXISTS.value());
        }
        return info;


//		adapter.openSession();
//		try {
//			Map<String, String> paramMap = new HashMap<String, String>();
//			paramMap.put("id", Integer.toString(user_id, 10));
//			int cnt = adapter.delete(paramMap, User.class);
//			logger.debug(cnt+"[id="+user_id+"] user(s) removed!");
//			if(cnt>=1){
//				//删除对应权限配置
//				paramMap.clear();
//				paramMap.put("party_id", String.valueOf(user_id));
//				paramMap.put("party_type", String.valueOf('U'));
//				cnt = adapter.delete(paramMap, Privilege.class);
//				logger.debug(cnt+"[id="+user_id+"] privilege(s) removed!");
//				info.setCode(MessageCode.SUCCESS.value());
//			}else{ // DML row count 说明该user_id不存在
//				info.setCode(MessageCode.DELETE_NOTEXISTS.value());
//			}
//		} catch (SQLException e) {
//			try {  // 事务回滚，很好很强大
//				adapter.rollback();
//			} catch (SQLException e1) {
//				logger.error("rollback err!", e1);
//			}
//			info.setCode(MessageCode.SQL_ERROR.value());
//			logger.error(MessageFormat.format("删除用户异常  id={0}", user_id), e);
//		}finally{
//			adapter.closeSession();
//		}
//		return info;
	}
	
	/** 查询所有用户，关联角色名称 */
	public List<User> getAll() {
        com.eurlanda.datashire.server.service.UserService userService = Constants.context.getBean(com.eurlanda.datashire.server.service.UserService.class);
        return userService.getAllUser();
//		return adapter.query2List("SELECT u.*,r.name AS ROLE_NAME FROM DS_SYS_USER u left outer join DS_SYS_ROLE r on u.role_id=r.id", null, User.class);
	}
	
	public List<User> getAllByRoleId(int role_id) {
		return adapter.query2List("SELECT u.*,r.name AS ROLE_NAME FROM DS_SYS_USER u left outer join DS_SYS_ROLE r on u.role_id=r.id WHERE u.role_id="+role_id, null, User.class);
	}
	
	public List<User> getAllByGroupId(int group_id) {
		return adapter.query2List("SELECT u.*,r.name AS ROLE_NAME FROM DS_SYS_USER u left outer join DS_SYS_ROLE r on u.role_id=r.id WHERE u.role_id in (SELECT id FROM DS_SYS_ROLE where group_id="+group_id+")", null, User.class);
	}
	
	/** 根据team_id查询所有用户，关联角色名称；同时找出还没有分配角色的用户 */
	public List<User> getAllByTeamId(int team_id) {
		return adapter.query2List(
			"SELECT u.*,r.name AS ROLE_NAME FROM DS_SYS_USER u left outer join DS_SYS_ROLE r on u.role_id=r.id WHERE u.role_id in"+
		   	" ( SELECT id FROM DS_SYS_ROLE where group_id in"+
			" 	( SELECT id FROM DS_SYS_GROUP where team_id="+team_id+") "+
		   	" ) union all"+ // 同时要查询出所有没有分配角色的用户
		   	" SELECT id, key, -1 AS ROLE_ID, user_name, password, full_name, email_address, status_id, is_active, last_logon_date, creation_date, '' AS ROLE_NAME"+
	   	   	" FROM DS_SYS_USER WHERE role_id is null", null, User.class);
	}
	
	/**
	 * 根据用户名、密码查询用户（登录调用）
	 * @param user_name
	 * @param password
	 * @return
	 */
	public User getUser(String user_name, String password) {
        com.eurlanda.datashire.server.service.UserService userService = Constants.context.getBean(com.eurlanda.datashire.server.service.UserService.class);
        return userService.findByUsernameAndPassword(user_name, password);
//
//        Map<String, String> paramMap = new HashMap<String, String>();
//		paramMap.put("user_name", user_name);
//		paramMap.put("password", password);
//		List<User> userList = adapter.query2List(paramMap, User.class);
//		return (userList==null||userList.isEmpty())?null:userList.get(0);
	}
	
}
