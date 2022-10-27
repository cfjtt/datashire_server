package com.eurlanda.datashire.sprint7.service.user.subservice;

import com.eurlanda.datashire.entity.Group;
import com.eurlanda.datashire.entity.Role;
import com.eurlanda.datashire.entity.Team;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.sprint7.packet.InfoPacket;
import com.eurlanda.datashire.utility.MessageCode;

import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * team业务处理
 * 
 * @author dang.lu 2013.09.12
 *
 */
public class TeamService extends AbstractService{
	
	/**
	 * Team 删除
	 * 
	 * @param team_id
	 * @return
	 */
	public InfoPacket remove(int team_id) {
		InfoPacket info = new InfoPacket();
		info.setId(team_id);
		info.setType(DSObjectType.TEAM);
		int cnt = 0;
		Map<String, String> paramMap = new HashMap<String, String>();
		adapter.openSession();
		try {
			// Team 删除业务组合（顺序不能乱）
			
			//1. 删除关联group和role对应权限
			cnt = adapter.execute("DELETE FROM DS_SYS_PRIVILEGE WHERE"
					+ "(party_type='G' and party_id in (SELECT id FROM DS_SYS_GROUP where team_id ="+team_id+") )"
					+ " OR (party_type='R' and party_id in(SELECT id FROM DS_SYS_ROLE where group_id in(SELECT id FROM DS_SYS_GROUP where team_id = "+team_id+")))", null);
			logger.debug(cnt+"[id="+team_id+"] group+role.privilege(s) removed!");
			
			//2. 将关联group-role-user的role_id置空
			cnt = adapter.execute("UPDATE DS_SYS_USER SET role_id=null WHERE role_id in (SELECT id FROM DS_SYS_ROLE where group_id in ( SELECT id FROM DS_SYS_GROUP where team_id="+team_id+" ) )", null);
			logger.debug(cnt+"[id="+team_id+"] user.role_id=null set ok!");
			
			//3. 删除关联group-role
			cnt = adapter.execute("DELETE FROM DS_SYS_ROLE WHERE group_id in( SELECT id FROM DS_SYS_GROUP where team_id="+team_id+" )", null);
			logger.debug(cnt+"[id="+team_id+"] role(s) removed!");
			
			//4. 删除关联group
			paramMap.clear();
			paramMap.put("team_id", Integer.toString(team_id, 10));
			cnt = adapter.delete(paramMap, Group.class);
			logger.debug(cnt+"[id="+team_id+"] group(s) removed!");
		
			//5. 删除(反注册)关联repository
			cnt = adapter.execute("DELETE FROM DS_SYS_REPOSITORY WHERE team_id = "+team_id, null);
			logger.debug(cnt+"[id="+team_id+"] repository(s) removed!");
			
			//6. 删除team
			paramMap.clear();
			paramMap.put("id", Integer.toString(team_id, 10));
			cnt = adapter.delete(paramMap, Team.class);
			logger.debug(cnt+"[id="+team_id+"] team(s) removed!");
			
			info.setCode(cnt>=1?MessageCode.SUCCESS.value():MessageCode.DELETE_NOTEXISTS.value());
		} catch (SQLException e) {
			try {  // 事务回滚，很好很强大
				adapter.rollback();
			} catch (SQLException e1) {
				logger.error("rollback err!", e1);
			}
			info.setCode(MessageCode.SQL_ERROR.value());
			logger.error(MessageFormat.format("删除Team异常  id={0}", team_id), e);
		}finally{
			adapter.closeSession();
		}
		return info;
	}

	/**
	  * 获取team详细信息（包含group列表等）
	  * @param id
	  * @return
	  */
	 public Team getDetail(int id) {
		Team team=null;
		List<Team> list = adapter.query2List("SELECT * FROM DS_SYS_TEAM where id="+id, null, Team.class);
		if(list!=null && !list.isEmpty()){
			team = list.get(0);
			if(team!=null){
				team.setGroupList(adapter.query2List("SELECT * FROM DS_SYS_GROUP where team_id="+id, null, Group.class));
				if(team.getGroupList()!=null){ // Deeply nested if..then statements are hard to read.
					  for(int i=0; i<team.getGroupList().size(); i++){
						  team.getGroupList().get(i).setRoleList(
								  adapter.query2List("SELECT * FROM DS_SYS_ROLE where group_id="+team.getGroupList().get(i).getId(), null, Role.class));
					  }
				 }
			}
		}
		return team;
	 }
	 
}
