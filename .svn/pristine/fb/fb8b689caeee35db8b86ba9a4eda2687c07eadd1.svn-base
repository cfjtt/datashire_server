package com.eurlanda.datashire.sprint7.service.user.subservice;

import com.eurlanda.datashire.entity.Privilege;
import com.eurlanda.datashire.entity.Role;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.sprint7.packet.InfoPacket;
import com.eurlanda.datashire.utility.MessageCode;

import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * role业务处理
 * 
 * @author dang.lu 2013.09.12
 *
 */
public class RoleService extends AbstractService{

	/**
	 * ROLE 删除
	 * 		1. 将关联用户的role_id置空
	 * 		2. 删除对应权限配置
	 * 		3. 删除数据
	 * @param role_id
	 * @return
	 */
	public InfoPacket remove(int role_id) {
		InfoPacket info = new InfoPacket();
		info.setId(role_id);
		info.setType(DSObjectType.ROLE);
		int cnt = 0;
		Map<String, String> paramMap = new HashMap<String, String>();
		adapter.openSession();
		try {
			// ROLE 删除业务组合（顺序不能乱）
			
			//1. 将关联用户的role_id置空
			cnt = adapter.execute("UPDATE DS_SYS_USER SET role_id=null WHERE role_id = "+role_id, null);
			logger.debug(cnt+"[id="+role_id+"] user.role_id=null set ok!");
			
			//2. 删除对应权限配置
			paramMap.put("party_id", Integer.toString(role_id, 10));
			paramMap.put("party_type", String.valueOf('R'));
			cnt = adapter.delete(paramMap, Privilege.class);
			logger.debug(cnt+"[id="+role_id+"] privilege(s) removed!");
			
			//3. 删除数据
			paramMap.clear();
			paramMap.put("id", Integer.toString(role_id, 10));
			cnt = adapter.delete(paramMap, Role.class);
			logger.debug(cnt+"[id="+role_id+"] role(s) removed!");
			
			info.setCode(cnt>=1?MessageCode.SUCCESS.value():MessageCode.DELETE_NOTEXISTS.value());
		} catch (SQLException e) {
			try {  // 事务回滚，很好很强大
				adapter.rollback();
			} catch (SQLException e1) {
				logger.error("rollback err!", e1);
			}
			info.setCode(MessageCode.SQL_ERROR.value());
			logger.error(MessageFormat.format("删除角色异常  id={0}", role_id), e);
		}finally{
			adapter.closeSession();
		}
		return info;
	}

	public List<Role> getAllByGroupId(int group_id) {
		return adapter.query2List("SELECT * FROM DS_SYS_ROLE where group_id="+group_id, null, Role.class);
	}
	
	public List<Role> getAllByTeamId(int team_id) {
		return adapter.query2List("SELECT * FROM DS_SYS_ROLE where group_id in ( SELECT id FROM DS_SYS_GROUP where team_id="+team_id+" )", null, Role.class);
	}
	
}
