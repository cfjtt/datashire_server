package com.eurlanda.datashire.sprint7.service.user.subservice;

import com.eurlanda.datashire.entity.Group;
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
 * group业务处理
 * 		（通用查询、新增、更新请参考父类实现）
 * 
 * @author dang.lu 2013.09.12
 *
 */
public class GroupService extends AbstractService{
	
	/**
	 * Group 删除
	 * 	1. 将直接下属group的parent_group_id置空
	 * 	2. 删除group对应权限配置
	 * 	3. 将关联role的user的role_id置空
	 * 	4. 删除关联role权限
	 * 	5. 删除关联role
	 * 	6. 删除数据
	 * @param group_id
	 * @return
	 */
	public InfoPacket remove(int group_id) {
		InfoPacket info = new InfoPacket();
		info.setId(group_id);
		info.setType(DSObjectType.GROUP);
		int cnt = 0; // 3. DD - Anomaly: A recently defined variable is redefined. This is ominous but don't have to be a bug.
		Map<String, String> paramMap = new HashMap<String, String>();
		adapter.openSession();
		try {
			// Group 删除业务组合（顺序不能乱）
			
			//1. 将直接下属group的parent_group_id置空
			cnt = adapter.execute("UPDATE DS_SYS_GROUP SET parent_group_id=null WHERE parent_group_id = "+group_id, null);
			logger.debug(cnt+"[id="+group_id+"] group.parent_group_id=null set ok!");
			
			//2. 删除group对应权限配置
			paramMap.put("party_id", Integer.toString(group_id, 10));
			paramMap.put("party_type", String.valueOf('G'));
			cnt = adapter.delete(paramMap, Privilege.class);
			logger.debug(cnt+"[id="+group_id+"] group.privilege(s) removed!");
			
			//3. 将关联role的user的role_id置空
			cnt = adapter.execute("UPDATE DS_SYS_USER SET role_id=null WHERE role_id in ( SELECT id FROM DS_SYS_ROLE where group_id = "+group_id+")", null);
			logger.debug(cnt+"[id="+group_id+"] user.role_id=null set ok!");
			
			//4. 删除关联role权限
			cnt = adapter.execute("DELETE FROM DS_SYS_PRIVILEGE WHERE party_id in ( SELECT id FROM DS_SYS_ROLE where group_id = "+group_id+" ) AND party_type='R'", null);
			logger.debug(cnt+"[id="+group_id+"] group-role.privilege(s) removed!");
			
			//5. 删除关联role
			paramMap.clear();
			paramMap.put("group_id", Integer.toString(group_id, 10));
			cnt = adapter.delete(paramMap, Role.class);
			logger.debug(cnt+"[id="+group_id+"] role(s) removed!");
			
			//6. 删除数据
			paramMap.clear();
			paramMap.put("id", Integer.toString(group_id, 10));
			cnt = adapter.delete(paramMap, Group.class);
			logger.debug(cnt+"[id="+group_id+"] group(s) removed!");
			
			info.setCode(cnt>=1?MessageCode.SUCCESS.value():MessageCode.DELETE_NOTEXISTS.value());
		} catch (SQLException e) {
			try {  // 事务回滚，很好很强大
				adapter.rollback();
			} catch (SQLException e1) {
				logger.error("rollback err!", e1);
			}
			info.setCode(MessageCode.SQL_ERROR.value());
			logger.error(MessageFormat.format("删除Group异常  id={0}", group_id), e);
		}finally{
			adapter.closeSession();
		}
		return info;
	}

	public Group getOne(int group_id) {
		Map<String, String> paramMap = new HashMap<String, String>();
		paramMap.put("id", Integer.toString(group_id));
		List<Group> list;
		list = adapter.query2List(paramMap, Group.class);
		return (list==null||list.isEmpty())?null:list.get(0);
	}
	
}
