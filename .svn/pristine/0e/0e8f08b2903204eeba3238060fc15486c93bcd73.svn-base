package com.eurlanda.datashire.sprint7.service.user.subservice;

import com.eurlanda.datashire.entity.Group;
import com.eurlanda.datashire.entity.Privilege;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.sprint7.packet.InfoPacket;
import com.eurlanda.datashire.utility.DebugUtil;
import com.eurlanda.datashire.utility.MessageCode;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 权限查询、设置处理
 * 
 * @author dang.lu 2013.09.12
 *
 */
public class PrivilegeService extends AbstractService{
// CyclomaticComplexity: Complexity is determined by the number of decision points in a method 
// plus one for the method entry. The decision points are 'if', 'while', 'for', and 'case labels'. 
// Generally, 1-4 is low complexity, 5-7 indicates moderate complexity, 8-10 is high complexity, 
// and 11+ is very high complexity. 

	/**
	 * 获取操作权限
	 *  1.如果当前对象权限未设置则全false
	 *  2.如果当前对象的直接上级权限未设置，如果是topGroup则全true, 否则也全false
	 * （因为数据库只存储当前对象权限，其直接上级权限需要关联查询，且逻辑较复杂，故由java程序处理） 
	 * （查询数据库操作只需要将当前对象及其上级对象权限查询出来即可，具体处理逻辑如下）
	 * 
	 * 注：这个是权限相关处理逻辑最复杂的一个方法，代码也不过100行而已
	 * 
	 * @param childPrivilegeList  当前对象权限(不含直接上级权限)
	 * @param parentPrivilegeList 直接上级权限
	 * @param id  当前对象id
	 * @param pid 直接上级id
	 * @param type 当前对象类型 (user role group)
	 * @return 当前对象权限(含直接上级权限)
	 */
	public List<Privilege> getPrivilegeList(List<Privilege> childPrivilegeList, List<Privilege> parentPrivilegeList, int id, int pid, char type){
		int len = DSObjectType.Entity_Type_ID.length;
		Privilege privilege = null;
		Map<Integer, boolean[]> canMap = null;
		boolean[] canAry =  null;
		
		// 当前对象权限是否已设置
		boolean isChildSet = childPrivilegeList!=null&&childPrivilegeList.size()==len;
		
		// 1. 直接上级权限没设置 (或者数据不一致/有缺失)
		if(parentPrivilegeList==null||parentPrivilegeList.size()!=len){
			// 如果当前group/role或其直接上级是topGroup, 则Parent_can_*全"true"
			boolean isTopGroup = false;
			if(type!='U'){
				GroupService service = new GroupService();
				Group group = service.getOne(pid);
				if(group==null || group.getParent_group_id()==0){
					isTopGroup = true;
				}
			}
			
			//如果当前对象或其直接上级是topGroup，同时当前对象已设置（topGroup即使未设置）也如实返回
			if(isTopGroup && isChildSet){
				canMap = new HashMap<Integer, boolean[]>();
				for(int i=0; i<len; i++){
					privilege = childPrivilegeList.get(i);
					canAry =  new boolean[4];
					canAry[0] = privilege.isCan_create();
					canAry[1] = privilege.isCan_delete();
					canAry[2] = privilege.isCan_update();
					canAry[3] = privilege.isCan_view();
					canMap.put(privilege.getEntity_type_id(), canAry);
				}
			}
			parentPrivilegeList = new ArrayList<Privilege>(); // AvoidReassigningParameters
			for(int i=0; i<len; i++){
				privilege = new Privilege();
				privilege.setParty_id(id);
				privilege.setParty_type(String.valueOf(type));
				privilege.setEntity_type_id(DSObjectType.Entity_Type_ID[i]);
				privilege.setEntity_type_name(DSObjectType.Entity_Type_Name[i]);
				isChildSet = canMap!=null&&canMap.get(privilege.getEntity_type_id())!=null;
				privilege.setCan_create(isChildSet?canMap.get(privilege.getEntity_type_id())[0]:false);
				privilege.setCan_delete(isChildSet?canMap.get(privilege.getEntity_type_id())[1]:false);
				privilege.setCan_update(isChildSet?canMap.get(privilege.getEntity_type_id())[2]:false);
				privilege.setCan_view(isChildSet?canMap.get(privilege.getEntity_type_id())[3]:false);
				privilege.setParent_can_create(isTopGroup);
				privilege.setParent_can_delete(isTopGroup);
				privilege.setParent_can_update(isTopGroup);
				privilege.setParent_can_view(isTopGroup);
				parentPrivilegeList.add(privilege);
			}
			return parentPrivilegeList;
		}else{
			canMap = new HashMap<Integer, boolean[]>();
			for(int i=0; i<len; i++){
				canAry =  new boolean[4];
				privilege = parentPrivilegeList.get(i);
				canAry[0] = privilege.isCan_create();
				canAry[1] = privilege.isCan_delete();
				canAry[2] = privilege.isCan_update();
				canAry[3] = privilege.isCan_view();
				canMap.put(privilege.getEntity_type_id(), canAry);
			}
			// 2. 当前对象权限没设置 (或者数据不一致/有缺失)
			if(!isChildSet){
				childPrivilegeList = new ArrayList<Privilege>();
				for(int i=0; i<len; i++){
					privilege = new Privilege();
					privilege.setParty_id(id);
					privilege.setParty_type(String.valueOf(type));
					privilege.setEntity_type_id(DSObjectType.Entity_Type_ID[i]);
					privilege.setEntity_type_name(DSObjectType.Entity_Type_Name[i]);
					privilege.setCan_create(false);
					privilege.setCan_delete(false);
					privilege.setCan_update(false);
					privilege.setCan_view(false);
					childPrivilegeList.add(privilege);
				}
			}
			// 3. 填充直接上级相关操作权限
			for(int i=0; i<len; i++){
				privilege = childPrivilegeList.get(i);
				privilege.setEntity_type_name(null); // isChildSet==true 重新设置其name
				privilege.setParent_can_create(canMap.get(privilege.getEntity_type_id())[0]);
				privilege.setParent_can_delete(canMap.get(privilege.getEntity_type_id())[1]);
				privilege.setParent_can_update(canMap.get(privilege.getEntity_type_id())[2]);
				privilege.setParent_can_view(canMap.get(privilege.getEntity_type_id())[3]);
			}
			
			if(DebugUtil.isDebugenabled()){
				for(int i=0; i<childPrivilegeList.size(); i++){
					logger.debug(i+"\t"+childPrivilegeList.get(i));
				}
			}
			
			return childPrivilegeList;
		}
	}

	/**
	 * 权限设置
	 * 		如果已设置则更新，否则新增
	 * 
	 * @param beans
	 * @return
	 */
	public List<InfoPacket> setPrivilege(List<Privilege> beans) {
		int len = DSObjectType.Entity_Type_ID.length;
		// 操作权限按组设置
		if(beans==null || beans.size()!=len){
			logger.warn(beans);
			return null;
		}else{
			List<InfoPacket> resultList = new ArrayList<InfoPacket>();
			InfoPacket info;
			  
			String sql=null;
			String type = beans.get(0).getParty_type();
			int id = beans.get(0).getParty_id();
			Map<String, String> paramMap = new HashMap<String, String>();
			  List<Privilege> list;
			  
			  paramMap.put("party_id", Integer.toString(id));
			  paramMap.put("party_type", type);
			  
			  List params = new ArrayList(len);
			  List param = null;
		    try {
				list = adapter.query2List(paramMap, Privilege.class);
				if(list==null || list.isEmpty()){
					sql = "INSERT INTO DS_SYS_PRIVILEGE(can_delete,can_update,can_create,can_view,entity_type_id,party_type,party_id) VALUES(?,?,?,?,?,?,?)";
				}else{
					sql = "UPDATE DS_SYS_PRIVILEGE SET can_delete=?,can_update=?,can_create=?,can_view=? WHERE entity_type_id=? AND party_type=? AND party_id=?";
				}
				len=len-1;
				for(; len>=0; len--){
					// 返回信息
					info = new InfoPacket();
					info.setId(beans.get(len).getParty_id());
					info.setType(DSObjectType.PRIVILEGE);
					info.setCode(MessageCode.SUCCESS.value());
					resultList.add(info);
					// 新增/更新参数
					param = new ArrayList(7);
					param.add(beans.get(len).isCan_delete()?'Y':'N');
					param.add(beans.get(len).isCan_update()?'Y':'N');
					param.add(beans.get(len).isCan_create()?'Y':'N');
					param.add(beans.get(len).isCan_view()?'Y':'N');
					param.add(beans.get(len).getEntity_type_id());
					param.add(beans.get(len).getParty_type());
					param.add(beans.get(len).getParty_id());
					params.add(param);
				}
				adapter.openSession();
				len = adapter.executeBatch(sql, params);
				logger.debug(len+" privileges setted !");
			} catch (SQLException e) {
				try {  // 事务回滚，很好很强大
					adapter.rollback();
				} catch (SQLException e1) {
					logger.error("rollback err!", e1);
				}
				logger.error("set privilege error", e);
			} finally {
				adapter.closeSession();
			}
		    return resultList;
		}
	}
	
	/**
	 * 获取操作权限列表
	 * @param id 操作对象ID
	 * @param type 操作对象类型（user/role/group）
	 * @return
	 */
	public List<Privilege> getPrivileges(int id, String type) {
		Map<String, String> paramMap = new HashMap<String, String>();
		paramMap.put("party_id", Integer.toString(id));
		paramMap.put("party_type", type);
		return adapter.query2List(paramMap, Privilege.class);
	}

	@Override
	@Deprecated // 这个方法暂无用途，系继承副产品
	public InfoPacket remove(int id) {
		return null;
	}
	
}
