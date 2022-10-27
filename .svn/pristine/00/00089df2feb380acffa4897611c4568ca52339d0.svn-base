package com.eurlanda.datashire.sprint7.service.squidflow.subservice;

import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.entity.MessageBubble;
import com.eurlanda.datashire.entity.ReferenceColumn;
import com.eurlanda.datashire.entity.ReferenceColumnGroup;
import com.eurlanda.datashire.entity.Squid;
import com.eurlanda.datashire.entity.SquidLink;
import com.eurlanda.datashire.enumeration.MessageBubbleCode;
import com.eurlanda.datashire.sprint7.packet.PushMessagePacket;
import com.eurlanda.datashire.sprint7.service.squidflow.AbstractRepositoryService;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import com.eurlanda.datashire.utility.StringUtils;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * SquidLink增删查改处理类
 * 
 * @author dang.lu 2013.11.21
 *
 */
public class SquidLinkServicesub  extends AbstractRepositoryService implements ISquidLinkService{
	Logger logger = Logger.getLogger(SquidLinkServicesub.class);// 记录日志
	public SquidLinkServicesub(String token) {
		super(token);
	}
	public SquidLinkServicesub(IRelationalDataManager adapter){
		super(adapter);
	}
	public SquidLinkServicesub(String token, IRelationalDataManager adapter){
		super(token, adapter);
	}
	
	/** 删除squid_link，同时删除引用列 */
	public boolean delete(int squid_link_id){
		try {
			Map<String, Object> map = adapter.query2Object("SELECT from_squid_id,to_squid_id FROM DS_SQUID_LINK WHERE ID="+squid_link_id, null);
			if(map==null || map.isEmpty()){
				logger.warn("删除squid_link不存在 squid_link_id"+squid_link_id);
				return false;
			}
			adapter.openSession();
			// 1. 删除squid_link
			Map<String, String> params = new HashMap<String, String>();
			params.put("ID", Integer.toString(squid_link_id, 10));
			int cnt = adapter.delete(params, SquidLink.class);
			logger.debug(cnt+"[id="+squid_link_id+"] squid_link(s) removed!");
			
			int from_squid_id = StringUtils.valueOfInt2(map, "from_squid_id");
			int to_squid_id = StringUtils.valueOfInt2(map, "to_squid_id");
			map = adapter.query2Object("SELECT distinct group_id FROM DS_REFERENCE_COLUMN WHERE "
					+ "host_squid_id="+from_squid_id+" AND reference_squid_id="+to_squid_id, null);
			if(map==null || map.isEmpty()){
				logger.warn("删除引用列不存在 from_squid_id="+from_squid_id+", to_squid_id="+to_squid_id);
				return false;
			}
			int group_id = StringUtils.valueOfInt2(map, "group_id");
			
			// 2. 删除ReferenceColumnGroup
			params.put("ID", Integer.toString(group_id, 10));
			cnt = adapter.delete(params, ReferenceColumnGroup.class);
			logger.debug(cnt+"[id="+group_id+"] ReferenceColumnGroup(s) removed!");
			// 3. 删除ReferenceColumn
			params.clear();
			params.put("group_id", Integer.toString(group_id, 10));
			params.put("reference_squid_id", Integer.toString(to_squid_id, 10));
			params.put("host_squid_id", Integer.toString(from_squid_id, 10));
			cnt = adapter.delete(params, ReferenceColumn.class);
			logger.debug(cnt+"[group_id="+group_id+"] ReferenceColumn(s) removed!");
			// 4. 删除目标squid的真实列 ?
			//删除squidjoin
			
		} catch (SQLException e) {
			try {
				adapter.rollback();
			} catch (SQLException e1) {
				logger.error("rollback err!", e1);
			}
			logger.error(MessageFormat.format("删除squid_link异常  id={0}", squid_link_id), e);
		}finally{
			adapter.closeSession();
		}
		return true;
	}
	
	/**
	 * 删除squid_link，同时删除引用列
	 * @param squid_link_id
	 * @param out
	 * @return
	 */
	public boolean deleteSquidLink(int squid_link_id,ReturnValue out){
		Map<String, String> params = new HashMap<String, String>();
		boolean executeResult=true;
		try {
			Map<String, Object> map = adapter.query2Object(true,"SELECT from_squid_id,to_squid_id FROM DS_SQUID_LINK WHERE ID="+squid_link_id, null);
			if(map==null || map.isEmpty()){
				logger.warn("删除squid_link不存在 squid_link_id"+squid_link_id);
				return false;
			}
			// 1. 删除squid_link
			params.clear();
			params.put("ID", Integer.toString(squid_link_id, 10));
			int cnt = adapter.delete(params, SquidLink.class);
			logger.debug(cnt+"[id="+squid_link_id+"] squid_link(s) removed!");
			int from_squid_id = StringUtils.valueOfInt2(map, "from_squid_id");
			int to_squid_id = StringUtils.valueOfInt2(map, "to_squid_id");
			map = adapter.query2Object(true,"SELECT distinct group_id FROM DS_REFERENCE_COLUMN WHERE "
					+ "host_squid_id="+from_squid_id+" AND reference_squid_id="+to_squid_id, null);
			if(map==null || map.isEmpty()){
				logger.warn("删除引用列不存在 from_squid_id="+from_squid_id+", to_squid_id="+to_squid_id);
				//return false;
			}else
			{
				int group_id = StringUtils.valueOfInt2(map, "group_id");
				// 2. 删除ReferenceColumnGroup
				ReferenceGroupService groupService=new ReferenceGroupService(token, adapter);
				executeResult=	groupService.deleteReferenceGroup(group_id, out);
			}
			if(executeResult)
			{   //根据from_id查询squid是否孤立
				List<MessageBubble> list=new ArrayList<MessageBubble>();
			    String queryfromSql= "select * from DS_SQUID_LINK where from_squid_id="
						+ from_squid_id + " or to_squid_id=" + from_squid_id + "";;
				List<SquidLink> fromSquidLinks=adapter.query2List(true,queryfromSql, null,SquidLink.class);
				if(fromSquidLinks.size()<1)
				{
					logger.debug("源squid孤立==================");
					//根据from_id查询出squid的key
					params.clear();
					params.put("id",String.valueOf(from_squid_id) );
					Squid fromSquid = adapter.query2Object2(true, params, Squid.class);
//					List<SquidModelBase> fromSquids=adapter.query2List(true, params, SquidModelBase.class);
//					list.add(new MessageBubble(fromSquids.get(0).getKey(), fromSquids.get(0).getKey(), MessageBubbleCode.WARN_SQUID_NO_LINK.value(),false));
					list.add(new MessageBubble(false, fromSquid.getId(), fromSquid.getId(), fromSquid.getName(), MessageBubbleCode.WARN_SQUID_NO_LINK.value(), MessageBubbleCode.WARN, null));
				}
			    String querytoSql= "select * from DS_SQUID_LINK where from_squid_id="
						+ to_squid_id + " or to_squid_id=" + to_squid_id + "";;
				List<SquidLink> toSquidLinks=adapter.query2List(true,querytoSql, null,SquidLink.class);
				if(toSquidLinks.size()<1)
				{
					logger.debug("目标squid孤立==================");
					//根据from_id查询出squid的key
					params.clear();
					params.put("id",String.valueOf(to_squid_id) );
					Squid toSquid = adapter.query2Object2(true, params, Squid.class);
					//List<SquidModelBase> toSquids=adapter.query2List(true, params, SquidModelBase.class);
					//list.add(new MessageBubble(toSquids.get(0).getKey(), toSquids.get(0).getKey(), MessageBubbleCode.WARN_SQUID_NO_LINK.value(),false));
					list.add(new MessageBubble(false, toSquid.getId(), toSquid.getId(), toSquid.getName(), MessageBubbleCode.WARN_SQUID_NO_LINK.value(), MessageBubbleCode.WARN, null));
				}
				PushMessagePacket.push(list, token);//推送消息泡
			}
			
		} catch (SQLException e) {
			logger.error("[删除deleteReferenceGroup=========================================exception]", e);
			try {
				adapter.rollback();
			} catch (SQLException e1) {
				logger.error("rollback err!", e1);
			}
			logger.error(MessageFormat.format("删除squid_link异常  id={0}", squid_link_id), e);
			out.setMessageCode(MessageCode.SQL_ERROR);
			return false;
		}
		return executeResult;
	}
}
