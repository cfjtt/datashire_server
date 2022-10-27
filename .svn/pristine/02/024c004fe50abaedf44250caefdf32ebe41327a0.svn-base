package com.eurlanda.datashire.sprint7.service.squidflow.subservice;

import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.entity.SquidJoin;
import com.eurlanda.datashire.entity.SquidLink;
import com.eurlanda.datashire.sprint7.service.squidflow.AbstractRepositoryService;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *Join处理类
 * @author dang.lu 2013.11.12
 *
 */
public class JoinServicesub extends AbstractRepositoryService implements IJoinService{
	Logger logger = Logger.getLogger(JoinServicesub.class);// 记录日志
	public JoinServicesub(String token) {
		super(token);
	}
	public JoinServicesub(IRelationalDataManager adapter){
		super(adapter);
	}
	public JoinServicesub(String token, IRelationalDataManager adapter){
		super(token, adapter);
	}

	public List<SquidJoin> getJoinByFlowId(int squid_flow_id) {
		return adapter.query2List("SELECT distinct t.* FROM ds_v_squid_flow_join t WHERE squid_flow_id="+squid_flow_id, null, SquidJoin.class);
	}
	
	public List<SquidJoin> getJoinBySquidId(int squid_id) {
		return adapter.query2List("SELECT * FROM ds_join t WHERE joined_squid_id="+squid_id, null, SquidJoin.class);
	}
	/**
	 * 根据join_id删除join
	 * @param join_id
	 * @param out
	 * @return
	 */
	public boolean deleteJoin(int join_id,ReturnValue out)
	{
		boolean delete=true;
		Map<String, String> paramMap = new HashMap<String, String>();
		try {
			// 根据id查询出join表中的target_squid_id和joined_squid_id
			paramMap.put("id", String.valueOf(join_id));
			List<SquidJoin> join=adapter.query2List(true, paramMap, SquidJoin.class);
			int target_squid_id=join.get(0).getTarget_squid_id();
			int joined_squid_id=join.get(0).getJoined_squid_id();
			// 根据target_squid_id和joined_squid_id查出link表的id(target_squid_id为-1时,不执行删除squidlink的操作)
			if(target_squid_id>0)
			{
				paramMap.clear();
				paramMap.put("to_squid_id", String.valueOf(joined_squid_id));
				paramMap.put("from_squid_id", String.valueOf(target_squid_id));
				List<SquidLink> squidLinks=adapter.query2List(true, paramMap, SquidLink.class);
				// 删除link
				 SquidLinkServicesub squidLinkService=new SquidLinkServicesub(token, adapter);
				 if(squidLinks.size()>0){
					 delete= squidLinkService.deleteSquidLink(squidLinks.get(0).getId(), out);
				 }
			}
			 if(delete)
			 {
				// 删除join表
					paramMap.clear();
					paramMap.put("id", String.valueOf(join_id));
					return adapter.delete(paramMap, SquidJoin.class)>=0?true:false;
			 }
		} catch (Exception e) {
			logger.error("[删除deleteJoin=========================================exception]", e);
			try {
				adapter.rollback();
			} catch (SQLException e1) {
				logger.error("rollback err!", e1);
			}
			out.setMessageCode(MessageCode.SQL_ERROR);
			return false;
		}
		return delete;
	}
}