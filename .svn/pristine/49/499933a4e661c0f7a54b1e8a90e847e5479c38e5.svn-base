package com.eurlanda.datashire.sprint7.service.squidflow.subservice;

import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.entity.DbSquid;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.sprint7.service.squidflow.AbstractRepositoryService;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * DbSquid增删查改
 * 
 * @author dang.lu 2013.11.12
 *
 */
public class DBSquidServicesub extends AbstractRepositoryService implements IDBSquidService{
	
	public DBSquidServicesub(String token) {
		super(token);
	}
	protected DBSquidServicesub(IRelationalDataManager adapter){
		super(adapter);
	}
	protected DBSquidServicesub(String token, IRelationalDataManager adapter){
		super(token, adapter);
	}
	
	
	/**
	 * 根据squid_id获取DbSquid（即db_source_squid的连接信息）
	 * @param squid_id
	 * @return
	 */
	public DbSquid getDBSquid(int squid_id){
		Map<String, String> paramMap = new HashMap<String, String>(1);
		paramMap.put("ID", Integer.toString(squid_id, 10));
		List<DbSquid> list2 = adapter.query2List(paramMap, DbSquid.class);
		return list2==null||list2.isEmpty()?null:list2.get(0);
	}
	
	
	/**
	 * <p>
	 * 作用描述：根据squidFlowId获取Database集合
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 * @param squidId
	 * @param out
	 * @return
	 */
	public Map<Integer, DbSquid> getAllDBSquids(int squidFlowId, ReturnValue out) {
		logger.debug(String.format("getSquidFlowConnection_squidFlowId=%s", squidFlowId));
		// 根据squidFlowId寻找listConnection
		String sqlStr = " SELECT * FROM DS_SQUID WHERE squid_flow_id=" + squidFlowId + "and SQUID_TYPE_ID = " + SquidTypeEnum.DBSOURCE.value();
		List<DbSquid> listConnection = adapter.query2List(sqlStr, null, DbSquid.class);
		if (listConnection == null || listConnection.size() == 0) {
			out.setMessageCode(MessageCode.NODATA);
			logger.debug(String.format("squidFlowId not found by listConnection(%s)...", squidFlowId));
			return null;
		}
		// 将数据转换为Database实体，并写入Map对象里面
		Map<Integer, DbSquid> databaseMap = new HashMap<Integer, DbSquid>();
		for (DbSquid columnsValue : listConnection) {
			databaseMap.put(columnsValue.getId(), columnsValue);
		}
		return databaseMap;
	}
	
}