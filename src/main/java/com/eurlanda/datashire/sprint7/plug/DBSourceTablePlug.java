package com.eurlanda.datashire.sprint7.plug;

import com.eurlanda.datashire.adapter.IDBAdapter;
import com.eurlanda.datashire.entity.DBSourceTable;
import com.eurlanda.datashire.entity.operation.DataEntity;
import com.eurlanda.datashire.entity.operation.WhereCondition;
import com.eurlanda.datashire.enumeration.DataStatusEnum;
import com.eurlanda.datashire.enumeration.MatchTypeEnum;
import com.eurlanda.datashire.enumeration.SystemTableEnum;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * DBSourceTable处理类
 * DBSourceTable对象业务处理，新增DBSourceTable对象，根据DBSourceTableID修改DBSourceTable对象；批量新增，批量修改，根据DBSourceTableID查询，根据key获得ID；根据SquidID获得DBSourceTable对象等；
 * <p>
 * Title :
 * </p>
 * <p>
 * Description:
 * </p>
 * <p>
 * Author :赵春花 2013-8-23
 * </p>
 * <p>
 * update :赵春花2013-8-23
 * </p>
 * <p>
 * Department : JAVA后端研发部
 * </p>
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司 </p>
 */
public class DBSourceTablePlug extends Support {
	static Logger logger = Logger.getLogger(DBSourceTablePlug.class);// 记录日志

	public DBSourceTablePlug(IDBAdapter adapter){
		super(adapter);
	}

	/**
	 * 根据ID获得DBSourceTable
	 * <p>
	 * 作用描述：
	 * 根据DBSourceTable ID获得相应的DBSourceTable 对象
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 * @param id DBSourceTableID
	 * @param out 异常处理
	 * @return
	 */
	public DBSourceTable getSourceTable(int id, ReturnValue out) {
		logger.debug(String.format("getSourceTable-id=%s", id));
		DBSourceTable sourceTable = null;

		// 封装查询条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		WhereCondition condition = new WhereCondition();
		condition.setAttributeName("ID");
		condition.setDataType(DataStatusEnum.STRING);
		condition.setMatchType(MatchTypeEnum.EQ);
		condition.setValue(id);

		whereCondList.add(condition);

		Map<String, Object> columnsValue = adapter.queryRecord(null, SystemTableEnum.DS_SOURCE_TABLE.toString(),
				whereCondList, out);
		if (columnsValue == null) {
			// 无数据返回
			out.setMessageCode(MessageCode.NODATA);
			return null;
		}
		// 将数据库查询结果转换为SquidFlow实体
		sourceTable = this.getSourceTable(columnsValue);
		out.setMessageCode(MessageCode.SUCCESS);
		return sourceTable;
	}
	
	/**
	 * 根据SquidID
	 * <p>
	 * 作用描述：
	 * 根据SquidID获得DBSourceTable i
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param id squidID
	 *@param out 异常处理
	 *@return
	 */
	public DBSourceTable getSourceTableBySquidId(int squidId, ReturnValue out) {
		logger.debug(String.format("getSourceTable-SquidId=%s", squidId));
		DBSourceTable sourceTable = null;
		
		// 封装查询条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		WhereCondition condition = new WhereCondition();
		condition.setAttributeName("SQUID_ID");
		condition.setDataType(DataStatusEnum.INT);
		condition.setMatchType(MatchTypeEnum.EQ);
		condition.setValue(squidId);
		
		whereCondList.add(condition);
		
		Map<String, Object> columnsValue = adapter.queryRecord(null, SystemTableEnum.DS_SOURCE_TABLE.toString(),
				whereCondList, out);
		if (columnsValue == null) {
			// 无数据返回
			out.setMessageCode(MessageCode.NODATA);
			return null;
		}
		// 将数据库查询结果转换为SquidFlow实体
		sourceTable = this.getSourceTable(columnsValue);
		out.setMessageCode(MessageCode.SUCCESS);
		return sourceTable;
	}

	/**
	 * 创建DBSourceTable对象
	 * <p>
	 * 作用描述：
	 * 创建DBSourceTable对象
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 * @param dbSourceTable DBSourceTable对象
	 * @param out 异常处理
	 * @return
	 */
	public boolean createSourceTable(DBSourceTable dbSourceTable, ReturnValue out) {
		logger.debug(String.format("createSourceTable-dbSourceTable", dbSourceTable));
		if (dbSourceTable == null) {
			out.setMessageCode(MessageCode.ERR_EDBSOURCETABLE_NULL);
			return false;
		}
		boolean create = false;
		// 将Column转换为数据包集合
		List<DataEntity> paramList = new ArrayList<DataEntity>();
		// 设置值
		GetParam getParam = new GetParam();
		getParam.getDBSourceTable(dbSourceTable, paramList);
		// 执行新增
		create = adapter.insert(SystemTableEnum.DS_SOURCE_TABLE.toString(), paramList, out);
		logger.debug(String.format("createColumn-result=%s", create));
		return create;
	}

	/**
	 * 根据squidID获得DBSourceTable对象
	 * <p>
	 * 作用描述：
	 * 根据Squidid获得相应的DBSourceTable对象
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 * @param squidId squidID
	 * @param tableName 表名
	 * @param out 异常处理
	 * @return
	 */
	public DBSourceTable getSourceTable(int squidId, String tableName, ReturnValue out) {
		logger.debug(String.format("getSourceTable-id=%s,tableName = %s", squidId, tableName));
		DBSourceTable sourceTable = null;

		// 封装查询条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		WhereCondition condition = new WhereCondition();
		condition.setAttributeName("SQUID_ID");
		condition.setDataType(DataStatusEnum.STRING);
		condition.setMatchType(MatchTypeEnum.EQ);
		condition.setValue(squidId);
		WhereCondition conditionTwo = new WhereCondition();
		conditionTwo.setAttributeName("TABLE_NAME");
		conditionTwo.setDataType(DataStatusEnum.STRING);
		conditionTwo.setMatchType(MatchTypeEnum.EQ);
		if(tableName!=null){
			tableName = "'"+tableName+"'";
		}
		conditionTwo.setValue(tableName);

		whereCondList.add(condition);
		whereCondList.add(conditionTwo);

		Map<String, Object> columnsValue = adapter.queryRecord(null, SystemTableEnum.DS_SOURCE_TABLE.toString(),
				whereCondList, out);
		if (columnsValue == null) {
			// 无数据返回
			out.setMessageCode(MessageCode.NODATA);
			return null;
		}
		// 将数据库查询结果转换为SquidFlow实体
		sourceTable = this.getSourceTable(columnsValue);
		out.setMessageCode(MessageCode.SUCCESS);
		return sourceTable;
	}

	
	
	/**
	 * 根据SquidID查询表
	 * <p>
	 * 作用描述：
	 * 根据SquidID获得表名
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param squidId squidＩｄ
	 *@param out 异常处理
	 *@return
	 */
	public DBSourceTable[] queryDSSourceTablesBySquidId(int squidId, ReturnValue out) {
		logger.debug(String.format("queryDSSourceTablesBySquidId-squidId=%s", squidId));
		// 封装查询条件
		List<WhereCondition> conditions = new ArrayList<WhereCondition>();
		WhereCondition condition = new WhereCondition();
		condition.setAttributeName("SQUID_ID");
		condition.setDataType(DataStatusEnum.INT);
		condition.setMatchType(MatchTypeEnum.EQ);
		condition.setValue(squidId);
		// 追加
		conditions.add(condition);
		// 接收结果
		List<Map<String, Object>> columnsValue = adapter.query(null, SystemTableEnum.DS_SOURCE_TABLE.toString(),
				conditions, out);
		if (columnsValue == null || columnsValue.size() == 0) {
			// 无数据返回
			out.setMessageCode(MessageCode.NODATA);
			return null;
		}
		DBSourceTable[] sourceTables = new DBSourceTable[columnsValue.size()];// 定义数组接收
		// 将数据库查询结果转换为SquidFlow实体
		int i = 0;
		for (Map<String, Object> column : columnsValue) {
			sourceTables[i] = this.getSourceTable(column);// 接收数据
			i++;
		}
		logger.debug(String.format("queryDSSourceTablesBySquidId-squidId=%s,result-size=%s", squidId, sourceTables));
		return sourceTables;
	}
	
	/**
	 * 将查询结果Map集合对象转换成SourceTable对象
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 * 
	 * @param columnsValue
	 * @return
	 */
	private DBSourceTable getSourceTable(Map<String, Object> columnsValue) {
		DBSourceTable dbSourceTable = new DBSourceTable();
		// ID
		Object objId = columnsValue.get("ID");
		Integer id = objId == null ? null : Integer.parseInt(String.valueOf(objId));
		// squidID
		Object objSquidId = columnsValue.get("SQUID_ID");
		Integer squidId = objSquidId == null ? null : Integer.parseInt(String.valueOf(objSquidId));
		// name
		String tableName = columnsValue.get("TABLE_NAME") == null ? null : String.valueOf(columnsValue
				.get("TABLE_NAME"));
		// 赋值
		dbSourceTable.setId(id);
		dbSourceTable.setSource_squid_id(squidId);
		dbSourceTable.setTable_name(tableName);
		return dbSourceTable;
	}
	
	/**
	 * 根据squidFlowId获取DBSourceTable集合
	 * <p>
	 * 作用描述：根据squidFlowId获取DBSourceTable集合
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 * 
	 * @param squidFlowId SquidFlowＩＤ
	 * @param out
	 * @return
	 */
	public Map<Integer, DBSourceTable> getSquidFlowDBSourceTable(int squidFlowId, ReturnValue out) {
		logger.debug(String.format("getSquidFlowEColumn_squidFlowId=%s", squidFlowId));
		// 根据squidFlowId寻找listEColumn
		String sqlStr = "SELECT t.id,t.table_name,t.squid_id FROM DS_SQUID s,DS_SOURCE_TABLE t WHERE t.squid_id=s.id AND s.squid_flow_id="
				+ squidFlowId;
		List<Map<String, Object>> listDBSourceTable = adapter.query(sqlStr, out);
		if (listDBSourceTable == null || listDBSourceTable.size() == 0) {
			out.setMessageCode(MessageCode.NODATA);
			logger.debug(String.format("squidFlowId not found by DBSourceTable(%s)...", squidFlowId));
			return null;
		}
		logger.debug(String.format("getSquidFlowDBSourceTable size=%s", listDBSourceTable.size()));

		// 将数据转换为EDBSourceTable实体
		// 将数据转换为Database实体，并写入Map对象里面
		Map<Integer, DBSourceTable> dbSourceTableMap = new HashMap<Integer, DBSourceTable>();
		for (Map<String, Object> obj : listDBSourceTable) {
			Integer id = Integer.parseInt(String.valueOf(obj.get("ID")));
			String tableName = String.valueOf(obj.get("TABLE_NAME"));
			Object objSquidId = obj.get("SQUID_ID");
			Integer squidId = objSquidId == null ? null : Integer.parseInt(String.valueOf(objSquidId));
			dbSourceTableMap.put(id, new DBSourceTable(id, tableName, squidId));
		}
		logger.debug(String.format("getSquidFlowDBSourceTable dbSourceTableMap size=%s", dbSourceTableMap.size()));
		out.setMessageCode(MessageCode.SUCCESS);
		return dbSourceTableMap;
	}
}
