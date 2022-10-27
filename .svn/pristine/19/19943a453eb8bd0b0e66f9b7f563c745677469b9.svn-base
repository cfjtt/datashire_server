package com.eurlanda.datashire.sprint7.plug;

import com.eurlanda.datashire.adapter.IDBAdapter;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.entity.SquidFlow;
import com.eurlanda.datashire.entity.operation.DataEntity;
import com.eurlanda.datashire.entity.operation.WhereCondition;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.enumeration.DataStatusEnum;
import com.eurlanda.datashire.enumeration.MatchTypeEnum;
import com.eurlanda.datashire.enumeration.SystemTableEnum;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import org.apache.log4j.Logger;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/***
 * SquidFlow处理类
 * SquidFlow对象业务处理类，SquidFlow新增业务；SquidFlow根据ＩＤ修改业务，SquidFlow根据ID得到SquidFlow业务；SquidFlow根据key获得相对应的ID
 * <p>
 * Title :
 * </p>
 * <p>
 * Description:
 * </p>
 * <p>
 * Author :Cheryl 2013-8-22
 * </p>
 * <p>
 * update :Cheryl2013-8-22
 * </p>
 * <p>
 * Department : JAVA后端研发部
 * </p>
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司 </p>
 */
public class SquidFlowPlug extends Support {
	static Logger logger = Logger.getLogger(SquidFlowPlug.class);// 记录日志
    private IRelationalDataManager newAdapter;
	public SquidFlowPlug(IDBAdapter adapter){
		super(adapter);
	}
	public SquidFlowPlug(IRelationalDataManager adapter){
		this.newAdapter=adapter;
	}

	/**
	 * 创建SquidFlow对象集合
	 * <p>
	 * 作用描述：
	 * 根据SquidFlow集合新增数据
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param squidFlows squidFlow集合对象
	 *@param out 异常处理
	 *@return
	 */
	public boolean createSquidFLows(List<SquidFlow> squidFlows,ReturnValue out){
		logger.debug(String.format("createSquidFLows-SquidFlowList.size()=%s", squidFlows.size()));
		boolean create = false;
		//封装批量新增数据集
		List<List<DataEntity>>  paramList = new ArrayList<List<DataEntity>>();
		try {
			//调用转换类
			GetParam getParam = new GetParam();
			for (SquidFlow squidFlow : squidFlows) {
				List<DataEntity> dataEntitys =  new ArrayList<DataEntity>();
				getParam.getSquidFlow(squidFlow, dataEntitys);
				// 默认系统时间
				Timestamp date = new Timestamp(System.currentTimeMillis());
				// 创建时间
				DataEntity	dataEntity = new DataEntity("CREATION_DATE", DataStatusEnum.DATE, "'"+date+"'");
				dataEntitys.add(dataEntity);
				paramList.add(dataEntitys);
			}
			//调用批量新增
			create = adapter.createBatch(SystemTableEnum.DS_SQUID_FLOW.toString(), paramList, out);
			logger.debug(String.format("createSquidFLows-return=%s", create));
		} catch (Exception e) {
			out.setMessageCode(MessageCode.ERR_ARRAYS);
			create = false;
			logger.debug(String.format("createSquidFLows-return=%s;Exception=%s;", create,e));
		}
		return create;
		
	}
	/**
	 * 创建SquidFlow
	 * <p>
	 * 作用描述： 新增一个SquidFlow 传入前必须确保SquidFlow对象不为NULL SquidFlowGUID不为空
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 * 
	 * @param squidflow squidFlow对象
	 * @param out 异常处理
	 * @return
	 */
	public boolean createSquidFlow(SquidFlow squidflow, ReturnValue out) {
		logger.debug(String.format("createSquidFlow-squidflow=%s", squidflow));
		boolean create = false;
		// 封装SquidFlow集合包
		List<DataEntity> paramList = new ArrayList<DataEntity>();
		// 设置值
		GetParam getParam = new GetParam();
		getParam.getSquidFlow(squidflow, paramList);// 设置值
		// 执行新增
		create = adapter.insert(SystemTableEnum.DS_SQUID_FLOW.toString(), paramList, out);
		out.setMessageCode(MessageCode.SUCCESS);
		logger.debug(String.format("createSquidFlow-guid=%s;result=%s", squidflow.getKey(), create));
		return create;

	}

	/**
	 * 修改SquidFlow
	 * <p>
	 * 作用描述： 根据传入的参数对象修改SquidFlow信息 传入前必须确保SquidFlow对象不为NULL SquidFlowID不为空
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 * 
	 * @param squidflow squidFlow对象
	 * @param out 异常处理
	 * @return
	 */
	public boolean updateSquidFlow(SquidFlow squidflow, ReturnValue out) {
		logger.debug(String.format("updateSquidFlow-squidFlow=%s;squidFlowId=%s;", squidflow, squidflow.getId()));
		boolean update = false;
		// 封装SquidFlow集合包
		List<DataEntity> paramList = new ArrayList<DataEntity>();
		GetParam getParam = new GetParam();
		getParam.getSquidFlow(squidflow, paramList);// 设置值
		// 条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		WhereCondition condition = new WhereCondition();
		condition.setAttributeName("ID");
		condition.setDataType(DataStatusEnum.INT);
		condition.setMatchType(MatchTypeEnum.EQ);
		condition.setValue(squidflow.getId());
		whereCondList.add(condition);
		// 执行更新
		update = adapter.update(SystemTableEnum.DS_SQUID_FLOW.toString(), paramList, whereCondList, out);
		logger.debug(String.format("updateSquidFlow-return", update));
		return update;
	}

	/**
	 * 根据SquidFlowID删除SquidFlow
	 * <p>
	 * 作用描述： 删除一个SquidFlow并删除下面关联的子项
	 * 
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 * 
	 * @param squidFlowId
	 * @param out
	 * @return
	 */
	public boolean deleteSquidFlow(int squidFlowId, ReturnValue out,
			String token) {
		logger.debug(String.format("deleteSquidFlow-squidFlowId=%d",
				squidFlowId));
		boolean delete = true;
		// 查询出squid的id
		SquidPlug squidPlug = new SquidPlug(adapter);
		String querySquidSql = "select id from DS_SQUID where squid_flow_id="
				+ squidFlowId;
		List<Map<String, Object>> SquidList = adapter.query(querySquidSql, out);
		if(null !=SquidList&&SquidList.size()>0)
		{
			logger.debug("DS_SQUID的长度==="+SquidList.size());
			for (int i = 0; i < SquidList.size(); i++) {
				delete = squidPlug.deleteSquid((Integer)SquidList.get(i).get("ID"), out, token);
				if (!delete) {
					break;
				}
			}
		}
		if (delete) {
			// 执行删除
			List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
			WhereCondition condition = new WhereCondition();
			condition.setAttributeName("ID");
			condition.setDataType(DataStatusEnum.INT);
			condition.setMatchType(MatchTypeEnum.EQ);
			condition.setValue(squidFlowId);
			whereCondList.add(condition);
			// delete = adapter.deleteBatch(tableNames, condList, out);
			delete = adapter.delete(SystemTableEnum.DS_SQUID_FLOW.toString(),
					whereCondList, out);
			logger.debug(String.format(
					"deleteSquidFlow-squidFlowId=%d,result=%s", squidFlowId,
					delete));
		}
		return delete;
	}

	/**
	 * 根据SquidFlowId获得SquidFlow对象
	 * <p>
	 * 作用描述： 根据SquidFlowID查询
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 * 
	 * @param squidFlowId
	 * @param out
	 * @return
	 */
	public SquidFlow getSquidFlow(int squidFlowId, ReturnValue out) {
		logger.debug(String.format("getSquidFlow-squidFlowId=%s", squidFlowId));
		SquidFlow squidFlow = null;
		// 封装查询条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		WhereCondition condition = new WhereCondition();
		condition.setAttributeName("ID");
		condition.setDataType(DataStatusEnum.INT);
		condition.setMatchType(MatchTypeEnum.EQ);
		condition.setValue(squidFlowId);
		whereCondList.add(condition);
		// 查询结果
		Map<String, Object> columnsValue = adapter.queryRecord(null, SystemTableEnum.DS_SQUID_FLOW.toString(),
				whereCondList, out);
		if (columnsValue != null) {
			// 将数据库查询结果转换为SquidFlow实体
			squidFlow = this.getSquidFlow(columnsValue);
			out.setMessageCode(MessageCode.SUCCESS);
		} else {
			// 无数据返回
			out.setMessageCode(MessageCode.NODATA);
			logger.debug(String.format("SquidFlowInfo not found by squidFlowId(%s)...", squidFlowId));
			return null;
		}
		logger.debug(String.format("getSquidFlow-return", squidFlow));
		return squidFlow;
	}

	/**
	 * 根据key获得SquidFlowID
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 * 
	 * @param key
	 * @param out
	 * @return
	 */
	public int getSquidFlowId(String key, ReturnValue out) {
		logger.debug(String.format("getSquidFlowId-guid=%s", key));
		// 查询列数据
		List<String> columnList = new ArrayList<String>();
		columnList.add("ID");
		// 封装查询条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		WhereCondition condition = new WhereCondition();
		condition.setAttributeName("KEY");
		condition.setMatchType(MatchTypeEnum.EQ);
		if(key!=null){
			key ="'"+key+"'";
		}
		condition.setValue(key);
		whereCondList.add(condition);
		// 定义集合接收
		Map<String, Object> columnsValue = adapter.queryRecord(columnList, SystemTableEnum.DS_SQUID_FLOW.toString(),
				whereCondList, out);
		if (columnsValue == null) {
			out.setMessageCode(MessageCode.NODATA);
			return 0;
		}
		int id = Integer.parseInt(columnsValue.get("ID").toString());
		logger.debug(String.format("getSquidFlowId-guid=%s;result=%s", key, id));
		return id;
	}

	/**
	 * 根据唯一标识查询SquidFlow信息
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 * 
	 * @param guid
	 * @param out
	 * @return
	 */
	public SquidFlow getSquidFlow(String guid, ReturnValue out) {
		logger.debug(String.format("getSquidFlow-guid=%s", guid));
		SquidFlow squidFlow = null;
		// 封装查询条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		WhereCondition condition = new WhereCondition();
		condition.setAttributeName("GUID");
		condition.setDataType(DataStatusEnum.STRING);
		condition.setMatchType(MatchTypeEnum.EQ);
		if(guid!=null){
			guid ="'"+guid+"'";
		}
		condition.setValue(guid);
		whereCondList.add(condition);
		Map<String, Object> columnsValue = adapter.queryRecord(null, SystemTableEnum.DS_SQUID_FLOW.toString(),
				whereCondList, out);
		if (columnsValue == null || columnsValue.size() == 0) {
			// 无数据返回
			out.setMessageCode(MessageCode.NODATA);
			logger.debug(String.format("SquidFlowInfo not found by guid(%s)...", guid));
			return null;
		}
		// 将数据库查询结果转换为SquidFlow实体
		squidFlow = this.getSquidFlow(columnsValue);
		logger.debug(String.format("getSquidFlow-guid=%s;result=%s", guid, squidFlow));
		return squidFlow;

	}

	/**
	 * 根据ProjectId查询SquidFlow对象集合
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 * 
	 * @param projectId
	 * @param out 异常处理
	 * @return
	 */
	public List<SquidFlow> getSquidFlows(int projectId, ReturnValue out,IRelationalDataManager adapter) {
		logger.debug(String.format("getSquidFlows-projectId", projectId));
		List<SquidFlow> squidFlowInfosArray=new ArrayList<SquidFlow>();
		Map<String, Object> params = new HashMap<String, Object>(); // 查询参数
		params.put("project_id", projectId);
		try {
			squidFlowInfosArray=adapter.query2List2(true, params, SquidFlow.class);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
			logger.error("查询所有squidflowy异常");
		}/*finally
		{
			newAdapter.closeSession();
		}*/
		/*// 封装查询条件
		SquidFlow[] squidFlowInfosArray = new SquidFlow[0];
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		WhereCondition condition = new WhereCondition();
		condition.setAttributeName("PROJECT_ID");
		condition.setDataType(DataStatusEnum.INT);
		condition.setMatchType(MatchTypeEnum.EQ);
		condition.setValue(projectId);
		whereCondList.add(condition);
		// 定义集合接收查询结果
		List<Map<String, Object>> columnsValue = adapter.query(null, SystemTableEnum.DS_SQUID_FLOW.toString(),
				whereCondList, out);
		if (columnsValue == null || columnsValue.size() == 0) {
			out.setMessageCode(MessageCode.NODATA);
			return squidFlowInfosArray;
		}
		squidFlowInfosArray = new SquidFlow[columnsValue.size()];// 定义数组接收
		// 将数据库查询结果转换为SquidFlow实体
		int i = 0;
		for (Map<String, Object> column : columnsValue) {
			squidFlowInfosArray[i] = this.getSquidFlow(column);// 接收数据
			i++;
		}*/
		return squidFlowInfosArray;
	}

	/**
	 * 将查询结果Map集合的数据封装到SquidFLow对象中
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
	private SquidFlow getSquidFlow(Map<String, Object> columnsValue) {
		SquidFlow squidFlow = null;
		int id = (Integer) columnsValue.get("ID");
		squidFlow = new SquidFlow();
		// id
		squidFlow.setId(id);
		// 名称
		squidFlow.setName(columnsValue.get("NAME") == null ? null : columnsValue.get("NAME").toString()+"_"+columnsValue.get("ID"));
		// 创建时间
		if (columnsValue.get("CREATION_DATE") != null) {
			squidFlow.setCreation_date(columnsValue.get("CREATION_DATE").toString());
		}

		// 修改时间
		if (columnsValue.get("MODIFICATION_DATE") != null) {
			squidFlow.setModification_date(columnsValue.get("MODIFICATION_DATE").toString());
		}
		// 创建人
		squidFlow.setCreator(columnsValue.get("CREATOR") == null ? null : columnsValue.get("CREATOR").toString());

		// 项目ID
		if (columnsValue.get("PROJECT_ID") != null) {
			squidFlow.setProject_id(Integer.parseInt(columnsValue.get("PROJECT_ID").toString()));
		}

		// 唯一标识
		squidFlow.setKey(columnsValue.get("KEY").toString());

		// 描述
		squidFlow.setDescription(columnsValue.get("DESCRIPTION") == null ? null : "'"+columnsValue.get("DESCRIPTION").toString()+"'");

		//类型
		squidFlow.setType(DSObjectType.SQUID_FLOW.value());
		return squidFlow;
	}

	
}
