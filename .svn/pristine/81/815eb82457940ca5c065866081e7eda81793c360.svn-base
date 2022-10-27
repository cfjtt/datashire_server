package com.eurlanda.datashire.sprint7.plug;

import com.eurlanda.datashire.adapter.IDBAdapter;
import com.eurlanda.datashire.entity.SquidJoin;
import com.eurlanda.datashire.entity.operation.DataEntity;
import com.eurlanda.datashire.entity.operation.WhereCondition;
import com.eurlanda.datashire.enumeration.DataStatusEnum;
import com.eurlanda.datashire.enumeration.MatchTypeEnum;
import com.eurlanda.datashire.enumeration.SystemTableEnum;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 
 * Join的数据层操作类，例如增删改查
 * @version 1.0
 * @author lei.bin
 * @created 2013-10-21
 */
public class JoinPlug   {
	/**
	 * 对JoinPlug记录日志
	 */
	static Logger logger = Logger.getLogger(JoinPlug.class);

	/**
	 * 获取数据库连接池对象
	 */
	protected IDBAdapter adapter;

	public JoinPlug(IDBAdapter adapter) {
		this.adapter = adapter;
	}

	/**
	 * 根据ID删除join
	 * <p>
	 * 作用描述：根据ID删除join
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param id id编号
	 *@param out 结果返回代码
	 *@return boolean
	 */
	public boolean deleteJoin(int id, ReturnValue out) {
		logger.debug(String.format("deleteJoin-id=%s", id));
		boolean delete = false;
		// 删除Join信息
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		WhereCondition condition = new WhereCondition();
		condition.setAttributeName("ID");
		condition.setDataType(DataStatusEnum.INT);
		condition.setMatchType(MatchTypeEnum.EQ);
		condition.setValue(id);
		whereCondList.add(condition);
		// 执行删除
		delete = adapter.delete(SystemTableEnum.DS_JOIN.toString(), whereCondList, out);
		//adapter.commitAdapter();
		return delete;
	}
	
	
	/**
	 * 根据前端传送的信息对join进行更新
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param join 
	 *@param out  
	 *@return boolean
	 */
	public boolean updateJoin(SquidJoin join, ReturnValue out) {
		boolean update = false;
		// 将ESquidLink转换为数据包集合
		List<DataEntity> paramList = new ArrayList<DataEntity>();
		this.getParamValue(join, paramList);
		// 封装条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		WhereCondition condition = new WhereCondition();
		condition.setAttributeName("ID");
		condition.setDataType(DataStatusEnum.INT);
		condition.setMatchType(MatchTypeEnum.EQ);
		condition.setValue(join.getId());
		whereCondList.add(condition);
		// 执行更新操作
		update = adapter.update(SystemTableEnum.DS_JOIN.toString(), paramList, whereCondList, out);
		adapter.commitAdapter();
		return update;
	}

	/**
	 * 根据key,查询join表的记录
	 * @param key 
	 * @param out 
	 * @return Join
	 */
	public SquidJoin getJoinByKey(String key, ReturnValue out) {
		SquidJoin join = new SquidJoin();
		try {
			// 封装查询条件
			List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
			WhereCondition condition = new WhereCondition();
			condition.setAttributeName("KEY");
			condition.setDataType(DataStatusEnum.STRING);
			condition.setMatchType(MatchTypeEnum.EQ);
			if (key != null) {
				key = "'" + key + "'";
			}
			condition.setValue(key);
			whereCondList.add(condition);
			Map<String, Object> columnsValue = adapter.queryRecord(null,
					SystemTableEnum.DS_JOIN.toString(), whereCondList, out);
			if (columnsValue == null) {
				// 无数据返回
				out.setMessageCode(MessageCode.NODATA);
				return null;
			}
			join=this.getColumn(columnsValue);
		} catch (Exception e) {
			out.setMessageCode(MessageCode.ERR_GETJOINBYID);
		} 
		return join;
	} 
	
	/**
	 * 根据Key获得流水号ID
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param key
	 *@param out
	 *@return
	 */
	public int getJoinIdByKey(String key, ReturnValue out) {
		int id = 0;		
		try {
			// 封装查询条件
			List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
			WhereCondition condition = new WhereCondition();
			condition.setAttributeName("KEY");
			condition.setDataType(DataStatusEnum.STRING);
			condition.setMatchType(MatchTypeEnum.EQ);
			if (key != null) {
				key = "'" + key + "'";
			}
			condition.setValue(key);
			whereCondList.add(condition);
			Map<String, Object> columnsValue = adapter.queryRecord(null,
					SystemTableEnum.DS_JOIN.toString(), whereCondList, out);
			if (columnsValue == null) {
				// 无数据返回
				out.setMessageCode(MessageCode.NODATA);
				return id;
			}
			id= columnsValue.get("ID")== null?0:Integer.parseInt(columnsValue.get("ID").toString());
		} catch (Exception e) {
			out.setMessageCode(MessageCode.ERR_GETJOINBYID);
		} 
		
		return id;
	} 
	/**
	 * 根据id,查询join表的记录
	 * @param key 
	 * @param out 
	 * @return Join
	 */
	public SquidJoin getJoinById(int id, ReturnValue out) {
		logger.debug("根据id查询squidjoin表的记录");
		SquidJoin join = new SquidJoin();
		try {
			// 封装查询条件
			List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
			WhereCondition condition = new WhereCondition();
			condition.setAttributeName("ID");
			condition.setDataType(DataStatusEnum.INT);
			condition.setMatchType(MatchTypeEnum.EQ);
			condition.setValue(id);
			whereCondList.add(condition);
			Map<String, Object> columnsValue = adapter.queryRecord(null,
					SystemTableEnum.DS_JOIN.toString(), whereCondList, out);
			if (columnsValue == null) {
				// 无数据返回
				out.setMessageCode(MessageCode.NODATA);
				return null;
			}
			join=this.getColumn(columnsValue);
		} catch (Exception e) {
			out.setMessageCode(MessageCode.ERR_GETJOINBYID);
		}
		return join;
	} 
	/**
	 * 根据输入的join对象，来创建join
	 * @param join 
	 * @param out 
	 * @return boolean
	 */
	public boolean createJoin(SquidJoin join,ReturnValue out)
	{
		boolean create=false;
		List<DataEntity> paramValue = new ArrayList<DataEntity>();
		this.getParamValue(join, paramValue);
		create=adapter.insert(SystemTableEnum.DS_JOIN.toString(), paramValue, out);
		//adapter.commitAdapter();
		return create;
	}
    /**
     * 根据前端传入的值，来封装插入表对象值
     * @param join 
     * @param paramValue 
     */
	private void getParamValue(SquidJoin join, List<DataEntity> paramValue) {
		DataEntity dataEntity = null;
		// key
		String key =  join.getKey();
		if(key!=null){
			key = "'"+key+"'";
		}
		dataEntity = new DataEntity("KEY", DataStatusEnum.STRING,key);
		paramValue.add(dataEntity);

		// target_squid_id 
		int  target_squid_id =  join.getTarget_squid_id();
		dataEntity = new DataEntity("TARGET_SQUID_ID", DataStatusEnum.INT, target_squid_id);
		paramValue.add(dataEntity);

		// joined_squid_id
		int joined_squid_id =  join.getJoined_squid_id();
		dataEntity = new DataEntity("JOINED_SQUID_ID", DataStatusEnum.INT, joined_squid_id);
		paramValue.add(dataEntity);

		// prior_join_id
		int  prior_join_id =  join.getPrior_join_id();
		dataEntity = new DataEntity("PRIOR_JOIN_ID", DataStatusEnum.INT, prior_join_id);
		paramValue.add(dataEntity);

		// join_type_id
		int join_type_id=join.getJoinType();
		dataEntity = new DataEntity("JOIN_TYPE_ID", DataStatusEnum.INT, join_type_id);
		paramValue.add(dataEntity);
		// join_condition
		String join_condition = join.getJoin_Condition();
		if(join_condition!=null){
			join_condition = "'"+join_condition+"'";
		}
		dataEntity = new DataEntity("JOIN_CONDITION", DataStatusEnum.STRING, join_condition);
		paramValue.add(dataEntity);
	}
	/**
	 * 根据前端传入的join对象，来获取join集合
	 * @param id 
	 * @param out 
	 * @return List<Join>
	 */
	public List<SquidJoin> queryJoinsById(SquidJoin join, ReturnValue out) {
		List<SquidJoin> joins = new ArrayList<SquidJoin>();
		// 封装查询条件
		int id=join.getId();
		int targetSquidId=join.getTarget_squid_id();
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
//		WhereCondition condition = new WhereCondition();
//		condition.setAttributeName("TARGET_SQUID_ID");
//		condition.setDataType(DataStatusEnum.INT);
//		condition.setMatchType(MatchTypeEnum.EQ);
//		condition.setValue(targetSquidId);
//		whereCondList.add(condition);
		if(id!=0)
		{
			WhereCondition conditionTwo = new WhereCondition();
			conditionTwo.setAttributeName("ID");
			conditionTwo.setDataType(DataStatusEnum.INT);
			conditionTwo.setMatchType(MatchTypeEnum.EQ);
			conditionTwo.setValue(id);
			whereCondList.add(conditionTwo);
		}
		List<Map<String, Object>> columnsValue = adapter.query(null, SystemTableEnum.DS_JOIN.toString(),
				whereCondList, out);
		if (columnsValue == null || columnsValue.size() == 0) {
			out.setMessageCode(MessageCode.NODATA);
			return joins;
		}
		// 将数据库查询结果转换为Column实体
		for (Map<String, Object> joinColumn : columnsValue) {
			joins.add(this.getColumn(joinColumn));// 接收数据
		}
		return joins;
	}
	/**
	 * 根据查询到的值，封装join对象
	 * @param columnsValue 字段名
	 * @return Join
	 * @author lei.bin
	 */
	private SquidJoin getColumn(Map<String, Object> columnsValue) {
		SquidJoin join = new SquidJoin();
		// ID
		join.setId(columnsValue.get("ID") == null ? 0 : Integer.parseInt(String
				.valueOf(columnsValue.get("ID"))));
		// KEY
		join.setKey(columnsValue.get("KEY") == null ? null : String
				.valueOf(columnsValue.get("KEY")));
		// TARGET_SQUID_ID
		join.setTarget_squid_id(columnsValue.get("TARGET_SQUID_ID") == null ? 0
				: Integer.parseInt(String.valueOf(columnsValue
						.get("TARGET_SQUID_ID"))));
		// JOINED_SQUID_ID
		join.setJoined_squid_id(columnsValue.get("JOINED_SQUID_ID") == null ? 0
				: Integer.parseInt(String.valueOf(columnsValue
						.get("JOINED_SQUID_ID"))));
		// PRIOR_JOIN_ID
		join.setPrior_join_id(columnsValue.get("PRIOR_JOIN_ID") == null ? 0
				: Integer.parseInt(String.valueOf(columnsValue
						.get("PRIOR_JOIN_ID"))));
		// JOIN_TYPE_ID
		join.setJoinType(columnsValue.get("JOIN_TYPE_ID") == null ? 0
				: Integer.parseInt(String.valueOf(columnsValue
						.get("JOIN_TYPE_ID"))));
		// JOIN_CONDITION
		join.setJoin_Condition(columnsValue.get("JOIN_CONDITION") == null ? null
				: String.valueOf(columnsValue.get("JOIN_CONDITION")));
		return join;
	}
	
	/**
	 * 根据TargetSquidID获得最大PriorJoinId
	 * <p>
	 * 作用描述：
	 * 查询出最有级别的JoinID
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param priorJoinId
	 *@param targetSquidId
	 *@param out
	 *@author chunhua.zhao
	 *@return
	 */
	public int getJoinByPriorJoinId(int targetSquidId,ReturnValue out){
		logger.debug(String.format("getJoinByPriorJoinId-targetSquidId= %s;",targetSquidId));
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		WhereCondition condition = new WhereCondition();
		condition.setAttributeName("TARGET_SQUID_ID");
		condition.setDataType(DataStatusEnum.INT);
		condition.setMatchType(MatchTypeEnum.EQ);
		condition.setValue(targetSquidId);
		whereCondList.add(condition);
		List<String> columns = new ArrayList<String>();
		columns.add("max(PRIOR_JOIN_ID) as prior");
		Map<String, Object> map = adapter.queryRecord(columns, SystemTableEnum.DS_JOIN.toString(), whereCondList, out);
		int priorId = map.get("PRIOR").toString()==null?0:Integer.parseInt(map.get("PRIOR").toString());
		return priorId +1;
		
	}
	
	/**
	 * 根据2个squid的id 查询出对应的join对象
	 * @param targetSquidId
	 * @param joinSquidId
	 * @param out
	 * @return
	 */
	public SquidJoin getJoinByParms(int targetSquidId, int joinSquidId,ReturnValue out){
		logger.debug(String.format("getJoinByPriorJoinId-targetSquidId= %s;",targetSquidId));
		SquidJoin join = new SquidJoin();
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		WhereCondition condition = new WhereCondition();
		condition.setAttributeName("TARGET_SQUID_ID");
		condition.setDataType(DataStatusEnum.INT);
		condition.setMatchType(MatchTypeEnum.EQ);
		condition.setValue(targetSquidId);
		whereCondList.add(condition);
		
		WhereCondition condition2 = new WhereCondition();
		condition2.setAttributeName("JOINED_SQUID_ID");
		condition2.setDataType(DataStatusEnum.INT);
		condition2.setMatchType(MatchTypeEnum.EQ);
		condition2.setValue(joinSquidId);
		whereCondList.add(condition2);
		List<String> columns = new ArrayList<String>();
		columns.add("max(PRIOR_JOIN_ID) as prior");
		Map<String, Object> map = adapter.queryRecord(columns, SystemTableEnum.DS_JOIN.toString(), whereCondList, out);
		//int priorId = map.get("PRIOR").toString()==null?0:Integer.parseInt(map.get("PRIOR").toString());
		if (map == null) {
			// 无数据返回
			out.setMessageCode(MessageCode.NODATA);
			return null;
		}
		join = this.getColumn(map);
		return join;
	}
	
	/**
	 * 新增Join集合
	 * <p>
	 * 作用描述：
	 * 根据Join集合对象新增数据
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param joins join集合对象
	 *@param out 异常处理
	 *@author chunhua.zhao
	 *@return
	 */
	public boolean createJoin(List<SquidJoin> joins,ReturnValue out){
		logger.debug(String.format("createJoin-joins= %s;",joins));
		boolean create=false;
		List<List<DataEntity>> paramList = new ArrayList<List<DataEntity>>();
		for (SquidJoin join : joins) {
			 List<DataEntity>  paramValue = new ArrayList<DataEntity>();
			this.getParamValue(join, paramValue);
			paramList.add(paramValue);
		}
		create = adapter.createBatch(SystemTableEnum.DS_JOIN.toString(), paramList, out);
		return create;	
	}
}
