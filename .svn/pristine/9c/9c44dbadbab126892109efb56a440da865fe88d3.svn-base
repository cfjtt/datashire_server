package com.eurlanda.datashire.sprint7.plug;

import com.eurlanda.datashire.adapter.IDBAdapter;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.entity.SquidJoin;
import com.eurlanda.datashire.entity.SquidLink;
import com.eurlanda.datashire.entity.operation.DataEntity;
import com.eurlanda.datashire.entity.operation.WhereCondition;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.enumeration.DataStatusEnum;
import com.eurlanda.datashire.enumeration.MatchTypeEnum;
import com.eurlanda.datashire.enumeration.SystemTableEnum;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import com.eurlanda.datashire.utility.StringUtils;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * SquidLink处理类
 * SquidLink对象业务处理类 ：SquidLink新增业务；SquidLink根据ID修改业务；SquidLink根据ID得到相应的SquidLink对象业务；SquidLink根据key获得ID业务
 * 
 * <p>
 * Title :
 * </p>
 * <p>
 * Description:
 * </p>
 * <p>
 * Author :Cheryl 2013-8-23
 * </p>
 * <p>
 * update :Cheryl2013-8-23
 * </p>
 * <p>
 * Department : JAVA后端研发部
 * </p>
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司 </p>
 */
public class SquidLinkPlug extends Support {
	static Logger logger = Logger.getLogger(SquidLinkPlug.class);// 记录日志
	static IRelationalDataManager adapter2;
	public SquidLinkPlug(IDBAdapter adapter){
		super(adapter);
	}
	public SquidLinkPlug(IRelationalDataManager adapter){
		adapter2 = adapter;
	}
	public SquidLinkPlug(){
		
	}

	/**
	 * 创建SquidLink对象集合
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param squidLinks squidLink集合对象
	 *@param out 异常处理
	 *@return
	 */
	public boolean createSquidLinks(List<SquidLink> squidLinks,ReturnValue out){
		logger.debug(String.format("createSquidLinks-squidLinkList.size()=%s", squidLinks.size()));
		boolean create = false;
		try {
			//封装批量新增数据集
			List<List<DataEntity>>  paramList = new ArrayList<List<DataEntity>>();
			//调用转换类
			GetParam getParam = new GetParam();
			for (SquidLink squidLink : squidLinks) {
				List<DataEntity> dataEntitys =  new ArrayList<DataEntity>();
				getParam.getSquidLink(squidLink, dataEntitys);
				paramList.add(dataEntitys);
			}
			create = adapter.createBatch(SystemTableEnum.DS_SQUID_LINK.toString(), paramList, out);
		} catch (Exception e) {
			out.setMessageCode(MessageCode.ERR_ARRAYS);
		}
		logger.debug(String.format("createSquidLinks-result=%s", create));
		return create;
		
	}
	
	/**
	 * 创建SquidLink对象
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param squidLinks squidLink集合对象
	 *@param out 异常处理
	 *@return
	 */
	public boolean createSquidLink(SquidLink squidLink,ReturnValue out){
		logger.debug(String.format("createSquidLink-SquidLink=%s", squidLink.toString()));
		boolean create = false;
		//封装批量新增数据集
		//调用转换类
		GetParam getParam = new GetParam();
		List<DataEntity> dataEntitys =  new ArrayList<DataEntity>();
		getParam.getSquidLink(squidLink, dataEntitys);
		create = adapter.insert(SystemTableEnum.DS_SQUID_LINK.toString(), dataEntitys, out);
		logger.debug(String.format("createSquidLink-result=%s",create));
		return create;
		
	}
	/**
	 * 根据SquidID获得SquidLink对象集合
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 * 
	 * @param squidId squidID
	 * @param out 异常处理
	 * @return
	 */
	public SquidLink[] querySquidLinks(int squidId, ReturnValue out) {
		logger.debug(String.format("querySquidLinks-squidId=%s", squidId));
		// 封装查询条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		WhereCondition condition = new WhereCondition();
		condition.setAttributeName("TO_SQUID_ID");
		condition.setDataType(DataStatusEnum.STRING);
		condition.setMatchType(MatchTypeEnum.EQ);
		condition.setValue(squidId);
		// 追加
		whereCondList.add(condition);
		// 接收数据
		List<Map<String, Object>> columnsValue = adapter.query(null, SystemTableEnum.DS_SQUID_LINK.toString(),
				whereCondList, out);
		if (columnsValue == null || columnsValue.size() == 0) {
			// 异常处理
			out.setMessageCode(MessageCode.NODATA);
			return null;
		}
		SquidLink[] squidLinks = new SquidLink[columnsValue.size()];// 定义数组接收
		// 将数据库查询结果转换为SquidFlow实体
		int i = 0;
		for (Map<String, Object> column : columnsValue) {
			squidLinks[i] = this.getSquidLink(column);// 接收数据
			i++;
		}
		out.setMessageCode(MessageCode.SUCCESS);
		logger.debug(String.format("querySquidLinks-squidId=%s,result-size=%s", squidId, squidLinks.length));
		return squidLinks;
	}

	/**
	 * 根据SquidFlowID查询SquidLink
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 * 
	 * @param squidFlowId  squidFlowID
	 * @param out 异常处理
	 * @return
	 */
	public SquidLink[] querySquidLinksBySquidFlowId(int squidFlowId, ReturnValue out) {
		logger.debug(String.format("querySquidLinks-squidId=%s", squidFlowId));
		// 封装查询条件
		List<WhereCondition> conditions = new ArrayList<WhereCondition>();
		WhereCondition condition = new WhereCondition();
		condition.setAttributeName("SQUID_FLOW_ID");
		condition.setDataType(DataStatusEnum.INT);
		condition.setMatchType(MatchTypeEnum.EQ);
		condition.setValue(squidFlowId);
		// 追加
		conditions.add(condition);
		// 接收查询结果
		List<Map<String, Object>> columnsValue = adapter.query(null, SystemTableEnum.DS_SQUID_LINK.toString(),
				conditions, out);
		if (columnsValue == null || columnsValue.size() == 0) {
			// 无数据返回
			out.setMessageCode(MessageCode.NODATA);
			return null;
		}
		SquidLink[] squidLinks = new SquidLink[columnsValue.size()];// 定义数组接收
		// 将数据库查询结果转换为SquidFlow实体
		int i = 0;
		for (Map<String, Object> column : columnsValue) {
			squidLinks[i] = this.getSquidLink(column);// 接收数据
			i++;
		}
		logger.debug(String.format("querySquidLinks-squidId=%s,result-size=%s", squidFlowId, squidLinks.length));
		return squidLinks;
	}

	/**
	 * 根据SquidID删除
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param squidId squidId
	 *@param out 异常处理
	 *@return
	 */
	public boolean deletSquidLinks(int squidId, ReturnValue out) {
		logger.debug(String.format("deletSquidLinks-squidId=%s", squidId));
		boolean delete = false;

		// 封装条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		// 条件
		WhereCondition condition = new WhereCondition();
		condition.setAttributeName("TO_SQUID_ID");
		condition.setDataType(DataStatusEnum.INT);
		condition.setMatchType(MatchTypeEnum.EQ);
		condition.setValue(squidId);

		whereCondList.add(condition);
		// 执行删除
		delete = adapter.delete(SystemTableEnum.DS_SQUID_LINK.toString(), whereCondList, out);
		logger.debug(String.format("deletSquidLinks-squidId=%s,result=%s", squidId, delete));
		return delete;
	}
	
	
	
	
	/**
	 * 根据唯一标识符查询SquidLink
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
	public SquidLink getSquidLink(String key, ReturnValue out) {
		logger.debug(String.format("getTransformation-squidFlowId=%s", key));
		SquidLink squidLink = null;
		// 封装查询条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		WhereCondition condition = new WhereCondition();
		condition.setAttributeName("KEY");
		condition.setDataType(DataStatusEnum.STRING);
		condition.setMatchType(MatchTypeEnum.EQ);
		if(key!=null){
			key ="'"+key+"'";
		}
		condition.setValue(key);
		whereCondList.add(condition);
		// 定义集合接收数据
		Map<String, Object> columnsValue = adapter.queryRecord(null, SystemTableEnum.DS_SQUID_LINK.toString(),
				whereCondList, out);
		if (columnsValue == null) {
			out.setMessageCode(MessageCode.NODATA);
			return null;
		}
		// 将数据库查询结果转换为SquidFlow实体
		squidLink = this.getSquidLink(columnsValue);
		out.setMessageCode(MessageCode.SUCCESS);
		return squidLink;
	}
	
	/**
	 * 根据Key获得流水号ID
	 * <p>
	 * 作用描述：
	 * 调用方法前确定Key的值不为空
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param key
	 *@param out
	 *@return
	 */
	public int getSquidLinkId(String key, ReturnValue out) {
		logger.debug(String.format("getTransformation-squidFlowId=%s", key));
		int id = 0;
		// 封装查询条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		WhereCondition condition = new WhereCondition();
		condition.setAttributeName("KEY");
		condition.setDataType(DataStatusEnum.STRING);
		condition.setMatchType(MatchTypeEnum.EQ);
		if(key!=null){
			key ="'"+key+"'";
		}
		condition.setValue(key);
		whereCondList.add(condition);
		// 定义集合接收数据
		Map<String, Object> columnsValue = adapter.queryRecord(null, SystemTableEnum.DS_SQUID_LINK.toString(),
				whereCondList, out);
		if (columnsValue == null) {
			out.setMessageCode(MessageCode.NODATA);
			return id;
		}
		//获得流水号
		id = columnsValue.get("ID")== null ? null : Integer.parseInt(columnsValue.get("ID").toString());
		out.setMessageCode(MessageCode.SUCCESS);
		return id;
	}

	/**
	 * 根据ID获得SquidLink对象
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 * 
	 * @param id squidLinkID
	 * @param out 异常处理
	 * @return
	 */
	public SquidLink getSquidLink(int id, ReturnValue out) {
		logger.debug(String.format("getTransformation-squidFlowId=%s", id));
		SquidLink squidLink = null;

		// 封装查询条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		WhereCondition condition = new WhereCondition();
		condition.setAttributeName("ID");
		condition.setDataType(DataStatusEnum.INT);
		condition.setMatchType(MatchTypeEnum.EQ);
		condition.setValue(id);
		whereCondList.add(condition);
		// 定义集合接收
		Map<String, Object> columnsValue = adapter.queryRecord(null, SystemTableEnum.DS_SQUID_LINK.toString(),
				whereCondList, out);
		if (columnsValue == null) {
			out.setMessageCode(MessageCode.NODATA);
			return null;
		}
		// 将数据库查询结果转换为SquidFlow实体
		squidLink = this.getSquidLink(columnsValue);
		return squidLink;
	}

	/**
	 * 根据Squid获得来源
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 * 
	 * @param tosquid 目标squidID
	 * @param out 异常处理
	 * @return
	 */
	public int getSourceSquidId(int tosquid, ReturnValue out) {
		// 封装查询条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		WhereCondition condition = new WhereCondition();
		condition.setAttributeName("TO_SQUID_ID");
		condition.setValue(tosquid);
		condition.setMatchType(MatchTypeEnum.EQ);
		whereCondList.add(condition);
		// 定义集合接收
		Map<String, Object> columnsValue = adapter.queryRecord(null, SystemTableEnum.DS_SQUID_LINK.toString(),
				whereCondList, out);
//		if (columnsValue == null || !columnsValue.containsKey("FROM_SQUID_ID")) {
//			out.setMessageCode(MessageCode.NODATA);
//			return 0;
//		}
		//int id = Integer.parseInt(columnsValue.get("FROM_SQUID_ID").toString());
		//logger.debug(String.format("getSquidLinkId-tosquid=%s;result=%s", tosquid, id));
		return StringUtils.valueOfInt(columnsValue, "FROM_SQUID_ID");
	}
	
	/**
	 * 根据来源squid获得squidLinks
	 * <p>
	 * 作用描述：
	 * 根据SquidID获得SquidLink集合对象
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param sourceSquid squidID
	 *@param out 异常处理
	 *@return
	 */
	public List<SquidLink> getFromSquidLinks(int sourceSquid, ReturnValue out) {
		logger.debug(String.format("getSquidLinkId-squid=%s", sourceSquid));
		// 封装查询条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		WhereCondition condition = new WhereCondition();
		condition.setAttributeName("FROM_SQUID_ID");
		condition.setValue(sourceSquid);
		condition.setMatchType(MatchTypeEnum.EQ);
		whereCondList.add(condition);
		List<Map<String, Object>> list = adapter.query(null, SystemTableEnum.DS_SQUID_LINK.toString(),
				whereCondList, out);
		List<SquidLink> squidLinks = new ArrayList<SquidLink>();
		if(list!= null){
			for (Map<String, Object> map : list) {
				squidLinks.add(this.getSquidLink(map));
			}
		}
		return squidLinks;
	}
	/**
	 * 根据目标Squid获得SquidLinks
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param tosquid
	 *@param out
	 *@return
	 */
	public List<SquidLink> getToSquidLinks(int tosquid, ReturnValue out) {
		logger.debug(String.format("getSquidLinkId-tosquid=%s", tosquid));
		// 封装查询条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		WhereCondition condition = new WhereCondition();
		condition.setAttributeName("TO_SQUID_ID");
		condition.setValue(tosquid);
		condition.setMatchType(MatchTypeEnum.EQ);
		whereCondList.add(condition);
		List<Map<String, Object>> list = adapter.query(null, SystemTableEnum.DS_SQUID_LINK.toString(),
				whereCondList, out);
		List<SquidLink> squidLinks = new ArrayList<SquidLink>();
		if(list!= null){
			for (Map<String, Object> map : list) {
				squidLinks.add(this.getSquidLink(map));
			}
		}
		return squidLinks;
	}
	/**
	 * 将查询结果Map集合转换成SquidLink对象
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
	//TODO
	public  SquidLink getSquidLink(Map<String, Object> columnsValue) {
		SquidLink squidLink = null;
		int id = (Integer) columnsValue.get("ID");
		squidLink = new SquidLink();
		// ID
		squidLink.setId(id);
		// squidFlowID
		String squidFlowID = columnsValue.get("SQUID_FLOW_ID") == null ? null : columnsValue.get("SQUID_FLOW_ID")
				.toString();
		// 来自Suid
		String fSuid = columnsValue.get("FROM_SQUID_ID") == null ? null : columnsValue.get("FROM_SQUID_ID").toString();
		// 放到哪
		String toSuid = columnsValue.get("TO_SQUID_ID") == null ? null : columnsValue.get("TO_SQUID_ID").toString();
		if (squidFlowID != null) {
			squidLink.setSquid_flow_id(Integer.parseInt(squidFlowID));
		}
		if (fSuid != null) {
			squidLink.setFrom_squid_id(Integer.parseInt(fSuid));
		}
		if (toSuid != null) {
			squidLink.setTo_squid_id(Integer.parseInt(toSuid));
		}
		//end_x
		int end_x = columnsValue.get("END_X")== null?0:(Integer)columnsValue.get("END_X") ;
		squidLink.setEnd_x(end_x);
		//end_y
		int end_y = columnsValue.get("END_X")== null?0:(Integer)columnsValue.get("END_Y") ;
		squidLink.setEnd_y(end_y);
		//arrows_style
		int arrows_style = columnsValue.get("ARROWS_STYLE")== null?0:(Integer)columnsValue.get("ARROWS_STYLE") ;
		squidLink.setArrows_style(arrows_style);
		//endmiddle_x
		int endmiddle_x = columnsValue.get("ENDMIDDLE_X")== null?0:(Integer)columnsValue.get("ENDMIDDLE_X") ;
		squidLink.setEndmiddle_x(endmiddle_x);
		//endmiddle_y
		int endmiddle_y = columnsValue.get("ENDMIDDLE_Y")== null?0:(Integer)columnsValue.get("ENDMIDDLE_Y") ;
		squidLink.setEndmiddle_y(endmiddle_y);
		//start_x
		int start_x = columnsValue.get("START_X")== null?0:(Integer)columnsValue.get("START_X") ;
		squidLink.setStart_x(start_x);
		//start_y
		int start_y = columnsValue.get("START_Y")== null?0:(Integer)columnsValue.get("START_Y") ;
		squidLink.setStart_y(start_y);
		//startmiddle_x
		int startmiddle_x = columnsValue.get("STARTMIDDLE_X")== null?0:(Integer)columnsValue.get("STARTMIDDLE_X") ;
		squidLink.setStartmiddle_x(startmiddle_x);
		//startmiddle_y
		int startmiddle_y = columnsValue.get("STARTMIDDLE_Y")== null?0:(Integer)columnsValue.get("STARTMIDDLE_Y") ;
		int lineType = columnsValue.get("LINE_TYPE")== null?0:(Integer)columnsValue.get("LINE_TYPE") ;
		squidLink.setLine_type(lineType);
		String lineColor = columnsValue.get("LINE_COLOR")==null?"":columnsValue.get("LINE_COLOR").toString();
		squidLink.setLine_color(lineColor);
		squidLink.setStartmiddle_y(startmiddle_y);
		squidLink.setKey(columnsValue.get("KEY") == null ? null : columnsValue.get("KEY").toString());
		squidLink.setType(DSObjectType.SQUIDLINK.value());
		return squidLink;
	}

	/**
	 * <p>
	 * 作用描述：根据squidFlowId集合,查找SquidLink集合
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 * 
	 * @param squidID
	 * @param out
	 * @return
	 */
	public SquidLink[] getSquidFlowSquidLink(int squidFlowId, ReturnValue out) {
		logger.debug(String.format("getSquidFlowSquidLink_squidFlowId=%s", squidFlowId));
		// 封装条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		// 条件
		WhereCondition whereCondition = null;
		whereCondition = new WhereCondition();
		whereCondition.setAttributeName("SQUID_FLOW_ID");
		whereCondition.setMatchType(MatchTypeEnum.EQ);
		whereCondition.setValue(squidFlowId);
		whereCondList.add(whereCondition);
		// 查询列及结果
		List<String> columnList = new ArrayList<String>();
		List<Map<String, Object>> SquidLinkList = adapter.query(columnList, SystemTableEnum.DS_SQUID_LINK.toString(),
				whereCondList, out);
		if (SquidLinkList == null) {
			// 无数据返回
			out.setMessageCode(MessageCode.NODATA);
			logger.debug(String.format("squidLink not found by squidFlowId(%s)...", squidFlowId));
			return null;
		}
		logger.debug(String.format("getSquidFlowSquidLink size=%s", SquidLinkList.size()));
		// 将数据转换为SquidLink实体
		int i = 0;
		SquidLink[] squidLinkArray = new SquidLink[SquidLinkList.size()];// 定义数组接收
		for (Map<String, Object> column : SquidLinkList) {
			squidLinkArray[i] = this.getSquidLink(column);// 接收数据
			i++;
		}
		logger.debug(String.format("getSquidFlowSquidLink SquidLink[] size=%s", squidLinkArray.length));
		out.setMessageCode(MessageCode.SUCCESS);
		return squidLinkArray;
	}
	/**
	 * 对squidLink进行更新
	 * @param squidLink
	 * @param out
	 * @return
	 */
	public boolean updateSquidLink(SquidLink squidLink, ReturnValue out) {
		boolean update = false;
		// 将ESquidLink转换为数据包集合
		List<DataEntity> paramList = new ArrayList<DataEntity>();
		//this.getParamValue(squidLink, paramList);
		// 封装条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		WhereCondition condition = new WhereCondition();
		condition.setAttributeName("ID");
		condition.setDataType(DataStatusEnum.INT);
		condition.setMatchType(MatchTypeEnum.EQ);
		condition.setValue(squidLink.getId());
		whereCondList.add(condition);
		// 执行更新操作Y
		update = adapter.update(SystemTableEnum.DS_JOIN.toString(), paramList, whereCondList, out);
		adapter.commitAdapter();
		return update;
	}
	/**
	 * 根据TO_SQUID_ID查找squidLink类
	 * @param id
	 * @param out
	 * @return
	 */
	public SquidLink getSquidLinkById(int id, ReturnValue out) {
		SquidLink squidLink = null;
		// 封装查询条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		WhereCondition condition = new WhereCondition();
		condition.setAttributeName("TO_SQUID_ID");
		condition.setDataType(DataStatusEnum.INT);
		condition.setMatchType(MatchTypeEnum.EQ);
		condition.setValue(id);
		whereCondList.add(condition);
		// 定义集合接收数据
		Map<String, Object> columnsValue = adapter.queryRecord(null, SystemTableEnum.DS_SQUID_LINK.toString(),
				whereCondList, out);
		if (columnsValue == null) {
			out.setMessageCode(MessageCode.NODATA);
			return null;
		}
		// 将数据库查询结果转换为SquidFlow实体
		squidLink = this.getSquidLink(columnsValue);
		out.setMessageCode(MessageCode.SUCCESS);
		return squidLink;
	}
	/**
	 * 根据sql更新squidLink
	 * @param join
	 * @param out
	 * @return
	 */
	public int updateSquidLinkBySql(SquidJoin join, ReturnValue out) {
		int result = 0;
		// id
		int id = join.getId();
		// target_squid_id
		int target_squid_id = join.getTarget_squid_id();
		int joined_squid_id=join.getJoined_squid_id();
		String sql = "UPDATE DS_SQUID_LINK SET from_squid_id = "+target_squid_id+" where  " +
				"from_squid_id =(select  target_squid_id from ds_join where id="+id+")  " +
						"and to_squid_id="+joined_squid_id+"";
		result = adapter.executeSQL(sql, out);
		adapter.commitAdapter();
		return result;
	}
	/**
	 * 根据join对象查找SquidLink
	 * @param join
	 * @param out
	 * @return
	 */
	public SquidLink getSquidLinkByJoin(SquidJoin join, ReturnValue out) {
		logger.debug("根据join查找SquidLink");
		SquidLink squidLink = null;
		int target_squid_id=join.getTarget_squid_id();
	    int joined_squid_id=join.getJoined_squid_id();
		// 拼接TARGET_SQUID_ID
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		WhereCondition condition = new WhereCondition();
		condition.setAttributeName("FROM_SQUID_ID");
		condition.setDataType(DataStatusEnum.INT);
		condition.setMatchType(MatchTypeEnum.EQ);
		condition.setValue(target_squid_id);
		whereCondList.add(condition);
		//拼接
		WhereCondition conditionTwo = new WhereCondition();
		conditionTwo.setAttributeName("TO_SQUID_ID");
		conditionTwo.setDataType(DataStatusEnum.INT);
		conditionTwo.setMatchType(MatchTypeEnum.EQ);
		conditionTwo.setValue(joined_squid_id);
		whereCondList.add(conditionTwo);
		// 定义集合接收数据
		Map<String, Object> columnsValue = adapter.queryRecord(null, SystemTableEnum.DS_SQUID_LINK.toString(),
				whereCondList, out);
		if (columnsValue == null) {
			out.setMessageCode(MessageCode.NODATA);
			return null;
		}
		// 将数据库查询结果转换为SquidFlow实体
		squidLink = this.getSquidLink(columnsValue);
		out.setMessageCode(MessageCode.SUCCESS);
		return squidLink;
	}
}
