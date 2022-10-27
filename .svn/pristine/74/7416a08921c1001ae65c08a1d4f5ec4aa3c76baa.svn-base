package com.eurlanda.datashire.sprint7.plug;

import com.eurlanda.datashire.adapter.IDBAdapter;
import com.eurlanda.datashire.entity.TransformationLink;
import com.eurlanda.datashire.entity.operation.DataEntity;
import com.eurlanda.datashire.entity.operation.WhereCondition;
import com.eurlanda.datashire.enumeration.DSObjectType;
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
 * TransformationLink处理类
 * TransformationLink 对象业务处理类：TransformationLink的批量新增；根据ID的TransformationLink的批量修改；根据TransformationLinkID获得相应的TransformationLink对象；
 * <p>
 * Title :
 * </p>
 * <p>
 * Description:
 * </p>
 * <p>
 * Author :Cheryl2013-8-23
 * </p>
 * <p>
 * update :Cheryl2013-8-23
 * </p>
 * <p>
 * Department : JAVA后端研发部
 * </p>
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司 </p>
 */
public class TransformationLinkPlug extends Support {
	static Logger logger = Logger.getLogger(TransformationLinkPlug.class);// 记录日志

	public TransformationLinkPlug(IDBAdapter adapter){
		super(adapter);
	}
	
	/**
	 * 新增TransformationLink对象集合
	 * <p>
	 * 作用描述：
	 * 根据TransformationLink对象集合的对象批量新增数据
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param transformationLinks TransformationLink集合对象
	 *@param out 异常处理
	 *@return
	 */
	public boolean createTransformationLinks(List<TransformationLink> transformationLinks,ReturnValue out){
		logger.debug(String.format("createTransformationLinks-TransfromationLinkList.size()=%s", transformationLinks.size()));
		boolean create = false;
		try {
			//封装批量新增数据集
			List<List<DataEntity>>  paramList = new ArrayList<List<DataEntity>>();
			//调用转换类
			GetParam getParam = new GetParam();
			for (TransformationLink transformationLink : transformationLinks) {
				List<DataEntity> dataEntitys =  new ArrayList<DataEntity>();
				getParam.getTransformationLink(transformationLink, dataEntitys);
				paramList.add(dataEntitys);
			}
			create = adapter.createBatch(SystemTableEnum.DS_TRANSFORMATION_LINK.toString(), paramList, out);
			logger.debug(String.format("createTransformationLinks-result=%s",create));
		} catch (Exception e) {
			out.setMessageCode(MessageCode.ERR_ARRAYS);
		}
		return create; 
		
	}
	/**
	 * 新增单个
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param transformationLink transformationLink对象
	 *@param out 异常处理
	 *@return
	 */
	public boolean createTransformationLink(TransformationLink transformationLink,ReturnValue out){
		logger.debug(String.format("createTransformationLinks-TransfromationLinkList.size()=%s", transformationLink));
		boolean create = false;
		//封装批量新增数据集
		//调用转换类
		GetParam getParam = new GetParam();
			List<DataEntity> dataEntitys =  new ArrayList<DataEntity>();
			getParam.getTransformationLink(transformationLink, dataEntitys);
		return create = adapter.insert(SystemTableEnum.DS_TRANSFORMATION_LINK.toString(), dataEntitys, out);
		
	}
	
	/**
	 * 根据唯一标识删除转换流程
	 * 
	 * @param guid
	 * @param out
	 */
	public boolean deleteTransformationLink(String guid, ReturnValue out) {
		logger.debug(String.format("deleteTransformationLink-guid=%s", guid));
		boolean delete = false;
		// 封装条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		WhereCondition condition = new WhereCondition();
		condition.setAttributeName("KEY");
		condition.setDataType(DataStatusEnum.INT);
		condition.setMatchType(MatchTypeEnum.EQ);
		if(guid!=null){
			guid ="'"+guid+"'";
		}
		condition.setValue(guid);
		whereCondList.add(condition);
		// 执行删除
		delete = adapter.delete(SystemTableEnum.DS_TRANSFORMATION_LINK.toString(), whereCondList, out);
		logger.debug(String.format("deleteTransformationLink-guid=%s,result=%s", guid, delete));
		return delete;
	}
	
	/**
	 * 根据ＩＤ删除
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param id transformationLinkID
	 *@param out 异常处理
	 *@return
	 */
	public boolean deleteTransformationLink(int id, ReturnValue out) {
		logger.debug(String.format("deleteTransformationLink-id=%s", id));
		boolean delete = false;
		// 封装条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		WhereCondition condition = new WhereCondition();
		condition.setAttributeName("ID");
		condition.setDataType(DataStatusEnum.INT);
		condition.setMatchType(MatchTypeEnum.EQ);
		condition.setValue(id);
		whereCondList.add(condition);
		// 执行删除
		delete = adapter.delete(SystemTableEnum.DS_TRANSFORMATION_LINK.toString(), whereCondList, out);
		logger.debug(String.format("deleteTransformationLink-guid=%d,result=%s", id, delete));
		return delete;
	}


	/**
	 * 根据Guid查询TransformationLink信息
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 * 
	 * @param key
	 * @param out 异常处理
	 * @return
	 */
	public TransformationLink getTransformationLink(String key, ReturnValue out) {
		logger.debug(String.format("getTransformationLink-guid=%s", key));

		// 封装查询条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		WhereCondition condition = new WhereCondition();
		condition.setAttributeName("KEY");
		condition.setDataType(DataStatusEnum.INT);
		condition.setMatchType(MatchTypeEnum.EQ);
		key = "'"+key+"'";
		condition.setValue(key);
		whereCondList.add(condition);
		// 定义集合接收
		Map<String, Object> columnsValue = adapter.queryRecord(null, SystemTableEnum.DS_TRANSFORMATION_LINK.toString(),
				whereCondList, out);
		if (columnsValue == null) {
			out.setMessageCode(MessageCode.NODATA);
			return null;
		}
		// 将数据库查询结果转换为SquidFlow实体
		TransformationLink transformationLink = this.getTransformationLink(columnsValue);
		out.setMessageCode(MessageCode.SUCCESS);
		return transformationLink;

	}
	/**
	 * 根据key查询ID信息
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 * @param key
	 * @param out
	 * @return
	 */
	public int getTransformationLinkId(String key, ReturnValue out) {
		logger.debug(String.format("getTransformationLink-guid=%s", key));
		
		// 封装查询条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		WhereCondition condition = new WhereCondition();
		condition.setAttributeName("KEY");
		condition.setDataType(DataStatusEnum.INT);
		condition.setMatchType(MatchTypeEnum.EQ);
		key = "'"+key+"'";
		condition.setValue(key);
		whereCondList.add(condition);
		// 定义集合接收
		Map<String, Object> columnsValue = adapter.queryRecord(null, SystemTableEnum.DS_TRANSFORMATION_LINK.toString(),
				whereCondList, out);
		if (columnsValue == null) {
			out.setMessageCode(MessageCode.NODATA);
			return 0;
		}
		int id = columnsValue.get("ID")== null ? 0:Integer.parseInt(columnsValue.get("ID").toString());
		out.setMessageCode(MessageCode.SUCCESS);
		return id;
		
	}

	/**
	 * 根据来源TransformationＩＤ查询TransformationLink
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 * @param fromId 来源TransformationＩＤ
	 * @param out
	 * @return
	 */
	public TransformationLink[] getTransformationLinks(int fromId, ReturnValue out) {
		logger.debug(String.format("getTransformations_squidId=%s", fromId));
		// 封装查询条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		WhereCondition condition = new WhereCondition();
		condition.setAttributeName("FROM_TRANSFORMATION_ID");
		condition.setDataType(DataStatusEnum.INT);
		condition.setMatchType(MatchTypeEnum.EQ);
		condition.setValue(fromId);
		whereCondList.add(condition);
		List<Map<String, Object>> columnsValue = adapter.query(null, SystemTableEnum.DS_TRANSFORMATION_LINK.toString(),
				whereCondList, out);
		if (columnsValue == null || columnsValue.size() == 0) {
			out.setMessageCode(MessageCode.NODATA);
			return null;
		}
		TransformationLink[] transformationLinks = new TransformationLink[columnsValue.size()];// 定义数组接收
		// 将数据库查询结果转换为SquidFlow实体
		int i = 0;
		for (Map<String, Object> column : columnsValue) {
			transformationLinks[i] = this.getTransformationLink(column);// 接收数据
			i++;
		}
		logger.debug(String.format("getTransformationLinks_result-size()=%s", transformationLinks.length));
		return transformationLinks;
	}

	/**
	 * 根据TotransformationId查询TransformationLink
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 * 
	 * @param toId
	 * @param out
	 * @return
	 */
	public TransformationLink[] getTransformationLinksTo(int toTransformationId, ReturnValue out) {
		logger.debug(String.format("getTransformationLinksTo_stoTransformationId=%s", toTransformationId));
		// 封装查询条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		WhereCondition condition = new WhereCondition();
		condition.setAttributeName("TO_TRANSFORMATION_ID");
		condition.setDataType(DataStatusEnum.INT);
		condition.setMatchType(MatchTypeEnum.EQ);
		condition.setValue(toTransformationId);
		whereCondList.add(condition);
		// 定义集合接收
		List<Map<String, Object>> columnsValue = adapter.query(null, SystemTableEnum.DS_TRANSFORMATION_LINK.toString(),
				whereCondList, out);
		if (columnsValue == null || columnsValue.size() == 0) {
			out.setMessageCode(MessageCode.NODATA);
			return null;
		}
		TransformationLink[] transformationLinks = new TransformationLink[columnsValue.size()];// 定义数组接收
		// 将数据库查询结果转换为SquidFlow实体
		int i = 0;
		for (Map<String, Object> column : columnsValue) {
			transformationLinks[i] = this.getTransformationLink(column);// 接收数据
			i++;
		}
		logger.debug(String.format("getTransformationLinks_result-size()=%s", transformationLinks.length));
		return transformationLinks;
	}

	
	
	/**
	 * 将查询结果Map集合转换成TransformationLink对象
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 * @param columnsValue
	 * @return
	 */
	private TransformationLink getTransformationLink(Map<String, Object> columnsValue) {
		TransformationLink transformationLink = null;
		int id = (Integer) columnsValue.get("ID");
		transformationLink = new TransformationLink();

		// id
		transformationLink.setId(id);

		// 来自哪
		String forTransId = columnsValue.get("FROM_TRANSFORMATION_ID") == null ? null : columnsValue.get(
				"FROM_TRANSFORMATION_ID").toString();
		if (forTransId != null) {
			transformationLink.setFrom_transformation_id(Integer.parseInt(forTransId));
		}
		// 放到哪
		String toTransId = columnsValue.get("TO_TRANSFORMATION_ID") == null ? null : columnsValue.get(
				"TO_TRANSFORMATION_ID").toString();
		if (toTransId != null) {
			transformationLink.setTo_transformation_id(Integer.parseInt(toTransId));
		}
		// 命令
		String inOrder = columnsValue.get("IN_ORDER") == null ? null : columnsValue.get("IN_ORDER").toString();
		if (inOrder != null) {

			transformationLink.setIn_order(Integer.parseInt(inOrder));
		}
		// 唯一标识
		String guid = String.valueOf(columnsValue.get("KEY"));
		transformationLink.setKey(guid);
		transformationLink.setType(DSObjectType.TRANSFORMATIONLINK.value());
		return transformationLink;
	}

	/**
	 * 根据ID查询TransformationLink
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 * 
	 * @param id
	 * @param out
	 * @return
	 */
	public TransformationLink getTransformationLink(int id, ReturnValue out) {
		logger.debug(String.format("getTransformationLink-squidFlowId=%s", id));
		TransformationLink transformationLink = null;

		// 封装查询条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		WhereCondition condition = new WhereCondition();
		condition.setAttributeName("ID");
		condition.setDataType(DataStatusEnum.INT);
		condition.setMatchType(MatchTypeEnum.EQ);
		condition.setValue(id);
		whereCondList.add(condition);
		Map<String, Object> columnsValue = adapter.queryRecord(null, SystemTableEnum.DS_TRANSFORMATION_LINK.toString(),
				whereCondList, out);
		if (columnsValue == null) {
			// 无数据返回
			out.setMessageCode(MessageCode.NODATA);
			return null;
		}
		// 将数据库查询结果转换为SquidFlow实体
		transformationLink = this.getTransformationLink(columnsValue);
		logger.debug(String.format("getTransformationLink-result-id=%s", transformationLink.getId()));
		return transformationLink;
	}

	/**
	 * <p>
	 * 作用描述：根据squidFlowId集合,查找TransformationLink集合
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 * 
	 * @param squidID
	 * @param out
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public TransformationLink[] getSquidFlowTransformationLink(int squidFlowId, ReturnValue out) {
		logger.debug(String.format("getSquidFlowTransformationLink_squidFlowId=%s", squidFlowId));
		

		// 根据squidFlowId寻找TransformationLink
		String sqlStr = " SELECT  k.id,k.from_transformation_id,k.to_transformation_id,k.in_order,k.key"
				+ " FROM DS_SQUID s,DS_TRANSFORMATION t,DS_TRANSFORMATION_LINK k WHERE s.id=t.squid_id"
				+ " AND t.id=k.from_transformation_id AND s.squid_flow_id=" + squidFlowId;

		List<Map<String, Object>> listTransformationLink = adapter.query(sqlStr, out);
		if (listTransformationLink == null || listTransformationLink.size() == 0) {
			out.setMessageCode(MessageCode.NODATA);
			logger.debug(String.format("squidFlowId not found by TransformationLink(%s)...", squidFlowId));
			return null;
		}
		logger.debug(String.format("getSquidFlowTransformations size=%s", listTransformationLink.size()));
		// 将数据转换为TransformationLink实体
		int i = 0;
		TransformationLink[] transformationLinks = new TransformationLink[listTransformationLink.size()];// 定义数组接收
		for (Map<String, Object> column : listTransformationLink) {
			transformationLinks[i] = this.getTransformationLink(column);// 接收数据
			i++;
		}
		logger.debug(String.format("getSquidFlowTransformations TransformationLink[] size=%s",
				transformationLinks.length));
		out.setMessageCode(MessageCode.SUCCESS);
		return transformationLinks;
	}
}
