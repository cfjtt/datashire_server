package com.eurlanda.datashire.sprint7.plug;

import com.eurlanda.datashire.adapter.IDBAdapter;
import com.eurlanda.datashire.entity.Transformation;
import com.eurlanda.datashire.entity.operation.DataEntity;
import com.eurlanda.datashire.entity.operation.WhereCondition;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.enumeration.DataStatusEnum;
import com.eurlanda.datashire.enumeration.MatchTypeEnum;
import com.eurlanda.datashire.enumeration.SystemTableEnum;
import com.eurlanda.datashire.sprint7.service.squidflow.RepositoryServiceHelper;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 
 * Transformation处理类
 * Transformation对象业务处理类：Transformation对象的批量新增；根据ＩＤTransformation的批量修改；根据Ｉｄ获得相应的Transformation对象；根据ＳｑｕｉｄＩＤ获得相应的Transformation集合对象；
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
public class TransformationPlug extends Support {
	static Logger logger = Logger.getLogger(TransformationPlug.class);// 记录日志

	public TransformationPlug(IDBAdapter adapter){
		super(adapter);
	}
	
	/**
	 * 批量创建
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param transformations　transformation集合对象
	 *@param out　异常处理
	 *@return
	 */
	public boolean  createTransformations(List<Transformation> transformations,ReturnValue out){
		logger.debug(String.format("createTransformations-TransfromationList.size()=%s", transformations.size()));
		boolean create = false;
		//封装批量新增数据集
		List<List<DataEntity>>  paramList = new ArrayList<List<DataEntity>>();
		//调用转换类
		GetParam getParam = new GetParam();
		for (Transformation transformation : transformations) {
			List<DataEntity> dataEntitys =  new ArrayList<DataEntity>();
			getParam.getTransformation(transformation, dataEntitys);
			paramList.add(dataEntitys);
		}
		return create = adapter.createBatch(SystemTableEnum.DS_TRANSFORMATION.toString(), paramList, out);
	}
	/**
	 * 单个创建
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param transformation　transformation对象
	 *@param out　异常处理
	 *@return
	 */
	public boolean  createTransformation(Transformation transformation,ReturnValue out){
		logger.debug(String.format("createTransformations-TransfromationList=%s", transformation));
		boolean create = false;
		//封装批量新增数据集
		//调用转换类
		GetParam getParam = new GetParam();
			List<DataEntity> dataEntitys =  new ArrayList<DataEntity>();
			getParam.getTransformation(transformation, dataEntitys);
		return create = adapter.insert(SystemTableEnum.DS_TRANSFORMATION.toString(), dataEntitys, out);
	}

	/**
	 * 根据TransformationId删除Transformation对象
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 * 
	 * @param id
	 *            　transformationID
	 * @param out
	 *            异常处理
	 * @return
	 */
	public boolean deleteTransformation(int id, ReturnValue out, String token) {
		logger.debug(String.format("deleteTransformation-id=%s", id));
		boolean delete = true;
		// 根据transformation id 删除transformationlink
		String queryTranSql = "select id from DS_TRANSFORMATION_LINK where from_transformation_id="
				+ id + " or to_transformation_id=" + id + "";
		List<Map<String, Object>> tranIdList = adapter.query(queryTranSql, out);
		if(null!=tranIdList&&tranIdList.size()>0)
		{
			logger.debug("DS_TRANSFORMATION_LINK的长度=="+tranIdList.size());
			RepositoryServiceHelper helper = new RepositoryServiceHelper(token);
			helper.getAdapter().openSession();
			for (int i = 0; i < tranIdList.size(); i++) {
				delete =helper.deleteTransLink((Integer) tranIdList.get(i).get("ID"));
				if (!delete) {
					break;
				}
			}
		}
		if (delete) {// 操作成功
						// 封装条件
			List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
			WhereCondition condition = new WhereCondition();
			condition.setAttributeName("ID");
			condition.setDataType(DataStatusEnum.INT);
			condition.setMatchType(MatchTypeEnum.EQ);
			condition.setValue(id);
			whereCondList.add(condition);
			// 执行删除
			delete = adapter.delete(
					SystemTableEnum.DS_TRANSFORMATION.toString(),
					whereCondList, out);
			logger.debug(String.format(
					"deleteTransformation-guid=%s,result=%s", id, delete));
		}

		return delete;
	}

	/**
	 * 根据SquidID查询Transformation对象集合
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 * 
	 * @param squidID squidID
	 * @param out 异常处理
	 * @return
	 */
	public List<Transformation> getTransformations(int squidID, ReturnValue out) {
		logger.debug(String.format("getTransformations_squidId=%s", squidID));
		// 封装查询条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		WhereCondition condition = new WhereCondition();
		condition.setAttributeName("SQUID_ID");
		condition.setDataType(DataStatusEnum.INT);
		condition.setMatchType(MatchTypeEnum.EQ);
		condition.setValue(squidID);
		whereCondList.add(condition);
		List<Map<String, Object>> columnsValue = adapter.query(null, SystemTableEnum.DS_TRANSFORMATION.toString(),
				whereCondList, out);
		if (columnsValue == null || columnsValue.size() == 0) {
			out.setMessageCode(MessageCode.NODATA);
			return null;
		}
		List<Transformation> transformations = new ArrayList<Transformation>();// 定义数组接收
		// 将数据库查询结果转换为SquidFlow实体
		//GetParam getParam = new GetParam();
		for (Map<String, Object> column : columnsValue) {
			
			transformations.add(this.getTransformation(column));// 接收数据
		}
		logger.debug(String.format("getTransformationLinks_result-size()=%s", transformations.size()));
		return transformations;
	}

	/**
	 * 根据Key获得Id
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
	public int getTransformationId(String key, ReturnValue out) {
		logger.debug(String.format("getTransformationId-guid=%s", key));

		// 查询列数据
		List<String> columnList = new ArrayList<String>();
		columnList.add("ID");
		// 封装查询条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		WhereCondition condition = new WhereCondition();
		condition.setAttributeName("KEY");
		if(key!=null){
			key ="'"+key+"'";
		}
		condition.setValue(key);
		condition.setMatchType(MatchTypeEnum.EQ);
		whereCondList.add(condition);
		// 定义集合接收查询结果
		Map<String, Object> columnsValue = adapter.queryRecord(columnList,
				SystemTableEnum.DS_TRANSFORMATION.toString(), whereCondList, out);
		if (columnsValue == null) {
			// 没有数据
			out.setMessageCode(MessageCode.NODATA);
			return 0;
		}
		int id = Integer.parseInt(columnsValue.get("ID").toString());
		logger.debug(String.format("getTransformationId-guid=%s;result=%s", key, id));
		return id;
	}

	/**
	 * 根据唯一标识符查询Transformation对象
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
	public Transformation getTransformation(String key, ReturnValue out) {
		logger.debug(String.format("getTransformation-squidFlowId=%s", key));
		Transformation transformation = null;
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
		Map<String, Object> columnsValue = adapter.queryRecord(null, SystemTableEnum.DS_TRANSFORMATION.toString(),
				whereCondList, out);
		if (columnsValue == null) {
			// 无数据返回
			out.setMessageCode(MessageCode.NODATA);
			return null;
		}
		// 将数据库查询结果转换为SquidFlow实体
		transformation = this.getTransformation(columnsValue);
		out.setMessageCode(MessageCode.SUCCESS);
		return transformation;
	}

	/**
	 * 根据TransformationID查询Transformation对象
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 * 
	 * @param tid
	 * @param out
	 * @return
	 */
	public Transformation getTransformation(int transformationID, ReturnValue out) {
		logger.debug(String.format("getTransformation-transformationID=%s", transformationID));
		Transformation transformation = null;

		// 封装查询条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		WhereCondition condition = new WhereCondition();
		condition.setAttributeName("ID");
		condition.setDataType(DataStatusEnum.INT);
		condition.setMatchType(MatchTypeEnum.EQ);
		condition.setValue(transformationID);
		whereCondList.add(condition);
		// 定义集合接收
		Map<String, Object> columnsValue = adapter.queryRecord(null, SystemTableEnum.DS_TRANSFORMATION.toString(),
				whereCondList, out);
		if (columnsValue == null) {
			// 无数据返回
			out.setMessageCode(MessageCode.NODATA);
			logger.debug(String.format("getTransformation not found by squidFlowId(%s)...", transformation));
			return null;
		}
		// 将数据库查询结果转换为SquidFlow实体
		transformation = this.getTransformation(columnsValue);
		return transformation;

	}

	/**
	 * 根据column_id查询Transformation
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 * 
	 * @param columnId 
	 * @param out 异常处理
	 * @return
	 */
	public Transformation[] getTransformationsByColumnId(int columnId, ReturnValue out) {
		logger.debug(String.format("getTransformations_squidId=%s", columnId));

		// 封装查询条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		WhereCondition condition = new WhereCondition();
		condition.setAttributeName("COLUMN_ID");
		condition.setDataType(DataStatusEnum.INT);
		condition.setMatchType(MatchTypeEnum.EQ);
		condition.setValue(columnId);
		whereCondList.add(condition);
		// 定义集合接收
		List<Map<String, Object>> columnsValue = adapter.query(null, SystemTableEnum.DS_TRANSFORMATION.toString(),
				whereCondList, out);
		if (columnsValue == null || columnsValue.size() == 0) {
			out.setMessageCode(MessageCode.NODATA);
			return null;
		}
		Transformation[] transformations = new Transformation[columnsValue.size()];// 定义数组接收
		// 将数据库查询结果转换为SquidFlow实体
		int i = 0;
		for (Map<String, Object> column : columnsValue) {
			transformations[i] = this.getTransformation(column);// 接收数据
			i++;
		}
		logger.debug(String.format("getTransformationLinks_result-size()=%s", transformations.length));
		return transformations;
	}

	

	/**
	 * 将查询结果Map集合对象转换成Transformation对象
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
	/**
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param columnsValue
	 *@return
	 */
	private Transformation getTransformation(Map<String, Object> columnsValue) {
		Transformation transformation = null;
		int id = (Integer) columnsValue.get("ID");
		transformation = new Transformation();
		// ID
		transformation.setId(id);
		// SquidＩＤ
		String squidId = columnsValue.get("SQUID_ID") == null ? null : columnsValue.get("SQUID_ID").toString();
		// 类型
		String type = columnsValue.get("TYPE") == null ? null : columnsValue.get("TYPE").toString();
		// x轴
		String locationX = columnsValue.get("LOCATION_X") == null ? null : columnsValue.get("LOCATION_X").toString();
		// Y轴
		String locationY = columnsValue.get("LOCATION_Y") == null ? null : columnsValue.get("LOCATION_Y").toString();
		// 列ID
		String columnId = columnsValue.get("COLUMN_ID") == null ? null : columnsValue.get("COLUMN_ID").toString();
		// 唯一标识
		String guid = columnsValue.get("KEY") == null ? null : String.valueOf(columnsValue.get("KEY"));
		int ttype =  columnsValue.get("TRANSFORMATION_TYPE_ID") == null ? 0 : Integer.parseInt(columnsValue.get("TRANSFORMATION_TYPE_ID").toString());
		if (squidId != null) {
			transformation.setSquid_id(Integer.parseInt(squidId));
		}
		if (type != null) {
				transformation.setTranstype(Integer.parseInt(type));
		}
		if (locationX != null) {
			transformation.setLocation_x(Integer.parseInt(locationX));
		}
		if (locationY != null) {
			transformation.setLocation_y(Integer.parseInt(locationY));
		}
		if (columnId != null) {
			transformation.setColumn_id(Integer.parseInt(columnId));
		}
			transformation.setTranstype(ttype);
		transformation.setKey(guid);
		transformation.setType(DSObjectType.TRANSFORMATION.value());
		return transformation;
	}

	/**
	 * <p>
	 * 作用描述：根据squidFlowId集合,查找ETransformation集合
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 * squidFlowID
	 * 
	 * @param squidID
	 * @param out
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public Transformation[] getSquidFlowTransformation(int squidFlowId, ReturnValue out) {
		logger.debug(String.format("getSquidFlowTransformation_squidFlowId=%s", squidFlowId));
		

		// 根据squidFlowId寻找ETransformation
		String sqlStr = "SELECT t.id,t.squid_id,t.transformation_type_id,t.location_x,t.location_y,t.column_id,t.key "
				+ "FROM DS_SQUID s,DS_TRANSFORMATION t WHERE s.id=t.squid_id AND s.squid_flow_id=" + squidFlowId + "";

		List<Map<String, Object>> listETransformation = adapter.query(sqlStr, out);
		if (listETransformation == null || listETransformation.size() == 0) {
			out.setMessageCode(MessageCode.NODATA);
			logger.debug(String.format("squidFlowId not found by ETransformation(%s)...", squidFlowId));
			return null;
		}
		logger.debug(String.format("getSquidFlowTransformations size=%s", listETransformation.size()));
		// 将数据转换为ETransformation实体
		int i = 0;
		Transformation[] transformations = new Transformation[listETransformation.size()];// 定义数组接收
		for (Map<String, Object> column : listETransformation) {
			transformations[i] = this.getTransformation(column);// 接收数据
			i++;
		}
		logger.debug(String.format("getSquidFlowTransformations ETransformation[] size=%s", transformations.length));
		out.setMessageCode(MessageCode.SUCCESS);
		return transformations;
	}
}
