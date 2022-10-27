package com.eurlanda.datashire.sprint7.plug;

import com.eurlanda.datashire.adapter.IDBAdapter;
import com.eurlanda.datashire.entity.Column;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Column处理类
 * 对Column对象的业务处理,例如Column对象的创建,Column对象的修改,批量创建,批量修改等
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
 * update :Cheryl 2013-8-22
 * </p>
 * <p>
 * Department : JAVA后端研发部
 * </p>
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司 </p>
 */
public class ColumnPlug extends Support {
	static Logger logger = Logger.getLogger(ColumnPlug.class);// 记录日志

	public ColumnPlug(IDBAdapter adapter){
		super(adapter);
	}
	
	/**
	 * 创建Columns对象集合
	 * <p>
	 * 作用描述：
	 * 批量创建column对象集合
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param columns  column集合对象
	 *@param out
	 *@return
	 */
	public boolean createColumns(List<Column> columns,ReturnValue out){
		logger.debug(String.format("createColumns-ColumnList.size()=%s", columns.size()));
		//封装批量新增数据集
		List<List<DataEntity>>  paramList = new ArrayList<List<DataEntity>>();
		//调用转换类
		GetParam getParam = new GetParam();
		for (Column column : columns) {
			List<DataEntity> dataEntitys =  new ArrayList<DataEntity>();
			getParam.getColumn(column, dataEntitys);
			paramList.add(dataEntitys);
		}
		return adapter.createBatch(SystemTableEnum.DS_COLUMN.toString(), paramList, out);
	}

	/**
	 * 创建Column
	 * <p>
	 * 作用描述：
	 * 根据Column对象新增数据
	 * </p>
	 * <p>
	 * 修改说明：
	 * 创建Column对象
	 * </p>
	 * @param column Column对象
	 * @param out 异常处理
	 * @return
	 */
	public boolean createColumn(Column column, ReturnValue out) {
		logger.debug(String.format("createColumn-squid=%s;id=%s", column.getKey(), column.getId()));
		boolean create = false;
		// 将Column转换为数据包集合
		List<DataEntity> paramList = new ArrayList<DataEntity>();
		// 设置值
		getParam(column, paramList);
		// 执行新增
		create = adapter.insert(SystemTableEnum.DS_COLUMN.toString(), paramList, out);
		//
		logger.debug(String.format("createColumn-result=%s", create));
		return create;
	}
	
	

	/**
	 * 修改Column
	 * <p>
	 * 作用描述：
	 * 修改Column对象
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 * @param column Column对象
	 * @param out 异常处理
	 * @return
	 */
	public boolean updateColumn(Column column, ReturnValue out) {
		if (column == null) {
			out.setMessageCode(MessageCode.ERR_COLUMN_NULL);
			return false;
		}
		logger.debug(String.format("updateColumn-getSquidId=%d", column.getSquid_id()));
		boolean update = false;
		// 封装更新列
		// 将Column转换为数据包集合
		List<DataEntity> paramValue = new ArrayList<DataEntity>();
		// 设置值
		getParam(column, paramValue);

		// 封装条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		WhereCondition condition = new WhereCondition();
		condition.setAttributeName("ID");
		condition.setDataType(DataStatusEnum.INT);
		condition.setMatchType(MatchTypeEnum.EQ);
		condition.setValue(column.getId());
		whereCondList.add(condition);
		// 执行更新操作
		update = adapter.update(SystemTableEnum.DS_COLUMN.toString(), paramValue, whereCondList, out);
		
		logger.debug(String.format("updateColumn-result=%s", update));
		return update;
	}

	/**
	 * 根据Id删除Column对象
	 * <p>
	 * 作用描述：
	 * 根据ID删除相应的Column对象
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 * 
	 * @param id columnID 
	 * @param out 异常处理
	 * @return
	 */
	public boolean deleteColumn(int id, ReturnValue out,String token) {
		logger.debug(String.format("deleteColumn-id=%s", id));
		boolean delete = true;
		//根据column_id查询出transformation,根据id删除transformation
		List<Map<String, Object>> transformationList = adapter.query("select id from DS_TRANSFORMATION where column_id="+id, out);
		if(null!=transformationList&&transformationList.size()>0)
		{
			TransformationPlug transformationPlug=new TransformationPlug(adapter);
			for(int i=0;i<transformationList.size();i++)
			{
				logger.debug("根据目标列查询到的DS_TRANSFORMATION的长度"+transformationList.size());
				delete=transformationPlug.deleteTransformation((Integer)transformationList.get(i).get("ID"), out, token);
				if(!delete)
				{
					out.setMessageCode(MessageCode.ERR_DELETE_COLUM);
					return false;
				}
			}
		}
		if (delete) {
			// 封装条件
			List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
			WhereCondition condition = new WhereCondition();
			condition.setAttributeName("ID");
			condition.setDataType(DataStatusEnum.INT);
			condition.setMatchType(MatchTypeEnum.EQ);
			condition.setValue(id);
			whereCondList.add(condition);
			// 执行删除
			delete = adapter.delete(SystemTableEnum.DS_COLUMN.toString(),
					whereCondList, out);

			logger.debug(String.format("deleteColumn-guid=%d,result=%s", id,
					delete));
		}

		return delete;
	}

	/**
	 * 根据columnId查询Column
	 * <p>
	 * 作用描述：
	 * 根据ColumnID查询相应的Column对象
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 * 
	 * @param id ColumnID
	 * @param out 异常处理
	 * @return
	 */
	public Column getColumn(int id, ReturnValue out) {
		logger.debug(String.format("getSquidFlow-guid=%s", id));
		Column column = new Column();
		// 封装查询条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		WhereCondition condition = new WhereCondition();
		condition.setAttributeName("ID");
		condition.setDataType(DataStatusEnum.INT);
		condition.setMatchType(MatchTypeEnum.EQ);
		condition.setValue(id);
		whereCondList.add(condition);
		Map<String, Object> columnsValue = adapter.queryRecord(null, SystemTableEnum.DS_COLUMN.toString(),
				whereCondList, out);
		
		if (columnsValue == null) {
			// 无数据返回
			out.setMessageCode(MessageCode.NODATA);
			return column;
		}
		// 将数据库查询结果转换为SquidFlow实体
		column = this.getColumn(columnsValue);
		out.setMessageCode(MessageCode.SUCCESS);
		return column;
	}
	


	/**
	 * 根据squidId查询Column对象集合
	 * <p>
	 * 作用描述：
	 * 根据SquidID查询Column对象集合
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 * 
	 * @param squidId squidID
	 * @param out 异常返回
	 * @return
	 */
	public List<Column> getColumns(int squidId, ReturnValue out) {
		logger.debug(String.format("getColumns-squidId=%s", squidId));
		// 封装查询条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		WhereCondition condition = new WhereCondition();
		condition.setAttributeName("SQUID_ID");
		condition.setDataType(DataStatusEnum.INT);
		condition.setMatchType(MatchTypeEnum.EQ);
		condition.setValue(squidId);
		whereCondList.add(condition);
		List<Map<String, Object>> columnsValue = adapter.query(null, SystemTableEnum.DS_COLUMN.toString(),
				whereCondList, out);
		
		if (columnsValue == null || columnsValue.size() == 0) {
			out.setMessageCode(MessageCode.NODATA);
			return null;
		}
		List<Column> columnArray = new ArrayList<Column>();
		// 将数据库查询结果转换为Column实体
		for (Map<String, Object> column : columnsValue) {
			columnArray.add(this.getColumn(column));// 接收数据
		}
		logger.debug(String.format("getColumns-squidId=%s;result.size=%s", squidId, columnArray.size()));
		return columnArray;
	}
	
	/**
	 * 根据DB_SOURCE_TABLE_ID获得Column信息
	 * <p>
	 * 作用描述：
	 * 根据DbSourceTableID获得相应的Column信息
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param dbSourceTableId dbSourceTableID
	 *@param out 异常返回
	 *@return
	 */
	public List<Column> getColumnByDBSourceTableId(int dbSourceTableId, ReturnValue out) {
		logger.debug(String.format("getColumns-dbSourceTableId=%s", dbSourceTableId));
		// 封装查询条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		WhereCondition condition = new WhereCondition();
		condition.setAttributeName("DB_SOURCE_TABLE_ID");
		condition.setDataType(DataStatusEnum.INT);
		condition.setMatchType(MatchTypeEnum.EQ);
		condition.setValue(dbSourceTableId);
		whereCondList.add(condition);
		List<Map<String, Object>> columnsValue = adapter.query(null, SystemTableEnum.DS_COLUMN.toString(),
				whereCondList, out);
		
		if (columnsValue == null || columnsValue.size() == 0) {
			out.setMessageCode(MessageCode.NODATA);
			return null;
		}
		List<Column> columnArray = new ArrayList<Column>();
		// 将数据库查询结果转换为Column实体
		for (Map<String, Object> column : columnsValue) {
			columnArray.add(this.getColumn(column));// 接收数据
		}
		logger.debug(String.format("getColumns-squidId=%s;result.size=%s", dbSourceTableId, columnArray.size()));
		return columnArray;
	}
	
	/**
	 * 根据key查询Column
	 * <p>
	 * 作用描述：'
	 * 根据Key获得Column对象
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 * @param key
	 * @param out 异常处理
	 * @return
	 */
	public Column getColumn(String key, ReturnValue out) {
		logger.debug(String.format("getColumn-guid=%s", key));
		Column column = null;

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
		Map<String, Object> columnsValue = adapter.queryRecord(null, SystemTableEnum.DS_COLUMN.toString(),
				whereCondList, out);
		//
		if (columnsValue == null) {
			// 无数据返回
			out.setMessageCode(MessageCode.NODATA);
			return null;
		}
		// 将数据库查询结果转换为SquidFlow实体
		column = this.getColumn(columnsValue);
		out.setMessageCode(MessageCode.SUCCESS);
		return column;
	}
	
	/**
	 * 根据key查询ID
	 * <p>
	 * 作用描述：
	 * 根据Key查询相应的ID
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 * @param key 
	 * @param out 异常处理
	 * @return
	 */
	public int getColumnId(String key, ReturnValue out) {
		logger.debug(String.format("getColumn-guid=%s", key));
		
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
		Map<String, Object> columnsValue = adapter.queryRecord(null, SystemTableEnum.DS_COLUMN.toString(),
				whereCondList, out);
		//
		if (columnsValue == null) {
			// 无数据返回
			out.setMessageCode(MessageCode.NODATA);
			return 0;
		}
		int id = columnsValue.get("ID")== null ? 0:Integer.parseInt(columnsValue.get("ID").toString());
		out.setMessageCode(MessageCode.SUCCESS);
		return id;
	}

	/**
	 * 将查询结果map集合对象转换成Column对象
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
	private Column getColumn(Map<String, Object> columnsValue) {
		Column column = new Column();
		// ID
		Object objId = columnsValue.get("ID");
		int id = objId == null ? 0 : Integer.parseInt(String.valueOf(objId));
		// relative_order
		Object objRelative = columnsValue.get("RELATIVE_ORDER");
		Integer relativeOrder = objRelative == null ? null : Integer.parseInt(String.valueOf(objRelative));
		// squidID
		Object objSquidId = columnsValue.get("SQUID_ID");
		Integer squidId = objSquidId == null ? null : Integer.parseInt(String.valueOf(objSquidId));
		// name
		String name = String.valueOf(columnsValue.get("NAME"));
		// nullable
		Object objNullable = columnsValue.get("NULLABLE");
		//Integer nullable = objNullable == null ? null : Integer.parseInt(String.valueOf(objNullable));
		Boolean nullable = objNullable == null ? false : Boolean.parseBoolean(String.valueOf(objNullable));
		
		//Object objDBSourceTableId = columnsValue.get("DB_SOURCE_TABLE_ID");
		//Integer dbSourceTableId = objDBSourceTableId == null ? null : Integer.parseInt(String
		//		.valueOf(objDBSourceTableId));

		// guid
		String guid = String.valueOf(columnsValue.get("KEY"));
		// collation
		String collation = "";
		if(columnsValue.get("COLLATION")!=null){
			collation = String.valueOf(columnsValue.get("COLLATION"));
		}
		int length =  columnsValue.get("LENGTH")==null?0:Integer.parseInt(columnsValue.get("LENGTH").toString());
		column.setId(id);
		column.setRelative_order(relativeOrder);
		column.setSquid_id(squidId);
		column.setName(name);
		column.setData_type(StringUtils.valueOfInt(columnsValue, "DATA_TYPE"));
		//TODO
		/*if(dataType!=null&&dataType!=""){
			column.setColumntype(dataType == ""?0:Integer.parseInt(dataType));
		}*/
		column.setNullable(nullable);
		column.setLength(length);
		//if(dbSourceTableId!=null){
		//  column.setDb_source_table_id(dbSourceTableId);
		//}
		column.setKey(guid);
		column.setType(DSObjectType.COLUMN.value());
		return column;
	}

	/**
	 * 将Colum对象封装成DataEntity对象
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 * 
	 * @param column
	 * @param paramList
	 */
	private void getParam(Column column, List<DataEntity> paramList) {
		DataEntity dataEntity = null;
		// 相关命令
		dataEntity = new DataEntity("RELATIVE_ORDER", DataStatusEnum.INT, column.getRelative_order());
		paramList.add(dataEntity);
		// SquidID
		dataEntity = new DataEntity("SQUID_ID", DataStatusEnum.INT, column.getSquid_id());
		paramList.add(dataEntity);

		// 名称
		String name = column.getName();
		if (name != null) {
			name = "'" + name + "'";
		}
		dataEntity = new DataEntity("NAME", DataStatusEnum.STRING, name);
		paramList.add(dataEntity);

	/*	// 类型
		String type = column.getData_type();
		if (type != null) {
			type = "'" + type + "'";
		}
		dataEntity = new DataEntity("DATA_TYPE", DataStatusEnum.INT, type);
		paramList.add(dataEntity);*/
		//类型
		paramList.add(new DataEntity("DATA_TYPE", DataStatusEnum.INT, column.getData_type()));
		
		int length = column.getLength();
		
		dataEntity = new DataEntity("LENGTH", DataStatusEnum.INT, length);
		paramList.add(dataEntity);
		
		int precision = column.getPrecision();
		dataEntity = new DataEntity("PRECISION", DataStatusEnum.INT, precision);
		paramList.add(dataEntity);

		// 是否为空
		int isnullable = column.isNullable()==true?1:0;
		dataEntity = new DataEntity("NULLABLE", DataStatusEnum.STRING,isnullable);
		paramList.add(dataEntity);

		// 唯一标识
		String key = column.getKey();
		if (key != null) {
			key = "'" + key + "'";
		}
		dataEntity = new DataEntity("KEY", DataStatusEnum.STRING, key);
		paramList.add(dataEntity);
		// 唯一标识
		//dataEntity = new DataEntity("DB_SOURCE_TABLE_ID", DataStatusEnum.INT, column.getDb_source_table_id());
		//paramList.add(dataEntity);
	}

	/**
	 * 根据squidFlowId获取Column集合
	 * <p>
	 * 作用描述：根据squidFlowId获取Column集合
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 * @param squidFlowId squidFlowID
	 * @param out 异常处理
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public Map<Integer, List<Column>> getSquidFlowColumn(int squidFlowId, ReturnValue out) {
		logger.debug(String.format("getSquidFlowColumn_squidFlowId=%s", squidFlowId));
		// 根据squidFlowId寻找listColumn
		String sqlStr = " SELECT c.id,c.relative_order,c.squid_id,c.name,c.data_type,c.nullable,c.db_source_table_id,c.key "
				+ " FROM DS_SQUID s,DS_COLUMN c WHERE s.id=c.squid_id AND s.squid_flow_id=" + squidFlowId;
		List<Map<String, Object>> listColumn = adapter.query(sqlStr, out);
		if (listColumn == null || listColumn.size() == 0) {
			out.setMessageCode(MessageCode.NODATA);
			logger.debug(String.format("squidFlowId not found by listColumn(%s)...", squidFlowId));
			return null;
		}
		logger.debug(String.format("getSquidFlowColumn size=%s", listColumn.size()));
		// 将数据转换为Column实体，并放入columnMap
		Map<Integer, List<Column>> columnMap = new HashMap<Integer, List<Column>>();
		List<Column> list;
		for (Map<String, Object> columnsValue : listColumn) {
			Column eColumn = this.getColumn(columnsValue);
			Integer squidId = eColumn.getSquid_id();
			if (columnMap.containsKey(squidId)) {
				columnMap.get(squidId).add(eColumn);
			} else {
				list = new ArrayList<Column>();
				list.add(eColumn);
				columnMap.put(squidId, list);
			}
		}
		logger.debug(String.format("getSquidFlowColumn databaseMap size=%s", columnMap.size()));
		out.setMessageCode(MessageCode.SUCCESS);
		return columnMap;
	}
}
