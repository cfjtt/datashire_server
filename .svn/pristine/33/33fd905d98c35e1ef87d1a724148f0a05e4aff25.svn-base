package com.eurlanda.datashire.sprint7.service.squidflow.subservice;

import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.dao.IColumnDao;
import com.eurlanda.datashire.dao.IReferenceColumnDao;
import com.eurlanda.datashire.dao.ISquidIndexesDao;
import com.eurlanda.datashire.dao.IThirdPartyParamsDao;
import com.eurlanda.datashire.dao.ITransformationDao;
import com.eurlanda.datashire.dao.ITransformationInputsDao;
import com.eurlanda.datashire.dao.ITransformationLinkDao;
import com.eurlanda.datashire.dao.IVariableDao;
import com.eurlanda.datashire.dao.impl.ColumnDaoImpl;
import com.eurlanda.datashire.dao.impl.ReferenceColumnDaoImpl;
import com.eurlanda.datashire.dao.impl.SquidIndexesDaoImpl;
import com.eurlanda.datashire.dao.impl.ThirdPartyParamsDaoImpl;
import com.eurlanda.datashire.dao.impl.TransformationDaoImpl;
import com.eurlanda.datashire.dao.impl.TransformationInputsDaoImpl;
import com.eurlanda.datashire.dao.impl.TransformationLinkDaoImpl;
import com.eurlanda.datashire.dao.impl.VariableDaoImpl;
import com.eurlanda.datashire.entity.*;
import com.eurlanda.datashire.entity.operation.ExtractSquidAndSquidLink;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.enumeration.MessageBubbleCode;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.enumeration.TransformationTypeEnum;
import com.eurlanda.datashire.sprint7.packet.PushMessagePacket;
import com.eurlanda.datashire.sprint7.service.squidflow.AbstractRepositoryService;
import com.eurlanda.datashire.utility.DebugUtil;
import com.eurlanda.datashire.utility.StringUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Extract SquidModelBase 业务处理
 * 1.拖表创建，支持批量创建（从source table list选择一个或多个到面板空白区）
 * 2.拖列创建（从source table中选择一列或多列到面板空白区或空的extract上）
 * 3.详细信息加载（query details when load squid flow）
 * 
 * @author dang.lu 2013.11.12
 *
 */
public class ExtractService extends AbstractRepositoryService{
	
	public ExtractService(String token) {
		super(token);
	}
	public ExtractService(IRelationalDataManager adapter){
		super(adapter);
	}
	public ExtractService(String token, IRelationalDataManager adapter){
		super(token, adapter);
	}

	/*
	 * 兼容从source_column列表第一次或重复拖拽到空的extract或空白区(生成新的extract)
	 * （如果目标extract已经创建且已经有目标列，需要前台控制新导入的列是否重复或与已存在列冲突，同时需要是同一张表导入的）
	 * 
	 *  1.如果 extract squid 不存在则创建
	 *  2.如果 squid link 不存在则创建 （from_squid_id需要前台设置，即从哪个db_source_squid拖拽出来的列）
	 *  3.创建目标列和引用列（目标列自动增加一列：extraction_date,该列不参与虚拟变换，也不校验其是否参与虚拟变换）
	 *  4.如果来源 transformation不存在则创建 (虚拟变换)
	 *  5.创建目标 transformation (虚拟变换)
	 *  6.创建 transformation link (虚拟-虚拟，不包括自动增加的那一列)
	 */
	public ExtractSquidAndSquidLink drag2ExtractSquid(ExtractSquidAndSquidLink vo){
		//获得ExtractSquid信息
		TableExtractSquid extractSquid = vo.getExtractSquid();
		//获得SquidLink信息
		SquidLink squidLink = vo.getSquidLink();
		squidLink.setType(DSObjectType.SQUIDLINK.value());
		//得到来源ID
		int squidFromId = squidLink.getFrom_squid_id();
		List<Column> sourceColumns = extractSquid.getColumns();
		if(sourceColumns==null || sourceColumns.isEmpty()){
			logger.warn("source squid 列为空！ source_squid_id = "+squidFromId);
			if(extractSquid.getSourceColumns()==null || extractSquid.getSourceColumns().isEmpty()){
				return vo;
			}else{
				sourceColumns = BOHelper.convert3(extractSquid.getSourceColumns());
			}
		}
		
		CheckExtractService check = new CheckExtractService(token,adapter);
		String tableName = extractSquid.getTable_name();
		
		// 理论上link和extract都应该有squi_flow_id，且值相同（可能前台有一个没有设置）
		int squi_flow_id=0;
		if(extractSquid.getSquidflow_id()>0){
			squi_flow_id = extractSquid.getSquidflow_id();
		}
		if(squidLink.getSquid_flow_id()>0){
			squi_flow_id = squidLink.getSquid_flow_id();
		}
		if(squi_flow_id<=0){
			logger.warn("squi_flow_id = "+squi_flow_id);
			return vo;
		}
		extractSquid.setSquidflow_id(squi_flow_id);
		squidLink.setSquid_flow_id(squi_flow_id);
		
		// 需要创建/填充的数据
		List<ReferenceColumn> referenceColumns = new ArrayList<ReferenceColumn>(); //BOHelper.convert(sourceColumns);
		List<Column> columns = new ArrayList<Column>();
		List<Transformation> transformations = new ArrayList<Transformation>();
		List<TransformationLink> transformationLinks = new ArrayList<TransformationLink>();

		// 临时变量
		Map<String, Object> params = new HashMap<String, Object>(); // 查询参数
		List<Transformation> tmpTrans = null; // 看来源 transformation 是否已存在
		List<ReferenceColumnGroup> colGroup = null;  // 看引用列组是否存在
		List<Column> tmpCol = null;  // 看引用列组是否存在
		int tmpFromTransId = 0;
		int tmpToTransId = 0;
		int tmpIndex = 1;
		int columnSize;  // 已经存在的目标列
		try {
			adapter.openSession();
			params.clear();
			params.put("ID", squidFromId);
			String sourceKey = adapter.query2Object(true, params, Squid.class).getKey();
			
			// 创建 ExtractSquid
			if(StringUtils.isNull(extractSquid.getKey())){
				extractSquid.setKey(StringUtils.generateGUID());
			}
			if(extractSquid.getId()<=0){
				extractSquid.setSquid_type(SquidTypeEnum.EXTRACT.value());
				extractSquid.setId(adapter.insert2(extractSquid));
			}
			// 创建 SquidLink
			squidLink.setTo_squid_id(extractSquid.getId());
			if(StringUtils.isNull(squidLink.getKey())){
				squidLink.setKey(StringUtils.generateGUID());
			}
			if(squidLink.getId()<=0){
				squidLink.setLine_type(1);
				squidLink.setId(adapter.insert2(squidLink));
			}
			
			// 创建引用列组
			ReferenceColumnGroup columnGroup = null;	
			params.clear();
			params.put("reference_squid_id", extractSquid.getId());
			colGroup = adapter.query2List2(true, params, ReferenceColumnGroup.class);
			if(colGroup == null || colGroup.isEmpty() || colGroup.get(0)==null){
				columnGroup = new ReferenceColumnGroup();
				columnGroup.setKey(StringUtils.generateGUID());
				columnGroup.setReference_squid_id(extractSquid.getId());
				columnGroup.setRelative_order(1);
				columnGroup.setId(adapter.insert2(columnGroup));
			}else{
				columnGroup = colGroup.get(0);
			}
			//List<ReferenceColumnGroup> columnGroupList = new ArrayList<ReferenceColumnGroup>();
			//columnGroupList.add(columnGroup);
			
			params.clear();
			params.put("squid_id", extractSquid.getId());
			tmpCol = adapter.query2List2(true, params, Column.class);
			columnSize = tmpCol==null?0:tmpCol.size();
			
			for (tmpIndex=0; tmpIndex<sourceColumns.size(); tmpIndex++) {
				Column columnInfo = sourceColumns.get(tmpIndex);
				// 目标ExtractSquid真实列（变换面板左边）
				Column column = new Column();
				column.setCollation(0);
				column.setData_type(columnInfo.getData_type()==0?1:columnInfo.getData_type());
				column.setLength(columnInfo.getLength());
				column.setPrecision(columnInfo.getPrecision());
				column.setKey(StringUtils.generateGUID());
				column.setName(columnInfo.getName());
				column.setNullable(columnInfo.isNullable());
				column.setRelative_order(columnSize+tmpIndex+1);
				column.setSquid_id(extractSquid.getId());
				column.setId(adapter.insert2(column));
				columns.add(column);
				// 目标ExtractSquid引用列（变换面板右边，引用db_source_squid）
				ReferenceColumn ref = new ReferenceColumn();
				ref.setColumn_id(columnInfo.getId());
				ref.setId(ref.getColumn_id());
				ref.setCollation(0);
				ref.setData_type(column.getData_type());
				ref.setKey(StringUtils.generateGUID());
				ref.setName(column.getName());
				ref.setNullable(column.isNullable());
				ref.setRelative_order(column.getRelative_order());
				ref.setSquid_id(squidFromId);
				ref.setReference_squid_id(extractSquid.getId());
				ref.setHost_squid_id(squidFromId);
				ref.setIs_referenced(true);
				ref.setGroup_id(columnGroup.getId());
				adapter.insert2(ref);
				referenceColumns.add(ref);
			}
			//columnGroup.setReferenceColumnList(referenceColumns);
			extractSquid.setColumns(columns);
			extractSquid.setSourceColumns(referenceColumns);
			
			//新增Transformation
			for (tmpIndex=0; tmpIndex<columns.size(); tmpIndex++) {
				// 创建目标 transformation
				Transformation newTrans = new Transformation();
				newTrans.setColumn_id(columns.get(tmpIndex).getId());
				newTrans.setKey(StringUtils.generateGUID());
				newTrans.setSquid_id(extractSquid.getId());
				newTrans.setTranstype(TransformationTypeEnum.VIRTUAL.value());
				newTrans.setId(adapter.insert2(newTrans));
				transformations.add(newTrans);
				tmpToTransId = newTrans.getId();
				params.clear();
				params.put("SQUID_ID", squidFromId);
				params.put("column_id", referenceColumns.get(tmpIndex).getId());
				params.put("transformation_type_id", TransformationTypeEnum.VIRTUAL.value());
				tmpTrans = adapter.query2List2(true, params, Transformation.class);
				if(tmpTrans == null || tmpTrans.isEmpty()){
					// 创建来源 transformation
					newTrans = new Transformation();
					newTrans.setColumn_id(referenceColumns.get(tmpIndex).getId());
					newTrans.setKey(StringUtils.generateGUID());
					newTrans.setSquid_id(squidFromId);
					newTrans.setTranstype(TransformationTypeEnum.VIRTUAL.value());
					newTrans.setId(adapter.insert2(newTrans));
					transformations.add(newTrans);
					tmpFromTransId = newTrans.getId();
				}else{
					tmpFromTransId = tmpTrans.get(0).getId();
					transformations.add(tmpTrans.get(0));
				}
				// 创建 transformation link
				TransformationLink transLink = new TransformationLink();
				transLink.setIn_order(tmpIndex+1);
				transLink.setFrom_transformation_id(tmpFromTransId);
				transLink.setTo_transformation_id(tmpToTransId);
				transLink.setKey(StringUtils.generateGUID());
				transLink.setId(adapter.insert2(transLink));
				transformationLinks.add(transLink);
			}
			extractSquid.setTransformations(transformations);
			extractSquid.setTransformationLinks(transformationLinks);

			//调用是否抽取方法
			if(StringUtils.isNotNull(tableName))
			{
				check.updateExtract(tableName, squidFromId, "create", null, sourceKey, token);
			}
		} catch (Exception e) {
			logger.error("创建extract squid异常", e);
			try {
				adapter.rollback();
			} catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
				logger.fatal("rollback err!", e1);
			}
		} finally {
			adapter.closeSession();
		}
		return vo;
	}
	
	/** 直接从table list拖拽创建，有可能多选或全选(需要考虑性能问题，可以持续优化)  */
	public synchronized void drag2ExtractSquid(List<ExtractSquidAndSquidLink> voList){
		// 需要创建/填充的数据
		List<ReferenceColumn> referenceColumns;
		List<Column> columns;
		List<Transformation> transformations;
		List<TransformationLink> transformationLinks;
		// 临时变量
		Map<String, Object> params = new HashMap<String, Object>(3); // 查询参数
		Transformation tmpTrans = null; // 看来源 transformation 是否已存在
		int tmpFromTransId;
		int tmpToTransId;
		int tmpIndex;
		int sourceColumnSize;
		String sourceKey; // source squid 的key（推送table是否已抽取）
		CheckExtractService check = new CheckExtractService(token,adapter);
		try {
			adapter.openSession(); // 在循环外开启事务，所有操作都必须在同一个事务中，成功则提交，失败回滚
			for (int i=0; i<voList.size(); i++) {
				//获得ExtractSquid信息
				TableExtractSquid extractSquid = voList.get(i).getExtractSquid();
				//获得SquidLink信息
				SquidLink squidLink = voList.get(i).getSquidLink();
				squidLink.setType(DSObjectType.SQUIDLINK.value());
				//得到来源ID
				int squidFromId = squidLink.getFrom_squid_id();
				String tableName = extractSquid.getTable_name();
				List<SourceColumn> sourceColumns = adapter.query2List(true, "select c.* from DS_SOURCE_TABLE t, DS_SOURCE_COLUMN c"
						+ " where t.id=c.source_table_id and t.source_squid_id="+squidFromId+" and t.table_name='"+tableName+"'", null, SourceColumn.class);
				if(sourceColumns==null || sourceColumns.isEmpty()){
					logger.warn("source squid 列为空！ source_squid_id = "+squidFromId+", tableName = "+tableName);
					continue;
				}
				
				params.clear();
				params.put("ID", squidFromId);
				sourceKey = adapter.query2Object(true, params, Squid.class).getKey();

				// 创建 ExtractSquid
				if(StringUtils.isNull(extractSquid.getKey())){
					extractSquid.setKey(StringUtils.generateGUID());
				}
				if(extractSquid.getId()<=0){
					extractSquid.setSquid_type(SquidTypeEnum.EXTRACT.value());
					extractSquid.setId(adapter.insert2(extractSquid));
				}
				// 创建 SquidLink
				squidLink.setTo_squid_id(extractSquid.getId());
				if(StringUtils.isNull(squidLink.getKey())){
					squidLink.setKey(StringUtils.generateGUID());
				}
				if(squidLink.getId()<=0){
					squidLink.setLine_type(1);
					squidLink.setId(adapter.insert2(squidLink));
				}
				
				// 创建引用列组
				ReferenceColumnGroup columnGroup = new ReferenceColumnGroup();
				columnGroup.setKey(StringUtils.generateGUID());
				columnGroup.setReference_squid_id(extractSquid.getId());
				columnGroup.setRelative_order(1);
				columnGroup.setId(adapter.insert2(columnGroup));
			
				referenceColumns = new ArrayList<ReferenceColumn>();
				columns = new ArrayList<Column>();
				transformations = new ArrayList<Transformation>();
				transformationLinks = new ArrayList<TransformationLink>();
				sourceColumnSize=sourceColumns.size();
				for (tmpIndex=0; tmpIndex<sourceColumnSize; tmpIndex++) {
					SourceColumn columnInfo = sourceColumns.get(tmpIndex);
					// 目标ExtractSquid真实列（变换面板左边）
					Column column = new Column();
					column.setCollation(0);
					column.setData_type(columnInfo.getData_type()==0?1:columnInfo.getData_type());
					column.setLength(columnInfo.getLength());
					column.setPrecision(columnInfo.getPrecision());
					column.setKey(StringUtils.generateGUID());
					column.setName(columnInfo.getName());
					column.setNullable(columnInfo.isNullable());
					column.setRelative_order(tmpIndex+1);
					column.setSquid_id(extractSquid.getId());
					column.setId(adapter.insert2(column));
					columns.add(column);
					// 目标ExtractSquid引用列（变换面板右边，引用db_source_squid）
					ReferenceColumn ref = new ReferenceColumn();
					ref.setColumn_id(columnInfo.getId());
					ref.setId(ref.getColumn_id());
					ref.setCollation(0);
					ref.setData_type(column.getData_type());
					ref.setKey(StringUtils.generateGUID());
					ref.setName(column.getName());
					ref.setNullable(column.isNullable());
					ref.setRelative_order(tmpIndex+1);
					ref.setSquid_id(squidFromId);
					ref.setReference_squid_id(extractSquid.getId());
					ref.setHost_squid_id(squidFromId);
					ref.setIs_referenced(true);
					ref.setGroup_id(columnGroup.getId());
					adapter.insert2(ref);
					referenceColumns.add(ref);
				}
				extractSquid.setColumns(columns);
				extractSquid.setSourceColumns(referenceColumns);
		 
				//新增Transformation
				for (tmpIndex=0; tmpIndex<sourceColumnSize; tmpIndex++) {
					// 创建目标 transformation
					Transformation newTrans = new Transformation();
					newTrans.setColumn_id(columns.get(tmpIndex).getId());
					newTrans.setKey(StringUtils.generateGUID());
					newTrans.setLocation_x(0);
					newTrans.setLocation_y((tmpIndex+1) * 25 + 25 / 2);
					newTrans.setSquid_id(extractSquid.getId());
					newTrans.setTranstype(TransformationTypeEnum.VIRTUAL.value());
					newTrans.setId(adapter.insert2(newTrans));
					transformations.add(newTrans);
					tmpToTransId = newTrans.getId();
					params.clear();
					params.put("SQUID_ID", squidFromId);
					params.put("column_id", referenceColumns.get(tmpIndex).getColumn_id());
					params.put("transformation_type_id", TransformationTypeEnum.VIRTUAL.value());
					tmpTrans = adapter.query2Object(true, params, Transformation.class);
					if(tmpTrans == null){
						// 创建来源 transformation
						newTrans = new Transformation();
						newTrans.setColumn_id(referenceColumns.get(tmpIndex).getColumn_id());
						newTrans.setKey(StringUtils.generateGUID());
						newTrans.setLocation_x(199);
						newTrans.setLocation_y((tmpIndex+1) * 25 + 25 / 2);
						newTrans.setSquid_id(squidFromId);
						newTrans.setTranstype(TransformationTypeEnum.VIRTUAL.value());
						newTrans.setId(adapter.insert2(newTrans));
						transformations.add(newTrans);
						tmpFromTransId = newTrans.getId();
					}else{
						tmpFromTransId = tmpTrans.getId();
						transformations.add(tmpTrans);
					}
					// 创建 transformation link
					TransformationLink transLink = new TransformationLink();
					transLink.setIn_order(tmpIndex+1);
					transLink.setFrom_transformation_id(tmpFromTransId);
					transLink.setTo_transformation_id(tmpToTransId);
					transLink.setKey(StringUtils.generateGUID());
					transLink.setId(adapter.insert2(transLink));
					transformationLinks.add(transLink);
				}
				extractSquid.setTransformations(transformations);
				extractSquid.setTransformationLinks(transformationLinks);

				//调用是否抽取方法
				if(StringUtils.isNotNull(tableName))
				{
					check.updateExtract(tableName, squidFromId, "create", null, sourceKey, token);
				}
				if(i>=1)Thread.sleep(20); // 如果前台一次拖拽多个，需要睡眠，否则cpu会急速上升反而降低性能
				//调用取消孤立squid消息泡
				PushMessagePacket.push(new MessageBubble(sourceKey, sourceKey, MessageBubbleCode.WARN_SQUID_NO_LINK.value(),true),token);
			}
		} catch (Exception e) {
			logger.error("创建extract squid异常", e);
			try {
				adapter.rollback();
			} catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
				logger.fatal("rollback err!", e1);
			}
		} finally {
			adapter.closeSession();
		}
	}
	
	/** 获取ExtractSquid详细信息（如果不在同一个事务里，adapter连接打开、关闭需要调用函数自己处理） */
	public static void setExtractSquidData(String token, final TableExtractSquid squid, boolean inSession, IRelationalDataManager adapter) {
		squid.setSquid_type(DSObjectType.EXTRACT.value());
		if(squid!=null){
			logger.debug("(extract) squid detail, id="+squid.getId()+", name="+squid.getName()+", table_name="+squid.getTable_name()+", type="+SquidTypeEnum.parse(squid.getSquid_type()));
		}
		Map<String, String> paramMap = new HashMap<String, String>(1);
		Map<String, Object> paramMap2;
		List<ReferenceColumn> sourceColumnlist = null;
		List<Integer> sourceColumnId = new ArrayList<Integer>();
		List<Integer> transId = new ArrayList<Integer>();
		//boolean host_column_deleted = false;
		int source_squid_id=0;
		try {
			if(!inSession){
				adapter.openSession();
			}
		    //adapter.openSession();
			// 所有目标列
			if(squid.getColumns()==null || squid.getColumns().isEmpty()){
				paramMap.clear();
				paramMap.put("squid_id", Integer.toString(squid.getId(), 10));
				squid.setColumns(adapter.query2List(true, paramMap, Column.class));
			}
			// 是否增量抽取及表达式  updated by bo.dang
			paramMap.clear();
			paramMap.put("id", Integer.toString(squid.getId(), 10));
			List<DataSquid> dataSquidList = adapter.query2List(true, paramMap, DataSquid.class);
			if(null != dataSquidList && dataSquidList.size() >= 1){
				DataSquid dataSquid = dataSquidList.get(0);
				//squid.setSquid_type(dataSquid.getSquid_type());
				squid.setIs_incremental(dataSquid.isIs_incremental());
				squid.setIncremental_expression(dataSquid.getIncremental_expression());
				squid.setIs_indexed(dataSquid.isIs_indexed());
				squid.setIs_persisted(dataSquid.isIs_indexed());
				squid.setIs_persisted(dataSquid.isIs_persisted());
				squid.setDestination_squid_id(dataSquid.getDestination_squid_id());
				squid.setTop_n(dataSquid.getTop_n());
				squid.setTruncate_existing_data_flag(dataSquid.isTruncate_existing_data_flag());
				squid.setProcess_mode(dataSquid.getProcess_mode());
				squid.setLog_format(dataSquid.getLog_format());
				squid.setPost_process(dataSquid.getPost_process());
				squid.setCdc(dataSquid.getCdc());
				squid.setEncoding(dataSquid.getEncoding());
				squid.setException_handling_flag(dataSquid.isException_handling_flag());
				squid.setSource_table_id(dataSquid.getSource_table_id());
				paramMap2 = new HashMap<String, Object>();
				paramMap2.put("is_incremental", dataSquid.isIs_incremental());
				paramMap2.put("incremental_expression", dataSquid.getIncremental_expression());
			}
			else {
				paramMap2 = new HashMap<String, Object>(5);
			}
/*			paramMap2 = adapter.query2Object(true, "SELECT IS_INCREMENTAL, INCREMENTAL_EXPRESSION FROM DS_DATA_SQUID WHERE ID="+squid.getId(), null);
			if(paramMap2!=null && !paramMap2.isEmpty()){
				squid.setIs_incremental("Y".equals(StringUtils.valueOf2(paramMap2, "is_incremental")));
				squid.setIncremental_expression(StringUtils.valueOf2(paramMap2, "incremental_expression"));
			}else{
				paramMap2 = new HashMap<String, Object>(5);
			}*/
			// 所有引用列
			sourceColumnlist = adapter.query2List(true, "select c.id,r.host_squid_id squid_id,r.*, nvl2(c.id, 'N','Y') host_column_deleted from"
					+ " DS_SOURCE_COLUMN c right join DS_REFERENCE_COLUMN r on r.column_id=c.id"
					+ " where r.reference_squid_id="+squid.getId(), null, ReferenceColumn.class);
			squid.setSourceColumns(sourceColumnlist);
			
			// 所有ColumnId
			if(sourceColumnlist!=null && !sourceColumnlist.isEmpty()){
				for(int i=0; i<sourceColumnlist.size(); i++){
					if(!sourceColumnId.contains(sourceColumnlist.get(i).getColumn_id())){
						sourceColumnId.add(sourceColumnlist.get(i).getColumn_id());
					}
//					if(sourceColumnlist.get(i).isHost_column_deleted()){
//						logger.warn("host_column_deleted: "+sourceColumnlist.get(i));
//						host_column_deleted = true; // 只要有一个上游列被删除就表示有问题
//					}
					if(sourceColumnlist.get(i).getSquid_id()>0){
						source_squid_id = sourceColumnlist.get(i).getSquid_id();
					}
				}
			}
			
			// 所有transformation（包括虚拟、真实变换）
			//logger.debug("get all transformation by squid_id = "+squid.getId());
			paramMap.clear();
			paramMap.put("squid_id", Integer.toString(squid.getId(), 10));
			List<Transformation> transformations = adapter.query2List(true, paramMap, Transformation.class);
			if(transformations!=null && !transformations.isEmpty()){
 				ITransformationInputsDao transInputsDao = new TransformationInputsDaoImpl(adapter);
 				Map<Integer, List<TransformationInputs>> rsMap = transInputsDao.getTransInputsBySquidId(squid.getId());
 				for(int i=0; i<transformations.size(); i++){
 					transId.add(transformations.get(i).getId());
 					//List<TransformationInputs> list = serice.getTransInputsByTransId(adapter, transformations.get(i).getId());
 					int trans_id = transformations.get(i).getId();
 					if(rsMap!=null&&rsMap.containsKey(trans_id)){
 						transformations.get(i).setInputs(rsMap.get(trans_id));
 					}
 				}
 			}
			
			// 所有上游引用列的虚拟变换
			if(sourceColumnId!=null && !sourceColumnId.isEmpty()){
				paramMap2.clear();
				if(source_squid_id>0){
					paramMap2.put("squid_id", source_squid_id);
				}
				paramMap2.put("column_id", sourceColumnId);
				paramMap2.put("transformation_type_id", TransformationTypeEnum.VIRTUAL.value());
				StringUtils.addAll(transformations, adapter.query2List2(true, paramMap2, Transformation.class));
			}
			squid.setTransformations(transformations);
			
			// 所有transformation link
			List<TransformationLink> transformationLinks = null;
			if(transId!=null && !transId.isEmpty()){
				paramMap2.clear();
				paramMap2.put("TO_TRANSFORMATION_ID", transId);
				transformationLinks = adapter.query2List2(true, paramMap2, TransformationLink.class);
				squid.setTransformationLinks(transformationLinks);
			}
			// 上游的column被删除
//			PushMessagePacket.push(
//					new MessageBubble(squid.getKey(), squid.getKey(), MessageBubbleCode.ERROR_REFERENCE_COLUMN_DELETED.value(), !host_column_deleted), 
//					token);
			if(DebugUtil.isDebugenabled())logger.debug(DebugUtil.squidDetail(squid));
		} catch (Exception e) {
			logger.error("getExtractSquidData-datas", e);
		} finally {
			if(!inSession){
				adapter.closeSession();
			}
		}
	}
	/**
	 * 查询并赋值
	 * 2014-12-10
	 * @author Akachi
	 * @E-Mail zsts@hotmail.com
	 * @param adapter
	 * @param ds
	 * @return
	 */
	public static DataSquid setDSFCS(IRelationalDataManager adapter,DataSquid ds){
		IThirdPartyParamsDao tppDao = new ThirdPartyParamsDaoImpl(adapter);
		List<ThirdPartyParams> listThirdPartyParamsSource= tppDao.findThirdPartyParamsByWSEID(ds.getId());
		if(ds instanceof WebserviceExtractSquid){
			WebserviceExtractSquid wes = (WebserviceExtractSquid)ds;
			wes.setUrlParams(new ArrayList<ThirdPartyParams>());
			wes.setHeaderParams(new ArrayList<ThirdPartyParams>());
			wes.setContentParams(new ArrayList<ThirdPartyParams>());
			for (ThirdPartyParams thirdPartyParams : listThirdPartyParamsSource) {//0=get,1=head,2=content,post
				if(thirdPartyParams.getParams_type()==0){
					wes.getUrlParams().add(thirdPartyParams);
				}else if(thirdPartyParams.getParams_type()==1){
					wes.getHeaderParams().add(thirdPartyParams);
				}else if(thirdPartyParams.getParams_type()==2){
					wes.getContentParams().add(thirdPartyParams);
				}
			}
		}else if(ds instanceof HttpExtractSquid){
			HttpExtractSquid hes = (HttpExtractSquid)ds;
			hes.setContentParams(new ArrayList<ThirdPartyParams>());
			hes.setUrlParams(new ArrayList<ThirdPartyParams>());
			for (ThirdPartyParams thirdPartyParams : listThirdPartyParamsSource) {//0=get,1=head,2=content,post
				if(thirdPartyParams.getParams_type()==0){
					hes.getUrlParams().add(thirdPartyParams);
				}else if(thirdPartyParams.getParams_type()==2){
					hes.getContentParams().add(thirdPartyParams);
				}
			}
		}
		return ds;
	}
	
	/** 获取ExtractSquid详细信息（如果不在同一个事务里，adapter连接打开、关闭需要调用函数自己处理）
	 * 
	 * @param adapter
	 * @author bo.dang
	 * @date 2014年5月15日
	 */
    public static DataSquid setAllExtractSquidData(IRelationalDataManager adapter, final DataSquid newDataSquid) {
        // 当前ExtractSquid的id
        int squidId = newDataSquid.getId();
        if(newDataSquid!=null){
            logger.debug("(extract) squid detail, id="+squidId+", name="+newDataSquid.getName()+", table_name="+newDataSquid.getTable_name()+", type="+SquidTypeEnum.parse(newDataSquid.getSquid_type()));
        }
        ISquidIndexesDao squidIndexesDao = new SquidIndexesDaoImpl(adapter);
        IColumnDao columnDao = new ColumnDaoImpl(adapter);
        IReferenceColumnDao refColumnDao = new ReferenceColumnDaoImpl(adapter);
        ITransformationDao transDao = new TransformationDaoImpl(adapter);
        ITransformationInputsDao transInputsDao = new TransformationInputsDaoImpl(adapter);
        ITransformationLinkDao  transLinkDao = new TransformationLinkDaoImpl(adapter);
        IVariableDao variableDao = new VariableDaoImpl(adapter);
        try {
            // 设置索引index
            List<SquidIndexes> squidIndexesList = squidIndexesDao.getSquidIndexesBySquidId(newDataSquid.getId());
            newDataSquid.setSquidIndexesList(squidIndexesList);
            // 所有目标列
            if(newDataSquid.getColumns()==null || newDataSquid.getColumns().isEmpty()){
            	newDataSquid.setColumns(columnDao.getColumnListBySquidId(newDataSquid.getId()));
            }
            // 所有引用列
            List<ReferenceColumn> sourceColumnlist = refColumnDao.getRefColumnListByRefSquidId(newDataSquid.getId());
            newDataSquid.setSourceColumns(sourceColumnlist);
            // 所有transformation（包括虚拟、真实变换）
            //logger.debug("get all transformation by squid_id = "+squid.getId());
            List<Transformation> transformations = transDao.getTransListBySquidId(newDataSquid.getId());
            if(transformations!=null && !transformations.isEmpty()){
 				Map<Integer, List<TransformationInputs>> rsMap = transInputsDao.getTransInputsBySquidId(newDataSquid.getId());
 				for(int i=0; i<transformations.size(); i++){
 					int trans_id = transformations.get(i).getId();
 					if(rsMap!=null&&rsMap.containsKey(trans_id)){
 						transformations.get(i).setInputs(rsMap.get(trans_id));
 					}
 				}
 			}
            newDataSquid.setTransformations(transformations);
            // 所有transformation link
            List<TransformationLink> transformationLinks = transLinkDao.getTransLinkListBySquidId(newDataSquid.getId());
            newDataSquid.setTransformationLinks(transformationLinks);
            
            //查询变量集合
			List<DSVariable> variables = variableDao.getDSVariableByScope(2, squidId);
			if (variables==null){
				variables = new ArrayList<DSVariable>();
			}
			newDataSquid.setVariables(variables);
			
            if(DebugUtil.isDebugenabled())logger.debug(DebugUtil.squidDetail(newDataSquid));
        } catch (Exception e) {
            logger.error("getExtractSquidData-datas", e);
        } 
        return newDataSquid;
    }
    
    /** 获取ExtractSquid详细信息（如果不在同一个事务里，adapter连接打开、关闭需要调用函数自己处理） */
    public static void setDocExtractSquidData(IRelationalDataManager adapter, final DocExtractSquid newMongodbExtractSquid) {
        //newMongodbExtractSquid.setSquid_type(DSObjectType.DOC_EXTRACT.value());
        if(newMongodbExtractSquid!=null){
            logger.debug("(extract) squid detail, id="+newMongodbExtractSquid.getId()+", name="+newMongodbExtractSquid.getName()+", table_name="+newMongodbExtractSquid.getTable_name()+", type="+SquidTypeEnum.parse(newMongodbExtractSquid.getSquid_type()));
        }
        ISquidIndexesDao squidIndexesDao = new SquidIndexesDaoImpl(adapter);
        IColumnDao columnDao = new ColumnDaoImpl(adapter);
        IReferenceColumnDao refColumnDao = new ReferenceColumnDaoImpl(adapter);
        ITransformationDao transDao = new TransformationDaoImpl(adapter);
        ITransformationInputsDao transInputsDao = new TransformationInputsDaoImpl(adapter);
        ITransformationLinkDao  transLinkDao = new TransformationLinkDaoImpl(adapter);
        IVariableDao variableDao = new VariableDaoImpl(adapter);
        try {
            // 设置索引index
            List<SquidIndexes> squidIndexesList = squidIndexesDao.getSquidIndexesBySquidId(newMongodbExtractSquid.getId());
            newMongodbExtractSquid.setSquidIndexesList(squidIndexesList);
            // 所有目标列
            if(newMongodbExtractSquid.getColumns()==null || newMongodbExtractSquid.getColumns().isEmpty()){
                newMongodbExtractSquid.setColumns(columnDao.getColumnListBySquidId(newMongodbExtractSquid.getId()));
            }
            // 所有引用列
            List<ReferenceColumn> sourceColumnlist = refColumnDao.getRefColumnListByRefSquidId(newMongodbExtractSquid.getId());
            newMongodbExtractSquid.setSourceColumns(sourceColumnlist);
            // 所有transformation（包括虚拟、真实变换）
            //logger.debug("get all transformation by squid_id = "+squid.getId());
            List<Transformation> transformations = transDao.getTransListBySquidId(newMongodbExtractSquid.getId());
            if(transformations!=null && !transformations.isEmpty()){
 				Map<Integer, List<TransformationInputs>> rsMap = transInputsDao.getTransInputsBySquidId(newMongodbExtractSquid.getId());
 				for(int i=0; i<transformations.size(); i++){
 					int trans_id = transformations.get(i).getId();
 					if(rsMap!=null&&rsMap.containsKey(trans_id)){
 						transformations.get(i).setInputs(rsMap.get(trans_id));
 					}
 				}
 			}
            newMongodbExtractSquid.setTransformations(transformations);
            // 所有transformation link
            List<TransformationLink> transformationLinks = transLinkDao.getTransLinkListBySquidId(newMongodbExtractSquid.getId());
            newMongodbExtractSquid.setTransformationLinks(transformationLinks);
            
            //查询变量集合
			List<DSVariable> variables = variableDao.getDSVariableByScope(2, newMongodbExtractSquid.getId());
			if (variables==null){
				variables = new ArrayList<DSVariable>();
			}
			newMongodbExtractSquid.setVariables(variables);
            
            if(DebugUtil.isDebugenabled())logger.debug(DebugUtil.squidDetail(newMongodbExtractSquid));
        } catch (Exception e) {
            logger.error("getExtractSquidData-datas", e);
        } 
    }
    
    /** 获取ExtractSquid详细信息（如果不在同一个事务里，adapter连接打开、关闭需要调用函数自己处理） */
    public static void setMongodbExtractSquidData(IRelationalDataManager adapter, final MongodbExtractSquid newMongodbExtractSquid) {
        //newMongodbExtractSquid.setSquid_type(DSObjectType.DOC_EXTRACT.value());
        if(newMongodbExtractSquid!=null){
            logger.debug("(extract) squid detail, id="+newMongodbExtractSquid.getId()+", name="+newMongodbExtractSquid.getName()+", table_name="+newMongodbExtractSquid.getTable_name()+", type="+SquidTypeEnum.parse(newMongodbExtractSquid.getSquid_type()));
        }
        ISquidIndexesDao squidIndexesDao = new SquidIndexesDaoImpl(adapter);
        IColumnDao columnDao = new ColumnDaoImpl(adapter);
        IReferenceColumnDao refColumnDao = new ReferenceColumnDaoImpl(adapter);
        ITransformationDao transDao = new TransformationDaoImpl(adapter);
        ITransformationInputsDao transInputsDao = new TransformationInputsDaoImpl(adapter);
        ITransformationLinkDao  transLinkDao = new TransformationLinkDaoImpl(adapter);
        IVariableDao variableDao = new VariableDaoImpl(adapter);
        try {
            // 设置索引index
            List<SquidIndexes> squidIndexesList = squidIndexesDao.getSquidIndexesBySquidId(newMongodbExtractSquid.getId());
            newMongodbExtractSquid.setSquidIndexesList(squidIndexesList);
            // 所有目标列
            if(newMongodbExtractSquid.getColumns()==null || newMongodbExtractSquid.getColumns().isEmpty()){
            	newMongodbExtractSquid.setColumns(columnDao.getColumnListBySquidId(newMongodbExtractSquid.getId()));
            }
            // 所有引用列
            List<ReferenceColumn> sourceColumnlist = refColumnDao.getRefColumnListByRefSquidId(newMongodbExtractSquid.getId());
            newMongodbExtractSquid.setSourceColumns(sourceColumnlist);
            // 所有transformation（包括虚拟、真实变换）
            //logger.debug("get all transformation by squid_id = "+squid.getId());
            List<Transformation> transformations = transDao.getTransListBySquidId(newMongodbExtractSquid.getId());
            if(transformations!=null && !transformations.isEmpty()){
 				Map<Integer, List<TransformationInputs>> rsMap = transInputsDao.getTransInputsBySquidId(newMongodbExtractSquid.getId());
 				for(int i=0; i<transformations.size(); i++){
 					int trans_id = transformations.get(i).getId();
 					if(rsMap!=null&&rsMap.containsKey(trans_id)){
 						transformations.get(i).setInputs(rsMap.get(trans_id));
 					}
 				}
 			}
            newMongodbExtractSquid.setTransformations(transformations);
            // 所有transformation link
            List<TransformationLink> transformationLinks = transLinkDao.getTransLinkListBySquidId(newMongodbExtractSquid.getId());
            newMongodbExtractSquid.setTransformationLinks(transformationLinks);
            
            //查询变量集合
			List<DSVariable> variables = variableDao.getDSVariableByScope(2, newMongodbExtractSquid.getId());
			if (variables==null){
				variables = new ArrayList<DSVariable>();
			}
			newMongodbExtractSquid.setVariables(variables);
            
            if(DebugUtil.isDebugenabled())logger.debug(DebugUtil.squidDetail(newMongodbExtractSquid));
        } catch (Exception e) {
            logger.error("setMongodbExtractSquidData-datas", e);
        } 
    }
}