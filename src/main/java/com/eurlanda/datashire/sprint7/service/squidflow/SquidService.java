package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.adapter.datatype.DataTypeConvertor;
import com.eurlanda.datashire.dao.ISquidDao;
import com.eurlanda.datashire.dao.impl.SquidDaoImpl;
import com.eurlanda.datashire.entity.*;
import com.eurlanda.datashire.entity.operation.DataEntity;
import com.eurlanda.datashire.entity.operation.ExtractSquidAndSquidLink;
import com.eurlanda.datashire.entity.operation.TransformationAndCloumn;
import com.eurlanda.datashire.entity.operation.WhereCondition;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.enumeration.DataStatusEnum;
import com.eurlanda.datashire.enumeration.MatchTypeEnum;
import com.eurlanda.datashire.enumeration.MessageBubbleCode;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.enumeration.SystemTableEnum;
import com.eurlanda.datashire.enumeration.TransformationTypeEnum;
import com.eurlanda.datashire.sprint7.packet.InfoPacket;
import com.eurlanda.datashire.sprint7.packet.PushMessagePacket;
import com.eurlanda.datashire.sprint7.plug.ColumnPlug;
import com.eurlanda.datashire.sprint7.plug.GetParam;
import com.eurlanda.datashire.sprint7.plug.SquidPlug;
import com.eurlanda.datashire.sprint7.plug.SupportPlug;
import com.eurlanda.datashire.sprint7.plug.TransformationPlug;
import com.eurlanda.datashire.sprint7.service.squidflow.subservice.CheckExtractService;
import com.eurlanda.datashire.sprint7.service.squidflow.subservice.ExtractServicesub;
import com.eurlanda.datashire.sprint7.service.squidflow.subservice.MessageBubbleService;
import com.eurlanda.datashire.utility.CommonConsts;
import com.eurlanda.datashire.utility.EnumException;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import com.eurlanda.datashire.utility.StringUtils;
import com.eurlanda.datashire.validator.SquidValidationTask;
import org.apache.log4j.Logger;
import util.JsonUtil;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Squid业务处理类
 * Title : 
 * Description: 
 * Author :赵春花 2013-10-29
 * update :赵春花 2013-10-29
 * Department :  JAVA后端研发部
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司
 */
public class SquidService  extends SupportPlug implements ISquidService{
	static Logger logger = Logger.getLogger(SquidService.class);// 记录日志
	public SquidService(String token) {
		super(token);
	}

	/**
	 * 模拟前端创建的ExtractSquidAndSquidLink
	 * 2014-12-29想
	 * @author Akachi
	 * @E-Mail zsts@hotmail.com
	 * @param adapter
	 * @param sourceTableIds
	 * @param connectionSquidId
	 * @param squidFlowId
	 * @return
	 */
	private List<ExtractSquidAndSquidLink> createExtractSquidAndSquidLink(IRelationalDataManager adapter,List<Integer> sourceTableIds,Integer connectionSquidId,Integer squidFlowId,Integer x,Integer y) throws Exception {
		Map<String,String> params = new HashMap<String,String>();
		params.put("id", connectionSquidId+"");
		ISquidDao squidDao = new SquidDaoImpl(adapter);
		int squidTypeId = squidDao.getSquidTypeById(connectionSquidId);
//		原表 获取坐标
//		ISquidDao squidDao = new SquidDaoImpl(adapter);
		Squid dbSourceSquid = adapter.query2Object2(true,params, Squid.class);
		List<ExtractSquidAndSquidLink> listExtractSquidAndSquidLink = new ArrayList<ExtractSquidAndSquidLink>();
		if(x==null){
			x=100;
		}if(y==null){
			y=100;
		}
		boolean fob=false;;
		for (Integer sourceTableId : sourceTableIds) {
			ExtractSquidAndSquidLink esasl = new ExtractSquidAndSquidLink();
			TableExtractSquid tes = new TableExtractSquid();
			esasl.setExtractSquid(tes);
			params.clear();
			params.put("id", sourceTableId+"");
			DBSourceTable st = adapter.query2Object2(true,params, DBSourceTable.class);
			Map<String,Object> paramso = new HashMap<String,Object>();
			paramso.put("source_table_id",sourceTableId+"");
			List<SourceColumn> listSourceColumn = adapter.query2List2(true,paramso, SourceColumn.class);
			List<ReferenceColumn> listReferenceColumns = new ArrayList<ReferenceColumn>();
			List<Column> listColumns = new ArrayList<Column>();
			for (SourceColumn sourceColumn : listSourceColumn) {
				listReferenceColumns.add(DataTypeConvertor.getReferenceColumnBySourceColumn(sourceColumn));
				listColumns.add(DataTypeConvertor.getColumnBySourceColumn(sourceColumn));
			}
			tes.setColumns(listColumns);
			tes.setSourceColumns(listReferenceColumns);
			tes.setSquidflow_id(squidFlowId);
			tes.setSource_table_id(sourceTableId);//ID
			tes.setTable_name(st.getTable_name());
			tes.setName(getSquidNameAllowChinese(adapter,st.getTable_name(), squidFlowId));//NAME
			if(SquidTypeEnum.CASSANDRA.value()==squidTypeId){
				tes.setSquid_type(SquidTypeEnum.CASSANDRA_EXTRACT.value());
			} else if(SquidTypeEnum.HIVE.value()==squidTypeId){
				tes.setSquid_type(SquidTypeEnum.HIVEEXTRACT.value());
			} else {
				tes.setSquid_type(SquidTypeEnum.EXTRACT.value());
			}
//			x+=20;
			if(fob){
				y+=70;
			}else{
				fob=true;
			}
			tes.setLocation_x(x);
			tes.setLocation_y(y);
			SquidLink sl = new SquidLink();
			esasl.setSquidLink(sl);
			sl.setFrom_squid_id(connectionSquidId);
			sl.setSquid_flow_id(squidFlowId);
			listExtractSquidAndSquidLink.add(esasl);
		}
		return listExtractSquidAndSquidLink;
	}

	public List<ExtractSquidAndSquidLink>  createHiveExtractSquidAndLink(IRelationalDataManager adapter,List<Integer> sourceTableIds,Integer connectionSquidId,Integer squidFlowId,Integer x,Integer y){
		Map<String,String> params = new HashMap<String,String>();
		params.put("id", connectionSquidId+"");
//		原表 获取坐标
//		ISquidDao squidDao = new SquidDaoImpl(adapter);
		Squid dbSourceSquid = adapter.query2Object2(true,params, Squid.class);
		List<ExtractSquidAndSquidLink> listExtractSquidAndSquidLink = new ArrayList<ExtractSquidAndSquidLink>();
		if(x==null){
			x=100;
		}if(y==null){
			y=100;
		}
		boolean fob=false;;
		for (Integer sourceTableId : sourceTableIds) {
			ExtractSquidAndSquidLink esasl = new ExtractSquidAndSquidLink();
			TableExtractSquid tes = new TableExtractSquid();
			esasl.setExtractSquid(tes);
			params.clear();
			params.put("id", sourceTableId+"");
			DBSourceTable st = adapter.query2Object2(true,params, DBSourceTable.class);
			Map<String,Object> paramso = new HashMap<String,Object>();
			paramso.put("source_table_id",sourceTableId+"");
			List<SourceColumn> listSourceColumn = adapter.query2List2(true,paramso, SourceColumn.class);
			List<ReferenceColumn> listReferenceColumns = new ArrayList<ReferenceColumn>();
			List<Column> listColumns = new ArrayList<Column>();
			for (SourceColumn sourceColumn : listSourceColumn) {
				listReferenceColumns.add(DataTypeConvertor.getReferenceColumnBySourceColumn(sourceColumn));
				listColumns.add(DataTypeConvertor.getColumnBySourceColumn(sourceColumn));
			}
			tes.setColumns(listColumns);
			tes.setSourceColumns(listReferenceColumns);
			tes.setSquidflow_id(squidFlowId);
			tes.setSource_table_id(sourceTableId);//ID
			tes.setTable_name(st.getTable_name());
			tes.setName(getSquidName(adapter,st.getTable_name(), squidFlowId));//NAME
			tes.setSquid_type(SquidTypeEnum.HIVEEXTRACT.value());
//			x+=20;
			if(fob){
				y+=70;
			}else{
				fob=true;
			}
			tes.setLocation_x(x);
			tes.setLocation_y(y);
			SquidLink sl = new SquidLink();
			esasl.setSquidLink(sl);
			sl.setFrom_squid_id(connectionSquidId);
			sl.setSquid_flow_id(squidFlowId);
			listExtractSquidAndSquidLink.add(esasl);
		}
		return listExtractSquidAndSquidLink;
	}
	/**
	 * 设置重复名称后自动创建序号
	 * @param adapter3
	 * @param squidName
	 * @param squidFlowId
	 * @return
	 */
	public String getSquidName(IRelationalDataManager adapter3, String squidName, int squidFlowId){
		squidName=squidName.replaceAll("[^a-z,A-Z,0-9]", "_");
		Map<String, Object> params = new HashMap<>();
		int cnt = 1;
		squidName="e_"+squidName;
		squidName = squidName.substring(0,squidName.length()>=25 ? 25 : squidName.length());
		String tempName = squidName;
		while (true) {
			params.put("name", tempName);
			params.put("squid_flow_id", squidFlowId);
			Squid squid = adapter3.query2Object(true, params, Squid.class);
			if (squid==null){
				return tempName;
			}
			if(squid.getName().equals(tempName)){
				tempName = squidName +"_"+ cnt;
			}
			cnt++;
		}
	}

	/**
	 * 设置重复名字，自动创建序号，允许中文
	 * @param adapter3
	 * @param squidName
	 * @param squidFlowId
	 * @return
	 */
	public String getSquidNameAllowChinese(IRelationalDataManager adapter3, String squidName, int squidFlowId){
		//squidName=squidName.replaceAll("[^a-z,A-Z,0-9]", "_");
		Map<String, Object> params = new HashMap<>();
		int cnt = 1;
		squidName="e_"+squidName;
		squidName = squidName.substring(0,squidName.length()>=25 ? 25 : squidName.length());
		String tempName = squidName;
		while (true) {
			params.put("name", tempName);
			params.put("squid_flow_id", squidFlowId);
			Squid squid = adapter3.query2Object(true, params, Squid.class);
			if (squid==null){
				return tempName;
			}
			if(squid.getName().equals(tempName)){
				tempName = squidName +"_"+ cnt;
			}
			cnt++;
		}
	}

	/**
	 * 批量创建TableExtract
	 * 2014-12-29
	 * @author Akachi
	 * @E-Mail zsts@hotmail.com
	 * @param out
	 * @return
	 */
    public void createTableExtractSquids(List<Integer> sourceTableIds, Integer connectionSquidId, Integer squidFlowId, ReturnValue out, String cmd1, String cmd2, String key, Integer x, Integer y) {
        IRelationalDataManager adapter = null;
        try{
			adapter = DataAdapterFactory.getDefaultDataManager();
			adapter.openSession();
			List<ExtractSquidAndSquidLink> packet = createExtractSquidAndSquidLink(adapter,sourceTableIds, connectionSquidId, squidFlowId, x, y);
			int i =0;
			boolean b = false;
			List<Squid> listSquid= new ArrayList<Squid>();
			Map<String,Object> outputMap = new HashMap<String,Object>();
			List<SquidLink> listSquidLink = new ArrayList<SquidLink>();
			for (ExtractSquidAndSquidLink esaql : packet) {
				if(listSquid.size()>0&&listSquid.size()%20==0){
					outputMap.put("Squids", listSquid);
					outputMap.put("SquidLinks", listSquidLink);
					outputMap.put("SuccessCount", i);
		            PushMessagePacket.pushMap(outputMap, DSObjectType.EXTRACT, cmd1, cmd2, key, token,MessageCode.SUCCESS.value());
		            listSquidLink = new ArrayList<SquidLink>();
		            listSquid= new ArrayList<Squid>();
				}
				ExtractServicesub subService = new ExtractServicesub(token, adapter);
				subService.drag2ExtractSquid(esaql, out);
				if(out.getMessageCode()!=null&&out.getMessageCode().equals(MessageCode.SQL_ERROR)){
					b=true;
					continue;
				}
				listSquid.add(esaql.getExtractSquid());
				listSquidLink.add(esaql.getSquidLink());
                i++;
			}
			if(listSquid.size()>0){
				outputMap.put("Squids", listSquid);
				outputMap.put("SquidLinks", listSquidLink);
				outputMap.put("SuccessCount", i);
	            PushMessagePacket.pushMap(outputMap, DSObjectType.EXTRACT, cmd1, cmd2, key, token,MessageCode.SUCCESS.value());
			}
			if(b){
				Map<String, Object> param = new HashMap<String,Object>();
				param.put("SuccessCount", i);
				param.put("Squids", new ArrayList<Squid>());
				param.put("SquidLinks", new ArrayList<SquidLink>());
				PushMessagePacket.pushMap(param, DSObjectType.EXTRACT, cmd1, cmd2, key, token,MessageCode.COMPLETE.value());
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

	/**
	 * 创建HiveExtract的squidandsquidlink
	 * @param sourceTableIds
	 * @param connectionSquidId
	 * @param squidFlowId
	 * @param out
	 * @param cmd1
	 * @param cmd2
	 * @param key
     * @param x
     * @param y
     */
	public void createHiveExtractTable(List<Integer> sourceTableIds,Integer connectionSquidId,Integer squidFlowId,ReturnValue out,String cmd1,String cmd2,String key,Integer x,Integer y){
		IRelationalDataManager adapter = null;
		try{
			adapter = DataAdapterFactory.getDefaultDataManager();
			adapter.openSession();
			List<ExtractSquidAndSquidLink> packet = createExtractSquidAndSquidLink(adapter,sourceTableIds, connectionSquidId, squidFlowId, x, y);
			int i =0;
			boolean b = false;
			List<Squid> listSquid= new ArrayList<Squid>();
			Map<String,Object> outputMap = new HashMap<String,Object>();
			List<SquidLink> listSquidLink = new ArrayList<SquidLink>();
			for (ExtractSquidAndSquidLink esaql : packet) {
				if(listSquid.size()>0&&listSquid.size()%20==0){
					outputMap.put("Squids", listSquid);
					outputMap.put("SquidLinks", listSquidLink);
					outputMap.put("SuccessCount", i);
					PushMessagePacket.pushMap(outputMap, DSObjectType.HIVEEXTRACT, cmd1, cmd2, key, token,MessageCode.SUCCESS.value());
					listSquidLink = new ArrayList<SquidLink>();
					listSquid= new ArrayList<Squid>();
				}
				ExtractServicesub subService = new ExtractServicesub(token, adapter);
				subService.drag2ExtractSquid(esaql, out);
				if(out.getMessageCode()!=null&&out.getMessageCode().equals(MessageCode.SQL_ERROR)){
					b=true;
					continue;
				}
				listSquid.add(esaql.getExtractSquid());
				listSquidLink.add(esaql.getSquidLink());
				i++;
			}
			if(listSquid.size()>0){
				outputMap.put("Squids", listSquid);
				outputMap.put("SquidLinks", listSquidLink);
				outputMap.put("SuccessCount", i);
				PushMessagePacket.pushMap(outputMap, DSObjectType.HIVEEXTRACT, cmd1, cmd2, key, token,MessageCode.SUCCESS.value());
			}
			if(b){
				Map<String, Object> param = new HashMap<String,Object>();
				param.put("SuccessCount", i);
				param.put("Squids", new ArrayList<Squid>());
				param.put("SquidLinks", new ArrayList<SquidLink>());
				PushMessagePacket.pushMap(param, DSObjectType.HIVEEXTRACT, cmd1, cmd2, key, token,MessageCode.COMPLETE.value());
			}
		} catch (Exception e) {
			logger.error("创建extract squid异常", e);
			try {
				adapter.rollback();
			} catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
				logger.fatal("rollback err!", e1);
			}
		} finally {
			if(adapter!=null) {
				adapter.closeSession();
			}
		}
	}

	/**
	 * 批量创建Cassandra Extract
	 * @param sourceTableIds
	 * @param connectionSquidId
	 * @param squidFlowId
	 * @param out
	 * @param cmd1
	 * @param cmd2
	 * @param key
     * @param x
     * @param y
     */
	public void createCassandraExtractTable(List<Integer> sourceTableIds,Integer connectionSquidId,Integer squidFlowId,ReturnValue out,String cmd1,String cmd2,String key,Integer x,Integer y){
		IRelationalDataManager adapter = null;
		try{
			adapter = DataAdapterFactory.getDefaultDataManager();
			adapter.openSession();
			List<ExtractSquidAndSquidLink> packet = createExtractSquidAndSquidLink(adapter,sourceTableIds, connectionSquidId, squidFlowId, x, y);
			int i =0;
			boolean b = false;
			List<Squid> listSquid= new ArrayList<Squid>();
			Map<String,Object> outputMap = new HashMap<String,Object>();
			List<SquidLink> listSquidLink = new ArrayList<SquidLink>();
			for (ExtractSquidAndSquidLink esaql : packet) {
				if(listSquid.size()>0&&listSquid.size()%20==0){
					outputMap.put("Squids", listSquid);
					outputMap.put("SquidLinks", listSquidLink);
					outputMap.put("SuccessCount", i);
					PushMessagePacket.pushMap(outputMap, DSObjectType.CASSANDRA_EXTRACT, cmd1, cmd2, key, token,MessageCode.SUCCESS.value());
					listSquidLink = new ArrayList<SquidLink>();
					listSquid= new ArrayList<Squid>();
				}
				ExtractServicesub subService = new ExtractServicesub(token, adapter);
				subService.drag2ExtractSquid(esaql, out);
				if(out.getMessageCode()!=null&&out.getMessageCode().equals(MessageCode.SQL_ERROR)){
					b=true;
					continue;
				}
				listSquid.add(esaql.getExtractSquid());
				listSquidLink.add(esaql.getSquidLink());
				i++;
			}
			if(listSquid.size()>0){
				outputMap.put("Squids", listSquid);
				outputMap.put("SquidLinks", listSquidLink);
				outputMap.put("SuccessCount", i);
				PushMessagePacket.pushMap(outputMap, DSObjectType.CASSANDRA_EXTRACT, cmd1, cmd2, key, token,MessageCode.SUCCESS.value());
			}
			if(b){
				Map<String, Object> param = new HashMap<String,Object>();
				param.put("SuccessCount", i);
				param.put("Squids", new ArrayList<Squid>());
				param.put("SquidLinks", new ArrayList<SquidLink>());
				PushMessagePacket.pushMap(param, DSObjectType.CASSANDRA_EXTRACT, cmd1, cmd2, key, token,MessageCode.COMPLETE.value());
			}
		} catch (Exception e) {
			logger.error("创建extract squid异常", e);
			try {
				adapter.rollback();
			} catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
				logger.fatal("rollback err!", e1);
			}
		} finally {
			if(adapter!=null) {
				adapter.closeSession();
			}
		}
	}
	

	/**
	 * 根据传入的ExtractSquid,SquidLink创建ExtractSquid信息
	 * 推送
	 * ExtractSquidAndSquidLink对象
	 * 作用描述：
	 * 1、获得extractSquid信息
	 * 2、获得squidLink信息
	 * 3、创建ExtractSquid获得id
	 * 4、获得ExtractSquid的TableName
	 * 5、根据SquidLink的来源ID查询源的DBSourceSquid查询出连接信息切换连接信息查询出该表的列信息
	 * 6、查询SourceTable表是否有储存该Squid表名（是：获得相对应的ID；否：信息再获得ID）
	 * 7、根据查询的SourceColumn信息新增目标Column（squidID为目标id，sourceTableId为查询或新增的ID）
	 * 8、查询SourceSquid是否有Column信息（否：创建Column信息）
	 * 9、新增Transformation信息
	 * 10、新增TransformationLink信息
	 * 11、查询ExtractSquid相关信息组装推送
	 * 验证：
	 * extractSquidAndSquidLinks不能为空
	 * 修改说明：
	 *@param vo  extractSquidAndSquidLink集合对象
	 *@param out 异常处理
	 *@return
	 * @throws EnumException 
	 */
	
	private synchronized void createMoreExtractSquid(
			ExtractSquidAndSquidLink vo,
			String token, ReturnValue out){
		List<String> list = new ArrayList<String>(2);
		Map<String, Object> params = new HashMap<String, Object>(); // 查询参数
		IRelationalDataManager adapter = DataAdapterFactory.newInstance().getDataManagerByToken(token);
		try {
			adapter.openSession();
				//获得ExtractSquid信息
				TableExtractSquid extractSquid = vo.getExtractSquid();
				//获得SquidLink信息
				SquidLink squidLink = vo.getSquidLink();
				squidLink.setType(DSObjectType.SQUIDLINK.value());
				//得到来源ID
				int squidFromId = squidLink.getFrom_squid_id();
				String tableName = extractSquid.getTable_name();
				params.clear();
				params.put("ID", Integer.toString(squidFromId, 10));
				String sourceKey = adapter.query2Object(true, params, Squid.class).getKey();
				list.clear();
				list.add(Integer.toString(squidFromId, 10));
				list.add(tableName);
				List<SourceColumn> sourceColumns = adapter.query2List(true, "select c.* from DS_SOURCE_TABLE t, DS_SOURCE_COLUMN c"
						+ " where t.id=c.source_table_id and t.source_squid_id=? and t.table_name=?", list, SourceColumn.class);
				if(sourceColumns==null || sourceColumns.isEmpty()){
					logger.warn("source squid 列为空！ source_squid_id = "+squidFromId);
					return;
				}
				
				// 需要创建/填充的数据
				List<ReferenceColumn> referenceColumns = new ArrayList<ReferenceColumn>(); //BOHelper.convert(sourceColumns);
				List<Column> columns = new ArrayList<Column>();
				List<Transformation> transformations = new ArrayList<Transformation>();
				List<TransformationLink> transformationLinks = new ArrayList<TransformationLink>();

				// 临时变量
				List<Transformation> tmpTrans = null; // 看来源 transformation 是否已存在
				int tmpFromTransId = 0;
				int tmpToTransId = 0;
				int tmpIndex = 1;

				// 创建 ExtractSquid
				if(com.eurlanda.datashire.utility.StringUtils.isNull(extractSquid.getKey())){
					extractSquid.setKey(StringUtils.generateGUID());
				}
				if(extractSquid.getId()<=0){
					extractSquid.setSquid_type(SquidTypeEnum.EXTRACT.value());
					extractSquid.setId(adapter.insert2(extractSquid));
				}
				// 创建 SquidLink
				squidLink.setTo_squid_id(extractSquid.getId());
				if(com.eurlanda.datashire.utility.StringUtils.isNull(squidLink.getKey())){
					squidLink.setKey(StringUtils.generateGUID());
				}
				if(squidLink.getId()<=0){
					squidLink.setId(adapter.insert2(squidLink));
				}
				
				// 创建引用列组
				ReferenceColumnGroup columnGroup = new ReferenceColumnGroup();
				columnGroup.setKey(StringUtils.generateGUID());
				columnGroup.setReference_squid_id(extractSquid.getId());
				columnGroup.setRelative_order(1);
				columnGroup.setId(adapter.insert2(columnGroup));
				List<ReferenceColumnGroup> columnGroupList = new ArrayList<ReferenceColumnGroup>();
				columnGroupList.add(columnGroup);
				
				for (tmpIndex=0; tmpIndex<sourceColumns.size(); tmpIndex++) {
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
					ref.setData_type(columnInfo.getData_type());
					ref.setKey(StringUtils.generateGUID());
					ref.setName(columnInfo.getName());
					ref.setNullable(columnInfo.isNullable());
					ref.setRelative_order(tmpIndex+1);
					ref.setSquid_id(squidFromId);
					ref.setReference_squid_id(extractSquid.getId());
					ref.setHost_squid_id(squidFromId);
					ref.setIs_referenced(true);
					ref.setGroup_id(columnGroup.getId());
					adapter.insert2(ref);
					referenceColumns.add(ref);
				}
				//columnGroup.setReferenceColumnList(referenceColumns);
				if(extractSquid.getColumns()==null || extractSquid.getColumns().isEmpty()){
					extractSquid.setColumns(columns);
				}else{
					extractSquid.getColumns().addAll(columns);
				}
				if(extractSquid.getSourceColumns()==null || extractSquid.getSourceColumns().isEmpty()){
					extractSquid.setSourceColumns(referenceColumns);
				}else{
					extractSquid.getSourceColumns().addAll(referenceColumns);
				}
		 
				//新增Transformation
				for (tmpIndex=0; tmpIndex<columns.size(); tmpIndex++) {
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
					params.put("SQUID_ID", Integer.toString(squidFromId, 10));
					params.put("column_id", Integer.toString(referenceColumns.get(tmpIndex).getColumn_id(), 10));
					params.put("transformation_type_id", Integer.toString(TransformationTypeEnum.VIRTUAL.value(), 10));
					tmpTrans = adapter.query2List2(true, params, Transformation.class);
					if(tmpTrans == null || tmpTrans.isEmpty()){
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
				 CheckExtractService checkExtractService=new CheckExtractService(token,adapter);
				if(org.apache.commons.lang.StringUtils.isNotBlank(vo.getExtractSquid().getTable_name()))
				{
					 checkExtractService.updateExtract(tableName, squidFromId, "create", out, sourceKey, token);
				}
				Thread.sleep(100);
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
	
	/**
	 * 根据传入的ExtractSquid,SquidLink创建ExtractSquid信息
	 * 推送
	 * ExtractSquidAndSquidLink对象
	 * 作用描述：
	 * 1、获得extractSquid信息
	 * 2、获得squidLink信息
	 * 3、创建ExtractSquid获得id
	 * 4、获得ExtractSquid的TableName
	 * 5、根据SquidLink的来源ID查询源的DBSourceSquid查询出连接信息切换连接信息查询出该表的列信息
	 * 6、查询SourceTable表是否有储存该Squid表名（是：获得相对应的ID；否：信息再获得ID）
	 * 7、根据查询的SourceColumn信息新增目标Column（squidID为目标id，sourceTableId为查询或新增的ID）
	 * 8、查询SourceSquid是否有Column信息（否：创建Column信息）
	 * 9、新增Transformation信息
	 * 10、新增TransformationLink信息
	 * 11、查询ExtractSquid相关信息组装推送
	 * 验证：
	 * extractSquidAndSquidLinks不能为空
	 * 修改说明：
	 *@param extractSquidAndSquidLinks  extractSquidAndSquidLink集合对象
	 *@param out 异常处理
	 *@return
	 * @throws EnumException 
	 */
	
	public synchronized void createMoreExtractSquid(
			List<ExtractSquidAndSquidLink> extractSquidAndSquidLinks,
			String token, ReturnValue out){
		List<String> list = new ArrayList<String>(2);
		Map<String, Object> params = new HashMap<String, Object>(); // 查询参数
		IRelationalDataManager adapter = DataAdapterFactory.newInstance().getDataManagerByToken(token);
		try {
			adapter.openSession();
			for (ExtractSquidAndSquidLink vo : extractSquidAndSquidLinks) {
				//获得ExtractSquid信息
				TableExtractSquid extractSquid = vo.getExtractSquid();
				//获得SquidLink信息
				SquidLink squidLink = vo.getSquidLink();
				squidLink.setType(DSObjectType.SQUIDLINK.value());
				//得到来源ID
				int squidFromId = squidLink.getFrom_squid_id();
				String tableName = extractSquid.getTable_name();
				params.clear();
				params.put("ID", Integer.toString(squidFromId, 10));
				String sourceKey = adapter.query2Object(true, params, Squid.class).getKey();
				list.clear();
				list.add(Integer.toString(squidFromId, 10));
				list.add(tableName);
				List<SourceColumn> sourceColumns = adapter.query2List(true, "select c.* from DS_SOURCE_TABLE t, DS_SOURCE_COLUMN c"
						+ " where t.id=c.source_table_id and t.source_squid_id=? and t.table_name=?", list, SourceColumn.class);
				if(sourceColumns==null || sourceColumns.isEmpty()){
					logger.warn("source squid 列为空！ source_squid_id = "+squidFromId);
					continue;
				}
				
				// 需要创建/填充的数据
				List<ReferenceColumn> referenceColumns = new ArrayList<ReferenceColumn>(); //BOHelper.convert(sourceColumns);
				List<Column> columns = new ArrayList<Column>();
				List<Transformation> transformations = new ArrayList<Transformation>();
				List<TransformationLink> transformationLinks = new ArrayList<TransformationLink>();

				// 临时变量
				List<Transformation> tmpTrans = null; // 看来源 transformation 是否已存在
				int tmpFromTransId = 0;
				int tmpToTransId = 0;
				int tmpIndex = 1;

				// 创建 ExtractSquid
				if(com.eurlanda.datashire.utility.StringUtils.isNull(extractSquid.getKey())){
					extractSquid.setKey(StringUtils.generateGUID());
				}
				if(extractSquid.getId()<=0){
					extractSquid.setSquid_type(SquidTypeEnum.EXTRACT.value());
					extractSquid.setId(adapter.insert2(extractSquid));
				}
				// 创建 SquidLink
				squidLink.setTo_squid_id(extractSquid.getId());
				if(com.eurlanda.datashire.utility.StringUtils.isNull(squidLink.getKey())){
					squidLink.setKey(StringUtils.generateGUID());
				}
				if(squidLink.getId()<=0){
					squidLink.setId(adapter.insert2(squidLink));
				}
				
				// 创建引用列组
				ReferenceColumnGroup columnGroup = new ReferenceColumnGroup();
				columnGroup.setKey(StringUtils.generateGUID());
				columnGroup.setReference_squid_id(extractSquid.getId());
				columnGroup.setRelative_order(1);
				columnGroup.setId(adapter.insert2(columnGroup));
				List<ReferenceColumnGroup> columnGroupList = new ArrayList<ReferenceColumnGroup>();
				columnGroupList.add(columnGroup);
				
				for (tmpIndex=0; tmpIndex<sourceColumns.size(); tmpIndex++) {
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
					ref.setData_type(columnInfo.getData_type());
					ref.setKey(StringUtils.generateGUID());
					ref.setName(columnInfo.getName());
					ref.setNullable(columnInfo.isNullable());
					ref.setRelative_order(tmpIndex+1);
					ref.setSquid_id(squidFromId);
					ref.setReference_squid_id(extractSquid.getId());
					ref.setHost_squid_id(squidFromId);
					ref.setIs_referenced(true);
					ref.setGroup_id(columnGroup.getId());
					adapter.insert2(ref);
					referenceColumns.add(ref);
				}
				//columnGroup.setReferenceColumnList(referenceColumns);
				if(extractSquid.getColumns()==null || extractSquid.getColumns().isEmpty()){
					extractSquid.setColumns(columns);
				}else{
					extractSquid.getColumns().addAll(columns);
				}
				if(extractSquid.getSourceColumns()==null || extractSquid.getSourceColumns().isEmpty()){
					extractSquid.setSourceColumns(referenceColumns);
				}else{
					extractSquid.getSourceColumns().addAll(referenceColumns);
				}
		 
				//新增Transformation
				for (tmpIndex=0; tmpIndex<columns.size(); tmpIndex++) {
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
					params.put("SQUID_ID", Integer.toString(squidFromId, 10));
					params.put("column_id", Integer.toString(referenceColumns.get(tmpIndex).getColumn_id(), 10));
					params.put("transformation_type_id", Integer.toString(TransformationTypeEnum.VIRTUAL.value(), 10));
					tmpTrans = adapter.query2List2(true, params, Transformation.class);
					if(tmpTrans == null || tmpTrans.isEmpty()){
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
				 CheckExtractService checkExtractService=new CheckExtractService(token,adapter);
				if(org.apache.commons.lang.StringUtils.isNotBlank(vo.getExtractSquid().getTable_name()))
				{
					 checkExtractService.updateExtract(tableName, squidFromId, "create", out, sourceKey, token);
				}
				Thread.sleep(100);
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
	
	/**
	 * 创建DestinationSquid对象集合
	 * 作用描述：
	 * stageSquid对象集合不能为空且必须有数据
	 * stageSquid对象集合里的每个Key必须有值
	 * 根据传入的stageSquid对象集合创建stageSquid对象
	 * 创建成功——根据stageSquid集合对象里的Key查询相对应的ID
	 * 修改说明：
	 *@param dbDestinationSquids dbDestinationSquid集合对象
	 *@param out 异常处理
	 *@return
	 */
	public List<InfoPacket> createDestinationSquids(List<DBDestinationSquid> dbDestinationSquids,ReturnValue out){
		logger.debug(String.format("createDestinationSquids-dbDestinationSquidList.size()=%s", dbDestinationSquids.size()));
		//定义接收返回结果集
		List<InfoPacket> infoPackets = new ArrayList<InfoPacket>();
		boolean create = false;
		try {
			//封装批量新增数据集
			List<List<DataEntity>>  paramList = new ArrayList<List<DataEntity>>();
			//调用转换类
			GetParam getParam = new GetParam();
			for (DBDestinationSquid dbDestinationSquid : dbDestinationSquids) {
				List<DataEntity> dataEntitys =  new ArrayList<DataEntity>();
				getParam.getDBDestinationSquid(dbDestinationSquid, dataEntitys);
				paramList.add(dataEntitys);
			}
			//调用批量新增
			create = adapter.createBatch(SystemTableEnum.DS_SQUID.toString(), paramList, out);
			//新增成功
			if(create){
				//根据Key查询出新增的ID
				for (DBDestinationSquid dbDestinationSquid : dbDestinationSquids) {
					SquidPlug squidPlug = new SquidPlug(adapter);
					//获得Key
					String key = dbDestinationSquid.getKey();
					//根据Key获得ID
					int id = squidPlug.getSquidId(key, out);
					InfoPacket infoPacket = new InfoPacket();
					infoPacket.setId(id);
					infoPacket.setKey(key);
					infoPacket.setType(DSObjectType.DBSOURCE);
					infoPackets.add(infoPacket);
                    CommonConsts.addValidationTask(new SquidValidationTask(token, null, MessageBubbleService.setMessageBubble(infoPacket.getId(), infoPacket.getId(), null, MessageBubbleCode.WARN_SQUID_NO_LINK.value())));
				}
			}
			logger.debug(String.format("createDestinationSquids-return=%s",infoPackets ));
		} catch (Exception e) {
			logger.error("createDestinationSquids-return=%s", e);
			out.setMessageCode(MessageCode.ERR_ARRAYS);
		}finally{
			//释放连接
			adapter.commitAdapter();
		}
		return infoPackets;
	}
	
	/**
	 * DestinationSquid
	 * 作用描述：
	 * 修改DestinationSquid对象集合
	 * 调用方法前确保DestinationSquid对象集合不为空，并且DestinationSquid对象集合里的每个DestinationSquid对象的Id不为空
	 * 修改说明：
	 *@param dbDestinationSquids dbDestinationSquid集合对象
	 *@param out 异常处理
	 *@return
	 */
	public List<InfoPacket> updateDestinationSquids(List<DbSquid> dbDestinationSquids,ReturnValue out){
		logger.debug(String.format("updateDestinationSquids-dbDestinationSquidList.size()=%s", dbDestinationSquids.size()));
		List<InfoPacket> infoPackets = new ArrayList<InfoPacket>(dbDestinationSquids.size());
		DBSquidService dbSquidService = new DBSquidService(token);
		//封装批量新增数据集
		List<List<DataEntity>>  paramList = new ArrayList<List<DataEntity>>(dbDestinationSquids.size());
		//封装条件集合
		List<List<WhereCondition>>  whereCondList = new ArrayList<List<WhereCondition>>(dbDestinationSquids.size());
		try {
			//调用转换类
			GetParam getParam = new GetParam();
			for (DbSquid dbDestinationSquid: dbDestinationSquids) {
				DbSquid connection = dbSquidService.getDBSquid(dbDestinationSquid.getId());
				if(connection==null){
					dbSquidService.add(dbDestinationSquid);
				}else{
					dbSquidService.update(dbDestinationSquid);
				}
				//参数
				List<DataEntity> dataEntitys =  new ArrayList<DataEntity>();
				getParam.getDBDestinationSquid(dbDestinationSquid, dataEntitys);
				paramList.add(dataEntitys);
				//条件
				List<WhereCondition> whereConditions = new ArrayList<WhereCondition>(1);
				whereConditions.add(new WhereCondition("ID", MatchTypeEnum.EQ, dbDestinationSquid.getId()));
				whereCondList.add(whereConditions);
				
				InfoPacket infoPacket = new InfoPacket();
				infoPacket.setCode(1);
				infoPacket.setId(dbDestinationSquid.getId());
				infoPacket.setType(DSObjectType.DBSOURCE);
				infoPacket.setKey(dbDestinationSquid.getKey());
				infoPackets.add(infoPacket);
			}
			//调用批量修改
			adapter.updatBatch(SystemTableEnum.DS_SQUID.toString(), paramList, whereCondList, out);
		} catch (Exception e) {
			logger.error("updateDestinationSquids-return=%s",e);
			out.setMessageCode(MessageCode.ERR_ARRAYS);
		}finally{
			adapter.commitAdapter();
		}
		return infoPackets;
	}
	
	/**
	 * 根据传入的column对象集，Transformation对象集创建相对应的数据
	 * 作用描述：
	 * 验证：
	 * 调用方法前确保TransformationAndCloumn对象集合不为空
	 * 调用根据获得的对象集调用批量新增的方法，创建错误返回
	 * 成功：根据key查询ID返回InfoPacket集合对象
	 * 修改说明：
	 *@param transformationAndCloumns transformationAndCloumn集合对象
	 *@param out 异常处理
	 *@return
	 */
	public List<InfoPacket> createMoreStageSquid(
			List<TransformationAndCloumn> transformationAndCloumns,
			ReturnValue out) {
		logger.debug(String.format(
				"createStageSquids-transformationAndCloumns=%s",
				transformationAndCloumns));
		boolean create = false;
		List<InfoPacket> infoPackets = new ArrayList<InfoPacket>();
		// 实例化ColumnPlug对象
		ColumnPlug columnPlug = new ColumnPlug(adapter);
		TransformationPlug transformationPlug = new TransformationPlug(adapter);
		List<Column> columns = new ArrayList<Column>();
		List<Transformation> transformations = new ArrayList<Transformation>();
		try {
			// 循环遍历获得数据
			for (TransformationAndCloumn transformationAndCloumn : transformationAndCloumns) {
				columns.add(transformationAndCloumn.getColumn());
				transformations
						.add(transformationAndCloumn.getTransformation());
			}
			create = columnPlug.createColumns(columns, out);
			if (create == false) {
				
				// 错误记录
				return infoPackets;
			}
			create = transformationPlug.createTransformations(transformations,
					out);
			if (create == false) {
				
				// 错误记录
				return infoPackets;
			}
			for (TransformationAndCloumn transformationAndCloumn : transformationAndCloumns) {
				InfoPacket column = new InfoPacket();
				InfoPacket transformation = new InfoPacket();
				String columnKey = transformationAndCloumn.getColumn().getKey();
				String transformationKey = transformationAndCloumn
						.getTransformation().getKey();
				int columnId = columnPlug.getColumnId(columnKey, out);
				int transformationId = transformationPlug.getTransformationId(
						transformationKey, out);
				column.setId(columnId);
				column.setKey(columnKey);
				column.setType(DSObjectType.COLUMN);

				transformation.setId(transformationId);
				transformation.setKey(transformationKey);
				transformation.setType(DSObjectType.TRANSFORMATION);
				infoPackets.add(column);
				infoPackets.add(transformation);
			}
		} catch (Exception e) {
			logger.error("createStageSquids-transformationAndCloumns=%s", e);
		} finally {
			adapter.commitAdapter();
		}
		return infoPackets;
	}

	/**
	 * 修改ExtractSquid
	 * 作用描述：
	 * 修改extractSquid对象集合
	 * 调用方法前确保extractSquid对象集合不为空，并且extractSquid对象集合里的每个extractSquid对象的Id不为空
	 * 修改说明：
	 *@param extractSquids extractSquid集合对象
	 *@param out 异常处理
	 *@return
	 */
	public List<InfoPacket> updateExtractSquids(List<TableExtractSquid> extractSquids,ReturnValue out){
		logger.debug(String.format("updateExtractSquids-extractSquidList.size()=%s", extractSquids.size()));
		boolean create = false;
		List<InfoPacket> infoPackets = new ArrayList<InfoPacket>();
		try {
			//封装批量新增数据集
			List<List<DataEntity>>  paramList = new ArrayList<List<DataEntity>>();
			//封装条件集合
			List<List<WhereCondition>>  whereCondList = new ArrayList<List<WhereCondition>>(1);
			//调用转换类
			GetParam getParam = new GetParam();
			for (TableExtractSquid extractSquid: extractSquids) {
				//参数
				List<DataEntity> dataEntitys =  new ArrayList<DataEntity>();
				getParam.getSquid(extractSquid, dataEntitys);
				paramList.add(dataEntitys);
				//条件
				List<WhereCondition> whereConditions = new ArrayList<WhereCondition>(1);
				whereConditions.add(new WhereCondition("ID", MatchTypeEnum.EQ, extractSquid.getId()));
				whereCondList.add(whereConditions);
			}
			//调用批量修改
			create = adapter.updatBatch(SystemTableEnum.DS_SQUID.toString(), paramList, whereCondList, out);
			logger.debug(String.format("updateExtractSquids-return=%s",create ));
			
			//查询返回结果
			if(create){
				SquidPlug  squidPlug = new SquidPlug(adapter);
				//定义接收返回结果集
				infoPackets = new ArrayList<InfoPacket>();
				//根据Key查询出新增的ID
				for (TableExtractSquid extractSquid : extractSquids) {
					//获得Key
					String key = extractSquid.getKey();
					int id = squidPlug.getSquidId(key, out);
					InfoPacket infoPacket = new InfoPacket();
					infoPacket.setCode(1);
					infoPacket.setId(id);
					infoPacket.setType(DSObjectType.SQUID);
					infoPacket.setKey(key);
					infoPackets.add(infoPacket);
				}
			}
		} catch (Exception e) {
			logger.error("updateExtractSquids-return=%s",e);
			out.setMessageCode(MessageCode.ERR_ARRAYS);
		}finally{ //释放
			adapter.commitAdapter();
		}
		return infoPackets;
	}
	
	/**
	 * 
	 * 作用描述：
	 * 修改stageSquid对象集合
	 * 调用方法前确保stageSquid对象集合不为空，并且stageSquid对象集合里的每个stageSquid对象的Id不为空
	 * 修改说明：
	 *@param stageSquids stageSquid集合对象
	 *@param out 异常处理
	 *@return
	 */
	public List<InfoPacket> updateStageSquids(List<StageSquid> stageSquids,ReturnValue out){
		List<InfoPacket> infoPackets = new ArrayList<InfoPacket>();
		try {
			//封装批量新增数据集
			List<List<DataEntity>>  paramList = new ArrayList<List<DataEntity>>(1);
			//封装条件集合
			List<List<WhereCondition>>  whereCondList = new ArrayList<List<WhereCondition>>(1);
			//调用转换类
			GetParam getParam = new GetParam();
			for (StageSquid stageSquid: stageSquids) {
				//参数
				List<DataEntity> dataEntitys =  new ArrayList<DataEntity>(1);
				getParam.getSquid(stageSquid, dataEntitys);
				paramList.add(dataEntitys);
				//条件
				List<WhereCondition> whereConditions = new ArrayList<WhereCondition>(1);
				whereConditions.add(new WhereCondition("ID", MatchTypeEnum.EQ, stageSquid.getId()));
				whereCondList.add(whereConditions);
				
				infoPackets.add(new InfoPacket(stageSquid.getId(), stageSquid.getKey(),
						DSObjectType.STAGE, MessageCode.SUCCESS.value()));
			}
			//调用批量修改
			adapter.updatBatch(SystemTableEnum.DS_SQUID.toString(), paramList, whereCondList, out);
		} catch (Exception e) {
			logger.error("updateStageSquids-return=%s", e);
			out.setMessageCode(MessageCode.ERR_ARRAYS);
		}finally{ //释放
			adapter.commitAdapter();
		}
		return infoPackets;
	}
	
	/**
	 * 修改Squid
	 * 作用描述：
	 * 修改说明：
	 *@param squids squid对象集合
	 *@param out 异常处理
	 *@return
	 */
	public boolean updateSquids(List<Squid> squids,ReturnValue out){
		boolean update = false;
		//封装批量新增数据集
		List<List<DataEntity>>  paramList = new ArrayList<List<DataEntity>>(squids.size());
		//封装条件集合
		List<List<WhereCondition>>  whereCondList = new ArrayList<List<WhereCondition>>(squids.size());
		try {
			for (int i=0; i<squids.size(); i++) {
				// 调试：看前台到底一次发送了多了squid
				logger.debug(i+"/"+squids.size()+"\t[更新squid]\t"+squids.get(i).getId()
						+"\tx*y:"+squids.get(i).getLocation_x()+"*"+squids.get(i).getLocation_y()
						+"\tx*y:"+squids.get(i).getSource_transformation_group_x()+"*"+squids.get(i).getSource_transformation_group_y()
						);
				if(squids.get(i).getId()<=0){
					continue; // 这些应该是创建失败的squid，数据库里没有的，所以更新没意义
				}
				//参数
				List<DataEntity> dataEntitys =  new ArrayList<DataEntity>(4);
				//封装成DataEntity集合对象
				dataEntitys.add(new DataEntity("LOCATION_X", DataStatusEnum.INT, squids.get(i).getLocation_x()));
				dataEntitys.add(new DataEntity("LOCATION_Y", DataStatusEnum.INT, squids.get(i).getLocation_y()));
				dataEntitys.add(new DataEntity("SQUID_HEIGHT", DataStatusEnum.INT, squids.get(i).getSquid_height()));
				dataEntitys.add(new DataEntity("SQUID_WIDTH", DataStatusEnum.INT, squids.get(i).getSquid_width()));
				
				dataEntitys.add(new DataEntity("reference_group_x", DataStatusEnum.INT, squids.get(i).getSource_transformation_group_x()));
				dataEntitys.add(new DataEntity("reference_group_y", DataStatusEnum.INT, squids.get(i).getSource_transformation_group_y()));
				dataEntitys.add(new DataEntity("column_group_x", DataStatusEnum.INT, squids.get(i).getTransformation_group_x()));
				dataEntitys.add(new DataEntity("column_group_y", DataStatusEnum.INT, squids.get(i).getTransformation_group_y()));
				paramList.add(dataEntitys);
				//条件
				List<WhereCondition> whereConditions = new ArrayList<WhereCondition>(1);
				whereConditions.add(new WhereCondition("ID", MatchTypeEnum.EQ, squids.get(i).getId()));
				whereCondList.add(whereConditions);
			}
			//调用批量修改
			update = adapter.updatBatch(SystemTableEnum.DS_SQUID.toString(), paramList, whereCondList, out);
			logger.debug("[UpdateSquids] result="+update+", count="+squids.size());
		} catch (Exception e) {
			logger.error("updateSquids-return=%s", e);
			out.setMessageCode(MessageCode.ERR_ARRAYS);
		} finally { //释放
			if(adapter!=null)adapter.commitAdapter();
		}
		return update;
	}
	
	/**
	 * 修改DbsourceSquid
	 * 作用描述：
	 * 修改dbSourceSquids对象集合
	 * 调用方法前确保dbSourceSquids对象集合不为空，并且dbSourceSquids对象集合里的每个dbSourceSquids对象的Id不为空
	 * 根据SquidId查询是否保存了connection信息，储存的情况下进行修改连接信息，没有的情况下进行新增
	 * 修改说明：
	 * 该方法的Squid位置不需要更新
	 *@param dbSourceSquids dbSourceSquid集合对象
	 *@param out 异常处理
	 *@return
	 */
	public List<DbSquid> updatedbSourceSquids2(List<DbSquid> dbSourceSquids,IRelationalDataManager adapter2,ReturnValue out){
		logger.debug(String.format("dbSourceSquids-DBSourceSquidList.size()=%s", dbSourceSquids.size()));
		boolean update = false;
		DBSquidService dbSquidService = new DBSquidService(token);
		//返回集合
		List<DbSquid> dbSourceSquidList = new ArrayList<DbSquid>();
		try {
			//封装批量新增数据集
			List<List<DataEntity>>  paramList = new ArrayList<List<DataEntity>>();
			//封装条件集合
			List<List<WhereCondition>>  whereCondList = new ArrayList<List<WhereCondition>>(1);
			//调用转换类
			GetParam getParam = new GetParam();
			for (DbSquid dbSourceSquid: dbSourceSquids) {
				DbSquid connection = dbSquidService.getDBSquid(dbSourceSquid.getId());
				logger.debug("squid_id="+dbSourceSquid.getId()+"\t"+connection);
				Dbconnection dbconnection = new Dbconnection();
				DbSquid dbSquid = new DbSquid();
				dbconnection.setConnectionString(dbSourceSquid.getConnectionString());
				dbconnection.setDb_name(dbSourceSquid.getDb_name());
				dbconnection.setDb_type(dbSourceSquid.getDb_type());
				dbconnection.setHost(dbSourceSquid.getHost());
				dbconnection.setPort(dbSourceSquid.getPort());
				dbconnection.setPassword(dbSourceSquid.getPassword());
				dbconnection.setUser_name(dbSourceSquid.getUser_name());
				dbconnection.setId(dbSourceSquid.getId());
				dbSquid.setConnectionString(dbSourceSquid.getConnectionString());
				dbSquid.setDb_name(dbSourceSquid.getDb_name());
				dbSquid.setDb_type(dbSourceSquid.getDb_type());
				dbSquid.setHost(dbSourceSquid.getHost());
				dbSquid.setPort(dbSourceSquid.getPort());
				dbSquid.setPassword(dbSourceSquid.getPassword());
				dbSquid.setUser_name(dbSourceSquid.getUser_name());
				dbSquid.setId(dbSourceSquid.getId());
				if(connection==null){
					dbSquidService.add(dbconnection);
				}else{
					dbSquidService.update(dbconnection);
				}
				//参数
				List<DataEntity> dataEntitys =  new ArrayList<DataEntity>();
				//封装成DataEntity集合对象
				getParam.getSourceSquid2(dbSourceSquid, dataEntitys);
				paramList.add(dataEntitys);
				//条件
				List<WhereCondition> whereConditions = new ArrayList<WhereCondition>(1);
				whereConditions.add(new WhereCondition("ID", MatchTypeEnum.EQ, dbSourceSquid.getId()));
				whereCondList.add(whereConditions);
				//SourcesTable
				String info = JsonUtil.toGsonString(dbSourceSquids);
				ConnectProcess connectProcess = new ConnectProcess(token);
				List<SourceTable> sourceTables = connectProcess.getConnect2(dbSourceSquids,adapter2, out);
				dbSourceSquid.setSourceTableList(sourceTables);
				dbSourceSquidList.add(dbSourceSquid);
			}
			
			//调用批量修改
			update = adapter.updatBatch(SystemTableEnum.DS_SQUID.toString(), paramList, whereCondList, out);
			logger.debug(String.format("dbSourceSquids-return=%s",update ));
			//List<InfoPacket> infoPackets = null;
			//查询返回结果
			//推送 Connection连接信息是否完整、正确消息泡
			MessageBubbleService messageBubbleService=new MessageBubbleService(token);
			messageBubbleService.connectionValidation(dbSourceSquids);
			
		} catch (Exception e) {
			logger.error("error", e);
			out.setMessageCode(MessageCode.ERR_ARRAYS);
			return null;
		}finally{
			adapter.commitAdapter();
		}
		return dbSourceSquidList;
	}
	
	public List<DbSquid> updatedbSourceSquids(List<DbSquid> dbSourceSquids,ReturnValue out){
		logger.debug(String.format("dbSourceSquids-DBSourceSquidList.size()=%s", dbSourceSquids.size()));
		boolean update = false;
		DBSquidService dbSquidService = new DBSquidService(token);
		//返回集合
		List<DbSquid> dbSourceSquidList = new ArrayList<DbSquid>();
		try {
			//封装批量新增数据集
			List<List<DataEntity>>  paramList = new ArrayList<List<DataEntity>>();
			//封装条件集合
			List<List<WhereCondition>>  whereCondList = new ArrayList<List<WhereCondition>>(1);
			//调用转换类
			GetParam getParam = new GetParam();
			for (DbSquid dbSourceSquid: dbSourceSquids) {
				DbSquid connection = dbSquidService.getDBSquid(dbSourceSquid.getId());
				logger.debug("squid_id="+dbSourceSquid.getId()+"\t"+connection);
				if(connection==null){
					dbSquidService.add(dbSourceSquid);
				}else{
					dbSquidService.update(dbSourceSquid);
				}

				//参数
				List<DataEntity> dataEntitys =  new ArrayList<DataEntity>();
				//封装成DataEntity集合对象
				getParam.getSourceSquid(dbSourceSquid, dataEntitys);
				paramList.add(dataEntitys);
				//条件
				List<WhereCondition> whereConditions = new ArrayList<WhereCondition>(1);
				whereConditions.add(new WhereCondition("ID", MatchTypeEnum.EQ, dbSourceSquid.getId()));
				whereCondList.add(whereConditions);
			}
			//调用批量修改
			update = adapter.updatBatch(SystemTableEnum.DS_SQUID.toString(), paramList, whereCondList, out);
			logger.debug(String.format("dbSourceSquids-return=%s", update));
			//List<InfoPacket> infoPackets = null;
			//查询返回结果
/*			if(update){
				//定义接收返回结果集
				//根据Key查询出新增的ID
				DataPlug dataPlug = new DataPlug(token);
				//dbSourceSquidList.add(e);
				for (DbSquid sourceSquid : dbSourceSquids) {
					sourceSquid.setType(DSObjectType.SOURCE.value());
					
					// 如果前台返回了该信息，就没必要再查一次了（因为只是更新基本信息，source table list不会改）
					//获得该squid的表名及列信息
					List<SourceTable>  sourceTables = dataPlug.getSourceTable(sourceSquid.getId(), out);
					sourceSquid.setSourceTableList(sourceTables);
					
					dbSourceSquidList.add(sourceSquid);
				}
			}*/
		} catch (Exception e) {
			logger.error("error", e);
			out.setMessageCode(MessageCode.ERR_ARRAYS);
			return null;
		}finally{
			adapter.commitAdapter();
		}
		return dbSourceSquidList;
	}
	/**
	 * 新增DbsourceSquid
	 * 作用描述：
	 * 修改dbSourceSquids对象集合
	 * 调用方法前确保dbSourceSquids对象集合不为空，并且dbSourceSquids对象集合里的每个dbSourceSquids对象的Id不为空
	 * 根据SquidId查询是否保存了connection信息，储存的情况下进行修改连接信息，没有的情况下进行新增
	 * 修改说明：
	 *@param dbSourceSquids dbSourceSquid集合对象
	 *@param out 异常处理
	 *@return
	 */
	public List<DbSquid> dorpdbSourceSquids(List<DbSquid> dbSourceSquids,ReturnValue out){
		logger.debug(String.format("dbSourceSquids-DBSourceSquidList.size()=%s", dbSourceSquids.size()));
		boolean update = false;
		DBSquidService dbSquidService = new DBSquidService(token);
		//返回集合
		List<DbSquid> dbSourceSquidList = new ArrayList<DbSquid>();
		try {
			//封装批量新增数据集
			List<List<DataEntity>>  paramList = new ArrayList<List<DataEntity>>();
			//封装条件集合
			List<List<WhereCondition>>  whereCondList = new ArrayList<List<WhereCondition>>(1);
			//调用转换类
			GetParam getParam = new GetParam();
			for (DbSquid dbSourceSquid: dbSourceSquids) {
				DbSquid connection = dbSquidService.getDBSquid(dbSourceSquid.getId());
				logger.debug("squid_id="+dbSourceSquid.getId()+"\t"+connection);
				if(connection==null){
					dbSquidService.add(dbSourceSquid);
				}else{
					dbSquidService.update(dbSourceSquid);
				}
				//参数
				List<DataEntity> dataEntitys =  new ArrayList<DataEntity>();
				//封装成DataEntity集合对象
				getParam.getSourceSquid(dbSourceSquid, dataEntitys);
				paramList.add(dataEntitys);
				//条件
				List<WhereCondition> whereConditions = new ArrayList<WhereCondition>(1);
				whereConditions.add(new WhereCondition("ID", MatchTypeEnum.EQ, dbSourceSquid.getId()));
				whereCondList.add(whereConditions);
			}
			//调用批量修改
		//	update = adapter.insert(SystemTableEnum.DS_SQUID.toString(), paramList,whereCondList, out);
			logger.debug(String.format("dbSourceSquids-return=%s",update ));
			//List<InfoPacket> infoPackets = null;
			//查询返回结果
/*			if(update){
				//定义接收返回结果集
				//根据Key查询出新增的ID
				DataPlug dataPlug = new DataPlug(token);
				//dbSourceSquidList.add(e);
				for (DbSquid sourceSquid : dbSourceSquids) {
					sourceSquid.setType(DSObjectType.SOURCE.value());
					
					// 如果前台返回了该信息，就没必要再查一次了（因为只是更新基本信息，source table list不会改）
					//获得该squid的表名及列信息
					List<SourceTable>  sourceTables = dataPlug.getSourceTable(sourceSquid.getId(), out);
					sourceSquid.setSourceTableList(sourceTables);
					
					dbSourceSquidList.add(sourceSquid);
				}
			}*/
			//推送 Connection连接信息是否完整、正确消息泡
			MessageBubbleService messageBubbleService=new MessageBubbleService(token);
			messageBubbleService.connectionValidation(dbSourceSquids);
		} catch (Exception e) {
			logger.error("error", e);
			out.setMessageCode(MessageCode.ERR_ARRAYS);
			return null;
		}finally{
			adapter.commitAdapter();
		}
		return dbSourceSquidList;
	}
	/**
	 * 创建dbSourceSquid对象集合
	 * 作用描述：
	 * dbSourceSquid对象集合不能为空且必须有数据
	 * dbSourceSquid对象集合里的每个Key必须有值
	 * 根据传入的dbSourceSquid对象集合创建dbSourceSquid对象
	 * 创建成功——根据dbSourceSquid集合对象里的Key查询相对应的ID
	 * 修改说明：
	 *@param dbSourceSquids dbSourceSquid集合对象
	 *@param out 异常处理
	 *@return
	 */
	public List<InfoPacket> createDBSourceSquids(List<DBSourceSquid> dbSourceSquids,ReturnValue out){
		logger.debug(String.format("createDBSourceSquids-dbSourceSquidList.size()=%s", dbSourceSquids.size()));
		//定义接收返回结果集
		List<InfoPacket> infoPackets = new ArrayList<InfoPacket>();
		boolean create = false;
		try {
			SquidPlug squidPlug = new SquidPlug(adapter);
			create = squidPlug.createDBSourceSquids(dbSourceSquids, out);
			//新增成功
		
			if(create){
				//根据Key查询出新增的ID
				for (DBSourceSquid sourceSquid : dbSourceSquids) {
					//获得Key
					String key = sourceSquid.getKey();
					//根据Key获得ID
					int id = squidPlug.getSquidId(key, out);
					InfoPacket infoPacket = new InfoPacket();
					infoPacket.setId(id);
					infoPacket.setKey(key);
					infoPacket.setType(DSObjectType.DBSOURCE);
					infoPackets.add(infoPacket);
				}
			}
			logger.debug(String.format("createDBSourceSquids-return=%s",infoPackets ));
		} catch (Exception e) {
			logger.error("createDBSourceSquids-return=%s", e);
			out.setMessageCode(MessageCode.ERR_ARRAYS);
		}finally{
			//释放连接
			adapter.commitAdapter();
		}
		
		return infoPackets;
		
	}

	/**
	 * 创建extractSquid对象集合
	 * 作用描述：
	 * extractSquid对象集合不能为空且必须有数据
	 * extractSquid对象集合里的每个Key必须有值
	 * 根据传入的extractSquid对象集合创建extractSquid对象
	 * 创建成功——根据extractSquid集合对象里的Key查询相对应的ID
	 * 修改说明：
	 *@param extractSquids extractSquid集合对象
	 *@param out 异常处理
	 *@return
	 */
	public List<InfoPacket> createExtractSquids(List<TableExtractSquid> extractSquids,ReturnValue out){
		logger.debug(String.format("createExtractSquids-extractSquidList.size()=%s", extractSquids.size()));
		boolean create = false;
		//定义接收返回结果集
		List<InfoPacket> infoPackets = new ArrayList<InfoPacket>();
		try {
			SquidPlug squidPlug = new SquidPlug(adapter);
			//调用批量新增
			create = squidPlug.createExtractSquids(extractSquids, out);
			//新增成功
			if(create){
				//根据Key查询出新增的ID
				for (TableExtractSquid extractSquid : extractSquids) {
					//获得Key
					String key = extractSquid.getKey();
					//根据Key获得ID
					int id = squidPlug.getSquidId(key, out);
					InfoPacket infoPacket = new InfoPacket();
					infoPacket.setId(id);
					infoPacket.setKey(key);
					infoPacket.setType(DSObjectType.EXTRACT);
					infoPackets.add(infoPacket);
				}
			}
			logger.debug(String.format("createDBSourceSquids-return=%s",infoPackets ));
		} catch (Exception e) {
			logger.error("createDBSourceSquids-return=%s", e);
			out.setMessageCode(MessageCode.ERR_ARRAYS);
		}finally{ //释放连接
			adapter.commitAdapter();
		}
		return infoPackets;
	}
	
	/**
	 * 
	 * @param squidList
	 * @return
	 * @author bo.dang
	 * @date 2014-4-26
	 */
	public Map<String, Object> paseSquid(List<Squid> squidList){
		
		if(null != squidList){
		    Squid squid = null;
		    DSObjectType type = null;
		    Integer squidId = null;
			for(int i=0; i<squidList.size(); i++){
				squid = squidList.get(i);
				squidId = squid.getId();
				type = DSObjectType.parse(squid.getType());
				switch(type){
				case DBSOURCE:
				DbSquid dbSquid = new DBSquidService(token).getDBSquid(squidId);
				//dbSquid.setName(name);
				}
			}
			
		}
		
		return null;
	}

}
