package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.adapter.datatype.DataTypeConvertor;
import com.eurlanda.datashire.dao.IReferenceColumnDao;
import com.eurlanda.datashire.dao.ISquidDao;
import com.eurlanda.datashire.dao.impl.ReferenceColumnDaoImpl;
import com.eurlanda.datashire.dao.impl.SquidDaoImpl;
import com.eurlanda.datashire.entity.Column;
import com.eurlanda.datashire.entity.ExceptionSquid;
import com.eurlanda.datashire.entity.ReferenceColumn;
import com.eurlanda.datashire.entity.ReferenceColumnGroup;
import com.eurlanda.datashire.entity.SourceColumn;
import com.eurlanda.datashire.entity.Squid;
import com.eurlanda.datashire.entity.SquidLink;
import com.eurlanda.datashire.entity.Transformation;
import com.eurlanda.datashire.entity.TransformationLink;
import com.eurlanda.datashire.entity.operation.BeyondSquidException;
import com.eurlanda.datashire.enumeration.MessageBubbleCode;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.enumeration.TransformationTypeEnum;
import com.eurlanda.datashire.enumeration.datatype.SystemDatatype;
import com.eurlanda.datashire.server.utils.TokenUtil;
import com.eurlanda.datashire.sprint7.service.squidflow.subservice.MessageBubbleService;
import com.eurlanda.datashire.utility.CommonConsts;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import com.eurlanda.datashire.utility.StringUtils;
import com.eurlanda.datashire.validator.SquidValidationTask;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 对exceptionSquid操作的业务处理类
 * 
 * @version 1.0
 * @author yi.zhou
 * @created 2014-05-13
 */
public class ExceptionSquidProcess {
	/**
	 * 记录exceptionSquid日志
	 */
	static Logger logger = Logger.getLogger(ExceptionSquidProcess.class);

	//异常处理机制
    ReturnValue out = new ReturnValue();

    private String token;//令牌根据令牌得到相应的连接信息

	public ExceptionSquidProcess(String token) {
		this.token = token;
	}

	/**
	 * 创建ExceptionSquid
	 * @param info
	 * @param out
	 * @return
	 */
	public Map<String, Object> createExceptionSquid(String info, ReturnValue out){
		Map<String, Object> outputMap = new HashMap<String, Object>();
		IRelationalDataManager adapter3 = null;
		try {
			Map<String, Object> parmsMap = JsonUtil.toHashMap(info);
			ExceptionSquid newSquid = JsonUtil.toGsonBean(parmsMap.get("ExceptionSquid")+"", ExceptionSquid.class);
			SquidLink newSquidLink = JsonUtil.toGsonBean(parmsMap.get("SquidLink")+"", SquidLink.class);
			if (newSquid!=null&&newSquidLink!=null){
				adapter3 = DataAdapterFactory.getDefaultDataManager();
				adapter3.openSession();
				ISquidDao squidDao = new SquidDaoImpl(adapter3);
				Squid squid = squidDao.getObjectById(newSquidLink.getFrom_squid_id(), Squid.class);
				if (squid!=null){
				    CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(), adapter3,  MessageBubbleService.setMessageBubble(squid.getId(), squid.getId(), squid.getName(), MessageBubbleCode.WARN_DATA_SQUID_NO_EXCEPTION_SQUID.value())));
					newSquid.setId(squidDao.insert2(newSquid));
					outputMap.putAll(getExceptionReference(adapter3, newSquid, newSquidLink));
					return outputMap;
				}else{
					out.setMessageCode(MessageCode.ERR_DS_SQUID_NULL);
					return null;
				}
			} else{
				out.setMessageCode(MessageCode.NODATA);
				return null;
			}
		} catch (BeyondSquidException e){
			try {
				if (adapter3!=null) adapter3.rollback();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			out.setMessageCode(MessageCode.ERR_SQUID_COUNT_MAX);
			logger.error("copySquidFlowForParams异常",e );
		} catch (Exception e) {
			logger.error("[获取createExceptionSquid=========================================exception]", e);
			try {
				adapter3.rollback();
			} catch (SQLException e1) {
				logger.error("rollback err!", e1);
			}
			out.setMessageCode(MessageCode.SQL_ERROR);
		} finally {
			adapter3.closeSession();
		}
		return null;
	}

	/**
	 * 修改ExceptionSquid的基本属性
	 * @param info
	 * @param out
	 * @return
	 */
	public Map<String, Object> updExceptionSquid(String info, ReturnValue out){
		IRelationalDataManager adapter3 = null;
		try{
			Map<String, Object> parmsMap = JsonUtil.toHashMap(info);
			ExceptionSquid squid = JsonUtil.toGsonBean(parmsMap.get("ExceptionSquid")+"", ExceptionSquid.class);
			if (squid!=null){
				adapter3 = DataAdapterFactory.getDefaultDataManager();
				adapter3.openSession();
				ISquidDao squidDao = new SquidDaoImpl(adapter3);
				Squid tempSquid = squidDao.getObjectById(squid.getId(), Squid.class);
				if (tempSquid!=null){
					squidDao.update(squid);
				}else{
					out.setMessageCode(MessageCode.ERR_EXTRACTSQUID_NOLINK);
					return null;
				}
			}else{
				out.setMessageCode(MessageCode.NODATA);
				return null;
			}
		} catch (Exception e) {
			logger.error("[获取updExceptionSquid=========================================exception]", e);
			try {
				adapter3.rollback();
			} catch (SQLException e1) {
				logger.error("rollback err!", e1);
			}
			out.setMessageCode(MessageCode.SQL_ERROR);
		} finally {
			adapter3.closeSession();
		}
		return null;
	}

	/**
	 * 根据squid属性及link信息 创建referenceColumn、Column、transformation等信息
	 * @param adapter3
	 * @param newSquid
	 * @param squidLink
	 * @return
	 * @throws Exception
	 */
	public Map<String, Object> getExceptionReference(IRelationalDataManager adapter3, ExceptionSquid newSquid, SquidLink squidLink) throws Exception{
		Map<String, Object> outputMap = new HashMap<String, Object>();
		IReferenceColumnDao refColumnDao = new ReferenceColumnDaoImpl(adapter3);
		ISquidDao squidDao = new SquidDaoImpl(adapter3);
		int formSquidId = squidLink.getFrom_squid_id();
		// 创建 SquidLink
        squidLink.setTo_squid_id(newSquid.getId());
        if (StringUtils.isNull(squidLink.getKey())) {
            squidLink.setKey(StringUtils.generateGUID());
        }
        if (squidLink.getId() <= 0) {
            squidLink.setLink_type(1); // 设置linkType为 Exception
            squidLink.setId(adapter3.insert2(squidLink));
        }

		int cnt = 0;
		int columnCnt = 0;
		List<ReferenceColumn> referenceColumns = new ArrayList<ReferenceColumn>();
		List<Column> newColumns = new ArrayList<Column>();
		List<Transformation> transformations = new ArrayList<Transformation>();
		List<TransformationLink> transformationLinks = new ArrayList<TransformationLink>();
		Map<Integer, String> squidNameMap = new HashMap<Integer, String>();

		List<ReferenceColumnGroup> group = refColumnDao.getRefColumnGroupListBySquidId(formSquidId);
		Squid fromSquid = squidDao.getObjectById(formSquidId, Squid.class);

		int dbType = 0;
		String fromSquidName = "";
		List<Map<String, Object>>  maps = squidDao.getFromSquidListByChildId(formSquidId);
		if (maps!=null&&maps.size()>0){
			for (Map<String, Object> map : maps) {
				if (map.containsKey("NAME")){
					fromSquidName = String.valueOf(map.get("NAME"));
				}
				if (map.containsKey("DB_TYPE_ID") && map.get("DB_TYPE_ID") != null){
					dbType = Integer.parseInt(String.valueOf(map.get("DB_TYPE_ID")));
    				break;
				}
			}
		}

		TransformationService transformationService = new TransformationService(TokenUtil.getToken());
		if (group!=null&&group.size()>0){
			for (ReferenceColumnGroup referenceColumnGroup : group) {
				ReferenceColumnGroup columnGroup = new ReferenceColumnGroup();
				columnGroup.setKey(StringUtils.generateGUID());
				columnGroup.setReference_squid_id(newSquid.getId());
				columnGroup.setRelative_order(cnt+1);
				columnGroup.setId(refColumnDao.insert2(columnGroup));

				List<ReferenceColumn> columns = refColumnDao.getRefColumnListByGroupId(referenceColumnGroup.getId());
				if (columns!=null&&columns.size()>0){
					//初始化ReferenceAndTrans对象
					int index = 0;
					for (ReferenceColumn referenceColumn : columns) {
						int squidId = referenceColumn.getHost_squid_id();
						int columnId = referenceColumn.getColumn_id();
						int toSquidId = referenceColumn.getReference_squid_id();
						if (!squidNameMap.containsKey(squidId)){
							String squidName = squidDao.getSquidNameByColumnId(referenceColumn.getColumn_id());
							if(StringUtils.isHavaChinese(squidName)){
								SquidTypeEnum types=SquidTypeEnum.valueOf(squidDao.getSquidTypeById(squidId));
								squidName=types.toString()+squidId;
							}
							squidNameMap.put(squidId, squidName);
						}
						if (fromSquid.getSquid_type()==SquidTypeEnum.EXTRACT.value()){
							SourceColumn col = DataTypeConvertor.getSourceColumnByReferenceColumns(referenceColumn);
		    				SourceColumn newSourceColumn = DataTypeConvertor.getInColumnBySourceColumn(dbType, col);
		    				referenceColumn = DataTypeConvertor.getReferenceColumnBySourceColumn(newSourceColumn);
							referenceColumn.setColumn_id(columnId);
							referenceColumn.setHost_squid_id(squidId);
							referenceColumn.setReference_squid_id(toSquidId);
						}
						//创建新的ReferenceColumn  客户端右侧
						ReferenceColumn newRC = transformationService.initReferenceByException(adapter3, referenceColumn, referenceColumn.getColumn_id(),
								index+1, formSquidId, "", newSquid.getId(), columnGroup);
						newRC.setGroup_name(fromSquidName);
						newRC.setGroup_order(columnGroup.getRelative_order());
						referenceColumns.add(newRC);

						//创建新的Column, ExceptionSquidColumn 客户端左侧
						Column newColumn = transformationService.initColumn2(adapter3, referenceColumn,
								columnCnt+1, newSquid.getId(), squidNameMap);
						newColumns.add(newColumn);

						//根据Reference创建Transformation 右侧的Trans
						transformationService.initLinkByColumn(newColumn, newRC, index, newSquid.getId(),
								adapter3, transformations, transformationLinks);
						index++;
						columnCnt++;
					}
				}
				cnt++;
			}

		}
		ReferenceColumn newRef = new ReferenceColumn();
		newRef.setLength(1000);
		newRef.setData_type(SystemDatatype.NVARCHAR.value());
		newRef.setName("ERROR");
		newRef.setNullable(false);
		newRef.setPrecision(0);
		Column newColumn = transformationService.initColumn2(adapter3, newRef,
				columnCnt+1, newSquid.getId(), null);
		newColumns.add(newColumn);

		//添加ERROR的Transforamtion
		Transformation newTrans = transformationService.initTransformation(adapter3, newSquid.getId(),
				newColumn.getId(), TransformationTypeEnum.VIRTUAL.value(),
				newColumn.getData_type(), 1);
		transformations.add(newTrans);
		outputMap.put("newSquidId", newSquid.getId());
		outputMap.put("newSquidLinkId", squidLink.getId());
		outputMap.put("ColumnList", newColumns);
		outputMap.put("ReferenceColumnList", referenceColumns);
		outputMap.put("TransformationList", transformations);
		outputMap.put("TransformationLinkList", transformationLinks);
		return outputMap;
	}
}
