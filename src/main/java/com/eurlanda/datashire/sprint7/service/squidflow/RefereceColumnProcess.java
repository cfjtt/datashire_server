package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.adapter.datatype.DataTypeConvertor;
import com.eurlanda.datashire.dao.IReferenceColumnDao;
import com.eurlanda.datashire.dao.ISourceColumnDao;
import com.eurlanda.datashire.dao.ISquidDao;
import com.eurlanda.datashire.dao.ITransformationDao;
import com.eurlanda.datashire.dao.ITransformationInputsDao;
import com.eurlanda.datashire.dao.ITransformationLinkDao;
import com.eurlanda.datashire.dao.impl.ReferenceColumnDaoImpl;
import com.eurlanda.datashire.dao.impl.SourceColumnDaoImpl;
import com.eurlanda.datashire.dao.impl.SquidDaoImpl;
import com.eurlanda.datashire.dao.impl.TransformationDaoImpl;
import com.eurlanda.datashire.dao.impl.TransformationInputsDaoImpl;
import com.eurlanda.datashire.dao.impl.TransformationLinkDaoImpl;
import com.eurlanda.datashire.entity.*;
import com.eurlanda.datashire.enumeration.DMLType;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.enumeration.TransformationTypeEnum;
import com.eurlanda.datashire.server.utils.TokenUtil;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import com.eurlanda.datashire.utility.StringUtils;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RefereceColumnProcess {
	/**
	 * 记录JoinProcess日志
	 */
	static Logger logger = Logger.getLogger(JoinProcess.class);

	private String token;
	
	private String key;
	
	public RefereceColumnProcess(String token) {
		this.token = token;
	}
	
	public RefereceColumnProcess(String token, String key) {
		this.token = token;
		this.key = key;
	}
	
	/**
	 * DocExtract创建ReferenceColumn
	 * @param info
	 * @param out
	 * @return
	 */
	public Map<String, Object> createReferenceColumn(String info, ReturnValue out){
		IRelationalDataManager adapter = null;
		Map<String, Object> outputMap = new HashMap<String, Object>();
		try {
			ReferenceColumn refCol = JsonUtil.object2HashMap(info, ReferenceColumn.class);
			if (refCol!=null){
				adapter = DataAdapterFactory.getDefaultDataManager();
				adapter.openSession();
				ISourceColumnDao sourceColDao = new SourceColumnDaoImpl(adapter);
				IReferenceColumnDao refColDao = new ReferenceColumnDaoImpl(adapter);
				ISquidDao squidDao = new SquidDaoImpl(adapter);
				int group_id = refCol.getGroup_id();
				int sourceTableId = 0;
				// 判断是否是文件docExtract，获取当前的squid的source_table_id
				Squid squid = squidDao.getObjectById(refCol.getReference_squid_id(), Squid.class);
				if (squid.getSquid_type()==7){
					DocExtractSquid docExtSquid = squidDao.getSquidForCond(squid.getId(), DocExtractSquid.class);
					sourceTableId = docExtSquid.getSource_table_id();
				} else if (squid.getSquid_type() == SquidTypeEnum.KAFKAEXTRACT.value()){
					KafkaExtractSquid kafkaExtractSquid = squidDao.getSquidForCond(squid.getId(), KafkaExtractSquid.class);
					sourceTableId = kafkaExtractSquid.getSource_table_id();
				} else if (squid.getSquid_type() == SquidTypeEnum.HBASEEXTRACT.value()){
					HBaseExtractSquid hbaseExtractSquid = squidDao.getSquidForCond(squid.getId(), HBaseExtractSquid.class);
					sourceTableId = hbaseExtractSquid.getSource_table_id();
				} else {
					ExtractSquid extSquid = squidDao.getSquidForCond(squid.getId(), ExtractSquid.class);
					sourceTableId = extSquid.getSource_table_id();
				}
				if (sourceTableId==0){
					out.setMessageCode(MessageCode.INSERT_ERROR);
					return outputMap;
				}
				// 判断group是否为空
				if (group_id==0){
					ReferenceColumnGroup group = new ReferenceColumnGroup();
					group.setKey(StringUtils.generateGUID());
					group.setReference_squid_id(refCol.getReference_squid_id());
					group.setRelative_order(1);
					group_id = refColDao.insert2(group);
				}
				//List<SourceColumn> sourceColumns = sourceColDao.getSourceColumnByTableId(sourceTableId);
				/*if (sourceColumns!=null&&sourceColumns.size()>0){
					sourceColDao.deleteSourceColumnByTableId(sourceTableId);
				}*/
				// 转换为sourceColumn
				SourceColumn sourceCol = DataTypeConvertor.getSourceColumnByReferenceColumns(refCol);
				sourceCol.setSource_table_id(sourceTableId);
				// 添加新增的ReferenceColumn
				int newSourceColId = sourceColDao.insert2(sourceCol);
				refCol.setColumn_id(newSourceColId);
				refCol.setGroup_id(group_id);
				refColDao.insert2(refCol);
				outputMap.put("ReferenceColumnId", newSourceColId);
				TransformationService service = new TransformationService(TokenUtil.getToken());
				Transformation newTrans = service.initTransformation(adapter, refCol.getReference_squid_id(), 
						newSourceColId, TransformationTypeEnum.VIRTUAL.value(), 
						refCol.getColumntype(), 1);
				outputMap.put("VTransformation", newTrans);
				outputMap.put("GroupId", group_id);
				Squid connSquid = squidDao.getSquidForCond(refCol.getHost_squid_id(),Squid.class);
				outputMap.put("GroupName",connSquid.getName());
				//同步下游
				RepositoryServiceHelper helper = new RepositoryServiceHelper(TokenUtil.getToken(), adapter);
				helper.synchronousInsertRefColumn(adapter, refCol, DMLType.INSERT.value());
				return outputMap;
			}else{
				out.setMessageCode(MessageCode.NODATA);
			}
		} catch (Exception e) {
			if (adapter!=null){
				try {
					adapter.rollback();
				} catch (SQLException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
			out.setMessageCode(MessageCode.SQL_ERROR);
			logger.error("createReferenceColumn is error", e);
		} finally {
			if(adapter!=null) adapter.closeSession();
		}
		return outputMap;
	}
	
	/**
	 * DocExtract修改ReferenceColumn
	 * @param info
	 * @param out
	 * @return
	 */
	public Map<String, Object> updateReferenceColumn(String info, ReturnValue out){
		IRelationalDataManager adapter = null;
		Map<String, Object> outputMap = new HashMap<String, Object>();
		try {
			ReferenceColumn refCol = JsonUtil.object2HashMap(info, ReferenceColumn.class);
			if (refCol!=null){
				adapter = DataAdapterFactory.getDefaultDataManager();
				adapter.openSession();
				ISourceColumnDao sourceColumnDao = new SourceColumnDaoImpl(adapter);
				IReferenceColumnDao refColumnDao = new ReferenceColumnDaoImpl(adapter);
				ITransformationDao transDao = new TransformationDaoImpl(adapter);
				Transformation trans = transDao.getTransformationById(refCol.getReference_squid_id(), 
						refCol.getColumn_id());
				if (trans!=null){
					trans.setOutput_data_type(refCol.getData_type());
					transDao.update(trans);
				}
				SourceColumn sourceCol = sourceColumnDao.getObjectById(refCol.getColumn_id(), SourceColumn.class);
				if (sourceCol!=null){
					sourceCol = DataTypeConvertor.getSourceColumnByReferenceColumns(refCol, sourceCol);
					sourceColumnDao.update(sourceCol);
				}
				refColumnDao.updateRefColumnByObj(refCol);
				
				//同步下游
				RepositoryServiceHelper helper = new RepositoryServiceHelper(TokenUtil.getToken(), adapter);
				helper.synchronousUpdateRefColumn(adapter, refCol, DMLType.UPDATE.value(),out);
			}else{
				out.setMessageCode(MessageCode.NODATA);
			}
		} catch (Exception e) {
			if (adapter!=null){
				try {
					adapter.rollback();
				} catch (SQLException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
			out.setMessageCode(MessageCode.SQL_ERROR);
			logger.error("createReferenceColumn is error", e);
		} finally {
			if(adapter!=null) adapter.closeSession();
		}
		return outputMap;
	}
	
	/**
	 * 删除ReferenceColumn
	 * @param info
	 * @param out
	 * @return
	 */
	public Map<String, Object> deleteReferenceColumn(String info, ReturnValue out){
		IRelationalDataManager adapter = null;
		Map<String, Object> outputMap = new HashMap<String, Object>();
		try {
			Map<String, Object> infoMap = JsonUtil.toHashMap(info);
			List<Integer> list = JsonUtil.toGsonList(infoMap.get("ColumnIds")+"", Integer.class);
			int reference_squid_id = Integer.parseInt(infoMap.get("ReferenceSquidId")+"");
			if (list!=null&&list.size()>0&&reference_squid_id>0){
				adapter = DataAdapterFactory.getDefaultDataManager();
				adapter.openSession();
				ISourceColumnDao sourceColumnDao = new SourceColumnDaoImpl(adapter);
				ITransformationDao transDao = new TransformationDaoImpl(adapter);
				ITransformationInputsDao inputsDao = new TransformationInputsDaoImpl(adapter);
				ITransformationLinkDao linkDao = new TransformationLinkDaoImpl(adapter);
				for (Integer column_Id : list) {
					Transformation trans = transDao.getTransformationById(reference_squid_id, column_Id);
					if(trans!=null){
						// 删除所有相关的link，在删除之前重置link的source_trans
						List<TransformationLink> links = linkDao.getTransLinkByFromTrans(trans.getId());
						if (links!=null&&links.size()>0){
							for (TransformationLink transformationLink : links) {
								inputsDao.resetTransformationInput(
										transformationLink.getFrom_transformation_id(), 
										transformationLink.getTo_transformation_id(), null);
							}
							linkDao.delTransLinkByTranId(trans.getId());
						}
						//删除inputs集合
						inputsDao.delTransInputsByTransId(trans.getId());
						//删除link
						transDao.delete(trans.getId(), Transformation.class);
					}
					//删除 SourceColumnDao
					SourceColumn sourceCol = sourceColumnDao.getObjectById(column_Id, SourceColumn.class);
					if (sourceCol!=null){
						sourceColumnDao.delete(column_Id, SourceColumn.class);
					}
					// 删除ReferenceColumn
					IReferenceColumnDao refColumnDao = new ReferenceColumnDaoImpl(adapter);
					ReferenceColumn refCol = refColumnDao.getReferenceColumnById(reference_squid_id, column_Id);
					refColumnDao.delReferenceColumnByColumnId(column_Id, reference_squid_id);
					
					//同步下游
					RepositoryServiceHelper helper = new RepositoryServiceHelper(TokenUtil.getToken(), adapter);
                    helper.synchronousDeleteRefColumn(adapter, refCol, DMLType.DELETE.value(), null);
                }
			}else{
				out.setMessageCode(MessageCode.NODATA);
			}
		} catch (Exception e) {
			if (adapter!=null){
				try {
					adapter.rollback();
				} catch (SQLException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
			out.setMessageCode(MessageCode.SQL_ERROR);
			logger.error("createReferenceColumn is error", e);
		} finally {
			if(adapter!=null) adapter.closeSession();
		}
		return outputMap;
	}
	
	/**
	 * DocExtract修改ReferenceColumn
	 * @param info
	 * @param out
	 * @return
	 */
	public Map<String, Object> updateRefColumnForOrder(String info, ReturnValue out){
		IRelationalDataManager adapter = null;
		Map<String, Object> outputMap = new HashMap<String, Object>();
		try {
			Map<String, Object> infoMap = JsonUtil.toHashMap(info);
			int group_id = Integer.parseInt(infoMap.get("RefColumnGroupId")+"");
			if (group_id>0){
				adapter = DataAdapterFactory.getDefaultDataManager();
				adapter.openSession();
				IReferenceColumnDao refColumnDao = new ReferenceColumnDaoImpl(adapter);
				List<ReferenceColumn> list = refColumnDao.getRefColumnListByGroupId(group_id);
				if (list!=null&&list.size()>0){
					int i = 1;
					for (ReferenceColumn referenceColumn : list) {
						referenceColumn.setRelative_order(i);
						refColumnDao.updateRefColumnForOrder(referenceColumn);
						//同步下游
						RepositoryServiceHelper helper = new RepositoryServiceHelper(TokenUtil.getToken(), adapter);
						helper.synchronousUpdateRefColumn(adapter, referenceColumn, DMLType.UPDATE.value(),out);
						i++;
					}
				}
				outputMap.put("updReferenceColumns", list);
			}else{
				out.setMessageCode(MessageCode.NODATA);
			}
		} catch (Exception e) {
			if (adapter!=null){
				try {
					adapter.rollback();
				} catch (SQLException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
			out.setMessageCode(MessageCode.SQL_ERROR);
			logger.error("updateRefColumnForOrder is error", e);
		} finally {
			if(adapter!=null) adapter.closeSession();
		}
		return outputMap;
	}
}
