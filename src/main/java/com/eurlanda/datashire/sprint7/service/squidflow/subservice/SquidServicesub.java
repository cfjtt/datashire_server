package com.eurlanda.datashire.sprint7.service.squidflow.subservice;

import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.entity.*;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.sprint7.service.squidflow.AbstractRepositoryService;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * SquidModelBase 增删查改
 * 
 * @author dang.lu 2013.11.12
 *
 */
public class SquidServicesub extends AbstractRepositoryService implements ISquidService{
	Logger logger = Logger.getLogger(SquidServicesub.class);// 记录日志
	public SquidServicesub(String token) {
		super(token);
	}
	public SquidServicesub(IRelationalDataManager adapter){
		super(adapter);
	}
	public SquidServicesub(String token, IRelationalDataManager adapter){
		super(token, adapter);
	}
	
	/**
	 * 根据squid_id获取Squid
	 * @param squid_id
	 * @return
	 */
	public Squid getSquid(int squid_id){
		Map<String, String> paramMap = new HashMap<String, String>();
		paramMap.put("ID", Integer.toString(squid_id, 10));
		List<Squid> list2 = adapter.query2List(paramMap, Squid.class);
		return list2==null||list2.isEmpty()?null:list2.get(0);
	}
	/**
	 * 根据squid删除
	 * @param squid
	 * @param out
	 * @return
	 */
	public boolean deleteSquid(int squid, ReturnValue out) {
		boolean delete = true;
		Map<String, String> paramMap = new HashMap<String, String>();
		try {
			paramMap.clear();
			paramMap.put("id", String.valueOf(squid));
			List<Squid> squids=adapter.query2List(true, paramMap, Squid.class);
			if(squids.size()>0)
			{
				//当删除的squid为extrasquid并且同表名的个数为1时,推送取消表名恢复颜色命令(主要受事务的影响,放在此位置)
				CheckExtractService check = new CheckExtractService(token,adapter);
				if(org.apache.commons.lang.StringUtils.isNotBlank(squids.get(0).getTable_name())&&squids.get(0).getSquid_type()!=3)
				{
					check.updateExtract(squids.get(0).getTable_name(), squid, "delete", out, null, token);
				}
			}
			// 删除ReferenceGroup
			// 根据squid_id查出ReferenceGroup的id
			paramMap.clear();
			paramMap.put("reference_squid_id", String.valueOf(squid));
			List<ReferenceColumnGroup> referenceColumnGroups=adapter.query2List(true, paramMap, ReferenceColumnGroup.class);
			// 删除ReferenceGroup
			if(null!=referenceColumnGroups&&referenceColumnGroups.size()>0)
			{
				ReferenceGroupService referenceGroupService=new ReferenceGroupService(token, adapter);
				for(ReferenceColumnGroup columnGroup:referenceColumnGroups)
				{
					delete=referenceGroupService.deleteReferenceGroup(columnGroup.getId(), out);
				}
			}
			if (delete) {
				// 根据squid_id去查询transformation的id
				paramMap.clear();
				paramMap.put("squid_id", String.valueOf(squid));
				List<Transformation> transformations=adapter.query2List(true, paramMap, Transformation.class);
				if(null!=transformations&&transformations.size()>0)
				{
					TransformationServicesub transformationService=new TransformationServicesub(token, adapter);
					for(Transformation transformation:transformations)
					{
						delete=	transformationService.deleteTransformation(transformation.getId(), out);
					}
				}
				if (delete) {
					// 删除column
					paramMap.clear();
					paramMap.put("squid_id", String.valueOf(squid));
					List<Column> columns=adapter.query2List(true, paramMap, Column.class);
					if(null!=columns&&columns.size()>0){
						/*ColumnServicesub columnService=new ColumnServicesub(token, adapter);
						for(Column column:columns)
						{
							delete=	columnService.deleteColumn(column.getId(), out);
						}*/
						delete = adapter.delete(paramMap, Column.class) > 0 ? true : false;
					}
					if (delete) {
						// 删除squidlink
						String querySquidLink = "select * from DS_SQUID_LINK where from_squid_id="
								+ squid + " or to_squid_id=" + squid + "";
						List<SquidLink> squidLinks=adapter.query2List(true, querySquidLink, null, SquidLink.class);
						if(null!=squidLinks&&squidLinks.size()>0)
						{
							SquidLinkServicesub squidLinkService=new SquidLinkServicesub(token, adapter);
							for(SquidLink squidLink:squidLinks)
							{
								delete=	squidLinkService.deleteSquidLink(squidLink.getId(), out);
							}
						}
						if(delete){
						//删除squidJoin	
							String querySquidJoin="select * from DS_JOIN where target_squid_id="
								+ squid + " or joined_squid_id=" + squid + "";
							List<SquidJoin> squidJoins=adapter.query2List(true, querySquidJoin,null, SquidJoin.class);
							if(null!=squidJoins&&squidJoins.size()>0){
//								JoinServicesub joinService=new JoinServicesub(token, adapter);
								for(SquidJoin squidJoin:squidJoins){
									//delete=	joinService.deleteJoin(squidJoin.getId(), out);
									// 删除join表
									paramMap.clear();
									paramMap.put("id", String.valueOf(squidJoin.getId()));
									delete = adapter.delete(paramMap, SquidJoin.class)>=0?true:false;
								}
							}
						}
					}
				}
			}
			//删除squid
			if(delete)
			{
				/*//根据类型先删除子类
				delete=this.deleteChildrenSquid(squid, type, paramMap, out);*/
				//删除 
				paramMap.clear();
				paramMap.put("id", String.valueOf(squid));
				adapter.delete(paramMap, DataSquid.class);
				//根据传过来的类型删掉具体的squid,后续修改
				paramMap.clear();
				paramMap.put("id", String.valueOf(squid));
				adapter.delete(paramMap, FileFolderSquid.class);
				
				paramMap.clear();
				paramMap.put("id", String.valueOf(squid));
				adapter.delete(paramMap, FtpSquid.class);
				
				paramMap.clear();
				paramMap.put("id", String.valueOf(squid));
				adapter.delete(paramMap, HdfsSquid.class);
				
				paramMap.clear();
				paramMap.put("source_squid_id", String.valueOf(squid));
				adapter.delete(paramMap, DBSourceTable.class);
				
				paramMap.clear();
				paramMap.put("id", String.valueOf(squid));
				adapter.delete(paramMap, DbSquid.class);
				
				paramMap.clear();
				paramMap.put("id", String.valueOf(squid));
				adapter.delete(paramMap, WeiboSquid.class);
				
				paramMap.clear();
				paramMap.put("id", String.valueOf(squid));
				adapter.delete(paramMap, WebSquid.class);
				
				paramMap.clear();
				paramMap.put("id", String.valueOf(squid));
				adapter.delete(paramMap, ReportSquid.class);
				
                paramMap.clear();
                paramMap.put("id", String.valueOf(squid));
                adapter.delete(paramMap, DataMiningSquid.class);
				
				paramMap.clear();
				paramMap.put("id", String.valueOf(squid));
				adapter.delete(paramMap, ExceptionSquid.class);
				
				paramMap.clear();
				paramMap.put("id", String.valueOf(squid));
				return adapter.delete(paramMap, Squid.class) > 0 ? true : false;
			}
		} catch (SQLException e) {
			logger.error("[删除deleteSquid=========================================exception]", e);
			try {
				adapter.rollback();
			} catch (SQLException e1) {
				logger.error("rollback err!", e1);
			}
			out.setMessageCode(MessageCode.SQL_ERROR);
			return false;
		}
		return delete;
	}
	/**
	 * 根据枚举类型去删除子类
	 * @param squid
	 * @param type
	 * @return
	 */
	public boolean deleteChildrenSquid(int squid,DSObjectType type,Map<String, String> paramMap,ReturnValue out)
	{
		try {
			if(type==DSObjectType.DBSOURCE)
			{
				paramMap.clear();
				paramMap.put("id", String.valueOf(squid));
				return adapter.delete(paramMap, DbSquid.class)>0?true:false;
			}else if(type==DSObjectType.EXTRACT)
			{
				paramMap.clear();
				paramMap.put("id", String.valueOf(squid));
				return adapter.delete(paramMap, TableExtractSquid.class)>0?true:false;
			}else if(type==DSObjectType.STAGE)
			{
				paramMap.clear();
				paramMap.put("id", String.valueOf(squid));
				return adapter.delete(paramMap, StageSquid.class)>0?true:false;
			}
		} catch (Exception e) {
			logger.error("[删除deleteChildrenSquid=========================================exception]", e);
			try {
				adapter.rollback();
			} catch (SQLException e1) {
				logger.error("rollback err!", e1);
			}
			out.setMessageCode(MessageCode.SQL_ERROR);
			return false;
		}
		return true;
	}
}