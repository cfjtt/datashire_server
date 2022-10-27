package com.eurlanda.datashire.sprint7.plug;

import cn.com.jsoft.jframe.utils.ValidateUtils;
import com.eurlanda.datashire.adapter.IDBAdapter;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.entity.*;
import com.eurlanda.datashire.entity.operation.DataEntity;
import com.eurlanda.datashire.entity.operation.WhereCondition;
import com.eurlanda.datashire.enumeration.DataStatusEnum;
import com.eurlanda.datashire.enumeration.MatchTypeEnum;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.enumeration.SystemTableEnum;
import com.eurlanda.datashire.sprint7.service.squidflow.RepositoryServiceHelper;
import com.eurlanda.datashire.sprint7.service.squidflow.subservice.SquidLinkServicesub;
import com.eurlanda.datashire.utility.AnnotationHelper;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import com.eurlanda.datashire.utility.StringUtils;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Squid处理类
 * squid对象业务处理类：新增Squid对象；根据ID修改Squid对象；根据ID获得Squid对象；根据SquidFlowID获得Squid对象；
 * <p>
 * Title :
 * </p>
 * <p>
 * Description:
 * </p>
 * <p>
 * Author :CherylAug 22, 2013
 * </p>
 * <p>
 * update :CherylAug 22, 2013
 * </p>
 * <p>
 * Department : JAVA后端研发部
 * </p>
 * Copyright : ?2012-2013 悦岚（上海）数据服务有限公司 </p>
 */
public class SquidPlug extends Support {
	static Logger logger = Logger.getLogger(SquidPlug.class);// 记录日志

	public SquidPlug(IDBAdapter adapter){
		super(adapter);
	}
	/**
	 * 创建Extract对象集合
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param extractSquids squid集合对象
	 *@param out 异常处理
	 *@return
	 */
	public boolean createExtractSquids(List<TableExtractSquid> extractSquids,ReturnValue out){
		logger.debug(String.format("createExtractSquids-extractSquidList.size()=%s", extractSquids.size()));
		boolean create = false;
		//封装批量新增数据集
		List<List<DataEntity>>  paramList = new ArrayList<List<DataEntity>>();
		//调用转换类
		GetParam getParam = new GetParam();
		for (TableExtractSquid extractSquid : extractSquids) {
			List<DataEntity> dataEntitys =  new ArrayList<DataEntity>();
			getParam.getSquid(extractSquid, dataEntitys);
			
			paramList.add(dataEntitys);
			
		}
		//调用批量新增
		create = adapter.createBatch(SystemTableEnum.DS_SQUID.toString(), paramList, out);
		return create;
		
	}
	
	/**
	 * 创建DBSourceSquid
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param dbSourceSquids squid集合对象
	 *@param out 异常处理
	 *@return
	 */
	public boolean createDBSourceSquids(List<DBSourceSquid> dbSourceSquids,ReturnValue out){
		logger.debug(String.format("createDBSourceSquids-dbSourceSquids()=%s", dbSourceSquids.size()));
		boolean create = false;
		try {
			//封装批量新增数据集
			List<List<DataEntity>>  paramList = new ArrayList<List<DataEntity>>();
			//调用转换类
			GetParam getParam = new GetParam();
			for (DBSourceSquid dbSourceSquid : dbSourceSquids) {
				List<DataEntity> dataEntitys =  new ArrayList<DataEntity>();
				getParam.getSourceSquid(dbSourceSquid, dataEntitys);
				paramList.add(dataEntitys);
			}
			create = adapter.createBatch(SystemTableEnum.DS_SQUID.toString(), paramList, out);
		} catch (Exception e) {
			out.setMessageCode(MessageCode.ERR_ARRAYS);
		}
		return create;
	}
	
	/**
	 * 创建单个ExtractSquid
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param extractSquidquid集合对象
	 *@param out 异常处理
	 *@return
	 */
	public boolean createExtractSquid(TableExtractSquid extractSquid,ReturnValue out){
		logger.debug(String.format("createExtractSquid-extractSquidList=%s", extractSquid));
		boolean create = false;
		//调用转换类
		GetParam getParam = new GetParam();
		List<DataEntity> dataEntitys =  new ArrayList<DataEntity>();
		getParam.getSquid(extractSquid, dataEntitys);
		//调用批量新增
		create = adapter.insert(SystemTableEnum.DS_SQUID.toString(), dataEntitys, out);
		return create;
	}
	/**
	 * 根据SquidID查询Squid返回Squid对象
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 * 
	 * @param squidId squidID
	 *@param out 异常处理
	 * @return
	 */
	public Squid getSquid(int squidId, ReturnValue out) {
		logger.debug(String.format("getSquid-squidId=%s", squidId));
		Squid squid = null;
		// 封装条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		// 条件
		WhereCondition whereCondition = new WhereCondition();
		whereCondition.setAttributeName("ID");
		whereCondition.setMatchType(MatchTypeEnum.EQ);
		whereCondition.setValue(squidId);
		whereCondList.add(whereCondition);
		// 查询结果
		Map<String, Object> columnsValue = adapter.queryRecord(null, SystemTableEnum.DS_SQUID.toString(),
				whereCondList, out);
		if (columnsValue == null) {
			// 无数据返回
			out.setMessageCode(MessageCode.NODATA);
			logger.debug(String.format("SquidModelBase not found by guid(%s)...", squidId));
			return null;
		} else {
			// 将数据库查询结果转换为Squid实体
			squid = this.getSquid(columnsValue,out);
			out.setMessageCode(MessageCode.SUCCESS);
		}
		return squid;
	}
	
	
	public DbSquid getDBSquid(int squidId, ReturnValue out) {
		logger.debug(String.format("getSquid-squidId=%s", squidId));
		DbSquid dbSquid = null;
		// 封装条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		// 条件
		WhereCondition whereCondition = new WhereCondition();
		whereCondition.setAttributeName("ID");
		whereCondition.setMatchType(MatchTypeEnum.EQ);
		whereCondition.setValue(squidId);
		whereCondList.add(whereCondition);
		// 查询结果
		Map<String, Object> columnsValue = adapter.queryRecord(null, "DS_SQUID",
				whereCondList, out);
		if (columnsValue == null || columnsValue.isEmpty()) {
			// 无数据返回
			out.setMessageCode(MessageCode.NODATA);
			logger.debug(String.format("SquidModelBase not found by guid(%s)...", squidId));
			return null;
		} else {
			dbSquid = new DbSquid();
			dbSquid.setDb_name(StringUtils.valueOf2(columnsValue, "database_name"));
			dbSquid.setDb_type(Integer.valueOf(StringUtils.valueOf2(columnsValue, "db_type_id"), 10));
			dbSquid.setHost(StringUtils.valueOf2(columnsValue, "host"));
			dbSquid.setPassword(StringUtils.valueOf2(columnsValue, "password"));
			dbSquid.setUser_name(StringUtils.valueOf2(columnsValue, "user_name"));
			dbSquid.setPort(Integer.valueOf(StringUtils.valueOf2(columnsValue, "port"), 10));
			out.setMessageCode(MessageCode.SUCCESS);
		}
		return dbSquid;
	}
	//fileFolederSquid
	public FileFolderSquid getfileFolederSquid(int squidId, ReturnValue out) {
		logger.debug(String.format("getfileFolederSquid-squidId=%s", squidId));
		FileFolderSquid fileFolderSquid = null;
		// 封装条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		// 条件
		WhereCondition whereCondition = new WhereCondition();
		whereCondition.setAttributeName("ID");
		whereCondition.setMatchType(MatchTypeEnum.EQ);
		whereCondition.setValue(squidId);
		whereCondList.add(whereCondition);
		// 查询结果
		Map<String, Object> columnsValue = adapter.queryRecord(null, "DS_FILEFOLDER_CONNECTION",
				whereCondList, out);
		if (columnsValue == null || columnsValue.isEmpty()) {
			// 无数据返回
			out.setMessageCode(MessageCode.NODATA);
			logger.debug(String.format("SquidModelBase not found by guid(%s)...", squidId));
			return null;
		} else {
			fileFolderSquid = new FileFolderSquid();
			fileFolderSquid.setHost(StringUtils.valueOf2(columnsValue, "host"));
			fileFolderSquid.setPassword(StringUtils.valueOf2(columnsValue, "password"));
			fileFolderSquid.setUser_name(StringUtils.valueOf2(columnsValue, "user_name"));
			fileFolderSquid.setFile_path(StringUtils.valueOf2(columnsValue, "file_path"));
			fileFolderSquid.setIncluding_subfolders_flag(Integer.parseInt(StringUtils.valueOf2(columnsValue, "including_subfolders_flag")));
			fileFolderSquid.setUnionall_flag(Integer.parseInt(StringUtils.valueOf2(columnsValue, "unionall_flag")));
			out.setMessageCode(MessageCode.SUCCESS);
		}
		return fileFolderSquid;
	}

	// ftpSquid
	public FtpSquid getFtpSquid(int squidId, ReturnValue out) {
		logger.debug(String.format("getFtpSquid-squidId=%s", squidId));
		FtpSquid ftpSquid = null;
		// 封装条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		// 条件
		WhereCondition whereCondition = new WhereCondition();
		whereCondition.setAttributeName("ID");
		whereCondition.setMatchType(MatchTypeEnum.EQ);
		whereCondition.setValue(squidId);
		whereCondList.add(whereCondition);
		// 查询结果
		Map<String, Object> columnsValue = adapter.queryRecord(null,
				"DS_FTP_CONNECTION", whereCondList, out);
		if (columnsValue == null || columnsValue.isEmpty()) {
			// 无数据返回
			out.setMessageCode(MessageCode.NODATA);
			logger.debug(String.format("SquidModelBase not found by guid(%s)...",
					squidId));
			return null;
		} else {
			ftpSquid = new FtpSquid();
			ftpSquid.setHost(StringUtils.valueOf2(columnsValue, "host"));
			ftpSquid.setPassword(StringUtils.valueOf2(columnsValue, "password"));
			ftpSquid.setUser_name(StringUtils.valueOf2(columnsValue,
					"user_name"));
			ftpSquid.setFile_path(StringUtils.valueOf2(columnsValue,
					"file_path"));
			ftpSquid.setIncluding_subfolders_flag(Integer.parseInt(StringUtils
					.valueOf2(columnsValue, "including_subfolders_flag")));
			ftpSquid.setUnionall_flag(Integer.parseInt(StringUtils.valueOf2(
					columnsValue, "unionall_flag")));
			ftpSquid.setPostprocess(Integer.parseInt(StringUtils.valueOf2(
					columnsValue, "postprocess")));
			ftpSquid.setProtocol(Integer.parseInt(StringUtils.valueOf2(
					columnsValue, "protocol")));
			ftpSquid.setEncryption(Integer.parseInt(StringUtils.valueOf2(
					columnsValue, "encryption")));
			ftpSquid.setAllowanonymous_flag(Integer.parseInt(StringUtils
					.valueOf2(columnsValue, "allowanonymous_flag")));
			ftpSquid.setMaxconnections(Integer.parseInt(StringUtils.valueOf2(
					columnsValue, "maxconnections")));
			ftpSquid.setTransfermode_flag(Integer.parseInt(StringUtils
					.valueOf2(columnsValue, "transfermode_flag")));
			out.setMessageCode(MessageCode.SUCCESS);
		}
		return ftpSquid;
	}

	// HDFSSquid
	public HdfsSquid getHdfsSquid(int squidId, ReturnValue out) {
		logger.debug(String.format("getHdfsSquid-squidId=%s", squidId));
		HdfsSquid hdfsSquid = null;
		// 封装条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		// 条件
		WhereCondition whereCondition = new WhereCondition();
		whereCondition.setAttributeName("ID");
		whereCondition.setMatchType(MatchTypeEnum.EQ);
		whereCondition.setValue(squidId);
		whereCondList.add(whereCondition);
		// 查询结果
		Map<String, Object> columnsValue = adapter.queryRecord(null,
				"DS_HDFS_CONNECTION", whereCondList, out);
		if (columnsValue == null || columnsValue.isEmpty()) {
			// 无数据返回
			out.setMessageCode(MessageCode.NODATA);
			logger.debug(String.format("SquidModelBase not found by guid(%s)...",
					squidId));
			return null;
		} else {
			hdfsSquid = new HdfsSquid();
			hdfsSquid.setHost(StringUtils.valueOf2(columnsValue, "host"));
			hdfsSquid.setPassword(StringUtils
					.valueOf2(columnsValue, "password"));
			hdfsSquid.setUser_name(StringUtils.valueOf2(columnsValue,
					"user_name"));
			hdfsSquid.setFile_path(StringUtils.valueOf2(columnsValue,
					"file_path"));
			hdfsSquid.setUnionall_flag(Integer.parseInt(StringUtils.valueOf2(
					columnsValue, "unionall_flag")));
			out.setMessageCode(MessageCode.SUCCESS);
		}
		return hdfsSquid;
	}
	/**
	 * 根据TARGET_SQUID_ID获取squid对象
	 * @author lei.bin
	 * @param squidId
	 * @param out
	 * @return
	 */
	public Squid getSquidById(int id, ReturnValue out) {
		Squid squid = null;
		// 封装条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		// 条件
		WhereCondition whereCondition = new WhereCondition();
		whereCondition.setAttributeName("ID");
		whereCondition.setMatchType(MatchTypeEnum.EQ);
		whereCondition.setValue(id);
		whereCondList.add(whereCondition);
		// 查询结果
		Map<String, Object> columnsValue = adapter.queryRecord(null, SystemTableEnum.DS_SQUID.toString(),
				whereCondList, out);
		if (columnsValue == null) {
			// 无数据返回
			out.setMessageCode(MessageCode.NODATA);
			return null;
		} else {

			// 将数据库查询结果转换为Squid实体
			squid = this.getSquidTwo(columnsValue,out);
			out.setMessageCode(MessageCode.SUCCESS);
		}
		return squid;
	}
	/**
	 * 根据唯一标识符查询Squid
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
	public Squid getSquid(String key, ReturnValue out) {
		logger.debug(String.format("getSquid-key=%s", key));
		Squid squid = null;
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
		// 查询结果
		Map<String, Object> columnsValue = adapter.queryRecord(null, SystemTableEnum.DS_SQUID.toString(),
				whereCondList, out);
		Map<String, Object> dbValue = null;
		if (columnsValue == null) {
			// 无数据返回
			out.setMessageCode(MessageCode.NODATA);
			logger.debug(String.format("SquidModelBase not found by guid(%s)...", key));
			return null;
		} else {
			squid = this.getSquid(columnsValue, out);
			
		}
		return squid;
	}

	/**
	 * 根据SquidFlowID获得Squids
	 * <p>
	 * 作用描述： 根据SquidFlowID查询它所有的子项Squid
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 * @param squidFlowId  squidFlowId
	 * @param out 异常处理
	 * @return
	 */
	public Squid[] getSquidsByFlowId(int squidFlowId, ReturnValue out) {
		logger.debug(String.format("getSquidsByFlowId_id", squidFlowId));
		// 封装查询条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		WhereCondition condition = new WhereCondition();
		condition.setAttributeName("SQUID_FLOW_ID");
		condition.setDataType(DataStatusEnum.STRING);
		condition.setMatchType(MatchTypeEnum.EQ);
		condition.setValue(squidFlowId);
		whereCondList.add(condition);
		List<Map<String, Object>> columnsValue = adapter.query(null, SystemTableEnum.DS_SQUID.toString(),
				whereCondList, out);
		if (columnsValue == null || columnsValue.size() == 0) {
			out.setMessageCode(MessageCode.NODATA);
			return null;
		}
		Squid[] squids = new Squid[columnsValue.size()];// 定义数组接收
		// 将数据库查询结果转换为SquidFlow实体
		int i = 0;
		for (Map<String, Object> column : columnsValue) {

			squids[i] = this.getSquid(column,null);// 接收数据
			i++;
		}
		out.setMessageCode(MessageCode.SUCCESS);
		return squids;
	}


	/**
	 * 根据ID删除Squid
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
	public boolean deleteSquid(int id, ReturnValue out, String token) {
		logger.debug("删除squid begin");
		boolean delete = true;
		try {
			
			// 删除ReferenceGroup
			// 根据squid_id查出ReferenceGroup的id
			String queryGroupSql = "select id from  DS_REFERENCE_COLUMN_GROUP where reference_squid_id="
					+ id;
			List<Map<String, Object>> groupIdList = adapter.query(queryGroupSql, out);
			if(null!=groupIdList&&groupIdList.size()>0)
			{
				logger.debug("DS_REFERENCE_COLUMN_GROUP的长度========="+groupIdList.size());
				RepositoryServiceHelper helper = new RepositoryServiceHelper(token);
				helper.getAdapter().openSession();
				for (int i = 0; i < groupIdList.size(); i++) {
					delete = helper.deleteReferenceGroup((Integer)groupIdList.get(i).get("ID"),out);
					if (!delete) {
						break;
					}
				}
				helper.getAdapter().closeSession();
			}
			if (delete) {
				// 根据squid_id去查询transformation的id
				String queryTransSql = "select id from  DS_TRANSFORMATION  where squid_id="
						+ id;
				List<Map<String, Object>> queryTransList = adapter.query(queryTransSql, out);
				// 删除transformation
				TransformationPlug transformationPlug = new TransformationPlug(
						adapter);
				if(null !=queryTransList&&queryTransList.size()>0)
				{
					logger.debug("DS_TRANSFORMATION的长度========="+queryTransList.size());
					for (int i = 0; i < queryTransList.size(); i++) {
						delete = transformationPlug.deleteTransformation((Integer)queryTransList.get(i).get("ID"), out,token);
						if (!delete) {
							break;
						}
					}
				}
				if (delete) {
					// 删除column
					ColumnPlug columnPlug = new ColumnPlug(adapter);
					String columnSql = "select id from DS_COLUMN where squid_id="
							+ id;
					List<Map<String, Object>> querycolumnList = adapter.query(columnSql, out);
					if(null !=querycolumnList&&querycolumnList.size()>0)
					{
						logger.debug("DS_COLUMN的长度========="+querycolumnList.size());
						for (int i = 0; i < querycolumnList.size(); i++) {
							delete = columnPlug.deleteColumn((Integer)querycolumnList.get(i).get("ID"), out,token);
							if (!delete) {
								break;
							}
						}
					}
					if (delete) {
						// 删除squidlink
						// 查询squidlinkid
						String squidlinkSql = "select id from DS_SQUID_LINK where from_squid_id="
								+ id + " or to_squid_id=" + id + "";
						List<Map<String, Object>> querysquidlinList = adapter.query(squidlinkSql,
								out);
						if(null!=querysquidlinList&&querysquidlinList.size()>0)
						{
							logger.debug("DS_SQUID_LINK的长度========="+querysquidlinList.size());
							SquidLinkServicesub squidLinkService=new SquidLinkServicesub(token);
							for (int i = 0; i < querysquidlinList.size(); i++) {
								delete = squidLinkService.delete((Integer)querysquidlinList.get(i).get("ID"));
								if (!delete) {
									break;
								}
							}
						}
						if (delete) {
							List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
							WhereCondition condition = new WhereCondition();
							condition.setAttributeName("ID");
							condition.setDataType(DataStatusEnum.INT);
							condition.setMatchType(MatchTypeEnum.EQ);
							condition.setValue(id);
							whereCondList.add(condition);
							// 执行删除
							delete = adapter.delete(
									SystemTableEnum.DS_SQUID.toString(),
									whereCondList, out);
						}
					}
				}
			}
			logger.debug(String.format("deleteSquid-squidId=%s,result=%s", id,
					delete));
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error("deleteSquid is err" + e);
		}finally
		{
			
			adapter.commitAdapter();
		}
		return delete;
	}
	/**
	 * 根据唯一标识符查询SquidID
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
	public int getSquidId(String key, ReturnValue out) {
		logger.debug(String.format("getSquidId-key=%s", key));

		// 查询列数据
		List<String> columnList = new ArrayList<String>();
		columnList.add("ID");
		// 封装查询条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		WhereCondition condition = new WhereCondition();
		condition.setAttributeName("KEY");
		condition.setValue("'"+key+"'");
		condition.setMatchType(MatchTypeEnum.EQ);
		whereCondList.add(condition);
		// 定义集合接收
		Map<String, Object> columnsValue = adapter.queryRecord(columnList, SystemTableEnum.DS_SQUID.toString(),
				whereCondList, out);
		if (columnsValue == null) {
			out.setMessageCode(MessageCode.NODATA);
			return 0;
		}
		int id = Integer.parseInt(columnsValue.get("ID").toString());
		logger.debug(String.format("getSquidId-key=%s;result=%s", key, id));
		return id;
	}


	/**
	 * 根据得到Map集合值封装厂Squid对象
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 * 
	 * @param columnsValue
	 * @param dbValue
	 * @return
	 */
	private Squid getSquid(Map<String, Object> columnsValue, ReturnValue out) {
		if(columnsValue==null || columnsValue.isEmpty()){
			return null;
		}
		int id = StringUtils.valueOfInt(columnsValue, "ID");// 流水号
		int type = StringUtils.valueOfInt(columnsValue, "SQUID_TYPE_ID");
		SquidTypeEnum squidType = SquidTypeEnum.parse(type);
		try {
			if(squidType == SquidTypeEnum.DBSOURCE
					|| squidType == SquidTypeEnum.CLOUDDB
					|| squidType==SquidTypeEnum.TRAININGDBSQUID){
				DBSourceSquid dBSourceSquid = AnnotationHelper.result2Object(columnsValue, DBSourceSquid.class);
				DbSquid connection = getDBSquid(id, out);
				if(connection!=null){ //连接信息不为空
					dBSourceSquid.setDb_name(connection.getDb_name());
					dBSourceSquid.setDb_type(connection.getDb_type());
					dBSourceSquid.setHost(connection.getHost());
					dBSourceSquid.setPassword(connection.getPassword());
					dBSourceSquid.setUser_name(connection.getUser_name());
					dBSourceSquid.setPort(connection.getPort());
				}
				return dBSourceSquid;
			}else if(squidType==SquidTypeEnum.DBDESTINATION){
				DBDestinationSquid dbDestSquid = AnnotationHelper.result2Object(columnsValue, DBDestinationSquid.class);
				DbSquid connection = getDBSquid(id, out);
				if(connection!=null){ //连接信息不为空
					dbDestSquid.setDb_name(connection.getDb_name());
					dbDestSquid.setDb_type(connection.getDb_type());
					dbDestSquid.setHost(connection.getHost());
					dbDestSquid.setPassword(connection.getPassword());
					dbDestSquid.setUser_name(connection.getUser_name());
					dbDestSquid.setPort(connection.getPort());
				}
				return dbDestSquid;
			}else if(squidType == SquidTypeEnum.EXTRACT 
			        || squidType == SquidTypeEnum.DOC_EXTRACT
			        || squidType == SquidTypeEnum.XML_EXTRACT
			        || squidType == SquidTypeEnum.WEBLOGEXTRACT
			        || squidType == SquidTypeEnum.WEBEXTRACT){ // Extract
				//ExtractSquid eExtractSquid = AnnotationHelper.result2Object(columnsValue, ExtractSquid.class);
				// TODO 增量抽取
				return AnnotationHelper.result2Object(columnsValue, DataSquid.class);
			}else if(squidType==SquidTypeEnum.STAGE){
				// TODO id
				return AnnotationHelper.result2Object(columnsValue, StageSquid.class);
			}else if(squidType == SquidTypeEnum.REPORT){
				return AnnotationHelper.result2Object(columnsValue, ReportSquid.class);
			}else if(squidType == SquidTypeEnum.FILEFOLDER){
				return AnnotationHelper.result2Object(columnsValue, FileFolderSquid.class);
			}else if(squidType == SquidTypeEnum.FTP){
				return AnnotationHelper.result2Object(columnsValue, FtpSquid.class);
			}else if(squidType == SquidTypeEnum.HDFS || squidType==SquidTypeEnum.SOURCECLOUDFILE || squidType == SquidTypeEnum.TRAINNINGFILESQUID){
				return AnnotationHelper.result2Object(columnsValue, HdfsSquid.class);
			}else if(squidType == SquidTypeEnum.WEIBO){
				return AnnotationHelper.result2Object(columnsValue, WeiboSquid.class);
			}else if(squidType == SquidTypeEnum.WEB){
				return AnnotationHelper.result2Object(columnsValue, WebSquid.class);
			}else if(squidType == SquidTypeEnum.LOGREG 
			        || squidType == SquidTypeEnum.NAIVEBAYES
			        || squidType == SquidTypeEnum.SVM
			        || squidType == SquidTypeEnum.KMEANS
			        || squidType == SquidTypeEnum.ALS
			        || squidType == SquidTypeEnum.LINEREG
			        || squidType == SquidTypeEnum.RIDGEREG
			        || squidType == SquidTypeEnum.QUANTIFY
			        || squidType == SquidTypeEnum.DISCRETIZE
			        || squidType == SquidTypeEnum.DECISIONTREE
					|| squidType == SquidTypeEnum.ASSOCIATION_RULES){
			    return AnnotationHelper.result2Object(columnsValue, DataMiningSquid.class);
			}
			else{
				return AnnotationHelper.result2Object(columnsValue, Squid.class);
			}
		} catch (Exception e) {
			out.setMessageCode(MessageCode.ERR_ENUM);
			logger.error("error", e);
		}
		return null;
	}
	
	
	/**
	 * <p>
	 * 作用描述：根据squidFlowId,查找ESquid集合
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 * 
	 * @param squidFlowId
	 * @param out
	 * @return
	 */
	public Squid[] getSquidFlowESquid(int squidFlowId, ReturnValue out) {
		logger.debug(String.format("getSquidFlowTable-squidFlowId=%s", squidFlowId));
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
		List<Map<String, Object>> eSquidList = adapter.query(columnList, SystemTableEnum.DS_SQUID.toString(),
				whereCondList, out);
		if (eSquidList == null) {
			// 无数据返回
			out.setMessageCode(MessageCode.NODATA);
			logger.debug(String.format("squid not found by squidFlowId(%s)...", squidFlowId));
			return null;
		}
		logger.debug(String.format("getSquidFlowESquid size=%s", eSquidList.size()));
		// 将数据转换为ESquidLink实体
		int i = 0;
		Squid[] squidArray = new Squid[eSquidList.size()];// 定义数组接收
		for (Map<String, Object> column : eSquidList) {
			squidArray[i] = getSquid(column,out);// 接收数据
			i++;
		}
		logger.debug(String.format("squidArray getSquidFlowEsquid size=%s", squidArray.length));
		out.setMessageCode(MessageCode.SUCCESS);
		return squidArray;
	}
	
	/** 获取指定squid flow 的 squid数量*/
	public int getSquidCount(int squidFlowId, ReturnValue out) {
		// 封装条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>(1);
		// 条件
		whereCondList.add(new WhereCondition("SQUID_FLOW_ID", MatchTypeEnum.EQ, squidFlowId));
		// 查询列及结果
		List<Map<String, Object>> eSquidList = 
				adapter.query(null, SystemTableEnum.DS_SQUID.toString(), whereCondList, out);
		return eSquidList == null ? 0 : eSquidList.size();
	}
	
	/**
	 * 
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param squidFlowId
	 *@param type
	 *@param out
	 *@return
	 */
	public Squid[] getSquidFlowESquid(int squidFlowId,String type, ReturnValue out) {
		logger.debug(String.format("getSquidFlowTable-squidFlowId=%s", squidFlowId));
		//枚举类型
		int typeID = SquidTypeEnum.valueOf(type).value();
		// 封装条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		// 条件
		WhereCondition whereCondition = null;
		whereCondition = new WhereCondition();
		whereCondition.setAttributeName("SQUID_TYPE_ID");
		whereCondition.setMatchType(MatchTypeEnum.EQ);
		whereCondition.setValue(typeID);
		whereCondList.add(whereCondition);
		whereCondition = new WhereCondition();
		whereCondition.setAttributeName("SQUID_FLOW_ID");
		whereCondition.setMatchType(MatchTypeEnum.EQ);
		whereCondition.setValue(squidFlowId);
		whereCondList.add(whereCondition);
		// 查询列及结果
		List<String> columnList = new ArrayList<String>();
		List<Map<String, Object>> eSquidList = adapter.query(columnList, SystemTableEnum.DS_SQUID.toString(),
				whereCondList, out);
		if (eSquidList == null) {
			// 无数据返回
			out.setMessageCode(MessageCode.NODATA);
			logger.debug(String.format("squid not found by squidFlowId(%s)...", squidFlowId));
			return null;
		}
		logger.debug(String.format("getSquidFlowESquid size=%s", eSquidList.size()));
		// 将数据转换为ESquidLink实体
		int i = 0;
		Squid[] squidArray = new Squid[eSquidList.size()];// 定义数组接收
		for (Map<String, Object> column : eSquidList) {
			squidArray[i] = getSquid(column,out);// 接收数据
			i++;
		}
		logger.debug(String.format("squidArray getSquidFlowEsquid size=%s", squidArray.length));
		out.setMessageCode(MessageCode.SUCCESS);
		return squidArray;
	}
	
	/**
	 *根据squidFlowID获得 DBDestinationSquid
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param squidFlowId
	 *@param type
	 *@param out
	 *@return
	 */
	public DBDestinationSquid[] getSquidFlowDestSquid(int squidFlowId,String type, ReturnValue out) {
		logger.debug(String.format("getSquidFlowDestSquid-squidFlowId=%s", squidFlowId));
		//枚举类型
		int typeID = SquidTypeEnum.valueOf(type).value();
		// 封装条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		// 条件
		WhereCondition whereCondition = null;
		whereCondition = new WhereCondition();
		whereCondition.setAttributeName("SQUID_TYPE_ID");
		whereCondition.setMatchType(MatchTypeEnum.EQ);
		whereCondition.setValue(typeID);
		whereCondList.add(whereCondition);
		whereCondition = new WhereCondition();
		whereCondition.setAttributeName("SQUID_FLOW_ID");
		whereCondition.setMatchType(MatchTypeEnum.EQ);
		whereCondition.setValue(squidFlowId);
		whereCondList.add(whereCondition);
		// 查询列及结果
		List<String> columnList = new ArrayList<String>();
		List<Map<String, Object>> eSquidList = adapter.query(columnList, SystemTableEnum.DS_SQUID.toString(),
				whereCondList, out);
		if (eSquidList == null) {
			// 无数据返回
			out.setMessageCode(MessageCode.NODATA);
			logger.debug(String.format("squid not found by squidFlowId(%s)...", squidFlowId));
			return null;
		}
		logger.debug(String.format("getSquidFlowESquid size=%s", eSquidList.size()));
		// 将数据转换为ESquidLink实体
		int i = 0;
		DBDestinationSquid[] squidArray = new DBDestinationSquid[eSquidList.size()];// 定义数组接收
		for (Map<String, Object> column : eSquidList) {
			squidArray[i] = (DBDestinationSquid) getSquid(column,out);// 接收数据
			i++;
		}
		logger.debug(String.format("squidArray getSquidFlowDestSquid size=%s", squidArray.length));
		out.setMessageCode(MessageCode.SUCCESS);
		return squidArray;
	}
	
	/**
	 * 获得stagesquid
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param squidFlowId
	 *@param type
	 *@param out
	 *@return
	 * @throws Exception 
	 */
	public StageSquid[] getSquidFlowStagSquid(IRelationalDataManager adapter3, int squidFlowId, String squidIds, String type, ReturnValue out) throws Exception {
		logger.debug(String.format("getSquidFlowStagSquid-squidFlowId=%s", squidFlowId));
		String sql = "select * from ds_squid where squid_flow_id="+squidFlowId+" and squid_type_id="+SquidTypeEnum.valueOf(type).value();
        if (!ValidateUtils.isEmpty(squidIds)){
        	sql = sql + " and a.id in ("+squidIds+")";
        }
		List<Map<String, Object>> eSquidList = adapter3.query2List(true, sql, null);
		if (eSquidList == null) {
			// 无数据返回
			out.setMessageCode(MessageCode.NODATA);
			logger.debug(String.format("squid not found by squidFlowId(%s)...", squidFlowId));
			return null;
		}
		logger.debug(String.format("getSquidFlowESquid size=%s", eSquidList.size()));
		// 将数据转换为ESquidLink实体
		int i = 0;
		StageSquid[] squidArray = new StageSquid[eSquidList.size()];// 定义数组接收
		for (Map<String, Object> column : eSquidList) {
			squidArray[i] =AnnotationHelper.result2Object(column, StageSquid.class); 
			i++;
		}
		logger.debug(String.format("squidArray getSquidFlowStagSquid size=%s", squidArray.length));
		out.setMessageCode(MessageCode.SUCCESS);
		return squidArray;
	}
	   /**
	    * 获取DataMiningSquid的信息
	    * @param squidFlowId
	    * @param type
	    * @param out
	    * @return
	    * @author bo.dang
	 * @throws SQLException 
	    * @date 2014年5月23日
	    */
	   public DataMiningSquid[] getSquidFlowDataMiningSquid(IRelationalDataManager adapter3, int squidFlowId, String squidIds, String type, ReturnValue out) throws SQLException {
	        logger.debug(String.format("getSquidFlowDataMiningSquid-squidFlowId=%s", squidFlowId));
	        String sql = "select * from ds_squid where squid_flow_id="+squidFlowId+" and squid_type_id="+SquidTypeEnum.valueOf(type).value();
	        if (!ValidateUtils.isEmpty(squidIds)){
	        	sql = sql + " and ds.id in ("+squidIds+")";
	        }
	        List<Map<String, Object>> eSquidList = adapter3.query2List(true, sql, null);
	        if (eSquidList == null) {
	            // 无数据返回
	            out.setMessageCode(MessageCode.NODATA);
	            logger.debug(String.format("squid not found by squidFlowId(%s)...", squidFlowId));
	            return null;
	        }
	        logger.debug(String.format("getSquidFlowESquid size=%s", eSquidList.size()));
	        // 将数据转换为ESquidLink实体
	        int i = 0;
	        DataMiningSquid[] squidArray = new DataMiningSquid[eSquidList.size()];// 定义数组接收
	        for (Map<String, Object> column : eSquidList) {
	            squidArray[i] = (DataMiningSquid) getSquid(column,out);// 接收数据
	            i++;
	        }
	        logger.debug(String.format("squidArray getSquidFlowDataMiningSquid size=%s", squidArray.length));
	        out.setMessageCode(MessageCode.SUCCESS);
	        return squidArray;
	    }
	/**
	 * 取得ReportSquid
	 * @param squidFlowId
	 * @param type
	 * @param out
	 * @return
	 * @author bo.dang
	 * @throws Exception 
	 * @date 2014-4-24
	 */
	public ReportSquid[] getSquidFlowReportSquid(IRelationalDataManager adapter3, int squidFlowId, String squidIds, String type, ReturnValue out) throws Exception{
		logger.debug(String.format("getSquidFlowReportSquid-squidFlowId=%s", squidFlowId));
		String sql = "select * from ds_squid a inner join DS_REPORT_SQUID b on a.id=b.id where a.squid_flow_id="+squidFlowId+" and a.squid_type_id="+SquidTypeEnum.valueOf(type).value();
        if (!ValidateUtils.isEmpty(squidIds)){
        	sql = sql + " and a.id in ("+squidIds+")";
        }
		List<Map<String, Object>> rSquidList = adapter3.query2List(true, sql, null);
		if (rSquidList == null) {
			// 无数据返回
			out.setMessageCode(MessageCode.NODATA);
			logger.debug(String.format("squid not found by squidFlowId(%s)...", squidFlowId));
			return null;
		}
		logger.debug(String.format("getSquidFlowReportSquid size=%s", rSquidList.size()));
		// 将数据转换为ESquidLink实体
		int i = 0;
		ReportSquid[] squidArray = new ReportSquid[rSquidList.size()];// 定义数组接收
		for (Map<String, Object> column : rSquidList) {
			squidArray[i] =AnnotationHelper.result2Object(column, ReportSquid.class);  
			i++;
		}
		logger.debug(String.format("squidArray getSquidFlowReportSquid size=%s", squidArray.length));
		out.setMessageCode(MessageCode.SUCCESS);
		return squidArray;
	}
	
	/**
	 * 取得FileFolderSquid
	 * @param squidFlowId
	 * @param type
	 * @param out
	 * @return
	 * @author lei.bin
	 * @throws Exception 
	 * @date 2014-5-9
	 */
	public FileFolderSquid[] getSquidFlowFileFolderSquid(IRelationalDataManager adapter3,int squidFlowId,String squidIds,String type, ReturnValue out)throws Exception{
		logger.debug(String.format("getSquidFlowFileFolderSquid-squidFlowId=%s", squidFlowId));
		String sql = "select * from ds_squid where squid_flow_id="+squidFlowId+" and squid_type_id="+SquidTypeEnum.valueOf(type).value();
        if (!ValidateUtils.isEmpty(squidIds)){
        	sql = sql + " and a.id in ("+squidIds+")";
        }
		List<Map<String, Object>> rSquidList = adapter3.query2List(true, sql, null);
		if (rSquidList == null) {
			// 无数据返回
			out.setMessageCode(MessageCode.NODATA);
			logger.debug(String.format("squid not found by squidFlowId(%s)...", squidFlowId));
			return null;
		}
		logger.debug(String.format("getSquidFlowFileFolderSquid size=%s", rSquidList.size()));
		// 将数据转换为ESquidLink实体
		int i = 0;
		FileFolderSquid[] squidArray = new FileFolderSquid[rSquidList.size()];// 定义数组接收
		for (Map<String, Object> column : rSquidList) {
			squidArray[i] = AnnotationHelper.result2Object(column, FileFolderSquid.class);
			i++;
		}
		logger.debug(String.format("squidArray getSquidFlowFileFolderSquid size=%s", squidArray.length));
		out.setMessageCode(MessageCode.SUCCESS);
		return squidArray;
	}
	/**
	 * 取得FtpSquid
	 * @param squidFlowId
	 * @param type
	 * @param out
	 * @return
	 * @author lei.bin
	 * @throws Exception 
	 * @date 2014-5-9
	 */
	public FtpSquid[] getSquidFlowFtpSquid(IRelationalDataManager adapter3,int squidFlowId,String squidIds,String type, ReturnValue out) throws Exception{
		logger.debug(String.format("getSquidFlowFtpSquid-squidFlowId=%s", squidFlowId));
		String sql = "select * from ds_squid where squid_flow_id="+squidFlowId+" and squid_type_id="+SquidTypeEnum.valueOf(type).value();
        if (!ValidateUtils.isEmpty(squidIds)){
        	sql = sql + " and a.id in ("+squidIds+")";
        }
		List<Map<String, Object>> rSquidList = adapter3.query2List(true, sql, null);
		if (rSquidList == null) {
			// 无数据返回
			out.setMessageCode(MessageCode.NODATA);
			logger.debug(String.format("squid not found by squidFlowId(%s)...", squidFlowId));
			return null;
		}
		logger.debug(String.format("getSquidFlowFtpSquid size=%s", rSquidList.size()));
		// 将数据转换为ESquidLink实体
		int i = 0;
		FtpSquid[] squidArray = new FtpSquid[rSquidList.size()];// 定义数组接收
		for (Map<String, Object> column : rSquidList) {
			squidArray[i] = AnnotationHelper.result2Object(column, FtpSquid.class);
			i++;
		}
		logger.debug(String.format("squidArray getSquidFlowFtpSquid size=%s", squidArray.length));
		out.setMessageCode(MessageCode.SUCCESS);
		return squidArray;
	}
	/**
	 * 取得HdfsSquid
	 * @param squidFlowId
	 * @param type
	 * @param out
	 * @return
	 * @author lei.bin
	 * @throws Exception 
	 * @date 2014-5-9
	 */
	public HdfsSquid[] getSquidFlowHdfsSquid(IRelationalDataManager adapter3,int squidFlowId,String squidIds,String type, ReturnValue out) throws Exception{
		logger.debug(String.format("getSquidFlowHdfsSquid-squidFlowId=%s", squidFlowId));
		String sql = "select * from ds_squid where squid_flow_id="+squidFlowId+" and squid_type_id="+SquidTypeEnum.valueOf(type).value();
        if (!ValidateUtils.isEmpty(squidIds)){
        	sql = sql + " and a.id in ("+squidIds+")";
        }
		List<Map<String, Object>> rSquidList = adapter3.query2List(true, sql, null);
		if (rSquidList == null) {
			// 无数据返回
			out.setMessageCode(MessageCode.NODATA);
			logger.debug(String.format("squid not found by squidFlowId(%s)...", squidFlowId));
			return null;
		}
		logger.debug(String.format("getSquidFlowHdfsSquid size=%s", rSquidList.size()));
		// 将数据转换为ESquidLink实体
		int i = 0;
		HdfsSquid[] squidArray = new HdfsSquid[rSquidList.size()];// 定义数组接收
		for (Map<String, Object> column : rSquidList) {
			squidArray[i] = AnnotationHelper.result2Object(column, HdfsSquid.class);
			i++;
		}
		logger.debug(String.format("squidArray getSquidFlowHdfsSquid size=%s", squidArray.length));
		out.setMessageCode(MessageCode.SUCCESS);
		return squidArray;
	}
	/**
	 * 取得WeiBoSquid
	 * @param squidFlowId
	 * @param type
	 * @param out
	 * @return
	 * @author lei.bin
	 * @throws Exception 
	 * @date 2014-5-9
	 */
	@Deprecated
	public WeiboSquid[] getSquidFlowWeiBoSquid(IRelationalDataManager adapter3,int squidFlowId,String squidIds,String type, ReturnValue out) throws Exception{
		logger.debug(String.format("getSquidFlowWeiBoSquid-squidFlowId=%s", squidFlowId));
		String sql = "select * from ds_squid a inner join DS_WEIBO_CONNECTION b  on a.id=b.id  where a.squid_flow_id="+squidFlowId+" and a.squid_type_id="+SquidTypeEnum.valueOf(type).value();
        if (!ValidateUtils.isEmpty(squidIds)){
        	sql = sql + " and a.id in ("+squidIds+")";
        }
		List<Map<String, Object>> rSquidList = adapter3.query2List(true, sql, null);
		if (rSquidList == null) {
			// 无数据返回
			out.setMessageCode(MessageCode.NODATA);
			logger.debug(String.format("squid not found by squidFlowId(%s)...", squidFlowId));
			return null;
		}
		logger.debug(String.format("getSquidFlowWeiBoSquid size=%s", rSquidList.size()));
		// 将数据转换为ESquidLink实体
		int i = 0;
		WeiboSquid[] squidArray = new WeiboSquid[rSquidList.size()];// 定义数组接收
		for (Map<String, Object> column : rSquidList) {
			squidArray[i] = AnnotationHelper.result2Object(column, WeiboSquid.class);
			i++;
		}
		logger.debug(String.format("squidArray getSquidFlowWeiBoSquid size=%s", squidArray.length));
		out.setMessageCode(MessageCode.SUCCESS);
		return squidArray;
	}
	/**
	 * 取得WebSquid
	 * @param squidFlowId
	 * @param type
	 * @param out
	 * @return
	 * @author lei.bin
	 * @throws Exception 
	 * @date 2014-5-9
	 */
	@Deprecated
	public WebSquid[] getSquidFlowWebSquid(IRelationalDataManager adapter3,int squidFlowId,String squidIds,String type, ReturnValue out) throws Exception{
		logger.debug(String.format("getSquidFlowWebSquid-squidFlowId=%s", squidFlowId));
		String sql = "select * from ds_squid a inner join DS_WEB_CONNECTION b on a.id=b.id  where a.squid_flow_id="+squidFlowId+" and a.squid_type_id="+SquidTypeEnum.valueOf(type).value();
        if (!ValidateUtils.isEmpty(squidIds)){
        	sql = sql + " and a.id in ("+squidIds+")";
        }
		List<Map<String, Object>> rSquidList = adapter3.query2List(true, sql, null);
		if (rSquidList == null) {
			// 无数据返回
			out.setMessageCode(MessageCode.NODATA);
			logger.debug(String.format("squid not found by squidFlowId(%s)...", squidFlowId));
			return null;
		}
		logger.debug(String.format("getSquidFlowWebSquid size=%s", rSquidList.size()));
		// 将数据转换为ESquidLink实体
		int i = 0;
		WebSquid[] squidArray = new WebSquid[rSquidList.size()];// 定义数组接收
		for (Map<String, Object> column : rSquidList) {
			squidArray[i] = AnnotationHelper.result2Object(column, WebSquid.class);
			i++;
		}
		logger.debug(String.format("squidArray getSquidFlowWebSquid size=%s", squidArray.length));
		out.setMessageCode(MessageCode.SUCCESS);
		return squidArray;
	}
	/**
	 * 
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param squidFlowId
	 *@param type
	 *@param out
	 *@return
	 * @throws Exception 
	 */
	public List<DBDestinationSquid> getDestSquid(IRelationalDataManager adapter3, int squidFlowId, String squidIds, int typeID, ReturnValue out) throws Exception {
		//枚举类型
		//int typeID = SquidTypeEnum.valueOf(type).value();
		// 封装条件
		String sql = "select * from ds_squid where squid_flow_id="+squidFlowId+" and squid_type_id="+SquidTypeEnum.valueOf(typeID).value();
        if (!ValidateUtils.isEmpty(squidIds)){
        	sql = sql + " and id in ("+squidIds+")";
        }
		// 查询列及结果
		List<Map<String, Object>> eSquidList = adapter3.query2List(true, sql, null);
		logger.debug("[Dest SquidModelBase]\tsquid_flow_id="+squidFlowId+"\t"+eSquidList);
		if (eSquidList == null || eSquidList.isEmpty()) { // 无数据返回
			out.setMessageCode(MessageCode.NODATA);
			return null;
		}
		// 将数据转换为ESquidLink实体
		List<DBDestinationSquid> squidArray = new ArrayList<DBDestinationSquid>(eSquidList.size());// 定义数组接收
		/*for (int i=0; i<eSquidList.size(); i++) {
			SquidModelBase s = getSquid(eSquidList.get(i), out);// 接收数据
			if(s!=null && s instanceof DBDestinationSquid){
				squidArray.add((DBDestinationSquid) s);
			}
		}*/
		for(Map<String, Object> column : eSquidList)
		{
			DBDestinationSquid dbDestinationSquid=new DBDestinationSquid();
			dbDestinationSquid=AnnotationHelper.result2Object(column, DBDestinationSquid.class);
			squidArray.add(dbDestinationSquid);
		}
		out.setMessageCode(MessageCode.SUCCESS);
		return squidArray;
	}
	
	
	/**
	 * 根据squidFlowid查询ExtractSquid
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param squidFlowId
	 *@param type
	 *@param out
	 *@return
	 */
	public TableExtractSquid[] getSquidFlowExtractSquid(int squidFlowId,String type, ReturnValue out) {
		logger.debug(String.format("getSquidFlowExtractSquid-squidFlowId=%s", squidFlowId));
		//枚举类型
		int typeID = SquidTypeEnum.valueOf(type).value();
		// 封装条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		// 条件
		WhereCondition whereCondition = null;
		whereCondition = new WhereCondition();
		whereCondition.setAttributeName("SQUID_TYPE_ID");
		whereCondition.setMatchType(MatchTypeEnum.EQ);
		whereCondition.setValue(typeID);
		whereCondList.add(whereCondition);
		whereCondition = new WhereCondition();
		whereCondition.setAttributeName("SQUID_FLOW_ID");
		whereCondition.setMatchType(MatchTypeEnum.EQ);
		whereCondition.setValue(squidFlowId);
		whereCondList.add(whereCondition);
		// 查询列及结果
		List<String> columnList = new ArrayList<String>();
		List<Map<String, Object>> eSquidList = adapter.query(columnList, SystemTableEnum.DS_SQUID.toString(),
				whereCondList, out);
		if (eSquidList == null) {
			// 无数据返回
			out.setMessageCode(MessageCode.NODATA);
			logger.debug(String.format("squid not found by squidFlowId(%s)...", squidFlowId));
			return null;
		}
		logger.debug(String.format("getSquidFlowESquid size=%s", eSquidList.size()));
		// 将数据转换为ESquidLink实体
		int i = 0;
		TableExtractSquid[] squidArray = new TableExtractSquid[eSquidList.size()];// 定义数组接收
		for (Map<String, Object> column : eSquidList) {
			squidArray[i] = (TableExtractSquid) getSquid(column,out);// 接收数据
			i++;
		}
		logger.debug(String.format("squidArray getSquidFlowEsquid size=%s", squidArray.length));
		out.setMessageCode(MessageCode.SUCCESS);
		return squidArray;
	}
	
	/**
     * 根据squidFlowid查询ExtractSquid
     * <p>
     * 作用描述：
     * </p>
     * <p>
     * 修改说明：
     * </p>
     *@param squidFlowId
     *@param type
     *@param out
     *@return
     */
    public DataSquid[] getSquidFlowAllExtractSquid(int squidFlowId,String type, ReturnValue out) {
        logger.debug(String.format("getSquidFlowExtractSquid-squidFlowId=%s", squidFlowId));
        //枚举类型
        int typeID = SquidTypeEnum.valueOf(type).value();
        // 封装条件
        List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
        // 条件
        WhereCondition whereCondition = null;
        whereCondition = new WhereCondition();
        whereCondition.setAttributeName("SQUID_TYPE_ID");
        whereCondition.setMatchType(MatchTypeEnum.EQ);
        whereCondition.setValue(typeID);
        whereCondList.add(whereCondition);
        whereCondition = new WhereCondition();
        whereCondition.setAttributeName("SQUID_FLOW_ID");
        whereCondition.setMatchType(MatchTypeEnum.EQ);
        whereCondition.setValue(squidFlowId);
        whereCondList.add(whereCondition);
        // 查询列及结果
        List<String> columnList = new ArrayList<String>();
        List<Map<String, Object>> eSquidList = adapter.query(columnList, SystemTableEnum.DS_SQUID.toString(),
                whereCondList, out);
        if (eSquidList == null) {
            // 无数据返回
            out.setMessageCode(MessageCode.NODATA);
            logger.debug(String.format("squid not found by squidFlowId(%s)...", squidFlowId));
            return null;
        }
        logger.debug(String.format("getSquidFlowESquid size=%s", eSquidList.size()));
        // 将数据转换为ESquidLink实体
        int i = 0;
        DataSquid[] squidArray = new DataSquid[eSquidList.size()];// 定义数组接收
        for (Map<String, Object> column : eSquidList) {
            squidArray[i] = (DataSquid) getSquid(column,out);// 接收数据
            i++;
        }
        logger.debug(String.format("squidArray getSquidFlowEsquid size=%s", squidArray.length));
        out.setMessageCode(MessageCode.SUCCESS);
        return squidArray;
    }
	
	/**
	 *根据 SquidFlow查询所有DBSourceSquid
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param squidFlowId
	 *@param type
	 *@param out
	 *@return
	 * @throws Exception 
	 */
	public DBSourceSquid[] getSquidFlowEDBsouresSquid(IRelationalDataManager adapter3, int squidFlowId, String squidIds, String type, ReturnValue out) throws Exception {
		logger.debug(String.format("getSquidFlowTable-squidFlowId=%s", squidFlowId));
		String sql = "select * from ds_squid where squid_flow_id="+squidFlowId+" and squid_type_id="+SquidTypeEnum.valueOf(type).value();
        if (!ValidateUtils.isEmpty(squidIds)){
        	sql = sql + " and id in ("+squidIds+")";
        }
		List<Map<String, Object>> eSquidList = adapter3.query2List(true, sql, null);
		if (eSquidList == null) {
			// 无数据返回
			out.setMessageCode(MessageCode.NODATA);
			logger.debug(String.format("squid not found by squidFlowId(%s)...", squidFlowId));
			return null;
		}
		logger.debug(String.format("getSquidFlowESquid size=%s", eSquidList.size()));
		// 将数据转换为ESquidLink实体
		int i = 0;
		DBSourceSquid[] squidArray = new DBSourceSquid[eSquidList.size()];// 定义数组接收
		for (Map<String, Object> column : eSquidList) {
			squidArray[i] =AnnotationHelper.result2Object(column, DBSourceSquid.class);
			i++;
		}
		logger.debug(String.format("squidArray getSquidFlowEsquid size=%s", squidArray.length));
		out.setMessageCode(MessageCode.SUCCESS);
		return squidArray;
	}
	
	public ExceptionSquid[] getSquidFlowExceptionSquid(int squidFlowId,String type, ReturnValue out){
		logger.debug(String.format("getSquidFlowExceptionSquid-squidFlowId=%s", squidFlowId));
		// 封装条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		// 条件
		WhereCondition whereCondition = null;
		whereCondition = new WhereCondition();
		whereCondition.setAttributeName("SQUID_TYPE_ID");
		whereCondition.setMatchType(MatchTypeEnum.EQ);
		whereCondition.setValue(SquidTypeEnum.valueOf(type).value());
		whereCondList.add(whereCondition);
		whereCondition = new WhereCondition();
		whereCondition.setAttributeName("SQUID_FLOW_ID");
		whereCondition.setMatchType(MatchTypeEnum.EQ);
		whereCondition.setValue(squidFlowId);
		whereCondList.add(whereCondition);
		// 查询列及结果
		List<String> columnList = new ArrayList<String>();
		List<Map<String, Object>> eSquidList = adapter.query(columnList, SystemTableEnum.DS_SQUID.toString(),
				whereCondList, out);
		if (eSquidList == null) {
			// 无数据返回
			out.setMessageCode(MessageCode.NODATA);
			logger.debug(String.format("squid not found by squidFlowId(%s)...", squidFlowId));
			return null;
		}
		logger.debug(String.format("getSquidFlowExceptionSquid size=%s", eSquidList.size()));
		// 将数据转换为ESquidLink实体
		int i = 0;
		ExceptionSquid[] squidArray = new ExceptionSquid[eSquidList.size()];// 定义数组接收
		for (Map<String, Object> column : eSquidList) {
			squidArray[i] = (ExceptionSquid) this.getSquid(column,out);// 接收数据
			i++;
		}
		logger.debug(String.format("squidArray getSquidFlowExceptionSquid size=%s", squidArray.length));
		out.setMessageCode(MessageCode.SUCCESS);
		return squidArray;
	}

	/**
	 * 查询出SquidFlowID
	 * 
	 * @param columnsValue
	 * @param out
	 * @return
	 */
	private Squid getSquidTwo(Map<String, Object> columnsValue, ReturnValue out) {
		Squid squid = new Squid();
		try {
			squid.setId((Integer) columnsValue.get("ID"));// 流水号
			squid.setName(columnsValue.get("NAME") == null ? null
					: columnsValue.get("NAME").toString());// 名字
			squid.setSquidflow_id(columnsValue.get("SQUID_FLOW_ID") == null ? null
					: Integer.parseInt(columnsValue.get("SQUID_FLOW_ID")
							.toString()));// SquidFlowID
			/*
			 * Integer connectionId = columnsValue.get("CONNECTION_ID") == null
			 * ? null : Integer.parseInt(columnsValue.get("CONNECTION_ID")
			 * .toString()); String desc = columnsValue.get("DESCRIPTION") ==
			 * null ? null : columnsValue.get("DESCRIPTION").toString();// 描述
			 * Integer locationY = columnsValue.get("LOCATION_Y") == null ? 0 :
			 * Integer.parseInt(columnsValue.get("LOCATION_Y") .toString());//
			 * Y轴 Integer locationX = columnsValue.get("LOCATION_X") == null ? 0
			 * : Integer.parseInt(columnsValue.get("LOCATION_X") .toString());//
			 * X轴 // TODO Integer squidWidth = columnsValue.get("SQUID_WIDTH")
			 * == null ? 0 : (int)
			 * Double.parseDouble(columnsValue.get("SQUID_WIDTH")
			 * .toString());// 宽 Integer squidHeight =
			 * columnsValue.get("SQUID_HEIGHT") == null ? 0 : (int)
			 * Double.parseDouble(columnsValue.get("SQUID_HEIGHT")
			 * .toString());// 长
			 * 
			 * Integer isIndexed = columnsValue.get("IS_INDEXED") == null ? 0 :
			 * Integer.parseInt(columnsValue.get("IS_INDEXED ") .toString());//
			 * is_indexed Integer isHistory = columnsValue.get("IS_HISTORY") ==
			 * null ? 0 : Integer.parseInt(columnsValue.get("IS_HISTORY ")
			 * .toString());// isHistory Integer isPersisted =
			 * columnsValue.get("IS_PERSISTED") == null ? 0 :
			 * Integer.parseInt(columnsValue.get("IS_PERSISTED ")
			 * .toString());// isPersisted
			 * 
			 * // /is_show_all是否显示非变换列 boolean isShowAll =
			 * columnsValue.get("IS_SHOW_ALL") == null ? false :
			 * Integer.parseInt(columnsValue.get("IS_SHOW_ALL")
			 * .toString().trim()) == 0 ? false : true; // source_is_show_all
			 * 是否显示源非变换列 boolean sourceIsShowALL =
			 * columnsValue.get("SOURCE_IS_SHOW_ALL") == null ? false :
			 * Integer.parseInt(columnsValue.get("SOURCE_IS_SHOW_ALL")
			 * .toString().trim()) == 0 ? false : true; //
			 * transformation_group_x Integer transformationGroupx =
			 * columnsValue .get("TRANSFORMATION_GROUP_X") == null ? 0 : Integer
			 * .parseInt(columnsValue.get("TRANSFORMATION_GROUP_X")
			 * .toString()); // transformation_group_Y Integer
			 * transformationGroupy = columnsValue
			 * .get("TRANSFORMATION_GROUP_Y") == null ? 0 : Integer
			 * .parseInt(columnsValue.get("TRANSFORMATION_GROUP_Y")
			 * .toString());
			 * 
			 * // source_transformation_group_x Integer
			 * sourceTransformationGroupx = columnsValue
			 * .get("SOURCE_TRANSFORMATION_GROUP_X") == null ? 0 : Integer
			 * .parseInt(columnsValue.get("SOURCE_TRANSFORMATION_GROUP_X")
			 * .toString()); // source_transformation_group_Y Integer
			 * sourceTransformationGroupy = columnsValue
			 * .get("SOURCE_TRANSFORMATION_GROUP_Y") == null ? 0 : Integer
			 * .parseInt(columnsValue.get("SOURCE_TRANSFORMATION_GROUP_Y")
			 * .toString());
			 */
			// TODO
			// squid.setConnectionId(connectionId);

		} catch (Exception e) {
			out.setMessageCode(MessageCode.ERR_SQUIDTWO);
		}
		return squid;
	}
	
	/**
	 * 根据ID查KEY
	 * @param squidId
	 * @param out 查询结果应答码
	 * @return
	 */
	public String getSquidKeyById(int squidId, ReturnValue out) {
		// 封装过滤条件
		List<WhereCondition> whereList = new ArrayList<WhereCondition>();
		whereList.add(new WhereCondition("ID", MatchTypeEnum.EQ, squidId));
		// 封装查询的列
		List<String> columnList = new ArrayList<String>();
		columnList.add("KEY");
		// 查询结果
		return StringUtils.valueOf(
				adapter.queryRecord(columnList, SystemTableEnum.DS_SQUID.toString(), whereList, out), 
				"KEY");
	}
	
}

