package com.eurlanda.datashire.sprint7.service.squidflow.subservice;

import cn.com.jsoft.jframe.utils.ValidateUtils;
import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.dao.ISquidDao;
import com.eurlanda.datashire.dao.impl.SquidDaoImpl;
import com.eurlanda.datashire.entity.*;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.enumeration.MessageBubbleCode;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.sprint7.service.squidflow.AbstractRepositoryService;
import com.eurlanda.datashire.utility.CommonConsts;
import com.eurlanda.datashire.utility.DateUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import com.eurlanda.datashire.utility.StringUtils;
import com.eurlanda.datashire.validator.SquidValidationTask;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReportSquidServicesub extends AbstractRepositoryService{

	public ReportSquidServicesub(String token){
		super(token);
	}
    protected ReportSquidServicesub(IRelationalDataManager adapter) {
		super(adapter);
	}
    
    /**
     *
     * @param token
     * @param inSession
     * @param adapter
     * @param reportSquid
     * @author bo.dang
     * @date 2014-4-25
     */
    public static ReportSquid setReportSquidData(String token, boolean inSession, IRelationalDataManager adapter, ReportSquid reportSquid){
				int id = reportSquid.getId();
                Map<String, String> paramMap = new HashMap<String, String>();
				paramMap.put("id", Integer.toString(id, 10));
				ReportSquid reportSquidResult = (ReportSquid) adapter.query2Object(paramMap, ReportSquid.class);
				if(null != reportSquidResult){
				        /*IRelationalDataManager sysAdapter = adapterFactory.getDefaultDataManager();
				        int repositoryId = reportSquidResult.getRepository_id();
				        int folderId = reportSquidResult.getFolder_id();
				        paramMap.clear();
				        paramMap.put("repository_id", Integer.toString(repositoryId, 10));
				        paramMap.put("squid_id", Integer.toString(id, 10));
				        paramMap.put("folder_id", Integer.toString(folderId, 10));
			            ReportFolderMapping reportFolderMapping  = sysAdapter.query2Object(paramMap, ReportFolderMapping.class);
				    if(StringUtils.isNull(reportFolderMapping)){
				        // 设置folder_id
				        reportSquid.setFolder_id(0);
				    }
				    else {
				        // 设置folder_id
				        reportSquid.setFolder_id(folderId);
				    }*/
					// 设置是否是实时报表
					reportSquid.setIs_real_time(reportSquidResult.isIs_real_time());
					// 设置报表名称
					reportSquid.setReport_name(reportSquidResult.getReport_name());
					// 设置报表模板
					reportSquid.setReport_template(reportSquidResult.getReport_template());
					// 设置是否支持历史
					reportSquid.setIs_support_history(reportSquidResult.isIs_support_history());
					// 设置保留最大版本数
					reportSquid.setMax_history_count(reportSquidResult.getMax_history_count());
					// 设置是否发送邮件
					reportSquid.setIs_send_email(reportSquidResult.isIs_send_email());
					// 设置收件人
					reportSquid.setEmail_receivers(reportSquidResult.getEmail_receivers());
					// 设置邮件titile
					reportSquid.setEmail_title(reportSquidResult.getEmail_title());
					// 设置邮件格式
					reportSquid.setEmail_report_format(reportSquidResult.getEmail_report_format());
					// 设置是否打包
					reportSquid.setIs_packed(reportSquidResult.isIs_packed());
					// 设置是否压缩
					reportSquid.setIs_compressed(reportSquidResult.isIs_compressed());
					// 设置是否加密
					reportSquid.setIs_encrypted(reportSquidResult.isIs_encrypted());
					// 设置密码
					reportSquid.setPassword(reportSquidResult.getPassword());
					if (reportSquid.getFolder_id()>0){
						String path = getReportNodeName(adapter, reportSquid.getFolder_id());
						reportSquid.setPublishing_path(path);
					}
				}

		return reportSquid;
    }
    
    /**
     *
     * @param token
     * @param inSession
     * @param adapter
     * @param filefolderSquid
     * @author lei.bin
     * @date 2014-5-9
     */
    public static FileFolderSquid setFileFolderSquidData(String token, boolean inSession, IRelationalDataManager adapter, FileFolderSquid fileFolderSquid){
				Map<String, String> paramMap = new HashMap<String, String>();
				paramMap.put("id", Integer.toString(fileFolderSquid.getId(), 10));
				FileFolderSquid fileSquidResult = (FileFolderSquid) adapter.query2Object2(inSession,paramMap, FileFolderSquid.class);
		if (null != fileSquidResult) {
			fileFolderSquid.setHost(fileSquidResult.getHost());
			fileFolderSquid.setUser_name(fileSquidResult.getUser_name());
			fileFolderSquid.setPassword(fileSquidResult.getPassword());
			fileFolderSquid.setFile_path(fileSquidResult.getFile_path());
			fileFolderSquid.setIncluding_subfolders_flag(fileSquidResult
					.getIncluding_subfolders_flag());
			fileFolderSquid
					.setUnionall_flag(fileSquidResult.getUnionall_flag());
		}

		return fileFolderSquid;
    }
    
    /**
     *
     * @param token
     * @param inSession
     * @param adapter
     * @param ftpSquid
     * @author lei.bin
     * @date 2014-5-9
     */
    public static FtpSquid setFtpSquidData(String token, boolean inSession, IRelationalDataManager adapter, FtpSquid ftpSquid){
				Map<String, String> paramMap = new HashMap<String, String>();
				paramMap.put("id", Integer.toString(ftpSquid.getId(), 10));
				FtpSquid ftpSquidResult = (FtpSquid) adapter.query2Object2(inSession,paramMap, FtpSquid.class);
		if (null != ftpSquidResult) {
			ftpSquid.setHost(ftpSquidResult.getHost());
			ftpSquid.setUser_name(ftpSquidResult.getUser_name());
			ftpSquid.setPassword(ftpSquidResult.getPassword());
			ftpSquid.setFile_path(ftpSquidResult.getFile_path());
			ftpSquid.setIncluding_subfolders_flag(ftpSquidResult
					.getIncluding_subfolders_flag());
			ftpSquid
					.setUnionall_flag(ftpSquidResult.getUnionall_flag());
			ftpSquid.setPostprocess(ftpSquidResult.getPostprocess());
			ftpSquid.setProtocol(ftpSquidResult.getProtocol());
			ftpSquid.setEncryption(ftpSquidResult.getEncryption());
			ftpSquid.setAllowanonymous_flag(ftpSquidResult.getAllowanonymous_flag());
			ftpSquid.setMaxconnections(ftpSquidResult.getMaxconnections());
			ftpSquid.setTransfermode_flag(ftpSquidResult.getTransfermode_flag());
		}

		return ftpSquid;
    }
    
    /**
     *
     * @param token
     * @param inSession
     * @param adapter
     * @param hdfsSquid
     * @author lei.bin
     * @date 2014-5-9
     */
    public static HdfsSquid setHdfsSquidData(String token, boolean inSession, IRelationalDataManager adapter, HdfsSquid hdfsSquid){
				Map<String, String> paramMap = new HashMap<String, String>();
				paramMap.put("id", Integer.toString(hdfsSquid.getId(), 10));
				HdfsSquid hdfsSquidResult = (HdfsSquid) adapter.query2Object2(true,paramMap, HdfsSquid.class);
		if (null != hdfsSquidResult) {
			hdfsSquid.setHost(hdfsSquidResult.getHost());
			hdfsSquid.setUser_name(hdfsSquidResult.getUser_name());
			hdfsSquid.setPassword(hdfsSquidResult.getPassword());
			hdfsSquid.setFile_path(hdfsSquidResult.getFile_path());
			hdfsSquid.setUnionall_flag(hdfsSquidResult.getUnionall_flag());
		}
		return hdfsSquid;
    }
    
    /**
     *
     * @param token
     * @param inSession
     * @param adapter
     * @param weiboSquid
     * @author lei.bin
     * @date 2014-5-9
     */
    public static WeiboSquid setWeiboSquidData(String token, boolean inSession, IRelationalDataManager adapter, WeiboSquid weiboSquid){
				Map<String, String> paramMap = new HashMap<String, String>();
				paramMap.put("id", Integer.toString(weiboSquid.getId(), 10));
				WeiboSquid weiboSquidResult = (WeiboSquid) adapter.query2Object2(true,paramMap, WeiboSquid.class);
		if (null != weiboSquidResult) {
			weiboSquid.setUser_name(weiboSquidResult.getUser_name());
			weiboSquid.setPassword(weiboSquidResult.getPassword());
			weiboSquid.setService_provider(weiboSquidResult.getService_provider());
			weiboSquid.setStart_data_date(weiboSquidResult.getStart_data_date());
			weiboSquid.setEnd_data_date(weiboSquidResult.getEnd_data_date());
		}
		return weiboSquid;
    }

    /**
     *
     * @param token
     * @param inSession
     * @param adapter
     * @param webSquid
     * @author lei.bin
     * @date 2014-5-9
     */
    public static WebSquid setWebSquidData(String token, boolean inSession, IRelationalDataManager adapter, WebSquid webSquid){
				Map<String, String> paramMap = new HashMap<String, String>();
				paramMap.put("id", Integer.toString(webSquid.getId(), 10));
				WebSquid webSquidResult = (WebSquid) adapter.query2Object2(true,paramMap, WebSquid.class);
		if (null != webSquidResult) {
			webSquid.setMax_threads(webSquidResult.getMax_threads());
			webSquid.setMax_fetch_depth(webSquidResult.getMax_fetch_depth());
			webSquid.setDomain_limit_flag(webSquidResult.getDomain_limit_flag());
			webSquid.setStart_data_date(webSquidResult.getStart_data_date());
			webSquid.setEnd_data_date(webSquidResult.getEnd_data_date());
		}
		return webSquid;
    }
    
    public static String getReportNodeName(IRelationalDataManager adapter, int folderId){
    	String nodeName = "";
    	List<String> list = new ArrayList<String>();
    	try {
			//adapter.openSession();
	    	getReportFolderById(adapter, folderId, list);
	    	if (list!=null&&list.size()>0){
	    		int cnt = list.size()-1;
	    		for (int i = cnt; i >= 0; i--) {
	    			nodeName += "/" + list.get(i);
				}
	    	}
		} catch (Exception e) {
			e.printStackTrace();
		}
    	return nodeName;
    }

    public static void getReportFolderById(IRelationalDataManager adapter, int id, List<String> list) throws SQLException{
    	String sql = "select folder_name,pid from ds_sys_report_folder where id="+id;
		Map<String, Object> map = adapter.query2Object(true, sql, null);
		if (map.containsKey("PID")&&ValidateUtils.isNumeric(map.get("PID")+"")){
			int pid = Integer.parseInt(map.get("PID")+"");
			String name = map.get("FOLDER_NAME")+"";
			list.add(name);
			getReportFolderById(adapter, pid, list);
		}
    }

    public static void main(String[] args) {
		String ss = getReportNodeName(null, 306848);
		System.out.println(ss);
	}

    /**
     * 新增ReportSquid
     * @param reportSquid
     * @return
     * @author bo.dang
     * @date 2014-4-16
     */
    public ReportSquid addReportSquid(ReportSquid reportSquid){

    	try {
    		adapter.openSession();
    		// 当前时间
			Timestamp date = new Timestamp(System.currentTimeMillis());
			reportSquid.setCreation_date(date.toString());
			reportSquid.setId(adapter.insert2(reportSquid));
		} catch (SQLException e) {
			logger.error("新增ReportSquid失败！", e);
			try {
				adapter.rollback();
			} catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
				logger.fatal("rollback err!", e1);
			}
		} finally {
			adapter.closeSession();
		}
    	return reportSquid;
    }
    
    /**
     * 更新reportSquid信息
     * @param reportSquid
     * @return
     * @author bo.dang
     * @date 2014-4-16
     */
    public ReportSquid updateReportSquid(ReportSquid reportSquid, ReturnValue out){
		IRelationalDataManager adapter = null;
	    // 数据仓库ID
	    int repository_id = reportSquid.getRepository_id();
	    // ReportSquid ID
	    int squidId = reportSquid.getId();
	    // 文件ID
	    int folderId = reportSquid.getFolder_id();
	    // 报表名称
	    String reportName = reportSquid.getReport_name();
        try {
        	// 判断报表名称是否重复
            ReportFolderServicesub reportFolderServicesub = new ReportFolderServicesub(token);
            List<ReportNode> reportNodeList = reportFolderServicesub.getSubReportNodesById(folderId, out);
        	//重新获取数据连接对象
            adapter = DataAdapterFactory.getDefaultDataManager();
        	adapter.openSession();
			logger.info("updateReportSquid : reportName : " + reportName);
			Map<String, String> paramsMap = new HashMap<String, String>();
			paramsMap.put("repository_id", Integer.toString(repository_id, 10));
			paramsMap.put("squid_id", Integer.toString(squidId, 10));
			ReportFolderMapping reportFolderMapping  = adapter.query2Object2(true, paramsMap, ReportFolderMapping.class);
	        Boolean validateFlag = false;
			if(StringUtils.isNotNull(reportFolderMapping)){
			    validateFlag = true;
	        }
            if(StringUtils.isNotNull(reportNodeList) && !reportNodeList.isEmpty()){
                ReportNode reportNode = null;
                ReportNodeType reportNodeType = null;
                for (int i = 0; i < reportNodeList.size(); i++) {
                    reportNode = reportNodeList.get(i);
                    reportNodeType = reportNode.getReportNodeType();
                    if(validateFlag){
                        if(reportFolderMapping.getId() != reportNode.getId()){
                            // 报表名字重复
                            if(reportNodeType.getId() == 1 && reportName.equals(reportNode.getNodeName())){
                            	out.setMessageCode(MessageCode.ERR_DUPLICATE_REPORT_NAME);
                            	return null;
                            }
                        }
                    }
                    else {
                        // 报表名字重复
                        if(reportNodeType.getId() == 1 && reportName.equals(reportNode.getNodeName())){
                        	out.setMessageCode(MessageCode.ERR_DUPLICATE_REPORT_NAME);
                        	return null;
                        }
                    }
                }
            }

			if(StringUtils.isNull(reportFolderMapping)){
			     reportFolderMapping = new ReportFolderMapping();
		         reportFolderMapping.setReport_name(reportSquid.getReport_name());
		         reportFolderMapping.setRepository_id(repository_id);
		         reportFolderMapping.setSquid_id(squidId);
	             if(folderId == 0){
	                    reportFolderMapping.setFolder_id(null);
	                }
	                else {
	                    reportFolderMapping.setFolder_id(folderId);

	                }
		         reportFolderMapping.setId(adapter.insert2(reportFolderMapping));
			}
			else {
			    reportFolderMapping.setReport_name(reportSquid.getReport_name());
			    if(folderId == 0){
			        reportFolderMapping.setFolder_id(null);
			    }
			    else {
			        reportFolderMapping.setFolder_id(folderId);

			    }
			    adapter.update2(reportFolderMapping);
			}
            // 更新ReportSquid
			if (reportSquid.getId()>0){
				ISquidDao squidDao = new SquidDaoImpl(adapter);
				ReportSquid oldReportSquid = (ReportSquid)squidDao.getSquidForCond(reportSquid.getId(), ReportSquid.class);
				if (oldReportSquid!=null&&oldReportSquid.isIs_support_history()&&StringUtils.isNotNull(oldReportSquid.getReport_template())){
					String sqlstr = "select * from ds_report_version where squid_id="+reportSquid.getId()+" order by add_Date asc";
	            	List<ReportVersion> list = adapter.query2List(true, sqlstr, null, ReportVersion.class);
	            	int versionCnt = list.size();
	            	if (list!=null&&list.size()>0&&list.size()==oldReportSquid.getMax_history_count()){
	            		paramsMap.clear();
	            		paramsMap.put("id", list.get(0).getId()+"");
	            		adapter.delete(paramsMap, ReportVersion.class);
	            		versionCnt = versionCnt - 1;
	            	}
	            	if (((list==null||list.size()==0)&&oldReportSquid.getMax_history_count()>0)
	            		||(list.size()<=oldReportSquid.getMax_history_count())){
	            		ReportVersion version = new ReportVersion();
	            		version.setAdd_date(DateUtil.format(new Date(), "yyyy-MM-dd HH:mm:ss"));
	            		version.setSquid_id(reportSquid.getId());
	            		version.setTemplate(oldReportSquid.getReport_template());
	            		version.setVersion(versionCnt+1);
	            		adapter.insert2(version);
	            	}
	            }
				adapter.update2(reportSquid);
            	// 推送消息泡
                CommonConsts.addValidationTask(new SquidValidationTask(token, adapter,  MessageBubbleService.setMessageBubble(squidId, squidId, reportSquid.getName(), MessageBubbleCode.WARN_REPORT_SQUID_NO_PUBLISHING_FOLDER.value())));
                CommonConsts.addValidationTask(new SquidValidationTask(token, adapter,  MessageBubbleService.setMessageBubble(squidId, squidId, reportSquid.getName(), MessageBubbleCode.ERROR_REPORT_SQUID_NO_TEMPLATE_DEFINITION.value())));
                return reportSquid;
			}else{
				out.setMessageCode(MessageCode.NODATA);
			}
		} catch (Exception e) {
			logger.error("更新ReportSquid失败！", e);
			try {
				if (adapter!=null) adapter.rollback();
			} catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
				logger.fatal("rollback err!", e1);
			}
			out.setMessageCode(MessageCode.SQL_ERROR);
		} finally {
			if (adapter!=null) adapter.closeSession();
		}
    	return reportSquid;
    }
    
    /**
     * 删除ReportSquid
     * @param id
     * @return
     * @author bo.dang
     * @date 2014-4-16
     */
    public boolean deleteReportSquid(Integer id){

    	boolean flag = false;
    	Map<String, String> paramMap = new HashMap<String, String>();
    	// 设置id
    	paramMap.put("id", Integer.toString(id, 10));
    	try {
    		adapter.openSession();
    		// 删除ReportSquid信息
			if(adapter.delete(paramMap, ReportSquid.class) > 0){
				flag = true;
			}
			// TODO 删除SquidLink信息
			paramMap.clear();
			paramMap.put("to_squid_id", Integer.toString(id, 10));
			List<SquidLink> squidLinkList = adapter.query2List(paramMap, SquidLink.class);
			if(null != squidLinkList){
				SquidLink squidLink = null;
				for(int i=0; i<squidLinkList.size(); i++){
					squidLink = squidLinkList.get(i);
					paramMap.clear();
					paramMap.put("id", Integer.toString(squidLink.getId(), 10));
					adapter.delete(paramMap, SquidLink.class);
				}
			}
		} catch (SQLException e) {
			logger.error("删除ReportSquid失败！", e);
			try {
				adapter.rollback();
			} catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
				logger.fatal("rollback err!", e1);
			}
		} finally {
			adapter.closeSession();
		}

    	return flag;
    }
    
    /**
     * 创建到ReportSquid的squidLink
     * @param squidLink
     * @return
     * @author bo.dang
     * @date 2014-4-24
     */
    public SquidLink createReportSquidLink(SquidLink squidLink){
		    squidLink.setType(DSObjectType.SQUIDLINK.value());
			adapter.openSession();
			Integer from_squid_id = squidLink.getFrom_squid_id();
			Map<String, String> paramMap = new HashMap<String, String>();
			paramMap.put("id", Integer.toString(from_squid_id, 10));
			Squid squid = adapter.query2Object(paramMap, Squid.class);
			int squidTypeId = squid.getSquid_type();
			if(StringUtils.isNotNull(squid) &&
			          (SquidTypeEnum.STAGE.value() == squidTypeId
				  	|| SquidTypeEnum.STREAM_STAGE.value() == squidTypeId
					|| SquidTypeEnum.EXTRACT.value() == squidTypeId
					|| SquidTypeEnum.XML_EXTRACT.value() == squidTypeId
					|| SquidTypeEnum.DOC_EXTRACT.value() == squidTypeId
					|| SquidTypeEnum.WEBLOGEXTRACT.value() == squidTypeId
					|| SquidTypeEnum.WEBEXTRACT.value() == squidTypeId
					|| SquidTypeEnum.WEIBOEXTRACT.value() == squidTypeId
					|| SquidTypeEnum.LOGREG.value() == squidTypeId
					|| SquidTypeEnum.SVM.value() == squidTypeId
					|| SquidTypeEnum.NAIVEBAYES.value() == squidTypeId
					|| SquidTypeEnum.KMEANS.value() == squidTypeId
					|| SquidTypeEnum.ALS.value() == squidTypeId
					|| SquidTypeEnum.LINEREG.value() == squidTypeId
					|| SquidTypeEnum.RIDGEREG.value() == squidTypeId)
					|| SquidTypeEnum.QUANTIFY.value() == squidTypeId
					|| SquidTypeEnum.DISCRETIZE.value() == squidTypeId
					|| SquidTypeEnum.DECISIONTREE.value() == squidTypeId
					|| SquidTypeEnum.EXCEPTION.value() == squidTypeId
					|| SquidTypeEnum.WEBSERVICEEXTRACT.value() == squidTypeId
					|| SquidTypeEnum.HTTPEXTRACT.value() == squidTypeId
					|| SquidTypeEnum.KAFKAEXTRACT.value() == squidTypeId
					|| SquidTypeEnum.HBASEEXTRACT.value() == squidTypeId
					|| SquidTypeEnum.MONGODBEXTRACT.value() == squidTypeId
					|| SquidTypeEnum.ASSOCIATION_RULES.value() == squidTypeId){
				if(StringUtils.isNull(squidLink.getKey())){
					squidLink.setKey(StringUtils.generateGUID());
				}
				try {
					squidLink.setId(adapter.insert2(squidLink));
					// 推送消息泡
//					new MessageBubbleService(token).isolateSquid(DMLType.INSERT, DSObjectType.SQUIDLINK, squidLink.getFrom_squid_id(), squidLink.getTo_squid_id(), null, squidLink);
                    // 消息泡验证
					CommonConsts.addValidationTask(new SquidValidationTask(token, adapter, MessageBubbleService.setMessageBubble(squidLink.getFrom_squid_id(), squidLink.getFrom_squid_id(), null, MessageBubbleCode.WARN_SQUID_NO_LINK.value())));
                    CommonConsts.addValidationTask(new SquidValidationTask(token, adapter, MessageBubbleService.setMessageBubble(squidLink.getTo_squid_id(), squidLink.getTo_squid_id(), null, MessageBubbleCode.WARN_SQUID_NO_LINK.value())));
                    //CommonConsts.addValidationTask(new SquidValidationTask(token, adapter, MessageBubbleService.setMessageBubble(squidLink.getTo_squid_id(), squidLink.getTo_squid_id(), null, MessageBubbleCode.WARN_SQUID_NO_LINK.value())));
				} catch (SQLException e) {
					logger.error("创建到ReportSquide的SquidLink异常", e);
					try {
						adapter.rollback();
					} catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
						logger.fatal("rollback err!", e1);
					}
				} finally {
					adapter.closeSession();
				}
			}
			else {
				squidLink = null;
			}

    	return squidLink;
    }
    
    /**
     * 更新GISMapSquid信息
     * @param GISMapSquid
     * @return
     * @author bo.dang
     * @date 2014-4-16
     */
    public GISMapSquid updateGISMapSquid(GISMapSquid gisMapSquid, ReturnValue out){
		IRelationalDataManager adapter = null;
	    // 数据仓库ID
	    int repository_id = gisMapSquid.getRepository_id();
	    // ReportSquid ID
	    int squidId = gisMapSquid.getId();
	    // 文件ID
	    int folderId = gisMapSquid.getFolder_id();
	    // 报表名称
	    String mapName = gisMapSquid.getMap_name();
        try {
        	// 判断报表名称是否重复
            ReportFolderServicesub reportFolderServicesub = new ReportFolderServicesub(token);
            List<ReportNode> reportNodeList = reportFolderServicesub.getSubReportNodesById(folderId, out);
        	adapter= DataAdapterFactory.getDefaultDataManager();
        	adapter.openSession();
			logger.info("updateReportSquid : reportName : " + mapName);
			Map<String, String> paramsMap = new HashMap<String, String>();
			paramsMap.put("repository_id", Integer.toString(repository_id, 10));
			paramsMap.put("squid_id", Integer.toString(squidId, 10));
			ReportFolderMapping reportFolderMapping  = adapter.query2Object2(true, paramsMap, ReportFolderMapping.class);
	        Boolean validateFlag = false;
			if(StringUtils.isNotNull(reportFolderMapping)){
			    validateFlag = true;
	        }
            if(StringUtils.isNotNull(reportNodeList) && !reportNodeList.isEmpty()){
                ReportNode reportNode = null;
                ReportNodeType reportNodeType = null;
                for (int i = 0; i < reportNodeList.size(); i++) {
                    reportNode = reportNodeList.get(i);
                    reportNodeType = reportNode.getReportNodeType();
                    if(validateFlag){
                        if(reportFolderMapping.getId() != reportNode.getId()){
                            // 报表名字重复
                            if(reportNodeType.getId() == 1 && mapName.equals(reportNode.getNodeName())){
                                out.setMessageCode(MessageCode.ERR_DUPLICATE_REPORT_NAME);
                            	return null;
                            }

                        }
                    }
                    else {
                        // 报表名字重复
                        if(reportNodeType.getId() == 1 && mapName.equals(reportNode.getNodeName())){
                        	out.setMessageCode(MessageCode.ERR_DUPLICATE_REPORT_NAME);
                        	return null;
                        }
                    }
                }
            }
			if(StringUtils.isNull(reportFolderMapping)){
			     reportFolderMapping = new ReportFolderMapping();
		         reportFolderMapping.setReport_name(gisMapSquid.getMap_name());
		         reportFolderMapping.setRepository_id(repository_id);
		         reportFolderMapping.setSquid_id(squidId);
	             if(folderId == 0){
	                    reportFolderMapping.setFolder_id(null);
	                }
	                else {
	                    reportFolderMapping.setFolder_id(folderId);
	                }
		         reportFolderMapping.setId(adapter.insert2(reportFolderMapping));
			}
			else {
			    reportFolderMapping.setReport_name(gisMapSquid.getMap_name());
			    if(folderId == 0){
			        reportFolderMapping.setFolder_id(null);
			    }
			    else {
			        reportFolderMapping.setFolder_id(folderId);

			    }
			    adapter.update2(reportFolderMapping);
			}
            // 更新ReportSquid
			ISquidDao squidDao = new SquidDaoImpl(adapter);
			GISMapSquid oldGisMap = squidDao.getSquidForCond(squidId, GISMapSquid.class);
			if (oldGisMap!=null){
				gisMapSquid.setMap_template(oldGisMap.getMap_template());
				adapter.update2(gisMapSquid);
			}else{
				out.setMessageCode(MessageCode.NODATA);
			}
            // 推送消息泡
            CommonConsts.addValidationTask(new SquidValidationTask(token, adapter,  MessageBubbleService.setMessageBubble(squidId, squidId, gisMapSquid.getName(), MessageBubbleCode.WARN_GISMAP_SQUID_NO_PUBLISHING_FOLDER.value())));
            CommonConsts.addValidationTask(new SquidValidationTask(token, adapter,  MessageBubbleService.setMessageBubble(squidId, squidId, gisMapSquid.getName(), MessageBubbleCode.ERROR_GISMAP_SQUID_NO_TEMPLATE_DEFINITION.value())));
		} catch (Exception e) {
			logger.error("更新gisMapSquid失败！", e);
			try {
				if (adapter!=null) adapter.rollback();
			} catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
				logger.fatal("rollback err!", e1);
			}
			out.setMessageCode(MessageCode.SQL_ERROR);
		} finally {
	        if (adapter!=null) adapter.closeSession();
		}
    	return gisMapSquid;
    }
}
