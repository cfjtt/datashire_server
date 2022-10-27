package com.eurlanda.datashire.sprint7.service.squidflow.subservice;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.entity.ReportFolder;
import com.eurlanda.datashire.entity.ReportFolderMapping;
import com.eurlanda.datashire.entity.ReportNode;
import com.eurlanda.datashire.entity.ReportNodeType;
import com.eurlanda.datashire.entity.ReportSquid;
import com.eurlanda.datashire.sprint7.service.squidflow.AbstractRepositoryService;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import com.eurlanda.datashire.utility.StringUtils;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReportFolderServicesub extends AbstractRepositoryService {
    //public static IRelationalDataManager sysAdapter = null;
    
	public ReportFolderServicesub(String token){
		super(token);
	}
	public ReportFolderServicesub(IRelationalDataManager adapter) {
        super(adapter);
    }
    

    Logger logger = Logger.getLogger(ReportFolderServicesub.class);// 记录日志


    private Map<String, String> paramsMap = new HashMap<String, String>();
    private ReportSquid reportSquid = null;
    private ReportFolder reportFolder = null;
    private List<ReportFolder> reportFolderList = null;
    private ReportFolderMapping reportFolderMapping = null;
    private List<ReportFolderMapping> reportFolderMappingList = null;


    /**
     * 根据父节点查询ReportFolder信息
     * 
     * @param reportFolder
     * @return
     * @author bo.dang
     * @date 2014-4-14
     */
    public List<ReportNode> getSubReportNodesById(Integer id, ReturnValue out) {
    	Map<String, String> paramMap = new HashMap<String, String>();
        List<ReportFolder> reportFolderList = null;
        IRelationalDataManager adapter = null;
        List<ReportNode> reportNodeList = new ArrayList<ReportNode>();
        try {
        	adapter = DataAdapterFactory.getDefaultDataManager();
	        adapter.openSession();
	        // 如果是id是-1,代表是根目录，那么设置父节点pid为null
	        if (-1 == id) {
	            String reportFoldeSQL = "SELECT * FROM DS_SYS_REPORT_FOLDER WHERE PID=-1";
	            // 取得父节点下ReportFolder信息
	            reportFolderList = adapter.query2List(true, reportFoldeSQL,
	                    null, ReportFolder.class);
	        } else {
	            paramMap.put("pid", Integer.toString(id, 10));
	            // 取得父节点下ReportFolder信息
	            reportFolderList = adapter.query2List(true, paramMap,
	                    ReportFolder.class);
	        }
	        ReportFolder reportFolder = null;
	        ReportNode reportNode = null;
	        ReportNodeType nodeType = null;
	        // TODO 0：ReportFolder文件夹 1:报表文件
	        // 如果ReportFolderList不为null
	        if (null != reportFolderList) {
	            for (int i = 0; i < reportFolderList.size(); i++) {
	                reportFolder = reportFolderList.get(i);
	                reportNode = new ReportNode();
	                // 设置ID
	                reportNode.setId(reportFolder.getId());
	                reportNode.setNodeName(reportFolder.getFolder_name());
	                reportNode.setCreatedTime(reportFolder.getCreation_date());
	                // 如果是pid为null,代表是根目录，那么设置父节点pid为-1
	                if (null == reportFolder.getPid()) {
	                    reportNode.setPid(-1);
	                } else {
	                    reportNode.setPid(reportFolder.getPid());
	                }
	                // 是否显示
	                reportNode.setDisplay_flag(true);
	                // 设置报表类型
	                nodeType = new ReportNodeType();
	                nodeType.setId(0);// 0代表ReportFolder文件夹, 1:报表文件
	                nodeType.setTypeName("Report Folder");
	                reportNode.setReportNodeType(nodeType);
	                reportNodeList.add(reportNode);
	            }
	        }
	        // 每次通过父节点的ID查询其下的子节点，如果参数为-1，获得的就是Root级别的节点
	        paramMap.clear();
	        List<ReportFolderMapping> reportFolderSquidList = null;
	
	        if (null == id) {
	            String reportFolderMappingSQL = "SELECT * FROM DS_SYS_REPORT_FOLDER_MAPPING WHERE FOLDER_ID IS NULL";
	            // 取得父节点下报表信息
	            reportFolderSquidList = adapter.query2List(true,
	                    reportFolderMappingSQL, null, ReportFolderMapping.class);
	        } else {
	            paramMap.put("folder_id", Integer.toString(id, 10));
	            // 取得父节点下报表信息
	            reportFolderSquidList = adapter.query2List(true, paramMap,
	                    ReportFolderMapping.class);
	        }
	        adapter.closeSession();
	        ReportFolderMapping reportFolderSquid = null;
	
	        // 如果reportSquidList不为null
	        if (null != reportFolderSquidList) {
	            for (int j = 0; j < reportFolderSquidList.size(); j++) {
	                reportFolderSquid = reportFolderSquidList.get(j);
	                reportNode = new ReportNode();
	                reportNode.setId(reportFolderSquid.getId());
	                reportNode.setNodeName(reportFolderSquid.getReport_name());
	                reportNode.setCreatedTime(reportFolderSquid.getCreation_date());
	                // 设置父节点
	                // 如果是pid为null,代表是根目录，那么设置父节点pid为-1
	                if (null == reportFolderSquid.getFolder_id()) {
	                    reportNode.setPid(-1);
	                } else {
	                    reportNode.setPid(reportFolderSquid.getFolder_id());
	                }
	                // 是否显示
	                reportNode.setDisplay_flag(true);
	                // 设置报表类型
	                nodeType = new ReportNodeType();
	                nodeType.setId(1);// 0代表ReportFolder文件夹, 1:报表文件
	                nodeType.setTypeName("Report Form");
	                reportNode.setReportNodeType(nodeType);
	                reportNodeList.add(reportNode);
	            }
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
			logger.error("getSubReportNodesById 错误", e);
		} finally {
			if (adapter!=null){
				adapter.closeSession();
			}
		}
        return reportNodeList;
    }

    /**
     * 调用新增reportFolder
     * 
     * @param reportFolder
     * @return
     * @author bo.dang
     * @date 2014-4-14
     */
    public ReportNode addReportNode(ReportNode reportNode) {
    	IRelationalDataManager adapter = null;
        try {
        	adapter = DataAdapterFactory.getDefaultDataManager();
        	adapter.openSession();
            ReportFolder reportFolder = new ReportFolder();
            // 当前时间
            Timestamp date = new Timestamp(System.currentTimeMillis());
            reportNode.setCreatedTime(date.toString());
            // 从reportNode中读取数据设置到reportFolder中
            setReportFolderData(reportFolder, reportNode);

            reportFolder.setId(adapter.insert2(reportFolder));
            // 设置ID
            reportNode.setId(reportFolder.getId());
            // 把报表信息insert到数据库表reportNode
        } catch (SQLException e) {
            logger.error("创建ReportFolder异常", e);
            try {
            	adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!", e1);
            }
        } finally {
        	adapter.closeSession();
        }
        return reportNode;
    }

    /**
     * 根据id取得ReportFolder信息
     * 
     * @param id
     * @return
     * @author bo.dang
     * @date 2014-4-14
     */
    public List<ReportNode> getReportNodeById(Integer id, ReturnValue out) {
        // 根据id取得ReportFolder信息
    	IRelationalDataManager adapter = null;
    	List<ReportNode> reportNodeList = null;
    	try {
    		adapter = DataAdapterFactory.getDefaultDataManager();
    		adapter.openSession();
	        Map<String, String> paramMap = new HashMap<String, String>();
	        paramMap.put("id", Integer.toString(id, 10));
	        List<ReportFolder> reportFolderList = adapter
	                .query2List(true, paramMap, ReportFolder.class);
	        ReportNode reportNode = new ReportNode();
	        ReportNodeType nodeType = new ReportNodeType();
	        // 封装报表数据返回到前台
	         reportNodeList = setReportNodeData(reportFolderList,
	                reportNode, nodeType);
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
			logger.error("getReportNodeById error", e);
		} finally {
			if (adapter!=null) adapter.closeSession();
		}
        return reportNodeList;
    }

    /**
     * 删除ReportFolder信息
     * 
     * @param reportFolder
     * @return
     * @author bo.dang
     * @date 2014-4-14
     */
    public Boolean deleteReportNode(Integer id) {
        Boolean deleteFlag = true;
        IRelationalDataManager adapter = null;
        try {
        	adapter = DataAdapterFactory.getDefaultDataManager();
        	adapter.openSession();
            paramsMap.clear();
            paramsMap.put("id", Integer.toString(id, 10));
            reportFolder = adapter.query2Object(paramsMap, ReportFolder.class);
            if (StringUtils.isNotNull(reportFolder)) {
                
            	adapter.delete(paramsMap, ReportFolder.class);
            }
            else {
                paramsMap.clear();
                paramsMap.put("id", Integer.toString(id, 10));
                reportFolderMapping = adapter.query2Object(paramsMap, ReportFolderMapping.class);
                if(StringUtils.isNotNull(reportFolderMapping)){
                    paramsMap.clear();
                    paramsMap.put("id", Integer.toString(id, 10));
                    paramsMap.put("folder_id", Integer.toString(reportFolderMapping.getFolder_id(),10));
                    // 删除reportFolderMapping
                    adapter.delete(paramsMap, ReportFolderMapping.class);
                }
            }
        } catch (SQLException e) {
            deleteFlag = false;
            logger.error("删除ReportFolder", e);
            try {
            	adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!", e1);
            }
        } finally {
        	adapter.closeSession();
        }
        return deleteFlag;
    }

    /**
     * 更新reportFolder信息
     * 
     * @param reportFolder
     * @return
     * @author bo.dang
     * @date 2014-4-14
     */
    public Boolean updateReportNode(ReportNode reportNode, ReturnValue out) {
        Boolean updateFlag = false;
        IRelationalDataManager adapter = null;
        try {
        	adapter = DataAdapterFactory.getDefaultDataManager();
        	adapter.openSession();
            // TODO 0：ReportFolder文件夹 1:ReportSquid文件
            ReportFolder reportFolder = new ReportFolder();
            // 从reportNode中读取数据设置到reportFolder中
            setReportFolderData(reportFolder, reportNode);
            reportFolder.setId(reportNode.getId());
            updateFlag = adapter.update2(reportFolder);
        } catch (SQLException e) {
            logger.error("更新ReportFolder", e);
            try {
            	adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!", e1);
            }
            out.setMessageCode(MessageCode.SQL_ERROR);
        } finally {
        	adapter.closeSession();
        }
        return updateFlag;

    }

    /**
     * 从reportNode中读取数据设置到reportFolder中
     * 
     * @param reportFolder
     * @param reportNode
     * @author bo.dang
     * @date 2014-4-15
     */
    public void setReportFolderData(ReportFolder reportFolder,
            ReportNode reportNode) {
        // 如果是id是-1,代表是根目录，那么设置父节点pid为null
        /*if (null != reportNode) {
            reportNode.setPid(null);
        }*/
        reportFolder.setPid(reportNode.getPid());
        reportFolder.setFolder_name(reportNode.getNodeName());
        if (StringUtils.isNotNull(reportNode.getCreatedTime())){
        	reportFolder.setCreation_date(reportNode.getCreatedTime());
        }else{
        	SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        	reportFolder.setCreation_date(format.format(new Date()));
        }
        reportFolder.setIs_display(true);
    }

    /**
     * 封装报表数据返回到前台
     * 
     * @param reportFolder
     * @param reportNode
     * @param nodeType
     * @author bo.dang
     * @date 2014-4-15
     */
    public List<ReportNode> setReportNodeData(
            List<ReportFolder> reportFolderList, ReportNode reportNode,
            ReportNodeType nodeType) {
        List<ReportNode> reportNodeList = new ArrayList<ReportNode>();
        ReportFolder reportFolder = null;
        if (null != reportFolderList && reportFolderList.size() > 0) {
            for (int i = 0; i < reportFolderList.size(); i++) {
                reportFolder = reportFolderList.get(i);
                // 设置ID
                reportNode.setId(reportFolder.getId());
                reportNode.setNodeName(reportFolder.getFolder_name());
                reportNode.setCreatedTime(reportFolder.getCreation_date());
                // 如果是pid为null,代表是根目录，那么设置父节点pid为-1
                if (null == reportFolder.getPid()) {
                    reportNode.setPid(-1);
                } else {
                    reportNode.setPid(reportFolder.getPid());
                }
                // 设置报表类型
                nodeType.setId(0);// 0代表ReportFolder文件夹, 1:报表文件
                nodeType.setTypeName("Report Folder");
                reportNode.setReportNodeType(nodeType);
                reportNodeList.add(reportNode);
            }
        }
        return reportNodeList;
    }
    /**
     * 删除ReportFolderMapping和更新ReportSquid信息
     * @param id
     * @author bo.dang
     * @date 2014年5月16日
     */
    public Boolean deleteReportFolderMappingBySquidId(int repositoryId, int squidId){
        Boolean delFlag = false;
        IRelationalDataManager adapter = null;
        try {
        	adapter = DataAdapterFactory.getDefaultDataManager();
        	adapter.openSession();
            paramsMap.clear();
            paramsMap.put("repository_id", Integer.toString(repositoryId, 10));
            paramsMap.put("squid_id", Integer.toString(squidId, 10));
            delFlag = adapter.delete(paramsMap, ReportFolderMapping.class)>0 ? true:false;
        } catch (SQLException e) {
            logger.error("删除ReportFolder", e);
            try {
            	adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!", e1);
            }
        } finally {
        	adapter.closeSession();
        }
        
        return delFlag;
    }

    /**
     * 删除ReportFolderMapping和更新ReportSquid信息
     * @param id
     * @author bo.dang
     * @date 2014年5月16日
     */
    public void deleteReportFolderMappingById(int id){
        try {
            
            paramsMap.clear();
            paramsMap.put("id", Integer.toString(id, 10));
            reportFolderMapping = adapter.query2Object(paramsMap, ReportFolderMapping.class);
            if(StringUtils.isNotNull(reportFolderMapping)){
                paramsMap.clear();
                paramsMap.put("id", Integer.toString(reportFolderMapping.getSquid_id(), 10));
                paramsMap.put("folder_id", Integer.toString(reportFolderMapping.getFolder_id(),10));
                // 删除reportFolderMapping
                adapter.delete(paramsMap, ReportFolderMapping.class);
                reportSquid = adapter.query2Object(paramsMap, ReportSquid.class);
                if(StringUtils.isNotNull(reportSquid)){
                    reportSquid.setFolder_id(0);
                    adapter.update2(reportSquid);
                }
                
            }
        } catch (SQLException e) {
            logger.error("删除ReportFolder", e);
            try {
            	adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!", e1);
            }
        }
    }
    
}
