package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.entity.GISMapSquid;
import com.eurlanda.datashire.entity.ReportNode;
import com.eurlanda.datashire.entity.ReportSquid;
import com.eurlanda.datashire.entity.SquidLink;
import com.eurlanda.datashire.enumeration.DMLType;
import com.eurlanda.datashire.server.utils.TokenUtil;
import com.eurlanda.datashire.sprint7.packet.InfoNewMessagePacket;
import com.eurlanda.datashire.sprint7.service.squidflow.subservice.ExtractServicesub;
import com.eurlanda.datashire.sprint7.service.squidflow.subservice.ReportSquidServicesub;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import com.eurlanda.datashire.utility.SysConf;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
public class ReportSquidService {
    private String token;//令牌根据令牌得到相应的连接信息
    private String key;//key值
    
    //异常处理机制
    ReturnValue out = new ReturnValue();
    
    /**
     * 新增ReportSquid
     * @param info
     * @return
     * @author bo.dang
     * @date 2014-4-14
     */
    @SuppressWarnings("unchecked")
    public String addReportSquid(String info){
    	out  = new ReturnValue();
        Map<String, Class<?>> paramsMap = new HashMap<String, Class<?>>();
        paramsMap.put("ReportSquid", ReportSquid.class);
        
        Map<String, Object> resultMap = new ExtractServicesub(TokenUtil.getToken()).execute(info, paramsMap, DMLType.INSERT.value(), out);
        InfoNewMessagePacket<Map<String, Object>> infoMessage = new InfoNewMessagePacket<Map<String, Object>>();
        infoMessage.setCode(out.getMessageCode().value());
        if (out.getMessageCode()==MessageCode.SUCCESS){
        	infoMessage.setInfo(resultMap);
        }else{
        	infoMessage.setInfo(new HashMap<String, Object>());
        }
        infoMessage.setDesc("创建ReportSquid");
        return JsonUtil.object2Json(infoMessage);
    }
    
    /**
     * 更新ReportSquid
     * @param info
     * @return
     * @author bo.dang
     * @date 2014-4-16
     */
    @SuppressWarnings("unchecked")
    public String updateReportSquid(String info){
    	ReturnValue out = new ReturnValue();
        Map<String, Object> infoMap = JsonUtil.toHashMap(info);
        ReportSquid reportSquid = (ReportSquid)JsonUtil.toGsonBean(infoMap.get("ReportSquid").toString(), ReportSquid.class);
/*        Gson gson = new GsonBuilder().create();
        ReportSquid reportSquid = gson.fromJson(infoMap.get("ReportSquid").toString(), ReportSquid.class);*/
        ReportSquid newReportSquid = new ReportSquidServicesub(TokenUtil.getToken()).updateReportSquid(reportSquid, out);
        Map<String, Object> resultMap = new HashMap<String, Object>();
        InfoNewMessagePacket<Map<String, Object>> infoMessage = new InfoNewMessagePacket<Map<String,Object>>();
        if(out.getMessageCode()==MessageCode.SUCCESS){
            // 返回新增的FolderID
            resultMap.put("ReportSquidId", newReportSquid.getId());
        }
        infoMessage.setCode(out.getMessageCode().value());
        infoMessage.setInfo(resultMap);
        infoMessage.setDesc("更新ReportSquid");
        return JsonUtil.object2Json(infoMessage);
    }
    
/*    *//**
     * 删除reportSquid对象
     * @param info
     * @return
     * @author bo.dang
     * @date 2014-4-16
     *//*
    @SuppressWarnings("unchecked")
    public String deleteReportSquid(String info){
        Map<String, Object> infoMap = JsonUtil.toHashMap(info);
        Integer id =  (Integer) infoMap.get("id");
        Integer delID = new ReportSquidServicesub(token).deleteReportSquid(id);
        Map<String, Object> resultMap = new HashMap<String, Object>();
        // 返回新增的FolderID
        resultMap.put("id", delID);
        InfoNewMessagePacket<Map<String, Object>> infoMessage = new InfoNewMessagePacket<Map<String,Object>>();
        infoMessage.setInfo(resultMap);
        infoMessage.setDesc("根据节点查询ReportSquid信息");
        return JsonUtil.object2Json(infoMessage);
    }*/

    /**
     * debug模式启动，调用报表引擎
     * @param info
     * @return 是否成功
     * @author bo.dang
     * @date 2014-4-17
     */
    public String viewReportByURL(String info){
        Map<String, Object> infoMap = JsonUtil.toHashMap(info);
        Integer repositoryId = (Integer) infoMap.get("RepositoryId");
        Integer reportSquidId = (Integer) infoMap.get("SquidId");
        // TODO 需要调用引擎
        //EngineService.callSquidFlow(reportSquidId,repositoryId);
        
        String basePath = SysConf.getValue("basePathIp");
        String url = SysConf.getValue("report_url");
        url = String.format(basePath+url, repositoryId, reportSquidId);
//        String url = "http://192.168.137.223:8080/report/test/report.jspx?repositoryId=" + repositoryId + "&squidId=" + reportSquidId;
        Map<String, Object> resultMap = new HashMap<String, Object>();
        // 返回是否成功
        // 返回URL
        resultMap.put("ReportURL", url);
        InfoNewMessagePacket<Map<String, Object>> infoMessage = new InfoNewMessagePacket<Map<String,Object>>();
        infoMessage.setInfo(resultMap);
        infoMessage.setDesc("debugReport是否成功");
        return JsonUtil.object2Json(infoMessage);
    }
    
    /**
     * 创建到ReportSquid的squidLink
     * @param info
     * @return
     * @author bo.dang
     * @date 2014-4-24
     */
    public String createReportSquidLink(String info){
        @SuppressWarnings("unchecked")
        Map<String, Object> infoMap = JsonUtil.toHashMap(info);
        SquidLink squidLink = JsonUtil.toGsonBean(infoMap.get("SquidLink").toString(), SquidLink.class);
        SquidLink newSquidLink = new ReportSquidServicesub(TokenUtil.getToken()).createReportSquidLink(squidLink);
        Map<String, Object> resultMap = new HashMap<String, Object>();
        InfoNewMessagePacket<Map<String, Object>> infoMessage = new InfoNewMessagePacket<Map<String,Object>>();
        if (newSquidLink!=null&&newSquidLink.getId()>0){
        	resultMap.put("NewSquidLinkID", newSquidLink.getId());
        }else{
        	infoMessage.setCode(MessageCode.INSERT_ERROR.value());
        }
        infoMessage.setInfo(resultMap);
        return JsonUtil.object2Json(infoMessage);
    }
    
    /**
     * 取得reportNode的信息
     * @param info
     * @return
     * @author bo.dang
     * @date 2014-4-26
     */
    public String getReportNode(String info){
        @SuppressWarnings("unchecked")
        Map<String, Object> infoMap = JsonUtil.toHashMap(info);
        ReportNode reportNode = JsonUtil.toGsonBean(infoMap.get("ReportNode").toString(), ReportNode.class);
        return null;
    }
    
    /**
     * 新增GISMapSquid
     * @param info
     * @return
     * @author yi.zhou
     * @date 2014-11-14
     */
    public String addGISMapSquid(String info){
    	out = new ReturnValue();
        Map<String, Class<?>> paramsMap = new HashMap<String, Class<?>>();
        paramsMap.put("GISMapSquid", GISMapSquid.class);
        Map<String, Object> resultMap = new ExtractServicesub(TokenUtil.getToken()).execute(info, paramsMap, DMLType.INSERT.value(), out);
        InfoNewMessagePacket<Map<String, Object>> infoMessage = new InfoNewMessagePacket<Map<String, Object>>();
        infoMessage.setCode(out.getMessageCode().value());
        if (out.getMessageCode()==MessageCode.SUCCESS){
        	infoMessage.setInfo(resultMap);
        }else{
        	infoMessage.setInfo(new HashMap<String, Object>());
        }
        infoMessage.setDesc("创建GISMapSquid");
        return JsonUtil.object2Json(infoMessage);
    }
    
    /**
     * 更新ReportSquid
     * @param info
     * @return
     * @author bo.dang
     * @date 2014-4-16
     */
    @SuppressWarnings("unchecked")
    public String updateGISMapSquid(String info){
    	ReturnValue out = new ReturnValue();
        Map<String, Object> infoMap = JsonUtil.toHashMap(info);
        GISMapSquid gisMapSquid = (GISMapSquid)JsonUtil.toGsonBean(infoMap.get("GISMapSquid").toString(), GISMapSquid.class);
/*        Gson gson = new GsonBuilder().create();
        ReportSquid reportSquid = gson.fromJson(infoMap.get("ReportSquid").toString(), ReportSquid.class);*/
        GISMapSquid newGISMapSquid = new ReportSquidServicesub(TokenUtil.getToken()).updateGISMapSquid(gisMapSquid, out);
        Map<String, Object> resultMap = new HashMap<String, Object>();
        InfoNewMessagePacket<Map<String, Object>> infoMessage = new InfoNewMessagePacket<Map<String,Object>>();
        if(out.getMessageCode()==MessageCode.SUCCESS){
            // 返回新增的FolderID
            resultMap.put("GISMapSquidId", newGISMapSquid.getId());
        }
        infoMessage.setCode(out.getMessageCode().value());
        infoMessage.setInfo(resultMap);
        infoMessage.setDesc("更新GISMapSquid");
        return JsonUtil.object2Json(infoMessage);
    }
    
    /**
     * 查看地图报表地址
     * @param info
     * @return 是否成功
     * @author yi.zhou
     * @date 2014-11-17
     */
    public String viewMapByURL(String info){
        Map<String, Object> infoMap = JsonUtil.toHashMap(info);
        Integer repositoryId = (Integer) infoMap.get("RepositoryId");
        Integer reportSquidId = (Integer) infoMap.get("SquidId");
        // TODO 需要调用引擎
        //EngineService.callSquidFlow(reportSquidId,repositoryId);
        String basePath = SysConf.getValue("basePathIp");
        String url = SysConf.getValue("viewmap_url");
        url = String.format(basePath+url, repositoryId, reportSquidId);
//        String url = "http://192.168.137.223:8080/report/test/report.jspx?repositoryId=" + repositoryId + "&squidId=" + reportSquidId;
        Map<String, Object> resultMap = new HashMap<String, Object>();
        // 返回是否成功
        // 返回URL
        resultMap.put("ViewMapURL", url);
        InfoNewMessagePacket<Map<String, Object>> infoMessage = new InfoNewMessagePacket<Map<String,Object>>();
        infoMessage.setInfo(resultMap);
        infoMessage.setDesc("ViewMapURL是否成功");
        return JsonUtil.object2Json(infoMessage);
    }
    
    /**
     * 编辑地图报表地址
     * @param info
     * @return 是否成功
     * @author yi.zhou
     * @date 2014-11-17
     */
    public String editMapByURL(String info){
        Map<String, Object> infoMap = JsonUtil.toHashMap(info);
        Integer repositoryId = (Integer) infoMap.get("RepositoryId");
        Integer reportSquidId = (Integer) infoMap.get("SquidId");
        // TODO 需要调用引擎
        String basePath = SysConf.getValue("basePathIp");
        String url = SysConf.getValue("editmap_url");
        url = String.format(basePath+url, repositoryId, reportSquidId);
        Map<String, Object> resultMap = new HashMap<String, Object>();
        // 返回URL
        resultMap.put("EditMapURL", url);
        InfoNewMessagePacket<Map<String, Object>> infoMessage = new InfoNewMessagePacket<Map<String,Object>>();
        infoMessage.setInfo(resultMap);
        infoMessage.setDesc("EditMapURL是否成功");
        return JsonUtil.object2Json(infoMessage);
    }
    
    /**
     * @return the token
     */
    public String getToken() {
        return token;
    }

    /**
     * @param token the token to set
     */
    public void setToken(String token) {
        this.token = token;
    }

    /**
     * @return the key
     */
    public String getKey() {
        return key;
    }

    /**
     * @param key the key to set
     */
    public void setKey(String key) {
        this.key = key;
    }
}
