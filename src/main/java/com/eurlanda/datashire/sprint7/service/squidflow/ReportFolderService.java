package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.entity.ReportNode;
import com.eurlanda.datashire.server.utils.TokenUtil;
import com.eurlanda.datashire.sprint7.packet.InfoNewMessagePacket;
import com.eurlanda.datashire.sprint7.service.squidflow.subservice.ReportFolderServicesub;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class ReportFolderService {

	static Logger logger = Logger.getLogger(ReportFolderService.class);// 记录日志
	private String token;//令牌根据令牌得到相应的连接信息
	private String key;//key值
	
    
    /**
     * 根据父节点查询ReportNode信息
     * @param info
     * @return
     * @author bo.dang
     * @date 2014-4-14
     */
    public String getSubReportNodesById(String info){
    	ReturnValue out = new ReturnValue();
    	Map<String, Object> infoMap = JsonUtil.toHashMap(info);
    	Integer id = (Integer) infoMap.get("ReportNodeId");
    	InfoNewMessagePacket<Map<String, Object>> infoMessage = new InfoNewMessagePacket<Map<String,Object>>();
    	// 取得reportNodeList的信息
    	List<ReportNode> reportNodeList =  new ReportFolderServicesub(TokenUtil.getToken()).getSubReportNodesById(id, out);
		Map<String, Object> resultMap = new HashMap<String, Object>();
		// 返回新增的FolderID
		resultMap.put("ReportNodeList", reportNodeList);
		if (out.getMessageCode()==MessageCode.SUCCESS){
			infoMessage.setInfo(resultMap);
		}
		infoMessage.setCode(out.getMessageCode().value());
    	infoMessage.setDesc("根据父节点查询ReportFolder信息");
    	return JsonUtil.object2Json(infoMessage);
    }

    /**
     * 根据主键ID查询ReportNode信息
     * @param info
     * @return
     * @author bo.dang
     * @date 2014-4-14
     */
    public String getReportNodeById(String info){
    	ReturnValue out = new ReturnValue();
    	Map<String, Object> infoMap = JsonUtil.toHashMap(info);
    	Integer id = (Integer) infoMap.get("ReportNodeId");
    	List<ReportNode> reportNodeList = new ReportFolderServicesub(TokenUtil.getToken()).getReportNodeById(id, out);
		Map<String, Object> resultMap = new HashMap<String, Object>();
		// 返回新增的FolderID
		resultMap.put("ReportNodeList", reportNodeList);
		InfoNewMessagePacket<Map<String, Object>> infoMessage = new InfoNewMessagePacket<Map<String,Object>>();
    	if (out.getMessageCode()==MessageCode.SUCCESS){
    		infoMessage.setInfo(resultMap);
    	}
    	infoMessage.setCode(out.getMessageCode().value());
    	infoMessage.setDesc("根据主键ID查询ReportFolder信息");
    	return JsonUtil.object2Json(infoMessage);
    }
	
	/**
	 * 新增ReportNode
	 * @param info
	 * @return
	 * @author bo.dang
	 * @date 2014-4-11
	 */
	public String addReportNode(String info){
		
		@SuppressWarnings("unchecked")
		Map<String, Object> infoMap = JsonUtil.toHashMap(info);
		ReportNode reportNode = JsonUtil.toGsonBean(infoMap.get("ReportNode").toString(), ReportNode.class);
		// 取得创建ReportFolder的信息
		reportNode = new ReportFolderServicesub(TokenUtil.getToken()).addReportNode(reportNode);
		Map<String, Object> resultMap = new HashMap<String, Object>();
		// 返回新增的FolderID
		resultMap.put("ReportNodeId", reportNode.getId());
    	InfoNewMessagePacket<Map<String, Object>> infoMessage = new InfoNewMessagePacket<Map<String,Object>>();
    	infoMessage.setInfo(resultMap);
    	infoMessage.setDesc("新增reportFolder");
		return JsonUtil.object2Json(infoMessage);
	}
	
	/**
	 * 删除ReportNode
	 * @param info
	 * @return
	 * @author bo.dang
	 * @date 2014-4-14
	 */
	public String deleteReportNode(String info){
		Map<String, Object> infoMap = JsonUtil.toHashMap(info);
		Integer id = (Integer) infoMap.get("ReportNodeId");
		Boolean delFlag = new ReportFolderServicesub(TokenUtil.getToken()).deleteReportNode(id);
		Map<String, Object> resultMap = new HashMap<String, Object>();
		// 返回新增的FolderID
		resultMap.put("ReportNodeId", id);
		InfoNewMessagePacket<Map<String, Object>> infoMessage = new InfoNewMessagePacket<Map<String,Object>>();
    	infoMessage.setInfo(resultMap);
    	if(delFlag){
    		infoMessage.setDesc("删除ReportNode成功!");
    	}
    	else {
    		infoMessage.setDesc("删除ReportNode失败！");
    	}
		return JsonUtil.object2Json(infoMessage);
	}

	/**
	 * 更新ReportNode信息
	 * @param info
	 * @return
	 * @author bo.dang
	 * @date 2014-4-14
	 */
	public String updateReportNode(String info){
		ReturnValue out = new ReturnValue();
		Map<String, Object> infoMap = JsonUtil.toHashMap(info);
		ReportNode reportNode = JsonUtil.toGsonBean(infoMap.get("ReportNode").toString(), ReportNode.class);
		// 取得更新flag
		Boolean updateFlag = new ReportFolderServicesub(TokenUtil.getToken()).updateReportNode(reportNode, out);
		Map<String, Object> resultMap = new HashMap<String, Object>();
		// 返回新增的FolderID
		resultMap.put("ReportNodeId", reportNode.getId());
		InfoNewMessagePacket<Map<String, Object>> infoMessage = new InfoNewMessagePacket<Map<String,Object>>();
    	infoMessage.setInfo(resultMap);
    	if(updateFlag){
    		infoMessage.setDesc("更新ReportNode成功!");
    	}
    	else {
    		infoMessage.setDesc("更新ReportNode失败！");
    	}
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
