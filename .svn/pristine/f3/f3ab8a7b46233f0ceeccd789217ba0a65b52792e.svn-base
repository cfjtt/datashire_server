package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.entity.SquidLink;
import com.eurlanda.datashire.entity.WebserviceExtractSquid;
import com.eurlanda.datashire.enumeration.DMLType;
import com.eurlanda.datashire.server.utils.TokenUtil;
import com.eurlanda.datashire.sprint7.packet.InfoNewMessagePacket;
import com.eurlanda.datashire.sprint7.service.squidflow.subservice.ExtractServicesub;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

/**
 * WebserviceExtractSquidService接口类
 * 
 * @author lei.bin
 * 
 */
@Service
public class WebserviceExtractSquidService {
	public static final Logger logger = Logger
			.getLogger(HttpExtractSquidService.class);
	private String token;
	private String key;
	// 异常处理机制
	ReturnValue out = new ReturnValue();

	/**
	 * 创建WebServiceExtract 2014-12-3
	 * 
	 * @author Akachi
	 * @E-Mail zsts@hotmail.com
	 * @param info
	 * @return
	 */
	public String createWebserviceExtractSquid(String info) {
		out = new ReturnValue();
		Map<String, Class<?>> paramsMap = new HashMap<String, Class<?>>();
		paramsMap.put("SquidLink", SquidLink.class);
		paramsMap.put("WebserviceExtractSquid", WebserviceExtractSquid.class);
		ExtractServicesub hesp = new ExtractServicesub(TokenUtil.getToken());
		Map<String, Object> resultMap = hesp.execute(info, paramsMap,
				DMLType.INSERT.value(), out);
		out = new ReturnValue();
		InfoNewMessagePacket<Map<String, Object>> infoMessage = new InfoNewMessagePacket<Map<String, Object>>();
		infoMessage.setCode(out.getMessageCode().value());
		if (out.getMessageCode() == MessageCode.SUCCESS) {
			infoMessage.setInfo(resultMap);
		}
		infoMessage.setDesc("创建WebserviceExtractSquid");
		return JsonUtil.object2Json(infoMessage);
	}

	public String getSourceDataByWebserviceExtractSquid(String info) {
		Map<String, Class<?>> paramsMap = new HashMap<String, Class<?>>();
		paramsMap.put("WebserviceExtractSquid", WebserviceExtractSquid.class);
		WebserviceExtractSquidProcess wesp = new WebserviceExtractSquidProcess(TokenUtil.getToken());
		out = new ReturnValue();
		Map<String, Object> resultMap = wesp.getSourceDataByWebserviceExtractSquid(info, out);
		InfoNewMessagePacket<Map<String, Object>> infoMessage = new InfoNewMessagePacket<Map<String, Object>>();
		infoMessage.setCode(out.getMessageCode().value());
		infoMessage.setInfo(resultMap);
		infoMessage.setDesc("连接WebService");
		return JsonUtil.object2Json(infoMessage);
	}

	public String updateWebserviceExtractSquid(String info) {
		out = new ReturnValue();
		Map<String, Class<?>> paramsMap = new HashMap<String, Class<?>>();
		paramsMap.put("WebserviceExtractSquid", WebserviceExtractSquid.class);
		// 更新DocExtractSquid
		out = new ReturnValue();
		Map<String, Object> resultMap = new ExtractServicesub(TokenUtil.getToken()).execute(
				info, paramsMap, DMLType.UPDATE.value(), out);
		InfoNewMessagePacket<Map<String, Object>> infoMessage = new InfoNewMessagePacket<Map<String, Object>>();
		infoMessage.setCode(out.getMessageCode().value());
		if (out.getMessageCode() == MessageCode.SUCCESS) {
			infoMessage.setInfo(resultMap);
		}
		infoMessage.setDesc("更新WebserviceExtractSquid信息");
		return JsonUtil.object2Json(infoMessage);
	}

	public String getToken() {
		return token;
	}

	public void setToken(String token) {
		this.token = token;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public ReturnValue getOut() {
		return out;
	}

	public void setOut(ReturnValue out) {
		this.out = out;
	}

}
