package com.eurlanda.datashire.utility;

import com.alibaba.fastjson.JSON;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.sprint7.packet.InfoMessagePacket;
import com.eurlanda.datashire.sprint7.packet.ListMessagePacket;
import org.apache.log4j.Logger;

import java.text.MessageFormat;
import java.util.List;

/**
 * alibaba.fastjson
 * 
 * @author dang.lu 2014.2.10
 *
 */
public class FastJson {

	public static final <T> T json2Object(String jsonStr, Class<T> mainClass) {
		return JSON.parseObject(jsonStr, mainClass);
	}

	public static final String toString(List<?> obj, DSObjectType type, MessageCode out){
		if(out==null||out!=MessageCode.SUCCESS){
			logger.warn(MessageFormat.format("Service Execute Failed! (result-size = {0}, msg-code = {1}, msg-info = {2})", obj==null?-1:obj.size(), out==null?-1:out.value(), out));
		}
		ListMessagePacket listMessage = new ListMessagePacket();
		if(out!=null)listMessage.setCode(out.value());
		listMessage.setDataList(obj);
		listMessage.setType(type);
		//return JSONObject.fromObject(listMessage, jsonConfig).toString();
		//return JsonUtil.object2Json(listMessage);
		return JSON.toJSONString(listMessage);
	}
	
	public static final String toString(Object obj, DSObjectType type, ReturnValue out){
		//return toString(obj, type, out.getMessageCode());
		int c = -1;
		if(out!=null&&out.getMessageCode()!=null){
			c = out.getMessageCode().value();
		}
		if(c!=MessageCode.SUCCESS.value()){
			logger.warn(MessageFormat.format("Service Execute Failed! (msg-code = {0}, msg-info = {1}, obj-type = {2})", 
					c, MessageCode.parse(c), type));
		}
		InfoMessagePacket infoMessagePacket = new InfoMessagePacket();
		infoMessagePacket.setCode(c);
		infoMessagePacket.setInfo(obj);
		infoMessagePacket.setType(type);
		//return JSONObject.fromObject(infoMessagePacket, jsonConfig).toString(); // 不是一般的慢！
		//return JsonUtil.object2Json(infoMessagePacket); // GOOGLE api貌似很强大，有很多限制
		return JSON.toJSONString(infoMessagePacket); // alibaba.fastjson 就是快
		// TODO return "{"code":1,"info":".concat(obj).concat(","type":"UNKNOWN"}");
	}
	private transient static Logger logger = Logger.getLogger(FastJson.class);
}
