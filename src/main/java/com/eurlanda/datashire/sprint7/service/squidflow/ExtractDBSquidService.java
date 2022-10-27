package com.eurlanda.datashire.sprint7.service.squidflow;

/*
 * ExtractDBSquidService.java - 2013.12.4
 *
 * 版权声明: (c) 2013，悦岚（上海）数据服务有限公司，保留所有权利。
 *
 * 项目名称: datashire-server
 * 
 * 修改历史:
 * ===========================================
 *   修改人	     日期		     描述
 *   ---------------------------------------
 *   dang.lu  2013.12.4   create    
 * ===========================================
 */

import com.eurlanda.datashire.entity.SquidFlow;
import com.eurlanda.datashire.entity.operation.ExtractSquidAndSquidLink;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.sprint7.packet.InfoMessagePacket;
import com.eurlanda.datashire.sprint7.packet.ListMessagePacket;
import com.eurlanda.datashire.sprint7.service.squidflow.subservice.ExtractServicesub;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import com.eurlanda.datashire.utility.StringUtils;
import com.google.gson.FieldNamingPolicy;
import org.apache.log4j.Logger;

import java.util.List;

//import org.apache.poi.hssf.record.formula.functions.T;

/**
 * ExtractSquid-DB 组合业务处理类
 * 	直接从关系型数据库（同一张表中的全部或部分列）抽取出来的数据集
 * 	0.可以直接来源于source_squid,也可以是先创建后指定来源
 * 	1.数据可能经过筛选/过滤，全量/增量抽取产生；
 * 	2.但不会产生具体变换；
 * 	3.理论上可重新定义表名、列名并直接“落地（即存储）”至目标squid
 * 	4.也可以作为下一个/多个数据集的源参与相关变换
 * 
 * @author dang.lu 2013.12.4
 *
 */
public class ExtractDBSquidService {
	public static final Logger logger = Logger.getLogger(ExtractDBSquidService.class);
	private ExtractServicesub subService;
	private String token;
	private String key;
	
	public ExtractDBSquidService(String token, String key) {
		this.token = token;
		this.key = key;
		subService = new ExtractServicesub(token);
	}
	
	/**
	 * 拖拽source column集合到空的extract，并生成transformation (或link)
	 * @param info
	 * @return
	 */
	public String drag2EmptyExtractSquid(String info, ReturnValue out){
		ExtractSquidAndSquidLink vo = JsonUtil.json2Object(info , ExtractSquidAndSquidLink.class, FieldNamingPolicy.UPPER_CAMEL_CASE);
		InfoMessagePacket<ExtractSquidAndSquidLink> packet = new InfoMessagePacket<ExtractSquidAndSquidLink>();
		packet.setType(DSObjectType.EXTRACT);
		if(vo!=null && vo.getExtractSquid()!=null){
			packet.setInfo(subService.drag2ExtractSquid(vo, out));
			// 将squid flow的key填充到包头
			SquidFlow flow = subService.getOne(SquidFlow.class, vo.getExtractSquid().getSquidflow_id());
			if(flow!=null && StringUtils.isNotNull(flow.getKey())){
				this.setKey(flow.getKey());
			}
			packet.setCode(out.getMessageCode().value());
		}else{
			packet.setCode(MessageCode.DESERIALIZATION_FAILED.value());
		}
		return JsonUtil.object2Json(packet);
	}
	
	/**
	 * 拖拽source column集合到空白区，并生成extract、link和transformation
	 * @param info
	 * @return
	 */
	public String drag2NewExtractSquid(String info, ReturnValue out){
	    // fixed bug by bo.dang
//		ExtractSquidAndSquidLink vo = JsonUtil.json2Object(info, ExtractSquidAndSquidLink.class, FieldNamingPolicy.UPPER_CAMEL_CASE);
	    ExtractSquidAndSquidLink vo = JsonUtil.toGsonList(info, ExtractSquidAndSquidLink.class) == null? null:JsonUtil.toGsonList(info, ExtractSquidAndSquidLink.class).get(0);
		InfoMessagePacket<ExtractSquidAndSquidLink> packet = new InfoMessagePacket<ExtractSquidAndSquidLink>();
		packet.setType(DSObjectType.EXTRACT);
		if(vo!=null){
			packet.setInfo(subService.drag2ExtractSquid(vo, out));
	        packet.setCode(out.getMessageCode().value());
		}else{
			packet.setCode(MessageCode.DESERIALIZATION_FAILED.value());
		}
		return JsonUtil.object2Json(packet);
	}
	
	/** 直接从table list拖拽创建，有可能多选或全选  */
	public String dragTableName2NewExtractSquid(String info, ReturnValue out){
		List<ExtractSquidAndSquidLink> vo = JsonUtil.toGsonList(info, ExtractSquidAndSquidLink.class);
		ListMessagePacket<ExtractSquidAndSquidLink> packet = new ListMessagePacket<ExtractSquidAndSquidLink>();
		packet.setType(DSObjectType.EXTRACT);
		if(vo!=null){
			subService.drag2ExtractSquid(vo, out);
			packet.setDataList(vo);
			packet.setCode(out.getMessageCode().value());
		}else{
//			packet.setCode(MessageCode.DESERIALIZATION_FAILED.value());
			packet.setCode(MessageCode.ERR_EXTRACT_SQUID_NO_DATA.value());
		}
		return JsonUtil.object2Json(packet);
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
}
