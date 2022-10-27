package com.eurlanda.datashire.sprint7.action.metadata;

import com.eurlanda.datashire.entity.DBSourceSquid;
import com.eurlanda.datashire.entity.DbSquid;
import com.eurlanda.datashire.entity.MetadataNode;
import com.eurlanda.datashire.entity.SpecialSourceSquidAndSquidLinkAndExtranctSquid;
import com.eurlanda.datashire.entity.TableExtractSquid;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.exception.SystemErrorException;
import com.eurlanda.datashire.sprint7.packet.BasicMessagePacket;
import com.eurlanda.datashire.sprint7.packet.InfoNewPacket;
import com.eurlanda.datashire.sprint7.packet.ListMessagePacket;
import com.eurlanda.datashire.sprint7.service.metadata.MetadataTreeServiceImpl;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * 树实现
 * @author chunhua.zhao
 * @version 1.0
 * @created 09-一月-2014 14:39:03
 */
@Service
public class MetadataTreeActionImpl{
	protected String token;
	protected String key;
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

	MetadataTreeServiceImpl metadataTreeServiceImpl = new MetadataTreeServiceImpl();
	
	/**
	 * 新增节点信息
	 * @param info
	 * @return
	 */
	public String addNode(String info) {
		ListMessagePacket<MetadataNode> listMessagePacket = new ListMessagePacket<MetadataNode>();
		try {
			List<MetadataNode> nodes =  JsonUtil.toGsonList(info, MetadataNode.class);
			metadataTreeServiceImpl.setToken(token);
			nodes = metadataTreeServiceImpl.addNode(nodes);
			listMessagePacket.setDataList(nodes);
			listMessagePacket.setType(DSObjectType.METADATA);
		} catch (SystemErrorException e) {
			//异常
			listMessagePacket.setCode(e.getCode().value())	;
		}finally{
			
		}
		return JsonUtil.object2Json(listMessagePacket);
	}

	/**
	 *删除节点信息
	 * @param info
	 * @return
	 */
	public String deleteNode(String info) {
		BasicMessagePacket infoMessagePacket = new BasicMessagePacket();
		try {
			boolean delete = false;
			List<InfoNewPacket> infoPackets = JsonUtil.toGsonList(info, InfoNewPacket.class);
			for (InfoNewPacket infoNewPacket : infoPackets) {
				delete = metadataTreeServiceImpl.deleteNode(infoNewPacket.getId());
			}
			if(delete){
				infoMessagePacket.setCode(1);
			}else{
				infoMessagePacket.setCode(MessageCode.ERR_DELETE.value());
			}
			
		} catch (SystemErrorException e) {
			infoMessagePacket.setCode(e.getCode().value());
		}
		
		return JsonUtil.object2Json(infoMessagePacket);
	}

	/**
	 * 
	 * @param info
	 * @return
	 *//*
	public String dropToSquid(String info) {
		try {
			List<MetadataNode> nodes = JsonUtil.toGsonList(info, MetadataNode.class);
			metadataTreeServiceImpl.dropToSquid(nodes);
		} catch (SystemErrorException e) {
			// TODO: handle exception
		}
		return null;
	}
*/
	/**
	 * 拖拽创建squid
	 * @param info
	 * @return
	 */
	public String dropToSquidFlow(String info) {
		ListMessagePacket<DBSourceSquid> listMessagePacket = new ListMessagePacket<DBSourceSquid>();
		try {
			List<MetadataNode> nodes =  JsonUtil.toGsonList(info, MetadataNode.class);
			List<DBSourceSquid> infoPackets = metadataTreeServiceImpl.dropToSquidFlow(nodes);
			listMessagePacket.setDataList(infoPackets);
			listMessagePacket.setType(DSObjectType.METADATA);
		} catch (SystemErrorException e) {
			//异常
			listMessagePacket.setCode(e.getCode().value())	;
		}
		return JsonUtil.object2Json(listMessagePacket);
	}

/*	public String getMetadataTree(String info) {
		// TODO Auto-generated method stub
		return null;
	}

	public String getRootNodes(String info) {
		// TODO Auto-generated method stub
		return null;
	}*/

	/**
	 * 获得MetadataNode信息
	 * @param info
	 * @return
	 */
	public String getSubNodes(String info) {
		ListMessagePacket<MetadataNode> listMessagePacket = new ListMessagePacket<MetadataNode>();
		try {
			MetadataNode node =  JsonUtil.toGsonBean(info, MetadataNode.class);
			metadataTreeServiceImpl.setToken(token);
			List<MetadataNode> nodes =metadataTreeServiceImpl.getSubNodes(node.getId());
			listMessagePacket.setDataList(nodes);
			listMessagePacket.setCode(1);
			listMessagePacket.setType(DSObjectType.METADATA);
		} catch (SystemErrorException e) {                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
			//异常
			listMessagePacket.setCode(e.getCode().value())	;
			listMessagePacket.setDesc(e.getMessage());
		}
		return JsonUtil.object2Json(listMessagePacket);
	}

	/**
	 * 修改节点信息
	 * @param info
	 * @return
	 */
	public String renameNode(String info) {
		ListMessagePacket<MetadataNode> listMessagePacket = new ListMessagePacket<MetadataNode>();
		try {
			List<MetadataNode> nodes =  JsonUtil.toGsonList(info, MetadataNode.class);
			nodes = metadataTreeServiceImpl.renameNode(nodes);
			listMessagePacket.setDataList(nodes);
			listMessagePacket.setType(DSObjectType.METADATA);
			listMessagePacket.setCode(1);
		} catch (SystemErrorException e) {
			//异常
			listMessagePacket.setCode(e.getCode().value())	;
		}finally{
			
		}
		return JsonUtil.object2Json(listMessagePacket);
	
	}
	/**
	 * 从数据源浏览器中拖动列创建SourceSquid和ExtractSquid
	 * @param info
	 * @return
	 */
	public String dropToSpecialSquid(String info){
		ListMessagePacket<SpecialSourceSquidAndSquidLinkAndExtranctSquid> listMessagePacket = new ListMessagePacket<SpecialSourceSquidAndSquidLinkAndExtranctSquid>();
		try {
			List<MetadataNode> nodes =  JsonUtil.toGsonList(info, MetadataNode.class);
			List<SpecialSourceSquidAndSquidLinkAndExtranctSquid> specialSquid = metadataTreeServiceImpl.dropToSpecialSquid(nodes);
			listMessagePacket.setDataList(specialSquid);
			listMessagePacket.setType(DSObjectType.METADATA);
			listMessagePacket.setCode(1);
		} catch (SystemErrorException e) {
			//异常
			listMessagePacket.setCode(e.getCode().value())	;
			listMessagePacket.setDesc(e.getMessage());
		}finally{
			
		}
		return JsonUtil.object2Json(listMessagePacket);
		
	}
	/**
	 * 拖拽覆盖SourceSquid
	 * @param info
	 * @return
	 */
	public String dropToSquidFlowCoverSourceSquid(String info){
		ListMessagePacket<DbSquid> listMessagePacket = new ListMessagePacket<DbSquid>();
		try {
			List<MetadataNode> nodes =  JsonUtil.toGsonList(info, MetadataNode.class);
			List<DbSquid> specialSquid = metadataTreeServiceImpl.dropToSquidFlowCoverSourceSquid(nodes);
			listMessagePacket.setDataList(specialSquid);
			listMessagePacket.setType(DSObjectType.METADATA);
			listMessagePacket.setCode(1);
		} catch (SystemErrorException e) {
			//异常
			listMessagePacket.setCode(e.getCode().value())	;
		}finally{
			
		}
		return JsonUtil.object2Json(listMessagePacket);
		
	}
	
	/**
	 * 拖拽覆盖ExtractSquid
	 * @param info
	 * @return
	 */
	public String dropToSquidFlowCoverExtractSquid(String info){
		ListMessagePacket<TableExtractSquid> listMessagePacket = new ListMessagePacket<TableExtractSquid>();
		try {
			List<MetadataNode> nodes =  JsonUtil.toGsonList(info, MetadataNode.class);
			List<TableExtractSquid> specialSquid = metadataTreeServiceImpl.dropToSquidFlowCoverExtractSquid(nodes);
			listMessagePacket.setDataList(specialSquid);
			listMessagePacket.setType(DSObjectType.METADATA);
			listMessagePacket.setCode(1);
		} catch (SystemErrorException e) {
			//异常
			listMessagePacket.setCode(e.getCode().value())	;
		}finally{
			
		}
		return JsonUtil.object2Json(listMessagePacket);
		
	}
}