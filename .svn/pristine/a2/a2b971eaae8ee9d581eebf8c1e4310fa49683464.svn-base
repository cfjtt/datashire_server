package com.eurlanda.datashire.sprint7.action.metadata;

import com.eurlanda.datashire.entity.FileInfo;
import com.eurlanda.datashire.entity.MetadataNode;
import com.eurlanda.datashire.entity.SourceTable;
import com.eurlanda.datashire.entity.operation.SquidDataSet;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.sprint7.packet.InfoMessagePacket;
import com.eurlanda.datashire.sprint7.packet.ListMessagePacket;
import com.eurlanda.datashire.sprint7.service.metadata.IMetadataImportService;
import com.eurlanda.datashire.utility.JsonUtil;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
/**
 * 元数据游览导入(序列化反序列化json处理类)
 * @author lei.bin
 *
 */
@Service
public class MetadataImportAction {
	private static Logger logger = Logger.getLogger(MetadataImportAction.class);// 记录日志
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
	@Autowired
	IMetadataImportService iMetadataImportService;
	/**
	 * 添加节点的属性，属性是一个Map
	 * 要批量插入到数据库
	 * 
	 * @param nodeParams
	 */
	public String addNodeAttr(String info) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * 删除某节点的所有属性
	 * 
	 * @param nodeId
	 */
	public String deleteNodeAttr(String info) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * 修改一个结点的属性，要清空现有的属性，然后添加新的
	 * 
	 * @param nodeId
	 */
	public String modifyNodeAttr(String info) {
		// TODO Auto-generated method stub
		return null;
	}

	
	/**
	 * 取得数据库节点下的数据，如获取某张表中的数据或者某个列的数据
	 * 
	 */
	public String getConnectionNodeData(String info) {
		// TODO Auto-generated method stub
		ListMessagePacket<SourceTable> listMessage = new ListMessagePacket<SourceTable>();
		try {
			
		} catch (Exception e) {
			// TODO: handle exception
			logger.error("读取主节点具体异常", e);
		}
		return JsonUtil.object2Json(listMessage);
	}

	/**
	 * 取得文件节点的内容。
	 * @param nodeId
	 */
	public String getNodeData(String info) {
		// TODO Auto-generated method stub
		try {
			MetadataNode node =  JsonUtil.toGsonBean(info, MetadataNode.class);
			//根据类型判断调用具体接口
			if("1".equals(node.getAttributeMap().get("Type"))||"2".equals(node.getAttributeMap().get("Type")))//sqlserver(1),oralce(2)
			{
				InfoMessagePacket<SquidDataSet> infoMessagePacket = new InfoMessagePacket<SquidDataSet>();
				SquidDataSet squidDataSet =iMetadataImportService.getConnectionNodeData(node);
				infoMessagePacket.setInfo(squidDataSet);
				infoMessagePacket.setType(DSObjectType.BROWSE);
				return JsonUtil.object2Json(infoMessagePacket);
			}
			else if("6".equals(node.getAttributeMap().get("Type")))//filepath
			{
				InfoMessagePacket<String> listMessage = new InfoMessagePacket<String>();
				String filecontent=iMetadataImportService.getFileNodeData(node);
				listMessage.setInfo(filecontent);
				listMessage.setType(DSObjectType.LOCALFILE);
				return JsonUtil.object2Json(listMessage);
			}else if("7".equals(node.getAttributeMap().get("Type")))//ftp
			{
				InfoMessagePacket<String> listMessage = new InfoMessagePacket<String>();
				String filecontent=iMetadataImportService.getFtpFileContent(node);
				listMessage.setInfo(filecontent);
				listMessage.setType(DSObjectType.FTP);
				return JsonUtil.object2Json(listMessage);
			}
		} catch (Exception e) {
			// TODO: handle exception
		}
		return null;
	}

	/**
	 * 读取网页的内容
	 * @param nodeId
	 */
	public String getWebNodeData(String info) {
		// TODO Auto-generated method stub
		ListMessagePacket<FileInfo> listMessage = new ListMessagePacket<FileInfo>();
		try {
			
		} catch (Exception e) {
			// TODO: handle exception
		}
		return null;
	}

	/**
	 * 浏览数据库连接节点
	 * 返回本连接下所有的表和列的定义。
	 * @param nodeId
	 */
	public String browseConnectionNode(String info) {
		// TODO Auto-generated method stub
		ListMessagePacket<SourceTable> listMessage = new ListMessagePacket<SourceTable>();
		try {
			MetadataNode node =  JsonUtil.toGsonBean(info, MetadataNode.class);
			List<SourceTable> sourceTables=iMetadataImportService.browseConnectionNode(node);
			listMessage.setDataList(sourceTables);
			listMessage.setType(DSObjectType.BROWSE);
		} catch (Exception e) {
			// TODO: handle exception
			logger.error("游览数据库节点信息异常", e);
		}
		return JsonUtil.object2Json(listMessage);
	}

	/**
	 * 浏览本地文件夹下面的所有文件以及ftp浏览目录信息
	 * 返回该节点定义的目录下面的所有文件和目录
	 * @param nodeId
	 */
	public String browseFileNode(String info) {
		// TODO Auto-generated method stub
		ListMessagePacket<FileInfo> listMessage = new ListMessagePacket<FileInfo>();
		try {
			MetadataNode node = JsonUtil.toGsonBean(info, MetadataNode.class);
			if ("6".equals(node.getAttributeMap().get("Type")))// filepath
			{
				List<FileInfo> fileInfos = iMetadataImportService.browseFileNode(node);
				listMessage.setDataList(fileInfos);
				listMessage.setType(DSObjectType.LOCALFILE);
			} else if ("7".equals(node.getAttributeMap().get("Type"))) {//ftp
				List<FileInfo> fileInfos = iMetadataImportService.browseftpFileNode(node);
				listMessage.setDataList(fileInfos);
				listMessage.setType(DSObjectType.FTP);
			}
		} catch (Exception e) {
			// TODO: handle exception
			logger.error("游览文件节点信息异常", e);
		}
		return JsonUtil.object2Json(listMessage);
	}

	/**
	 * ftp游览文件夹
	 * 
	 * @param info
	 * @return
	 */
	public String browseFtpNode(String info) {
		ListMessagePacket<FileInfo> listMessage = new ListMessagePacket<FileInfo>();
		try {
			MetadataNode node =JsonUtil.toGsonBean(info, MetadataNode.class);
			List<FileInfo> fileInfos=iMetadataImportService.browseftpFileNode(node);
			listMessage.setDataList(fileInfos);
			listMessage.setType(DSObjectType.FTP);
		} catch (Exception e) {
			// TODO: handle exception
			logger.error("游览ftp节点信息异常", e);
		}
		return JsonUtil.object2Json(listMessage);
	}
	
}
