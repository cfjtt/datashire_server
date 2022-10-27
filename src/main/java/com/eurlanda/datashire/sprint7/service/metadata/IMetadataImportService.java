package com.eurlanda.datashire.sprint7.service.metadata;

import com.eurlanda.datashire.entity.FileInfo;
import com.eurlanda.datashire.entity.MetadataNode;
import com.eurlanda.datashire.entity.SourceTable;
import com.eurlanda.datashire.entity.operation.SquidDataSet;

import java.util.List;
import java.util.Map;

/**
 * 元数据导入、浏览等相关操作。
 * @author Gene
 * @version 1.0
 * @created 11-一月-2014 14:50:23
 */
public interface IMetadataImportService {

	/**
	 * 添加节点的属性，属性是一个Map
	 * 要批量插入到数据库
	 * 
	 * @param nodeParams
	 */
	public void addNodeAttr(Map<String,String> nodeParams);

	/**
	 * 删除某节点的所有属性
	 * 
	 * @param nodeId
	 */
	public void deleteNodeAttr(Integer nodeId);

	/**
	 * 修改一个结点的属性，要清空现有的属性，然后添加新的
	 * 
	 * @param nodeId
	 */
	public void modifyNodeAttr(Integer nodeId);

	/**
	 * 取得数据库节点下的数据，如获取某张表中的数据或者某个列的数据
	 * 
	 * 
	 * @param nodeId
	 */
	public SquidDataSet getConnectionNodeData(MetadataNode node);

	/**
	 * 取得文件节点的内容。
	 * 
	 * @param nodeId
	 */
	public String getFileNodeData(MetadataNode node);

	/**
	 * 读取网页的内容
	 * 
	 * @param nodeId
	 */
	public String getWebNodeData(Integer nodeId);

	/**
	 * 浏览数据库连接节点
	 * 返回本连接下所有的表和列的定义。
	 * 
	 * @param nodeId
	 */
	public List<SourceTable> browseConnectionNode(MetadataNode node);

	/**
	 * 浏览文件夹下面的所有文件。
	 * 返回该节点定义的目录下面的所有文件和目录
	 * @param nodeId
	 */
	public List<FileInfo> browseFileNode(MetadataNode node);
	/**
	 * 通过FTP浏览文件夹下面的所有文件。
	 * 返回该节点定义的目录下面的所有文件和目录
	 * @param nodeId
	 */
	public List<FileInfo> browseftpFileNode(MetadataNode node);
	/**
	 * 查看节点的文件内容
	 * @param node
	 * @return
	 */
	public String getFtpFileContent(MetadataNode node);

}