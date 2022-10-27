package com.eurlanda.datashire.sprint7.service.metadata;

import com.eurlanda.datashire.entity.DBSourceSquid;
import com.eurlanda.datashire.entity.DbSquid;
import com.eurlanda.datashire.entity.MetadataNode;
import com.eurlanda.datashire.entity.SpecialSourceSquidAndSquidLinkAndExtranctSquid;
import com.eurlanda.datashire.entity.Squid;
import com.eurlanda.datashire.entity.TableExtractSquid;

import java.util.List;

/**
 * 元数据数管理。
 * 包括数的节点创建，新建节点等
 * @author chunhua.zhao
 * @version 1.0
 * @updated 11-一月-2014 13:43:54
 */

public interface IMetadataTreeService {

	 
	/**
	 * 添加一个node
	 * 入参：MetadatNode
	 * 出参MetadataNode
	 * 
	 * @param node
	 */
	public List<MetadataNode> addNode(List<MetadataNode> nodes);

	/**
	 * 删除一个node
	 * 
	 * @param nodeId
	 */
	public boolean deleteNode(Integer nodeId);

	/**
	 * 节点重命名
	 * 
	 * @param node
	 */
	public List<MetadataNode> renameNode(List<MetadataNode> nodes);

	/**
	 * 一次性取得一颗树
	 */
	public String getMetadataTree();

	/**
	 * 将Node拖放到SquidFlow里面，创建新的SourceSquid
	 * 
	 * @param node
	 * @param squidFlowId
	 */
	public List<DBSourceSquid>dropToSquidFlow(List<MetadataNode> nodes);
	/**
	 * 将Node拖放到SquidFlow里面，修改SourceSquid
	 * @param nodes
	 * @return
	 */
	public List<DbSquid>dropToSquidFlowCoverSourceSquid(List<MetadataNode> nodes);
	
	/**
	 * 从数据源浏览器中拖动数据表或文件信息覆盖ExtractSquid
	 * @param nodes
	 * @return
	 */
	public List<TableExtractSquid> dropToSquidFlowCoverExtractSquid(List<MetadataNode> nodes);
	
	/**
	 * 将treeNode拖放到squid上面，如果node是数据源，那么就更新目标DBSquid的数据源信息,
	 * 如果node是列信息，那么就更新目标ExtractSquid的目标列
	 * 
	 * @param node
	 * @param squidId
	 */
	public List<Squid>  dropToSquid(List<MetadataNode> nodes);
	
	/**
	 * 
	 * @param nodes
	 * @return
	 */
	public List<SpecialSourceSquidAndSquidLinkAndExtranctSquid> dropToSpecialSquid(List<MetadataNode> nodes);

	/**
	 * 取得根节点
	 */
	List<MetadataNode> getRootNodes();

	/**
	 * 获得某一节点的所有子节点
	 * 
	 * @param parentNodeId
	 */
	public List<MetadataNode> getSubNodes(Integer parentNodeId);

}