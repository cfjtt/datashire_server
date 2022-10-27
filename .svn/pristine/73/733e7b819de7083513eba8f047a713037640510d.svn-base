package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;

import java.sql.Types;
import java.util.List;
import java.util.Map;


/**
 * metadata树节点
 * @author Gene
 * @version 1.0
 * @created 09-一月-2014 14:35:48
 * TODO
 */
@MultitableMapping(pk="", name = "DS_SYS_METADATA_NODE", desc = "")
public class MetadataNode extends DSBaseModel {
	@ColumnMpping(name="CREATION_DATE", desc="CREATION_DATE", nullable=true, precision=50, type=Types.TIMESTAMP, valueReg="")
	private String addDate;
	/*@ColumnMpping(name="ID", desc="", nullable=true, precision=100, type=Types.INTEGER, valueReg="")
	private int id;*/
	@ColumnMpping(name="NODE_NAME", desc="", nullable=true, precision=100, type=Types.VARCHAR, valueReg="")
	private String nodeName;
	@ColumnMpping(name="NODE_TYPE", desc="", nullable=true, precision=100, type=Types.INTEGER, valueReg="")
	private int nodeType;
	@ColumnMpping(name="ORDER_NUMBER", desc="", nullable=true, precision=100, type=Types.INTEGER, valueReg="")
	private int orderNumber;
	@ColumnMpping(name="PARENT_ID", desc="", nullable=true, precision=100, type=Types.INTEGER, valueReg="")
	private int parentNodeId;
	@ColumnMpping(name="NODE_DESC", desc="", nullable=true, precision=100, type=Types.VARCHAR, valueReg="")
	private String desc;
	@ColumnMpping(name="COLUMNATTRIBUTE", desc="", nullable=true, precision=100, type=Types.VARCHAR, valueReg="")
	private String columnAttribute;
	private Map<String,String> attributeMap;
	private int squidFLowId;
	private int squidId;
	private List<MetadataNode> metadataNodeList;
	private List<String> columnList;
	public MetadataNode(){

	}

	public void finalize() throws Throwable {

	}

	public String getAddDate() {
		return addDate;
	}

	public void setAddDate(String addDate) {
		this.addDate = addDate;
	}

	public Map<String, String> getAttributeMap() {
		return attributeMap;
	}

	public void setAttributeMap(Map<String, String> attributeMap) {
		this.attributeMap = attributeMap;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getNodeName() {
		return nodeName;
	}

	public void setNodeName(String nodeName) {
		this.nodeName = nodeName;
	}

	public int getNodeType() {
		return nodeType;
	}

	public void setNodeType(int nodeType) {
		this.nodeType = nodeType;
	}

	public int getOrderNumber() {
		return orderNumber;
	}

	public void setOrderNumber(int orderNumber) {
		this.orderNumber = orderNumber;
	}

	public int getParentNodeId() {
		return parentNodeId;
	}

	public void setParentNodeId(int parentNodeId) {
		this.parentNodeId = parentNodeId;
	}

	public String getDesc() {
		return desc;
	}

	public void setDesc(String desc) {
		this.desc = desc;
	}

	public int getSquidFLowId() {
		return squidFLowId;
	}

	public void setSquidFLowId(int squidFLowId) {
		this.squidFLowId = squidFLowId;
	}

	public int getSquidId() {
		return squidId;
	}

	public void setSquidId(int squidId) {
		this.squidId = squidId;
	}

	public List<MetadataNode> getMetadataNodeList() {
		return metadataNodeList;
	}

	public void setMetadataNodeList(List<MetadataNode> metadataNodeList) {
		this.metadataNodeList = metadataNodeList;
	}

	public String getColumnAttribute() {
		return columnAttribute;
	}

	public void setColumnAttribute(String columnAttribute) {
		this.columnAttribute = columnAttribute;
	}

	public List<String> getColumnList() {
		return columnList;
	}

	public void setColumnList(List<String> columnList) {
		this.columnList = columnList;
	}



}