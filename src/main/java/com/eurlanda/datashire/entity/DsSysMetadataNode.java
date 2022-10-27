package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import java.io.Serializable;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.HashSet;
import java.util.Set;

/**
 * DsSysMetadataNode entity. @author MyEclipse Persistence Tools
 */
@Entity
@Table(name = "DS_SYS_METADATA_NODE", schema = "PUBLIC", catalog = "PUBLIC")
@MultitableMapping(pk="", name = "DS_SYS_METADATA_NODE", desc = "")
public class DsSysMetadataNode implements Serializable{

	// Fields
	@ColumnMpping(name="ID", desc="", nullable=true, precision=100, type=Types.INTEGER, valueReg="")
	private Integer id;
	@ColumnMpping(name="PARENT_ID", desc="", nullable=true, precision=100, type=Types.INTEGER, valueReg="")
	private Integer parentId;
	@ColumnMpping(name="NODE_TYPE", desc="", nullable=true, precision=100, type=Types.INTEGER, valueReg="")
	private Integer nodeType;
	@ColumnMpping(name="NODE_NAME", desc="", nullable=true, precision=100, type=Types.VARCHAR, valueReg="")
	private String nodeName;
	@ColumnMpping(name="ORDER_NUMBER", desc="", nullable=true, precision=100, type=Types.INTEGER, valueReg="")
	private Integer orderNumber;
	@ColumnMpping(name="NODE_DESC", desc="", nullable=true, precision=100, type=Types.VARCHAR, valueReg="")
	private String nodeDesc;
	@ColumnMpping(name="CREATION_DATE", desc="CREATION_DATE", nullable=true, precision=50, type=Types.TIMESTAMP, valueReg="")
	private Timestamp creationDate;
	@ColumnMpping(name="COLUMNATTRIBUTE", desc="", nullable=true, precision=100, type=Types.VARCHAR, valueReg="")
	private String columnAttribute;
	@ColumnMpping(name="key", desc="", nullable=false, precision=36, type=Types.VARCHAR, valueReg="")
	private String key;
	private Set<DsSysMetadataNodeAttr> dsSysMetadataNodeAttrs = new HashSet<DsSysMetadataNodeAttr>(
			0);

	// Constructors

	/** default constructor */
	public DsSysMetadataNode() {
	}

	public String getColumnAttribute() {
		return columnAttribute;
	}

	public void setColumnAttribute(String columnAttribute) {
		this.columnAttribute = columnAttribute;
	}

	/** minimal constructor */
	public DsSysMetadataNode(Integer id) {
		this.id = id;
	}

	/** full constructor */
	public DsSysMetadataNode(Integer id, Integer parentId, Integer nodeType,
			String nodeName, Integer orderNumber, String nodeDesc,
			Timestamp creationDate,
			Set<DsSysMetadataNodeAttr> dsSysMetadataNodeAttrs) {
		this.id = id;
		this.parentId = parentId;
		this.nodeType = nodeType;
		this.nodeName = nodeName;
		this.orderNumber = orderNumber;
		this.nodeDesc = nodeDesc;
		this.creationDate = creationDate;
		this.dsSysMetadataNodeAttrs = dsSysMetadataNodeAttrs;
	}

	// Property accessors
	@Id
	@Column(name = "ID", unique = true, nullable = false)
	public Integer getId() {
		return this.id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	@Column(name = "PARENT_ID")
	public Integer getParentId() {
		return this.parentId;
	}

	public void setParentId(Integer parentId) {
		this.parentId = parentId;
	}

	@Column(name = "NODE_TYPE")
	public Integer getNodeType() {
		return this.nodeType;
	}

	public void setNodeType(Integer nodeType) {
		this.nodeType = nodeType;
	}

	@Column(name = "NODE_NAME", length = 100)
	public String getNodeName() {
		return this.nodeName;
	}

	public void setNodeName(String nodeName) {
		this.nodeName = nodeName;
	}

	@Column(name = "ORDER_NUMBER")
	public Integer getOrderNumber() {
		return this.orderNumber;
	}

	public void setOrderNumber(Integer orderNumber) {
		this.orderNumber = orderNumber;
	}

	@Column(name = "NODE_DESC", length = 200)
	public String getNodeDesc() {
		return this.nodeDesc;
	}

	public void setNodeDesc(String nodeDesc) {
		this.nodeDesc = nodeDesc;
	}

	@Column(name = "CREATION_DATE", length = 26)
	public Timestamp getCreationDate() {
		return this.creationDate;
	}

	public void setCreationDate(Timestamp creationDate) {
		this.creationDate = creationDate;
	}

	@OneToMany(cascade = CascadeType.ALL, fetch = FetchType.LAZY, mappedBy = "dsSysMetadataNode")
	public Set<DsSysMetadataNodeAttr> getDsSysMetadataNodeAttrs() {
		return this.dsSysMetadataNodeAttrs;
	}

	public void setDsSysMetadataNodeAttrs(
			Set<DsSysMetadataNodeAttr> dsSysMetadataNodeAttrs) {
		this.dsSysMetadataNodeAttrs = dsSysMetadataNodeAttrs;
	}
	@Override
	public String toString() {
		return "DS_SYS_METADATA_NODE [id=" + id + "]";
	}

	@Column(name ="KEY", length = 200)
	public String getKey() {
		return this.key;
	}

	public void setKey(String key) {
		this.key = key;
	}
	
}