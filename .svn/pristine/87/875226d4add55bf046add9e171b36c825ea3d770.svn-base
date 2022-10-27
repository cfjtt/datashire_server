package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import java.sql.Timestamp;
import java.sql.Types;

/**
 * DsSysMetadataNodeAttr entity. @author MyEclipse Persistence Tools
 */
@Entity
@Table(name = "DS_SYS_METADATA_NODE_ATTR", schema = "PUBLIC", catalog = "PUBLIC")
@MultitableMapping(pk="", name = "DS_SYS_METADATA_NODE_ATTR", desc = "")
public class DsSysMetadataNodeAttr implements java.io.Serializable {

	// Fields
	@ColumnMpping(name="ID", desc="", nullable=true, precision=100, type=Types.INTEGER, valueReg="")
	private Integer id;
	private DsSysMetadataNode dsSysMetadataNode;
	@ColumnMpping(name="ATTR_NAME", desc="", nullable=true, precision=100, type=Types.VARCHAR, valueReg="")
	private String attrName;
	@ColumnMpping(name="ATTR_VALUE", desc="", nullable=true, precision=100, type=Types.VARCHAR, valueReg="")
	private String attrValue;
	@ColumnMpping(name="CREATION_DATE", desc="CREATION_DATE", nullable=true, precision=50, type=Types.TIMESTAMP, valueReg="")
	private Timestamp creationDate;
	@ColumnMpping(name="NODE_ID", desc="", nullable=false, precision=100, type=Types.INTEGER, valueReg="")
	private Integer metadataNodeId;

	// Constructors

	/** default constructor */
	public DsSysMetadataNodeAttr() {
	}

	/** minimal constructor */
	public DsSysMetadataNodeAttr(Integer id) {
		this.id = id;
	}

	/** full constructor */
	public DsSysMetadataNodeAttr(Integer id,
			DsSysMetadataNode dsSysMetadataNode, String attrName,
			String attrValue, Timestamp creationDate) {
		this.id = id;
		this.dsSysMetadataNode = dsSysMetadataNode;
		this.attrName = attrName;
		this.attrValue = attrValue;
		this.creationDate = creationDate;
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

	@ManyToOne(fetch = FetchType.LAZY)
	@JoinColumn(name = "NODE_ID")
	public DsSysMetadataNode getDsSysMetadataNode() {
		return this.dsSysMetadataNode;
	}

	public void setDsSysMetadataNode(DsSysMetadataNode dsSysMetadataNode) {
		this.dsSysMetadataNode = dsSysMetadataNode;
	}

	@Column(name = "ATTR_NAME", length = 100)
	public String getAttrName() {
		return this.attrName;
	}

	public void setAttrName(String attrName) {
		this.attrName = attrName;
	}

	@Column(name = "ATTR_VALUE", length = 500)
	public String getAttrValue() {
		return this.attrValue;
	}

	public void setAttrValue(String attrValue) {
		this.attrValue = attrValue;
	}

	@Column(name = "CREATION_DATE", length = 26)
	public Timestamp getCreationDate() {
		return this.creationDate;
	}

	public void setCreationDate(Timestamp creationDate) {
		this.creationDate = creationDate;
	}
	@Column(name = "NODE_ID")
	public Integer getMetadataNodeId() {
		return metadataNodeId;
	}

	public void setMetadataNodeId(Integer metadataNodeId) {
		this.metadataNodeId = metadataNodeId;
	}

}