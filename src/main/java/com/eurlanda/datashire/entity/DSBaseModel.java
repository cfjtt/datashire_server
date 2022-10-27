package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.ColumnMpping;

import javax.xml.bind.annotation.XmlAttribute;
import java.io.Serializable;
import java.sql.Types;

/**
 * 
 * DSBaseModel
 * <p>
 * Title : 
 * </p>
 * <p>
 * Description: 
 * </p>
 * <p>
 * Author :赵春花 2013-9-5
 * </p>
 * <p>
 * update :赵春花 2013-9-5
 * </p>
 * <p>
 * Department :  JAVA后端研发部
 * </p>
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司
 * </p>
 */
public class DSBaseModel implements Serializable {

	@ColumnMpping(name="id", desc="", nullable=false, precision=0, type=Types.INTEGER, valueReg=">=1")
	protected int id;
	
	@ColumnMpping(name="key", desc="", nullable=true, precision=36, type=Types.VARCHAR, valueReg="")
	protected String key;
	
	//@ColumnMpping(name="creation_date", desc="创建时间", nullable=false, precision=0, type=Types.TIMESTAMP, defaultValue="sysdate")
	protected String creation_date;
	
	
	@ColumnMpping(name="name", desc="", nullable=true, precision=300, type=Types.VARCHAR, valueReg="")
	protected String name;
	
	/**
	 * 是否正在收发消息状态״̬
	 */
	private int status;
	/**
	 * 类型
	 */
	private int type;

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}
	@XmlAttribute(name="Name")
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public String getCreation_date() {
		return creation_date;
	}

	public void setCreation_date(String creation_date) {
		this.creation_date = creation_date;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + id;
		result = prime * result + ((key == null) ? 0 : key.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DSBaseModel other = (DSBaseModel) obj;
		if (id != other.id)
			return false;
		if (key == null) {
			if (other.key != null)
				return false;
		} else if (!key.equals(other.key))
			return false;
		return true;
	}

}