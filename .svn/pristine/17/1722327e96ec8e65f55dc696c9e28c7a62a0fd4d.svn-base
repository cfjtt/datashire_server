package com.eurlanda.datashire.entity.operation;

import com.eurlanda.datashire.adapter.datatype.DataType;
import com.eurlanda.datashire.entity.Column;
import com.eurlanda.datashire.enumeration.datatype.DbBaseDatatype;
import com.eurlanda.datashire.enumeration.datatype.SystemDatatype;

/**
 * 
 * <p>
 * Title :列的基本信息
 * </p>
 * <p>
 * Description: 用于处理表结构里，每个列的基本信息元素
 * </p>
 * <p>
 * Author :何科敏 Aug 20, 2013
 * </p>
 * <p>
 * update :何科敏 Aug 20, 2013
 * </p>
 * <p>
 * Department : JAVA后端研发部
 * </p>
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司 </p>
 * 
 * @see Column
 */

public class ColumnInfo {
	private int id;
	/**
	 * 名称
	 */
	private String name;
	/**
	 * 长度
	 */
	private Integer length;

	/**
	 * 精度
	 */
	private Integer precision;
	
	private Integer scale;

	/**
	 * 是否主键
	 */
	private boolean isPrimary = false;
	/**
	 * 是否唯一约束
	 */
	private boolean isUniqueness = false;
	/**
	 * 是否外键
	 */
	private boolean isForeign = false;

	/**
	 * 占位字节
	 */
	// private Long pbytes;
	/**
	 * 是否为空
	 */
	private boolean isNullable = true;
	/**
	 * 是否标识
	 */
	private boolean isIdentity = false;

	/**
	 * 默认值
	 */
	private String defaultValue;
	/**
	 * 排序位置。
	 */
	private Integer orderNumber;
	
	/**
	 * 落地时的类型
	 */
	private SystemDatatype systemDatatype;
	
	/**
	 * SourceColumn的类型
	 */
	private DbBaseDatatype dbBaseDatatype;
	/**
	 * 校对
	 */
	private String collation;
	private String typeName;
	private String comment;
	private String tableName;
	private int partition = 0;

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getTypeName() {
		return typeName;
	}

	public void setTypeName(String typeName) {
		this.typeName = typeName;
	}

	public String getComment() {
		return comment;
	}

	public void setComment(String comment) {
		this.comment = comment;
	}

	public int getPartition() {
		return partition;
	}

	public void setPartition(int partition) {
		this.partition = partition;
	}

	public ColumnInfo() {
	}

	public ColumnInfo(String name,Integer length, Integer precision,Integer scale, boolean isPrimary, boolean isUniqueness, boolean isForeign,
			boolean isNullable, boolean isIdentity, String defaultValue, String collation, String typeName) {
		super();
		this.name = name;
		this.length = length;
		this.precision = precision;
		this.isPrimary = isPrimary;
		this.scale = scale;
		this.isUniqueness = isUniqueness;
		this.isForeign = isForeign;
		this.isNullable = isNullable;
		this.isIdentity = isIdentity;
		this.defaultValue = defaultValue;
		this.collation = collation;
		this.typeName = typeName;
	}
	
	public ColumnInfo(String name, String typeName) {
		super();
		this.name = name;
		this.typeName = typeName;
	}
	/**
	 * 
	 * @param name 列名
	 * @param typeName 类型名
	 * @param typeLength 类型长度
	 */
	public ColumnInfo(String name, SystemDatatype systemDatatype, int typeLength) {
		super();
		this.name = name;
		this.length=typeLength;
		this.systemDatatype=systemDatatype;
	}
	/**
	 * 
	 * @param name 列名
	 * @param typeName 类型名
	 * @param typeLength 类型长度
	 */
	public ColumnInfo(String name,SystemDatatype systemDatatype) {
		super();
		this.name = name;
		this.systemDatatype=systemDatatype;
	}
	/**
	 * @return name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @param name
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * @return length
	 */
	public Integer getLength() {
		return length;
	}

	/**
	 * @param length
	 */
	public void setLength(Integer length) {
		this.length = length;
	}

	/**
	 * @return precision
	 */
	public Integer getPrecision() {
		return precision;
	}

	/**
	 * @param precision
	 */
	public void setPrecision(Integer precision) {
		this.precision = precision;
	}

	public Integer getScale() {
		return scale;
	}

	public void setScale(Integer scale) {
		this.scale = scale;
	}

	/**
	 * @return isPrimary
	 */
	public boolean isPrimary() {
		return isPrimary;
	}

	/**
	 * @param isPrimary
	 */
	public void setPrimary(boolean isPrimary) {
		this.isPrimary = isPrimary;
	}

	/**
	 * @return isUniqueness
	 */
	public boolean isUniqueness() {
		return isUniqueness;
	}

	/**
	 * @param isUniqueness
	 */
	public void setUniqueness(boolean isUniqueness) {
		this.isUniqueness = isUniqueness;
	}

	/**
	 * @return isForeign
	 */
	public boolean isForeign() {
		return isForeign;
	}

	/**
	 * @param isForeign
	 */
	public void setForeign(boolean isForeign) {
		this.isForeign = isForeign;
	}

	/**
	 * @return isNullable
	 */
	public boolean isNullable() {
		return isNullable;
	}

	/**
	 * @param isNullable
	 */
	public void setNullable(boolean isNullable) {
		this.isNullable = isNullable;
	}

	/**
	 * @return isIdentity
	 */
	public boolean isIdentity() {
		return isIdentity;
	}

	/**
	 * @param isIdentity
	 */
	public void setIdentity(boolean isIdentity) {
		this.isIdentity = isIdentity;
	}

	/**
	 * @return defaultValue
	 */
	public String getDefaultValue() {
		return defaultValue;
	}

	/**
	 * @param defaultValue
	 */
	public void setDefaultValue(String defaultValue) {
		this.defaultValue = defaultValue;
	}

	/**
	 * @return collation
	 */
	public String getCollation() {
		return collation;
	}

	/**
	 * @param collation
	 */
	public void setCollation(String collation) {
		this.collation = collation;
	}

	@Override
	public String toString() {
		return "ColumnInfo [name=" + name + ", length=" + length
				+ ", precision=" + precision + ", isPrimary=" + isPrimary
				+ ", isUniqueness=" + isUniqueness + ", isForeign=" + isForeign
				+ ", isNullable=" + isNullable + ", isIdentity=" + isIdentity
				+ ", defaultValue=" + defaultValue + ", orderNumber="
				+ orderNumber + ", systemDatatype=" + systemDatatype
				+ ", collation=" + collation + ", typeName=" + typeName
				+ ", comment=" + comment + ", tableName=" + tableName + ", scale=" + scale + "]";
	}

	public Integer getOrderNumber() {
		return orderNumber;
	}

	public void setOrderNumber(Integer orderNumber) {
		this.orderNumber = orderNumber;
	}
	
	public SystemDatatype getSystemDatatype() {
		return systemDatatype;
	}

	public void setSystemDatatype(SystemDatatype systemDatatype) {
		this.systemDatatype = systemDatatype;
	}
	
	public DbBaseDatatype getDbBaseDatatype() {
		return dbBaseDatatype;
	}

	public void setDbBaseDatatype(DbBaseDatatype dbBaseDatatype) {
		this.dbBaseDatatype = dbBaseDatatype;
	}

	public static ColumnInfo fromDataType(DataType type){
		// TODO: ssx
		return new ColumnInfo();
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}
}
