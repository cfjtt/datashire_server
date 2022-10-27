package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;
import com.eurlanda.datashire.enumeration.DSObjectType;

import java.sql.Types;

/**
 * 
 * Column
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
@MultitableMapping(pk="", name = "DS_COLUMN", desc = "")
public class Column extends DSBaseModel {
	{
		super.setType(DSObjectType.COLUMN.value());
	}

	@ColumnMpping(name="data_type", desc="", nullable=false, precision=0, type=Types.INTEGER, valueReg="")
	private int data_type;
	@ColumnMpping(name="nullable", desc="", nullable=true, precision=1, type=Types.VARCHAR, valueReg="")
	private boolean nullable;
	@ColumnMpping(name="relative_order", desc="0123...", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private int relative_order;
	@ColumnMpping(name="squid_id", desc="", nullable=false, precision=0, type=Types.INTEGER, valueReg=">=1")
	private int squid_id;
	@ColumnMpping(name="length", desc="", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private int length;
	@ColumnMpping(name="precision", desc="", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private int precision;
	@ColumnMpping(name="cdc", desc="", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private int cdc;
	@ColumnMpping(name="is_groupby", desc="界面勾选时更新", nullable=true, precision=1, type=Types.VARCHAR, valueReg="")
	private boolean is_groupby; // 是否
	
	@ColumnMpping(name="collation", desc="", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private int collation;

	@ColumnMpping(name="aggregation_type", desc="聚合类型，AVG SUM MAX COUNT等", nullable=true, precision=50, type=Types.INTEGER, valueReg="")
	private int aggregation_type;
	
	// select count(*) from table group by name
	// select name,count(name) from table group by name
	
	// select dept_name,avg(salary) from table group by dept_name
	
	// select dept_name, sum(salary*13) from table group by dept_name
	
	//@ColumnMpping(name="aggregation_exp", desc="聚合函数/表达式", nullable=true, precision=50, type=Types.VARCHAR, valueReg="")
	//private String aggregation_exp;
	
	@ColumnMpping(name="description", desc="", nullable=true, precision=500, type=Types.VARCHAR, valueReg="")
	private String description;
	
	
	@ColumnMpping(name="isUnique", desc="", nullable=true, precision=1, type=Types.VARCHAR, valueReg="")
	private boolean isUnique;

	/** 是否主键 */
	@ColumnMpping(name="isPK", desc="", nullable=true, precision=1, type=Types.VARCHAR, valueReg="")
	private boolean isPK;
	
	//规模
	@ColumnMpping(name="scale", desc="", nullable=true, precision=1, type=Types.INTEGER, valueReg="")
	private int scale;
	
	@ColumnMpping(name="is_Business_Key", desc="", nullable=true, precision=1, type=Types.INTEGER, valueReg="")
	private int is_Business_Key;
	
	@ColumnMpping(name="sort_level", desc="", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private int sort_level;
	
	@ColumnMpping(name="sort_type", desc="", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private int sort_type;
	
	private boolean isAutoIncrement;
	private Object value;
    private boolean isFK;
	private int tag;  //GroupTagging的column，表示打标签
	//@Deprecated
	//private String group_name;
	
	private int columntype;
	private String columnAttribute;//列属性
	
	public Column(){
	}

	public Column(String name, int columntype, boolean isAutoIncrement) {
		super();
		super.setName(name);
		this.columntype = columntype;
		this.isAutoIncrement = isAutoIncrement;
	}
	public Column(String name, Object value) {
		super.setName(name);
		this.value = value;
	}
	public Column(String name, Object value, boolean isPK) {
		super.setName(name);
		this.value = value;
		this.isPK = isPK;
	}
	public Column(String name, int dataType, int length){
		super.setName(name);
		this.data_type = dataType;
		this.length = length;
	}

	public int getData_type() {
		return data_type;
	}

	public void setData_type(int dataType) {
		data_type = dataType;
	}

	public boolean isNullable() {
		return nullable;
	}

	public void setNullable(boolean nullable) {
		this.nullable = nullable;
	}

	public int getRelative_order() {
		return relative_order;
	}

	public void setRelative_order(int relativeOrder) {
		relative_order = relativeOrder;
	}

	public int getSquid_id() {
		return squid_id;
	}

	public void setSquid_id(int squidId) {
		squid_id = squidId;
	}

	public int getLength() {
		return length;
	}

	public void setLength(int length) {
		this.length = length;
	}

	public int getPrecision() {
		return precision;
	}

	public void setPrecision(int precision) {
		this.precision = precision;
	}

	public int getColumntype() {
		return columntype;
	}

	public void setColumntype(int columntype) {
		this.columntype = columntype;
	}

	public boolean isIsAutoIncrement() {
		return isAutoIncrement;
	}

	public void setIsAutoIncrement(boolean isAutoIncrement) {
		this.isAutoIncrement = isAutoIncrement;
	}

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

	public boolean isIsPK() {
		return isPK;
	}

	public void setIsPK(boolean isPK) {
		this.isPK = isPK;
	}

	public boolean isIs_groupby() {
		return is_groupby;
	}

	public void setIs_groupby(boolean is_groupby) {
		this.is_groupby = is_groupby;
	}

	public int getCollation() {
		return collation;
	}

	public void setCollation(int collation) {
		this.collation = collation;
	}

	public int getAggregation_type() {
		return aggregation_type;
	}

	public void setAggregation_type(int aggregation_type) {
		this.aggregation_type = aggregation_type;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public boolean isIsUnique() {
		return isUnique;
	}

	public void setIsUnique(boolean isUnique) {
		this.isUnique = isUnique;
	}
    
	public boolean isFK() {
		return isFK;
	}

	public void setFK(boolean isFK) {
		this.isFK = isFK;
	}

	public String getColumnAttribute() {
		return columnAttribute;
	}

	public void setColumnAttribute(String columnAttribute) {
		this.columnAttribute = columnAttribute;
	}

	public int getCdc() {
		return cdc;
	}

	public void setCdc(int cdc) {
		this.cdc = cdc;
	}
	
	public int getScale() {
		return scale;
	}

	public void setScale(int scale) {
		this.scale = scale;
	}

	public int getIs_Business_Key() {
		return is_Business_Key;
	}

	public void setIs_Business_Key(int is_Business_Key) {
		this.is_Business_Key = is_Business_Key;
	}

	public int getSort_Level() {
		return sort_level;
	}
	public int getSort_level() {
		return sort_level;
	}

	public void setSort_Level(int sort_level) {
		this.sort_level = sort_level;
	}

	public void setSort_level(int sort_level) {
		this.sort_level = sort_level;
	}

	public int getSort_type() {
		return sort_type;
	}

	public void setSort_type(int sort_type) {
		this.sort_type = sort_type;
	}

/*	@Override
	public String toString() {
		return "Column [name=" + name + ", id=" + id + ", key=" + key + ", creation_date=" + creation_date + "]";
	}*/
}