package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;

import java.sql.Types;

/**
 * DS_COLUMN_CACHE缓存表
 * @author lei.bin
 *
 */
@MultitableMapping(pk="", name = "DS_SOURCE_COLUMN", desc = "缓存列")
public class SourceColumn extends DSBaseModel {
	@ColumnMpping(name = "source_table_id", desc = "", nullable = false, precision = 0, type = Types.INTEGER, valueReg = ">=1")
	private int source_table_id;
	@ColumnMpping(name = "data_type", desc = "", nullable = true, precision = 0, type = Types.INTEGER, valueReg = "")
	private int data_type;
	@ColumnMpping(name = "nullable", desc = "", nullable = true, precision = 1, type = Types.VARCHAR, valueReg = "")
	private boolean nullable;
	@ColumnMpping(name = "length", desc = "", nullable = true, precision = 0, type = Types.INTEGER, valueReg = "")
	private int length;
	@ColumnMpping(name = "precision", desc = "", nullable = true, precision = 0, type = Types.INTEGER, valueReg = "")
	private int precision;
	//小数位
	@ColumnMpping(name="scale", desc="", nullable=true, precision=1, type=Types.INTEGER, valueReg="")
	private int scale;
	@ColumnMpping(name="relative_order", desc="0123...", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private int relative_order;
	@ColumnMpping(name="isunique", desc="是否唯一约束", nullable=true, precision=1, type=Types.VARCHAR, valueReg="")
	private boolean isunique;
	@ColumnMpping(name="ispk", desc="是否主键", nullable=true, precision=1, type=Types.VARCHAR, valueReg="")
	private boolean ispk;
	//第一个版本先不做这个功能，这个字段为数据库关键字，用到会直接报错
/*	@ColumnMpping(name="collation", desc="字符编码", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private int collation;*/
	public SourceColumn(){
	}
	
	public SourceColumn(int id, int source_table_id, String name,
			int data_type, boolean nullable, int length, int precision, int scale,boolean isunique,boolean ispk) {
		this.id = id;
		this.source_table_id = source_table_id;
		this.setName(name);
		this.data_type = data_type;
		this.nullable = nullable;
		this.length = length;
		this.precision = precision;
		this.scale  = scale;
		this.isunique=isunique;
		this.ispk=ispk;
	}

	public int getSource_table_id() {
		return source_table_id;
	}

	public void setSource_table_id(int source_table_id) {
		this.source_table_id = source_table_id;
	}

	public int getData_type() {
		return data_type;
	}

	public void setData_type(int data_type) {
		this.data_type = data_type;
	}

	public boolean isNullable() {
		return nullable;
	}

	public void setNullable(boolean nullable) {
		this.nullable = nullable;
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
	
    public int getScale() {
		return scale;
	}
	public void setScale(int scale) {
		this.scale = scale;
	}
	
	public int getRelative_order() {
        return relative_order;
    }
    public void setRelative_order(int relative_order) {
        this.relative_order = relative_order;
    }

	public boolean isIsunique() {
		return isunique;
	}

	public void setIsunique(boolean isunique) {
		this.isunique = isunique;
	}

	public boolean isIspk() {
		return ispk;
	}

	public void setIspk(boolean ispk) {
		this.ispk = ispk;
	}


	@Override
	public String toString() {
		return "SourceColumn [source_table_id=" + source_table_id
				+ ", data_type=" + data_type + ", nullable=" + nullable
				+ ", length=" + length + ", precision=" + precision
				+ ", scale=" + scale + ", relative_order=" + relative_order
				+ "]";
	}
}
