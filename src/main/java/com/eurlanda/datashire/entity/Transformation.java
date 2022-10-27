package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.sql.Types;
import java.util.List;


/**
 * Transformation实体类
 * 
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
@XmlRootElement(name="Transformation")
@MultitableMapping(pk="", name = "DS_TRANSFORMATION", desc = "")
public class Transformation extends DSBaseModel {

	@ColumnMpping(name="column_id", desc="", nullable=false, precision=0, type=Types.INTEGER, valueReg="")
	private int column_id;

	@ColumnMpping(name="location_x", desc="", nullable=false, precision=0, type=Types.INTEGER, valueReg="")
	private int location_x;
	 
	@ColumnMpping(name="location_y", desc="", nullable=false, precision=0, type=Types.INTEGER, valueReg="")
	private int location_y;
	
	@ColumnMpping(name="squid_id", desc="", nullable=false, precision=0, type=Types.INTEGER, valueReg=">=1")
	private int squid_id;
	
	@ColumnMpping(name="transformation_type_id", desc="", nullable=false, precision=0, type=Types.INTEGER, valueReg="")
	private int transtype;
	
	@ColumnMpping(name="description", desc="", nullable=true, precision=500, type=Types.VARCHAR, valueReg="")
	private String description;
	
	@ColumnMpping(name="output_data_type", desc="", nullable=true, precision=50, type=Types.INTEGER, valueReg="")
	private int output_data_type;
	
	@ColumnMpping(name="constant_value", desc="", nullable=true, precision=4000, type=Types.VARCHAR, valueReg="")
	private String constant_value;

	@ColumnMpping(name="output_number", desc="", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private int output_number;

	@ColumnMpping(name="algorithm", desc="", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private int algorithm;
	
	@ColumnMpping(name="difference_type", desc="", nullable=true, precision=50, type=Types.INTEGER, valueReg="")
	private int difference_type;
	
	@ColumnMpping(name="tran_condition", desc="", nullable=true, precision=500, type=Types.VARCHAR, valueReg="")
	private String tran_condition;

	@ColumnMpping(name="modulus", desc="", nullable=true, precision=100, type=Types.VARCHAR, valueReg="")
	private String modulus;

	@ColumnMpping(name="is_using_dictionary", desc="", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private int is_using_dictionary;
	
	@ColumnMpping(name="dictionary_squid_id", desc="", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private int dictionary_squid_id;
	
	@ColumnMpping(name="bucket_count", desc="", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private int bucket_count;
	
	@ColumnMpping(name="model_squid_id", desc="", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private int model_squid_id;
	
	@ColumnMpping(name="model_version", desc="", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private int model_version;
	
	@ColumnMpping(name="operator", desc="", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private int operator;
	
	@ColumnMpping(name="date_format", desc="", nullable=true, precision=100, type=Types.VARCHAR, valueReg="")
	private String date_format;
	
	@ColumnMpping(name="inc_unit", desc="", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private int inc_unit;
	
	@ColumnMpping(name="split_type", desc="", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private int split_type;

	@ColumnMpping(name="encoding", desc="编码", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private int encoding;

	private List<TransformationInputs> inputs;

	private int inputDataType;
	private int squidModelType;

	@Override
	public String toString() {
		return "Transformation [id=" + id + ", key=" + key + ", name=" + name + ", squid_id=" + squid_id + ", transtype=" + transtype + ", tran_condition=" + tran_condition + "]";
	}

	public Transformation(){
	}

	public int getColumn_id() {
		return column_id;
	}

	public void setColumn_id(int columnId) {
		column_id = columnId;
	}

	public int getLocation_x() {
		return location_x;
	}

	public void setLocation_x(int locationX) {
		location_x = locationX;
	}

	public int getLocation_y() {
		return location_y;
	}

	public void setLocation_y(int locationY) {
		location_y = locationY;
	}

	public int getSquid_id() {
		return squid_id;
	}

	public void setSquid_id(int squidId) {
		squid_id = squidId;
	}

	public int getTranstype() {
		return transtype;
	}

	public void setTranstype(int transtype) {
		this.transtype = transtype;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public int getOutput_data_type() {
		return output_data_type;
	}

	public void setOutput_data_type(int outputDataType) {
		output_data_type = outputDataType;
	}

	public String getConstant_value() {
		return constant_value;
	}

	public void setConstant_value(String constantValue) {
		constant_value = constantValue;
	}

	public int getOutput_number() {
		return output_number;
	}

	public void setOutput_number(int outputNumber) {
		output_number = outputNumber;
	}

	public int getAlgorithm() {
		return algorithm;
	}

	public void setAlgorithm(int algorithm) {
		this.algorithm = algorithm;
	}

	public int getDifference_type() {
		return difference_type;
	}

	public void setDifference_type(int differenceType) {
		difference_type = differenceType;
	}

	public String getTran_condition() {
		return tran_condition;
	}

	public void setTran_condition(String tran_condition) {
		this.tran_condition = tran_condition;
	}
	

	public int getIs_using_dictionary() {
		return is_using_dictionary;
	}

	public void setIs_using_dictionary(int is_using_dictionary) {
		this.is_using_dictionary = is_using_dictionary;
	}

	public int getDictionary_squid_id() {
		return dictionary_squid_id;
	}

	public void setDictionary_squid_id(int dictionary_squid_id) {
		this.dictionary_squid_id = dictionary_squid_id;
	}

	public int getBucket_count() {
		return bucket_count;
	}

	public void setBucket_count(int bucket_count) {
		this.bucket_count = bucket_count;
	}

	public int getModel_squid_id() {
		return model_squid_id;
	}

	public void setModel_squid_id(int model_squid_id) {
		this.model_squid_id = model_squid_id;
	}

	public int getModel_version() {
		return model_version;
	}

	public void setModel_version(int model_version) {
		this.model_version = model_version;
	}

	public int getOperator() {
		return operator;
	}

	public void setOperator(int operator) {
		this.operator = operator;
	}

	@XmlElement(name="TransformationInputs")
	public List<TransformationInputs> getInputs() {
		return inputs;
	}

	public void setInputs(List<TransformationInputs> inputs) {
		this.inputs = inputs;
	}

	public int getInputDataType() {
		return inputDataType;
	}

	public void setInputDataType(int inputDataType) {
		this.inputDataType = inputDataType;
	}

	public int getSquidModelType() {
		return squidModelType;
	}

	public void setSquidModelType(int squidModelType) {
		this.squidModelType = squidModelType;
	}

	public String getDate_format() {
		return date_format;
	}

	public void setDate_format(String date_format) {
		this.date_format = date_format;
	}

	public String getModulus() {
		return modulus;
	}

	public void setModulus(String modulus) {
		this.modulus = modulus;
	}

	public int getInc_unit() {
		return inc_unit;
	}

	public void setInc_unit(int inc_unit) {
		this.inc_unit = inc_unit;
	}

	public int getSplit_type() {
		return split_type;
	}

	public void setSplit_type(int split_type) {
		this.split_type = split_type;
	}

	public int getEncoding() {
		return encoding;
	}

	public void setEncoding(int encoding) {
		this.encoding = encoding;
	}
}