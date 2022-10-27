package com.eurlanda.datashire.server.model;

import java.util.List;

/**
 * 新版Transformation的实体类，为了不让引擎报错。请单独使用。
 * 不要在Squid的属性中引用该实体类。
 * 暂时标注为过期，提醒大家
 */
@Deprecated
public class Transformation {
    private Integer id;

    private Integer squid_id;

    private Integer transtype;

    private Integer location_x;

    private Integer location_y;

    private Integer column_id;

    private String description;

    private String name;

    private Integer output_data_type;

    private String constant_value;

    private Integer output_number;

    private Integer algorithm;

    private String tran_condition;

    private Integer difference_type;

    private Integer is_using_dictionary;

    private Integer dictionary_squid_id;

    private Integer bucket_count;

    private Integer model_squid_id;

    private Integer model_version;

    private Integer operator;

    private String date_format;

    private Integer inc_unit;

    private Integer split_type;

    private Integer encoding;

    private int squidModelType;

    private List<TransformationInputs> inputs;

    public List<TransformationInputs> getInputs() {
        return inputs;
    }

    public void setInputs(List<TransformationInputs> inputs) {
        this.inputs = inputs;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getSquid_id() {
        return squid_id;
    }

    public void setSquid_id(Integer squid_id) {
        this.squid_id = squid_id;
    }

    public Integer getTranstype() {
        return transtype;
    }

    public void setTranstype(Integer transtype) {
        this.transtype = transtype;
    }

    public Integer getLocation_x() {
        return location_x;
    }

    public void setLocation_x(Integer location_x) {
        this.location_x = location_x;
    }

    public Integer getLocation_y() {
        return location_y;
    }

    public void setLocation_y(Integer location_y) {
        this.location_y = location_y;
    }

    public Integer getColumn_id() {
        return column_id;
    }

    public void setColumn_id(Integer column_id) {
        this.column_id = column_id;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getOutput_data_type() {
        return output_data_type;
    }

    public void setOutput_data_type(Integer output_data_type) {
        this.output_data_type = output_data_type;
    }

    public String getConstant_value() {
        return constant_value;
    }

    public void setConstant_value(String constant_value) {
        this.constant_value = constant_value;
    }

    public Integer getOutput_number() {
        return output_number;
    }

    public void setOutput_number(Integer output_number) {
        this.output_number = output_number;
    }

    public Integer getAlgorithm() {
        return algorithm;
    }

    public void setAlgorithm(Integer algorithm) {
        this.algorithm = algorithm;
    }

    public String getTran_condition() {
        return tran_condition;
    }

    public void setTran_condition(String tran_condition) {
        this.tran_condition = tran_condition;
    }

    public Integer getDifference_type() {
        return difference_type;
    }

    public void setDifference_type(Integer difference_type) {
        this.difference_type = difference_type;
    }

    public Integer getIs_using_dictionary() {
        return is_using_dictionary;
    }

    public void setIs_using_dictionary(Integer is_using_dictionary) {
        this.is_using_dictionary = is_using_dictionary;
    }

    public Integer getDictionary_squid_id() {
        return dictionary_squid_id;
    }

    public void setDictionary_squid_id(Integer dictionary_squid_id) {
        this.dictionary_squid_id = dictionary_squid_id;
    }

    public Integer getBucket_count() {
        return bucket_count;
    }

    public void setBucket_count(Integer bucket_count) {
        this.bucket_count = bucket_count;
    }

    public Integer getModel_squid_id() {
        return model_squid_id;
    }

    public void setModel_squid_id(Integer model_squid_id) {
        this.model_squid_id = model_squid_id;
    }

    public Integer getModel_version() {
        return model_version;
    }

    public void setModel_version(Integer model_version) {
        this.model_version = model_version;
    }

    public Integer getOperator() {
        return operator;
    }

    public void setOperator(Integer operator) {
        this.operator = operator;
    }

    public String getDate_format() {
        return date_format;
    }

    public void setDate_format(String date_format) {
        this.date_format = date_format;
    }

    public Integer getInc_unit() {
        return inc_unit;
    }

    public void setInc_unit(Integer inc_unit) {
        this.inc_unit = inc_unit;
    }

    public Integer getSplit_type() {
        return split_type;
    }

    public void setSplit_type(Integer split_type) {
        this.split_type = split_type;
    }

    public Integer getEncoding() {
        return encoding;
    }

    public void setEncoding(Integer encoding) {
        this.encoding = encoding;
    }

    public int getSquidModelType() {
        return squidModelType;
    }

    public void setSquidModelType(int squidModelType) {
        this.squidModelType = squidModelType;
    }
}