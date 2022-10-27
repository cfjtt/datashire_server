package com.eurlanda.datashire.server.model;

import com.eurlanda.datashire.server.model.Base.BaseObject;

import java.io.Serializable;

/**
 * 新版Column的实体类，为了不让引擎报错。请单独使用。
 * 不要在Squid的属性中引用该实体类。
 * 暂时标注为过期，提醒大家
 */
@Deprecated
public class Column extends BaseObject implements Serializable{

    private Integer relative_order;

    private Integer squid_id;

    private Integer data_type;

    private Integer collation;

    private boolean nullable;

    private Integer length;

    private Integer precision;

    private Integer scale;

    private Integer is_groupby;

    private Integer aggregation_type;

    private String description;

    private boolean isUnique;

    private boolean isPK;

    private Integer cdc;

    private Integer is_Business_Key;

    private Integer sort_level;

    private Integer sort_type;

    public Integer getRelative_order() {
        return relative_order;
    }

    public void setRelative_order(Integer relative_order) {
        this.relative_order = relative_order;
    }

    public Integer getSquid_id() {
        return squid_id;
    }

    public void setSquid_id(Integer squid_id) {
        this.squid_id = squid_id;
    }

    public Integer getData_type() {
        return data_type;
    }

    public void setData_type(Integer data_type) {
        this.data_type = data_type;
    }

    public Integer getCollation() {
        return collation;
    }

    public void setCollation(Integer collation) {
        this.collation = collation;
    }

    public Integer getLength() {
        return length;
    }

    public void setLength(Integer length) {
        this.length = length;
    }

    public Integer getPrecision() {
        return precision;
    }

    public void setPrecision(Integer precision) {
        this.precision = precision;
    }

    public Integer getScale() {
        return scale;
    }

    public void setScale(Integer scale) {
        this.scale = scale;
    }

    public Integer getIs_groupby() {
        return is_groupby;
    }

    public void setIs_groupby(Integer is_groupby) {
        this.is_groupby = is_groupby;
    }

    public Integer getAggregation_type() {
        return aggregation_type;
    }

    public void setAggregation_type(Integer aggregation_type) {
        this.aggregation_type = aggregation_type;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Integer getCdc() {
        return cdc;
    }

    public void setCdc(Integer cdc) {
        this.cdc = cdc;
    }

    public Integer getIs_Business_Key() {
        return is_Business_Key;
    }

    public void setIs_Business_Key(Integer is_Business_Key) {
        this.is_Business_Key = is_Business_Key;
    }

    public Integer getSort_level() {
        return sort_level;
    }

    public void setSort_level(Integer sort_level) {
        this.sort_level = sort_level;
    }

    public Integer getSort_type() {
        return sort_type;
    }

    public void setSort_type(Integer sort_type) {
        this.sort_type = sort_type;
    }

    public boolean isNullable() {
        return nullable;
    }

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    public boolean isUnique() {
        return isUnique;
    }

    public void setUnique(boolean unique) {
        isUnique = unique;
    }

    public boolean isPK() {
        return isPK;
    }

    public void setPK(boolean PK) {
        isPK = PK;
    }
}