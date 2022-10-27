package com.eurlanda.datashire.server.model;

import com.eurlanda.datashire.server.model.Base.BaseObject;

/**
 * ReferenceColumn实体类
 */
public class ReferenceColumn  extends BaseObject {
    private Integer column_id;

    private Integer reference_squid_id;

    private Integer data_type;

    private Integer collation;

    private Boolean nullable;

    private Integer length;

    private Integer precision;

    private Integer scale;

    private String description;

    private Boolean isUnique;

    private Boolean isPK;

    private Integer cdc;

    private Integer is_Business_Key;

    private Integer host_squid_id;

    private Boolean is_referenced;

    private Integer group_id;

    private Integer relative_order;

    private Integer squid_id;

    private String group_name;
    private Integer aggregation_type;
    private Integer is_groupby;

    public String getGroup_name() {
        return group_name;
    }

    public void setGroup_name(String group_name) {
        this.group_name = group_name;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Boolean getUnique() {
        return isUnique;
    }

    public void setUnique(Boolean unique) {
        isUnique = unique;
    }


    public Integer getSquid_id() {
        return squid_id;
    }

    public void setSquid_id(Integer squid_id) {
        this.squid_id = squid_id;
    }

    public Integer getColumn_id() {
        return column_id;
    }

    public void setColumn_id(Integer column_id) {
        this.column_id = column_id;
    }

    public Integer getReference_squid_id() {
        return reference_squid_id;
    }

    public void setReference_squid_id(Integer reference_squid_id) {
        this.reference_squid_id = reference_squid_id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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

    public Boolean getNullable() {
        return nullable;
    }

    public void setNullable(Boolean nullable) {
        this.nullable = nullable;
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

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Boolean getIsUnique() {
        return isUnique;
    }

    public void setIsUnique(Boolean isUnique) {
        this.isUnique = isUnique;
    }

    public Boolean getIsPK() {
        return isPK;
    }

    public void setIsPK(Boolean isPK) {
        this.isPK = isPK;
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

    public Integer getHost_squid_id() {
        return host_squid_id;
    }

    public void setHost_squid_id(Integer host_squid_id) {
        this.host_squid_id = host_squid_id;
    }

    public Boolean getIs_referenced() {
        return is_referenced;
    }

    public void setIs_referenced(Boolean is_referenced) {
        this.is_referenced = is_referenced;
    }

    public Integer getGroup_id() {
        return group_id;
    }

    public void setGroup_id(Integer group_id) {
        this.group_id = group_id;
    }

    public Integer getRelative_order() {
        return relative_order;
    }

    public void setRelative_order(Integer relative_order) {
        this.relative_order = relative_order;
    }

    public Integer getAggregation_type() {
        return aggregation_type;
    }

    public void setAggregation_type(Integer aggregation_type) {
        this.aggregation_type = aggregation_type;
    }

    public Integer getIs_groupby() {
        return is_groupby;
    }

    public void setIs_groupby(Integer is_groupby) {
        this.is_groupby = is_groupby;
    }
}