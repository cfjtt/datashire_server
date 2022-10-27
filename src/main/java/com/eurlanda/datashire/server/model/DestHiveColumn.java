package com.eurlanda.datashire.server.model;

import com.eurlanda.datashire.server.model.Base.DestColumnBaseModel;

/**
 * Hive落地Column
 */
public class DestHiveColumn extends DestColumnBaseModel {

    private Integer data_type;
    private Integer length;
    private Integer precision;
    private Integer scale;
    private Integer is_primary_column;
    private Integer column_order;

    public Integer getData_type() {
        return data_type;
    }
    public void setData_type(Integer data_type) {
        this.data_type = data_type;
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

    public Integer getIs_primary_column() {
        return is_primary_column;
    }
    public void setIs_primary_column(Integer is_primary_column) {
        this.is_primary_column = is_primary_column;
    }

    public Integer getColumn_order() {
        return column_order;
    }
    public void setColumn_order(Integer column_order) {
        this.column_order = column_order;
    }
}