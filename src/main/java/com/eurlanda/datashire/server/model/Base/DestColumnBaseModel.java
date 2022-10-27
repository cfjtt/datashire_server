package com.eurlanda.datashire.server.model.Base;

/**
 * Created by My PC on 7/5/2017.
 * 落地Column的父类
 */
public class DestColumnBaseModel {

    private Integer id;
    private Integer column_id;
    private Integer squid_id;
    private Integer is_dest_column;
    private String field_name;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getColumn_id() {
        return column_id;
    }

    public void setColumn_id(Integer column_id) {
        this.column_id = column_id;
    }

    public Integer getSquid_id() {
        return squid_id;
    }

    public void setSquid_id(Integer squid_id) {
        this.squid_id = squid_id;
    }

    public Integer getIs_dest_column() {
        return is_dest_column;
    }

    public void setIs_dest_column(Integer is_dest_column) {
        this.is_dest_column = is_dest_column;
    }

    public String getField_name() {
        return field_name;
    }

    public void setField_name(String field_name) {
        this.field_name = field_name;
    }



}
