package com.eurlanda.datashire.server.model;

import com.eurlanda.datashire.server.model.Base.DestColumnBaseModel;

/**
 * EsColumn实体类
 */
public class EsColumn extends DestColumnBaseModel {

    private Integer is_persist;
    private Integer is_mapping_id;

    public Integer getIs_mapping_id() {
        return is_mapping_id;
    }

    public void setIs_mapping_id(Integer is_mapping_id) {
        this.is_mapping_id = is_mapping_id;
    }

    public Integer getIs_persist() {
        return is_persist;
    }
    public void setIs_persist(Integer is_persist) {
        this.is_persist = is_persist;
    }
}