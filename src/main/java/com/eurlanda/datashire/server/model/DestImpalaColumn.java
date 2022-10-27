package com.eurlanda.datashire.server.model;

import com.eurlanda.datashire.server.model.Base.DestColumnBaseModel;

/**
 * Created by My PC on 7/5/2017.
 * Impala落地Column
 * */
public class DestImpalaColumn extends DestColumnBaseModel {

    private Integer column_order;

    public Integer getColumn_order() {
        return column_order;
    }
    public void setColumn_order(Integer column_order) {
        this.column_order = column_order;
    }

}