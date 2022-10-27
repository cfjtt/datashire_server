package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;

import java.sql.Types;
import java.util.List;

/**
 * Created by zhudebin on 16/1/20.
 */
@MultitableMapping(pk="id", name = {"DS_SQUID"}, desc = "")
public class KafkaSquid extends Squid implements StreamSquid {

    @ColumnMpping(name="zkquorum", desc="", nullable=true, precision=200, type= Types.VARCHAR, valueReg="")
    private String zkQuorum;

    private List<SourceTable> sourceTableList;

    public String getZkQuorum() {
        return zkQuorum;
    }

    public void setZkQuorum(String zkQuorum) {
        this.zkQuorum = zkQuorum;
    }

    public List<SourceTable> getSourceTableList() {
        return sourceTableList;
    }

    public void setSourceTableList(
            List<SourceTable> sourceTableList) {
        this.sourceTableList = sourceTableList;
    }
}
