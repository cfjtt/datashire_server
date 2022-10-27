package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;
import com.eurlanda.datashire.enumeration.DSObjectType;

import java.sql.Types;

/**
 * kafka 抽取squid
 *
 * Created by zhudebin on 16/1/20.
 */
@MultitableMapping(name = {"DS_SQUID"}, pk="ID", desc = "")
public class KafkaExtractSquid extends DataSquid implements StreamSquid {

    {
        this.setType(DSObjectType.KAFKAEXTRACT.value());
    }
    @ColumnMpping(name = "numPartitions", desc = "", nullable = true, precision = 0, type = Types.INTEGER, valueReg = "")
    private int numPartitions;
    @ColumnMpping(name = "group_id", desc = "group id", nullable = true, precision = 100, type = Types.VARCHAR, valueReg = "")
    private String group_id;

    public int getNumPartitions() {
        return numPartitions;
    }

    public void setNumPartitions(int numPartitions) {
        this.numPartitions = numPartitions;
    }

    public String getGroup_id() {
        return group_id;
    }

    public void setGroup_id(String group_id) {
        this.group_id = group_id;
    }
}
