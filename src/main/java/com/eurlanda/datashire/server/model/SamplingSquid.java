package com.eurlanda.datashire.server.model;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;
import com.eurlanda.datashire.entity.DataSquid;
import com.eurlanda.datashire.entity.Squid;
import com.eurlanda.datashire.enumeration.DSObjectType;

import java.sql.Types;

@MultitableMapping(pk="", name = {"DS_SQUID"}, desc = "")
public class SamplingSquid extends DataSquid {
    @ColumnMpping(name = "sampling_percent", desc = "抽样百分比", nullable = true, precision = 1, type = Types.DOUBLE, valueReg = "")
    private double samplingPercent;
    @ColumnMpping(name = "source_squidId", desc = "源SquidId", nullable = true, precision = 1, type = Types.INTEGER, valueReg = "")
    private int sourceSquidId;
    @ColumnMpping(name = "source_squidName", desc = "源Squid名称", nullable = true, precision = 1, type = Types.VARCHAR, valueReg = "")
    private String sourceSquidName;
    {
        super.setType(DSObjectType.SAMPLINGSQUID.value());
    }
    public int getSourceSquidId() {
        return sourceSquidId;
    }

    public void setSourceSquidId(int sourceSquidId) {
        this.sourceSquidId = sourceSquidId;
    }

    public String getSourceSquidName() {
        return sourceSquidName;
    }

    public void setSourceSquidName(String sourceSquidName) {
        this.sourceSquidName = sourceSquidName;
    }

    public double getSamplingPercent() {
        return samplingPercent;
    }

    public void setSamplingPercent(double samplingPercent) {
        this.samplingPercent = samplingPercent;
    }
}
