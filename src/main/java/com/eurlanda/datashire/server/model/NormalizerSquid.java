package com.eurlanda.datashire.server.model;

import com.eurlanda.datashire.entity.DataSquid;
import com.eurlanda.datashire.enumeration.DSObjectType;

/**
 * 标准化squid
 * Created by eurlanda - new 2 on 2017/6/27.
 */
public class NormalizerSquid extends DataSquid {
    {
        this.setType(DSObjectType.NORMALIZER.value());
    }
    private int method;
    private double min_value;
    private double max_value;
    private int versioning;

    public int getVersioning() {
        return versioning;
    }

    public void setVersioning(int versioning) {
        this.versioning = versioning;
    }

    public String getName() {
        return name;
    }

    public int getMethod() {
        return method;
    }

    public void setMethod(int method) {
        this.method = method;
    }

    public double getMin_value() {
        return min_value;
    }

    public void setMin_value(double min_value) {
        this.min_value = min_value;
    }

    public double getMax_value() {
        return max_value;
    }

    public void setMax_value(double max_value) {
        this.max_value = max_value;
    }
}
