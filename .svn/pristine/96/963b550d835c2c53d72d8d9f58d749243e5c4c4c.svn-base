package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;

import java.sql.Types;

/**
 * Created by Eurlanda on 2017/5/5.
 */
@MultitableMapping(name = {"ds_statistics_parameters_column"}, pk = "ID", desc = "")
public class StatisticsParameterColumn extends UserDefinedParameterColumn {
    @ColumnMpping(name = "type", desc = "", nullable = true, precision = 0, type = Types.INTEGER, valueReg = "")
    private int type;
    @ColumnMpping(name = "precision", desc = "", nullable = true, precision = 0, type = Types.INTEGER, valueReg = "")
    private int precision;
    @ColumnMpping(name = "scale", desc = "", nullable = true, precision = 0, type = Types.INTEGER, valueReg = "")
    private int scale;

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public int getPrecision() {
        return precision;
    }

    public void setPrecision(int precision) {
        this.precision = precision;
    }

    public int getScale() {
        return scale;
    }

    public void setScale(int scale) {
        this.scale = scale;
    }
}
