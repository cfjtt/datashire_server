package com.eurlanda.datashire.entity.dest;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;
import com.eurlanda.datashire.entity.dest.DestHDFSColumn;

import java.sql.Types;

/**
 * Created by Eurlanda on 2017/5/3.
 */
@MultitableMapping(pk="ID", name = "ds_dest_hive_column", desc = "hive 落地 Squid的Column")
public class DestHiveColumn extends DestHDFSColumn{
    @ColumnMpping(name="data_type", desc="", nullable=false, precision=0, type= Types.INTEGER, valueReg="")
    private int data_type;
    @ColumnMpping(name="length", desc="", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
    private int length;
    @ColumnMpping(name="precision", desc="", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
    private int precision;
    @ColumnMpping(name="scale", desc="", nullable=true, precision=1, type=Types.INTEGER, valueReg="")
    private int scale;

    public int getData_type() {
        return data_type;
    }

    public void setData_type(int data_type) {
        this.data_type = data_type;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
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
