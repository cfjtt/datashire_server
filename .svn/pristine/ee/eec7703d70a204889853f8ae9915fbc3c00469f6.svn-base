package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;

import java.io.Serializable;
import java.sql.Types;

/**
 * Created by Eurlanda on 2017/4/24.
 */
@MultitableMapping(name = {"DS_USERDEFINED_DATAMAP_COLUMN" }, pk = "ID", desc = "")
public class UserDefinedMappingColumn implements Serializable{

    @ColumnMpping(name="id", desc="", nullable=false, precision=0, type=Types.INTEGER, valueReg=">=1")
    private  int id;
    @ColumnMpping(name="squid_id", desc="", nullable=true, precision=0, type= Types.INTEGER, valueReg="")
    private int squid_id;
    @ColumnMpping(name="name", desc="", nullable=true, precision=300, type=Types.VARCHAR, valueReg="")
    private  String name;
    @ColumnMpping(name="type", desc="", nullable=true, precision=0, type= Types.INTEGER, valueReg="")
    private int type;
    @ColumnMpping(name="column_id", desc="", nullable=true, precision=0, type= Types.INTEGER, valueReg="")
    private int column_id;
    @ColumnMpping(name="description", desc="", nullable=true, precision=255, type= Types.VARCHAR, valueReg="")
    private String description;
    @ColumnMpping(name="precision", desc="", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
    private int precision;
    @ColumnMpping(name="scale", desc="", nullable=true, precision=1, type=Types.INTEGER, valueReg="")
    private int scale;


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

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getSquid_id() {
        return squid_id;
    }

    public void setSquid_id(int squid_id) {
        this.squid_id = squid_id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public int getColumn_id() {
        return column_id;
    }

    public void setColumn_id(int column_id) {
        this.column_id = column_id;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}
