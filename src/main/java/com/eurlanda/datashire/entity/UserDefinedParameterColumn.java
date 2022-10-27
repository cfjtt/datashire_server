package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;

import java.io.Serializable;
import java.sql.Types;

/**
 * Created by Eurlanda on 2017/4/24.
 */
@MultitableMapping(name = {"DS_USERDEFINED_PARAMETERS_COLUMN"}, pk = "ID", desc = "")
public class UserDefinedParameterColumn implements Serializable {
    @ColumnMpping(name = "id", desc = "", nullable = false, precision = 0, type = Types.INTEGER, valueReg = ">=1")
    protected int id;
    @ColumnMpping(name = "squid_id", desc = "", nullable = true, precision = 0, type = Types.INTEGER, valueReg = "")
    private int squid_id;
    @ColumnMpping(name = "name", desc = "", nullable = true, precision = 300, type = Types.VARCHAR, valueReg = "")
    protected String name;
    @ColumnMpping(name = "value", desc = "", nullable = true, precision = Integer.MAX_VALUE, type = Types.VARCHAR, valueReg = "")
    private String value;
    @ColumnMpping(name = "description", desc = "", nullable = true, precision = 255, type = Types.VARCHAR, valueReg = "")
    private String description;

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

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

}
