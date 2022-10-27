package com.eurlanda.datashire.entity.dest;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;
import com.eurlanda.datashire.entity.Column;

import java.sql.Types;

/**
 * Created by zhudebin on 15-9-17.
 */
@MultitableMapping(pk="ID", name = "DS_ES_COLUMN", desc = "")
public class EsColumn {
    @ColumnMpping(name="id", desc="id 主键", nullable=false, precision=0, type=Types.INTEGER, valueReg="")
    private int id;
    @ColumnMpping(name="column_id", desc="column 外键", nullable=false, precision=0, type=Types.INTEGER, valueReg="")
    private int column_id;
    @ColumnMpping(name="squid_id", desc="squid 外键", nullable=false, precision=0, type=Types.INTEGER, valueReg="")
    private int squid_id;
    @ColumnMpping(name="is_mapping_id", desc="是否映射为ID", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
    private int is_mapping_id;
    @ColumnMpping(name="field_name", desc="field name", nullable=true, precision=200, type=Types.VARCHAR, valueReg="")
    private String field_name;
    @ColumnMpping(name="is_persist", desc="是否落地，0不落地，1落地", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
    private int is_persist;

    private Column column;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getColumn_id() {
        return column_id;
    }

    public void setColumn_id(int column_id) {
        this.column_id = column_id;
    }

    public Column getColumn() {
        return column;
    }

    public void setColumn(Column column) {
        this.column = column;
    }

    public int getSquid_id() {
        return squid_id;
    }

    public void setSquid_id(int squid_id) {
        this.squid_id = squid_id;
    }

    public int getIs_mapping_id() {
        return is_mapping_id;
    }

    public String getField_name() {
        return field_name;
    }

    public void setField_name(String field_name) {
        this.field_name = field_name;
    }

    public int getIs_persist() {
        return is_persist;
    }

    public void setIs_persist(int is_persist) {
        this.is_persist = is_persist;
    }

    public void setIs_mapping_id(int is_mapping_id) {
        this.is_mapping_id = is_mapping_id;
    }
}
