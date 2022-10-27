package com.eurlanda.datashire.entity.dest;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;
import com.eurlanda.datashire.entity.Column;

import java.sql.Types;

/**
 * Created by yhc on 4/26/2016.
 */
@MultitableMapping(pk="ID", name = "ds_dest_hdfs_column", desc = "HDFS 落地 Squid的Column")
public class DestHDFSColumn {
    @ColumnMpping(name="id", desc="id 主键", nullable=false, precision=0, type=Types.INTEGER, valueReg="")
    private int id;

    public int getId() {
        return id;
    }
    public void setId(int id) {
        this.id = id;
    }

    @ColumnMpping(name="column_id", desc="column 外键", nullable=false, precision=0, type=Types.INTEGER, valueReg="")
    private int column_id;

    public int getColumn_id() {
        return column_id;
    }
    public void setColumn_id(int column_id) {
        this.column_id = column_id;
    }

    @ColumnMpping(name="squid_id", desc="squid 外键", nullable=false, precision=0, type=Types.INTEGER, valueReg="")
    private int squid_id;

    public int getSquid_id() {
        return squid_id;
    }
    public void setSquid_id(int squid_id) {
        this.squid_id = squid_id;
    }

    @ColumnMpping(name="IS_DEST_COLUMN", desc="是否是落地的Column", nullable=false, precision=0, type=Types.INTEGER, valueReg="")
    private int is_dest_column;
    public int getIs_dest_column() {
        return is_dest_column;
    }
    public void setIs_dest_column(int is_dest_column) {
        this.is_dest_column = is_dest_column;
    }

    @ColumnMpping(name="FIELD_NAME", desc="修改名称", nullable=false, precision=50, type=Types.VARCHAR, valueReg="")
    private String field_name;
    public String getField_name() {
        return field_name;
    }
    public void setField_name(String field_name) {
        this.field_name = field_name;
    }

    @ColumnMpping(name="COLUMN_ORDER", desc="Column的顺序", nullable=false, precision=0, type=Types.INTEGER, valueReg="")
    private int column_order;
    public int getColumn_order() {
        return column_order;
    }
    public void setColumn_order(int column_order) {
        this.column_order = column_order;
    }

    @ColumnMpping(name="IS_PARTITION_COLUMN",desc="是否分区",nullable=false,precision=0, type=Types.INTEGER, valueReg="")
    private int is_partition_column;
    public int getIs_partition_column() {return is_partition_column;}
    public void setIs_partition_column(int is_partition_column) {this.is_partition_column = is_partition_column;}

    private Column column;
    public Column getColumn() {
        return column;
    }
    public void setColumn(Column column) {
        this.column = column;
    }
}
