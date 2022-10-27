package com.eurlanda.datashire.entity.dest;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;
import com.eurlanda.datashire.entity.dest.DestHiveColumn;

import java.sql.Types;

/**
 * Created by Eurlanda on 2017/5/15.
 */
@MultitableMapping(pk="ID", name = "ds_dest_cassandra_column", desc = "hive 落地 Squid的Column")
public class DestCassandraColumn extends DestHiveColumn{
    @ColumnMpping(name="is_primary_column", desc="", nullable=false, precision=0, type= Types.INTEGER, valueReg="")
    private int is_primary_column;

    public int getIs_primary_column() {
        return is_primary_column;
    }

    public void setIs_primary_column(int is_primary_column) {
        this.is_primary_column = is_primary_column;
    }
}
