package com.eurlanda.datashire.entity.dest;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;
import com.eurlanda.datashire.entity.Squid;
import com.eurlanda.datashire.enumeration.DSObjectType;

import java.sql.Types;
import java.util.List;

/**
 * Created by Eurlanda on 2017/5/15.
 */
@MultitableMapping(pk="ID", name = {"DS_SQUID"}, desc = "Cassandra 落地 SquidModelBase")
public class DestCassandraSquid extends Squid{
    {
        this.setType(DSObjectType.DEST_CASSANDRA.value());
    }
    @ColumnMpping(name="SAVE_TYPE", desc="保存方式append,overwrite,ignore,errorifexist", nullable=true, precision=0, type= Types.INTEGER, valueReg="")
    private int save_type;
    @ColumnMpping(name="dest_squid_id", desc="", nullable=true, precision=0, type= Types.INTEGER, valueReg="")
    private int dest_squid_id;

    private List<DestCassandraColumn> cassandraColumns;

    public int getSave_type() {
        return save_type;
    }

    public void setSave_type(int save_type) {
        this.save_type = save_type;
    }

    public int getDest_squid_id() {
        return dest_squid_id;
    }

    public void setDest_squid_id(int dest_squid_id) {
        this.dest_squid_id = dest_squid_id;
    }

    public List<DestCassandraColumn> getCassandraColumns() {
        return cassandraColumns;
    }

    public void setCassandraColumns(List<DestCassandraColumn> cassandraColumns) {
        this.cassandraColumns = cassandraColumns;
    }
}
