package com.eurlanda.datashire.entity.dest;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;
import com.eurlanda.datashire.entity.Squid;
import com.eurlanda.datashire.enumeration.DSObjectType;

import java.sql.Types;
import java.util.List;

/**
 * Created by Eurlanda on 2017/5/3.
 */
@MultitableMapping(pk="ID", name = {"DS_SQUID"}, desc = "hive 落地 SquidModelBase")
public class DestHiveSquid extends Squid{
    {
        this.setType(DSObjectType.DEST_HIVE.value());
    }
    @ColumnMpping(name="SAVE_TYPE", desc="保存方式append,overwrite,ignore,errorifexist", nullable=true, precision=0, type= Types.INTEGER, valueReg="")
    private int save_type;
    @ColumnMpping(name="DB_NAME", desc="数据库名字", nullable=true, precision=100, type= Types.VARCHAR, valueReg="")
    private String db_name;
    private List<DestHiveColumn> hiveColumns;
    public int getSave_type() {
        return save_type;
    }

    public void setSave_type(int save_type) {
        this.save_type = save_type;
    }

    public List<DestHiveColumn> getHiveColumns() {
        return hiveColumns;
    }

    public void setHiveColumns(List<DestHiveColumn> hiveColumns) {
        this.hiveColumns = hiveColumns;
    }

    public String getDb_name() {
        return db_name;
    }

    public void setDb_name(String db_name) {
        this.db_name = db_name;
    }
}
