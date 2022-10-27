package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;
import com.eurlanda.datashire.enumeration.DSObjectType;

import java.sql.Types;
import java.util.List;

/**
 * Created by zhudebin on 16/2/22.
 */
@MultitableMapping(pk="id", name = {"DS_SQUID"}, desc = "")
public class HBaseConnectionSquid extends Squid {
    {
        this.setType(DSObjectType.HBASE.value());
    }
    @ColumnMpping(name="url", desc="", nullable=true, precision=500, type= Types.VARCHAR, valueReg="")
    private String url;
    @ColumnMpping(name="db_type_id",desc = "",nullable = true,precision = 0,type= Types.INTEGER,valueReg = "")
    private int db_type;
    private List<SourceTable> sourceTableList;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public List<SourceTable> getSourceTableList() {
        return sourceTableList;
    }

    public void setSourceTableList(
            List<SourceTable> sourceTableList) {
        this.sourceTableList = sourceTableList;
    }

    public int getDb_type() {
        return db_type;
    }

    public void setDb_type(int db_type) {
        this.db_type = db_type;
    }
}
