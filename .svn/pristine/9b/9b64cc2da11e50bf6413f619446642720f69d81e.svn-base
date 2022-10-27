package com.eurlanda.datashire.entity.dest;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;
import com.eurlanda.datashire.entity.Squid;

import java.sql.Types;

/**
 * Created by zhudebin on 15-9-17.
 */
@MultitableMapping(pk="ID", name = {"DS_SQUID"}, desc = "ES 落地squid")
public class DestESSquid extends Squid implements DestSquid {

    @ColumnMpping(name="ip", desc="", nullable=true, precision=20, type= Types.VARCHAR, valueReg="")
    private String host;
    @ColumnMpping(name="esindex", desc="index", nullable=true, precision=200, type=Types.VARCHAR, valueReg="")
    private String esindex;
    @ColumnMpping(name="estype", desc="type", nullable=true, precision=200, type=Types.VARCHAR, valueReg="")
    private String estype;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getEsindex() {
        return esindex;
    }

    public void setEsindex(String esindex) {
        this.esindex = esindex;
    }

    public String getEstype() {
        return estype;
    }

    public void setEstype(String estype) {
        this.estype = estype;
    }
}
