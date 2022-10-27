package com.eurlanda.datashire.entity.dest;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;
import com.eurlanda.datashire.entity.Squid;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;

import java.sql.Types;

/**
 * Created by yhc on 5/10/2016.
 */
@MultitableMapping(pk="ID", name = {"DS_SQUID"}, desc = "impala 落地 SquidModelBase")
public class DestImpalaSquid extends Squid implements DestSquid {
    public DestImpalaSquid(){
        this.setSquid_type(SquidTypeEnum.DEST_IMPALA.value());
        this.setType(DSObjectType.DEST_IMPALA.value());
    }
    @ColumnMpping(name="HOST", desc="", nullable=true, precision=50, type= Types.VARCHAR, valueReg="")
    private String host;
    @ColumnMpping(name="STORE_NAME", desc="", nullable=true, precision=50, type= Types.VARCHAR, valueReg="")
    private String store_name;
    @ColumnMpping(name="IMPALA_TABLE_NAME", desc="", nullable=true, precision=50, type= Types.VARCHAR, valueReg="")
    private String impala_table_name;
    @ColumnMpping(name="AUTHENTICATION_TYPE", desc="", nullable=true, precision=0, type= Types.INTEGER, valueReg="")
    private int authentication_type;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getStore_name() {
        return store_name;
    }

    public void setStore_name(String store_name) {
        this.store_name = store_name;
    }

    public String getImpala_table_name() {
        return impala_table_name;
    }

    public void setImpala_table_name(String impala_table_name) {
        this.impala_table_name = impala_table_name;
    }

    public int getAuthentication_type() {
        return authentication_type;
    }

    public void setAuthentication_type(int authentication_type) {
        this.authentication_type = authentication_type;
    }
}
