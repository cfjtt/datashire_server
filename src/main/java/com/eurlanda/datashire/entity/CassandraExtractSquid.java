package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.MultitableMapping;
import com.eurlanda.datashire.enumeration.DSObjectType;

/**
 * Created by Eurlanda on 2017/4/10.
 */
@MultitableMapping(name = {"DS_SQUID"}, pk="ID", desc = "")
public class CassandraExtractSquid extends DataSquid{
    {
        this.setType(DSObjectType.CASSANDRA_EXTRACT.value());
    }
}
