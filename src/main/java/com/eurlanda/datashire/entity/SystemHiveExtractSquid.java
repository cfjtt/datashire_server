package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.MultitableMapping;
import com.eurlanda.datashire.enumeration.DSObjectType;

/**
 * Created by Eurlanda on 2017/3/22.
 */
@MultitableMapping(name = {"DS_SQUID"}, pk="ID", desc = "")
public class SystemHiveExtractSquid extends DataSquid {
    {
        this.setType(DSObjectType.HIVEEXTRACT.value());
    }
}
