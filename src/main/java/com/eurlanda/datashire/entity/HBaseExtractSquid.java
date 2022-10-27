package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;
import com.eurlanda.datashire.enumeration.DSObjectType;

import java.sql.Types;

/**
 * kafka 抽取squid
 *
 * Created by zhudebin on 16/1/20.
 */
@MultitableMapping(name = {"DS_SQUID"}, pk="ID", desc = "")
public class HBaseExtractSquid extends DataSquid {

    {
        this.setType(DSObjectType.HBASEEXTRACT.value());
    }
    @ColumnMpping(name = "filter_type", desc = "0:表达式,1:scan", nullable = true, precision = 0, type = Types.INTEGER, valueReg = "")
    private int filterType;

    @ColumnMpping(name = "scan", desc = "scan 序列化后的字符", nullable = true, precision = 0, type = Types.CLOB, valueReg = "")
    private String scan;
    @ColumnMpping(name = "code", desc = "生成scan 的代码", nullable = true, precision = 0, type = Types.CLOB, valueReg = "")
    private String code;

    public int getFilterType() {
        return filterType;
    }

    public void setFilterType(int filterType) {
        this.filterType = filterType;
    }

    public String getScan() {
        return scan;
    }

    public void setScan(String scan) {
        this.scan = scan;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }
}
