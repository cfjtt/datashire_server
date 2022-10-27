package com.eurlanda.datashire.enumeration;

/**
 * Created by YHC on 5/31/2016.
 * 抽取数据格式的枚举
 */

public enum ExtractRowFormatEnum {

    DELIMITER(0),
    FIXEDLENGTH(1),
    TEXT(2);

    private int value;

    private ExtractRowFormatEnum(int v) {
        value = v;
    }

    public int value() {
        return value;
    }

    public static ExtractRowFormatEnum parse(int t) {
        for (ExtractRowFormatEnum result : values()) {
            if (result.value() == t) {
                return result;
            }
        }
        return null;
    }

    public static ExtractRowFormatEnum parse(String name) {
        return (ExtractRowFormatEnum)Enum.valueOf(ExtractRowFormatEnum.class, name);
    }
}
