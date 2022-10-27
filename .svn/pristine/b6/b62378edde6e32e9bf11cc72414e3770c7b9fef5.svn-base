package com.eurlanda.datashire.enumeration.datatype;

public enum SystemDatatype {
    // datashire v1.0
    BIGINT(1),
    INT(2),
    TINYINT(3),
    BIT(4), //逻辑型，可表示true/false Y/N 0/1 M/F on/off
    DECIMAL(5),
    DOUBLE(6),
    NCHAR(8),
    NVARCHAR(9),
    CSN(10),
    BINARY(11), // 定长二进制
    VARBINARY(12), // 变长二进制
    DATETIME(13),
    SMALLINT(14),
    CSV(15),
    FLOAT(58),
    DATE(18),
    OBJECT(21),
    MAP(1022),
    ARRAY(86),

    UNKNOWN(0);

    private int value;

    private SystemDatatype(int v) {
        value = v;
    }

    public int value() {
        return value;
    }

    public static SystemDatatype parse(int t) {
        for (SystemDatatype result : values()) {
            if (result.value() == t) {
                return result;
            }
        }
        return UNKNOWN;
    }

    public static SystemDatatype parse(String name) {
        SystemDatatype sys = null;
        try {
            sys = Enum.valueOf(SystemDatatype.class, name.toUpperCase());
        } catch (Exception e) {
        }
        return sys == null ? UNKNOWN : sys;
    }

}
