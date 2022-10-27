package com.eurlanda.datashire.enumeration.datatype;

public enum OracleDataType {
	
	/* Oracle11G */
	BINARY_DOUBLE(1),
	BINARY_FLOAT(2),
	BLOB(3),
	CHAR(4),
	CLOB(5),
	DATE(6),
	DECIMAL(7),
	INTEGER(8),
	LONG(9),
	LONG_RAW(10),
	NCHAR(11),
	NCLOB(12),
	NUMBER(13),
	NVARCHAR2(14),
	RAW(15),
	TIMESTAMP(16),
	VARCHAR2(17),
	
	UNKNOWN(0);
	
	private int value;
	
	private OracleDataType(int v) {
		value = v;
	}
	
	public int value() {
		return value;
	}
	
    public static OracleDataType parse(int t) {
        for (OracleDataType result : values()) {
            if (result.value() == t) {
                return result;
            }
        }
        return null;
    }
    
    public static OracleDataType parse(String name) {
    	OracleDataType e =  null;
    	try {
    		e = Enum.valueOf(OracleDataType.class, name);
		} catch (Exception ex) {
		}
    	return e == null ? UNKNOWN : e;
    }
    
}
