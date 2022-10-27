package com.eurlanda.datashire.enumeration.datatype;

public enum HbaseDatatype {
	
	/* Hbase 5.6.13-log */
	INTEGER(1),
	FLOAT(2),
	UNSIGNED_INT(3),
	UNSIGNED_FLOAT(4),
	UNSIGNED_TIME(5),
	BIGINT(6),
	DOUBLE(7),
	UNSIGNED_DATE(8),
	UNSIGNED_LONG(9),
	UNSIGNED_DOUBLE(10),
	UNSIGNED_TIMESTAMP(11),
	TINYINT(12),
	DECIMAL(13),
	VARCHAR(14),
	UNSIGNED_TINYINT(15),
	BOOLEAN(16),
	CHAR(17),
	SMALLINT(18),
	TIME(19),
	BINARY(20),
	UNSIGNED_SMALLINT(21),
	DATE(22),
	VARBINARY(23),
	UNKNOWN(0);
	
	private int value;
	
	private HbaseDatatype(int v) {
		value = v;
	}
	
	public int value() {
		return value;
	}
	
    public static HbaseDatatype parse(int t) {
        for (HbaseDatatype result : values()) {
            if (result.value() == t) {
                return result;
            }
        }
        return null;
    }
    
    public static HbaseDatatype parse(String name) {
    	HbaseDatatype e =  null;
    	try {
    		e = Enum.valueOf(HbaseDatatype.class, name);
		} catch (Exception ex) {
		}
    	return e == null ? UNKNOWN : e;
    }
    
}
