package com.eurlanda.datashire.enumeration.datatype;

public enum MysqlDatatype {
	
	/* MySQL 5.6.13-log */
	BIGINT(1),
	BLOB(2),
	CHAR(3),
	DATETIME(4),
	DECIMAL(5),
	DOUBLE(6),
	ENUM(7),
	FLOAT(8),
	INT(9),
	LONGBLOB(10),
	LONGTEXT(11),
	MEDIUMINT(12),
	MEDIUMTEXT(13),
	SET(14),
	SMALLINT(15),
	TEXT(16),
	TIME(17),
	TIMESTAMP(18),
	TINYINT(19),
	VARCHAR(20),
	YEAR(21),
	
	UNKNOWN(0);
	
	private int value;
	
	private MysqlDatatype(int v) {
		value = v;
	}
	
	public int value() {
		return value;
	}
	
    public static MysqlDatatype parse(int t) {
        for (MysqlDatatype result : values()) {
            if (result.value() == t) {
                return result;
            }
        }
        return null;
    }
    
    public static MysqlDatatype parse(String name) {
    	MysqlDatatype e =  null;
    	try {
			e = Enum.valueOf(MysqlDatatype.class, name);
		} catch (Exception ex) {
		}
    	return e == null ? UNKNOWN : e;
    }
    
}
