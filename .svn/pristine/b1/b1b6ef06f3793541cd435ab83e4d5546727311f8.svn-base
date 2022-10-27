package com.eurlanda.datashire.enumeration.datatype;

import com.eurlanda.datashire.utility.EnumException;

import java.util.HashMap;
import java.util.Map;

public enum SqlServerDataType {
	UNKNOWN(0),
	
	// sqlserver2012
	BIGINT(1),
	BINARY(2),
	BIT(3),
	CHAR(4),
	DATE(5),
	DATETIME(6),
	DATETIME2(7),
	DATETIMEOFFSET(8),
	DECIMAL(9),
	FLOAT(10),
	GEOGRAPHY(11),
	GEOMETRY(12),
	HIERARCHYID(13),
	IMAGE(94),
	INT(15),
	MONEY(16),
	NCHAR(17),
	NTEXT(18),
	NUMERIC(19),
	NVARCHAR(20),
	REAL(21),
	SMALLDATETIME(95),
	SMALLINT(23),
	SMALLMONEY(24),
	SQL_VARIANT(25),
	SYSNAME(26),
	TEXT(27),
	TIME(28),
	TIMESTAMP(29),
	TINYINT(30),
	UNIQUEIDENTIFIER(31),
	VARBINARY(32),
	VARCHAR(33),
	XML(34);
	
	private int _value;
	
	private static Map<Integer, SqlServerDataType> map;

	/**
	 * 构造方法
	 * 
	 * @param value
	 */
	private SqlServerDataType(int value) {
		_value = value;
	}

	/**
	 * 得到枚举值
	 * 
	 * @return
	 */
	public int value() {
		return _value;
	}
	
	/**
	 * 从int到enum的转换函数
	 * 
	 * @param value
	 * @return
	 * @throws Exception
	 */
	public static SqlServerDataType valueOf(int value) throws EnumException {
		SqlServerDataType type = null;
		if (map == null) {
			map = new HashMap<Integer, SqlServerDataType>();
			SqlServerDataType[] types = SqlServerDataType.values();
			for (SqlServerDataType tmp : types) {
				map.put(tmp.value(), tmp);
			}
		}
		type = map.get(value);
		if (type == null) {
			throw new EnumException();
		}
		return type;
	}
	
	
    public static SqlServerDataType parse(int t) {
        for (SqlServerDataType result : values()) {
            if (result.value() == t) {
                return result;
            }
        }
        return null;
    }
    
    public static SqlServerDataType parse(String name) {
    	SqlServerDataType e = null;
    	try {
    		e = Enum.valueOf(SqlServerDataType.class, name);
		} catch (Exception e2) {
		}
    	return e == null ? UNKNOWN : e;
    }
    
}
