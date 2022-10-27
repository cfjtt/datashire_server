package com.eurlanda.datashire.enumeration;

import com.eurlanda.datashire.utility.EnumException;

import java.util.HashMap;
import java.util.Map;

/**
 * 数据类型枚举
 * @author Fumin
 * @version 1.0
 * @created 20-May-2013 9:50:09 AM
 * @deprecated @see SystemDatatype
 */
public enum DataStatusEnum {
	UNKNOWN(0),
	/**
	 * 整型
	 */
	INT(1),
	
	/**
	 * 字符串型
	 */
	STRING(2),
	
	/**
	 * 时间类型
	 */
	DATE(3),
	
	/**
	 * BOOLEAN
	 */
	BOOLEAN(4),
	
	/**
	 * BYTE
	 */
	BYTE(5),
	
	/**
	 * SHORT
	 */
	SHORT(6),
	
	/**
	 * LONG
	 */
	LONG(7),
	
	/**
	 * FLOAT
	 */
	FLOAT(8),
	
	/**
	 * DOUBLE
	 */
	DOUBLE(9),
	
	/**
	 * TIME
	 */
	TIME(10),
	
	/**
	 * TIMESTAMP
	 */
	TIMESTAMP(11),
	
	/**
	 * DIGDECIMAL
	 */
	DIGDECIMAL(12),
	NCHAR(13),
	NVARCHAR(14),
	MONEY(15),
	XML(16),
	/**
	 * 不为表字段类型，为表记录
	 */
	TABLE(-1);
	
	private int _value;
	
	private static Map<Integer, DataStatusEnum> map;

	/**
	 * 构造方法
	 * 
	 * @param value
	 */
	private DataStatusEnum(int value) {
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
	public static DataStatusEnum valueOf(int value) throws EnumException {
		DataStatusEnum type = null;
		if (map == null) {
			map = new HashMap<Integer, DataStatusEnum>();
			DataStatusEnum[] types = DataStatusEnum.values();
			for (DataStatusEnum tmp : types) {
				map.put(tmp.value(), tmp);
			}
		}
		type = map.get(value);
		if (type == null) {
			throw new EnumException();
		}
		return type;
	}

    public static DataStatusEnum parse(int t) {
        for (DataStatusEnum result : values()) {
            if (result.value() == t) {
                return result;
            }
        }
        return null;
    }
    
}