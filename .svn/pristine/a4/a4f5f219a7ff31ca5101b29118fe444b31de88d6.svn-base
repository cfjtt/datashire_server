package com.eurlanda.datashire.enumeration;

import com.eurlanda.datashire.utility.EnumException;

import java.util.HashMap;
import java.util.Map;

/**
 * 
 * <p>
 * Title : DataTypeEnum 枚举类
 * </p>
 * <p>
 * Description: 
 * </p>
 * <p>
 * Author :何科敏 Aug 20, 2013
 * </p>
 * <p>
 * update :何科敏 Aug 20, 2013
 * </p>
 * <p>
 * Department :  JAVA后端研发部
 * </p>
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司
 * </p>
 */
public enum NoSQLDataBaseType {
	MONGODB(1);

	private int _value;

	private static Map<Integer, NoSQLDataBaseType> map;

	/**
	 * 构造方法
	 * 
	 * @param value
	 */
	private NoSQLDataBaseType(int value) {
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
	public static NoSQLDataBaseType valueOf(int value) throws EnumException {
		NoSQLDataBaseType type = null;
		if (map == null) {
			map = new HashMap<Integer, NoSQLDataBaseType>();
			NoSQLDataBaseType[] types = NoSQLDataBaseType.values();
			for (NoSQLDataBaseType tmp : types) {
				map.put(tmp.value(), tmp);
			}
		}
		type = map.get(value);
		if (type == null) {
			throw new EnumException();
		}
		return type;
	}

	public static NoSQLDataBaseType parse(int t) {
		for (NoSQLDataBaseType result : values()) {
			if (result.value() == t) {
				return result;
			}
		}
		return null;
	}

	public static NoSQLDataBaseType parse(String name) {
		return (NoSQLDataBaseType) Enum.valueOf(NoSQLDataBaseType.class, name);
	}
}