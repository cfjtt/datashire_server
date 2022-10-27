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
public enum DataBaseType {
	SQLSERVER(1),

	ORACLE(2), DB2(3), INFORMIX(4), MYSQL(5), TERADATA(6), MSACCESS(7), POSTGRESQL(
			8), HBASE_PHOENIX(9),HSQLDB(10),SYBASE(11), HANA(12), IMPALA(13),HIVE(14),CASSANDRA(15);

	private int _value;

	private static Map<Integer, DataBaseType> map;

	/**
	 * 构造方法
	 * 
	 * @param value
	 */
	private DataBaseType(int value) {
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
	public static DataBaseType valueOf(int value) throws EnumException {
		DataBaseType type = null;
		if (map == null) {
			map = new HashMap<>();
			DataBaseType[] types = DataBaseType.values();
			for (DataBaseType tmp : types) {
				map.put(tmp.value(), tmp);
			}
		}
		type = map.get(value);
		if (type == null) {
			throw new EnumException();
		}
		return type;
	}

	public static DataBaseType parse(int t) {
		for (DataBaseType result : values()) {
			if (result.value() == t) {
				return result;
			}
		}
		return null;
	}

	public static DataBaseType parse(String name) {
		return (DataBaseType) Enum.valueOf(DataBaseType.class, name);
	}
}