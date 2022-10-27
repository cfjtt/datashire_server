package com.eurlanda.datashire.entity.operation;

import com.eurlanda.datashire.utility.EnumException;

import java.util.HashMap;
import java.util.Map;

public enum IncUnitEnum {
	YEAR(0, "yyyy"),
	MONTH(1, "MM"),
	DAY(2, "dd"),
	HOURS(3, "HH"),
	MINUTES(4, "mm"),
	SECONDS(5, "ss"),
	MS(6, "SSS"),
	NANOSECOND(7, "nano");
	
	private int _key;
    private String _value;
	
	public int get_key() {
		return _key;
	}

	public void set_key(int _key) {
		this._key = _key;
	}

	public String get_value() {
		return _value;
	}

	public void set_value(String _value) {
		this._value = _value;
	}

	private static Map<Integer, IncUnitEnum> map;

	/**
	 * 构造方法
	 * 
	 * @param key
	 * @param show
	 * @param value
	 */
	private IncUnitEnum(int key, String value) {
		_key = key;
		_value = value;
	}
	
	/**
	 * 从int到enum的转换函数
	 * 
	 * @param value
	 * @return
	 * @throws Exception
	 */
	public static IncUnitEnum valueOf(int value) throws EnumException {
		IncUnitEnum type = null;
		if (map == null) {
			map = new HashMap<Integer, IncUnitEnum>();
			IncUnitEnum[] types = IncUnitEnum.values();
			for (IncUnitEnum tmp : types) {
				map.put(tmp.get_key(), tmp);
			}
		}
		type = map.get(value);
		if (type == null) {
			throw new EnumException();
		}
		return type;
	}
}
