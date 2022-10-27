package com.eurlanda.datashire.enumeration;

import com.eurlanda.datashire.utility.EnumException;

import java.util.HashMap;
import java.util.Map;

/**
 * 条件匹配规则枚举类
 * @author Fumin
 *
 */
public enum MatchTypeEnum {
	
	LIKE(1),
	
	EQ(2),
	
	GT(3),
	
	LT(4),
	
	GTE(5),
	
	LTE(6),
	
	NQE(7);
	
	private int _value;
	
	private static Map<Integer, MatchTypeEnum> map;

	/**
	 * 构造方法
	 * 
	 * @param value
	 */
	private MatchTypeEnum(int value) {
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
	public static MatchTypeEnum valueOf(int value) throws EnumException {
		MatchTypeEnum type = null;
		if (map == null) {
			map = new HashMap<Integer, MatchTypeEnum>();
			MatchTypeEnum[] types = MatchTypeEnum.values();
			for (MatchTypeEnum tmp : types) {
				map.put(tmp.value(), tmp);
			}
		}
		type = map.get(value);
		if (type == null) {
			throw new EnumException();
		}
		return type;
	}
}
