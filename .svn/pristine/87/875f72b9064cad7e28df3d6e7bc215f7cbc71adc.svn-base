package com.eurlanda.datashire.enumeration;

import com.eurlanda.datashire.utility.EnumException;

import java.util.HashMap;
import java.util.Map;

/**
 * JoinType
 * 
 * <p>
 * Title : 
 * </p>
 * <p>
 * Description: 
 * </p>
 * <p>
 * Author :赵春花 2013-9-5
 * </p>
 * <p>
 * update :赵春花 2013-9-5
 * </p>
 * <p>
 * Department :  JAVA后端研发部
 * </p>
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司
 * </p>
 */
public enum JoinType {
	BaseTable(0),
	
	InnerJoin(1),
	
	LeftOuterJoin(2),
	
	RightOuterJoin(3),
	
	FullJoin(4),
	
	CrossJoin(5),
	
	Unoin(6),
	
	UnoinAll(7);

	private int _value;
	
	private static Map<Integer, JoinType> map;

	/**
	 * 构造方法
	 * 
	 * @param value
	 */
	private JoinType(int value) {
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
	
    public static JoinType parse(int t) {
        for (JoinType result : values()) {
            if (result.value() == t) {
                return result;
            }
        }
        return null;
    }
    
	/**
	 * 从int到enum的转换函数
	 * 
	 * @param value
	 * @return
	 * @throws Exception
	 */
	public static JoinType valueOf(int value) throws EnumException {
		JoinType type = null;
		if (map == null) {
			map = new HashMap<Integer, JoinType>();
			JoinType[] types = JoinType.values();
			for (JoinType tmp : types) {
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