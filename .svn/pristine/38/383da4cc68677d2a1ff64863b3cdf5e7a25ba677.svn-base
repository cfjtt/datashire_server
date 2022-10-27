package com.eurlanda.datashire.enumeration;

import com.eurlanda.datashire.utility.EnumException;

import java.util.HashMap;
import java.util.Map;

public enum SquidLinkArrowsStatus {
	
	BASEARROW(1),
	
	SOLIDARROW(2),
	
	CIRCLEARROW(3),
	
	RECTARROW(4),
	
	NOARROW(5);

   
	private int _value;
	
	private static Map<Integer, SquidLinkArrowsStatus> map;

	/**
	 * 构造方法
	 * @param value
	 */
	private SquidLinkArrowsStatus(int value) {
		_value = value;
	}

	/**
	 * 得到枚举值
	 * @return
	 */
	public int value() {
		return _value;
	}
	
	/**
	 * 从int到enum的转换函数
	 * @param value
	 * @return
	 * @throws Exception 
	 */
	public static SquidLinkArrowsStatus valueOf(int value) throws EnumException {
		SquidLinkArrowsStatus type = null;
		if(map==null){
			map = new HashMap<Integer, SquidLinkArrowsStatus>();
			SquidLinkArrowsStatus[] types = SquidLinkArrowsStatus.values();
			for(SquidLinkArrowsStatus tmp : types){
				
				map.put(tmp.value(), tmp);
			}
		}
		type = map.get(value);
		if(type==null){
			
			throw new EnumException();
		}
		return type;
    }

}
