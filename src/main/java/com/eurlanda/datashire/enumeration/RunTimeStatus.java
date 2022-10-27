package com.eurlanda.datashire.enumeration;

import com.eurlanda.datashire.utility.EnumException;

import java.util.HashMap;
import java.util.Map;

/**
 * 
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
public enum RunTimeStatus {
	READY(0),
	
	RUNNING(1),
	
	COMPLETED(2),
	
	WARINING(3),
	
	FAILURE(4);
	private int _value;
	
	private static Map<Integer, RunTimeStatus> map;

	/**
	 * 构造方法
	 * 
	 * @param value
	 */
	private RunTimeStatus(int value) {
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
	public static RunTimeStatus valueOf(int value) throws EnumException {
		RunTimeStatus type = null;
		if (map == null) {
			map = new HashMap<Integer, RunTimeStatus>();
			RunTimeStatus[] types = RunTimeStatus.values();
			for (RunTimeStatus tmp : types) {
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