package com.eurlanda.datashire.enumeration;

import com.eurlanda.datashire.utility.EnumException;

import java.util.Map;

/**
 * Squid执行状态
 * @author Fumin
 *
 */
public enum SquidStatusEnum {
	
	/**
	 * 等待执行
	 */
	READY(0),
	
	/**
	 * 执行中
	 */
	RUNNING(1),
	
	/**
	 * 执行结束
	 */
	COMPLETED(2),
	
	/**
	 * 警报状态
	 */
	WARINING(3),
	
	/**
	 * 异常中断
	 */
	FAILURE(4);
	

	private int _value;
	
	private static Map<Integer, SquidStatusEnum> map;

	/**
	 * 构造方法
	 * @param value
	 */
	private SquidStatusEnum(int value) {
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
	public static SquidStatusEnum valueOf(int value) throws EnumException {
		SquidStatusEnum type = null;
		if(map==null){
			SquidStatusEnum[] types = SquidStatusEnum.values();
			for(SquidStatusEnum tmp : types){
				
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
