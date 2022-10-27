package com.eurlanda.datashire.enumeration;

import com.eurlanda.datashire.utility.EnumException;

import java.util.Map;

/**
 * TransformationStatus执行状态
 * @author Fumin
 *
 */
public enum TransformationStatusEnum {
	
	/**
	 * 等待执行
	 */
	READY(1),
	
	/**
	 * 执行中
	 */
	RUNNING(2),
	
	/**
	 * 执行结束
	 */
	COMPLETED(3),
	
	/**
	 * 警报状态
	 */
	WARINING(4),
	
	/**
	 * 异常中断
	 */
	FAILURE(5);

	private int _value;
	
	private static Map<Integer, TransformationStatusEnum> map;

	/**
	 * 构造方法
	 * @param value
	 */
	private TransformationStatusEnum(int value) {
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
	public static TransformationStatusEnum valueOf(int value) throws EnumException {
		TransformationStatusEnum type = null;
		if(map==null){
			TransformationStatusEnum[] types = TransformationStatusEnum.values();
			for(TransformationStatusEnum tmp : types){
				
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
