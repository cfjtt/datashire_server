package com.eurlanda.datashire.enumeration;

import com.eurlanda.datashire.utility.EnumException;

import java.util.Map;

/**
 * SyncStatusEnum同步状态
 * @author bo.dang
 *
 */
public enum SyncStatusEnum {
	
    // 未知
	UNKNOWN(0),
	// 已同步
	SYNCHRONIZED(1),
	// 未同步不包含历史数据
	NOT_SYNCHRONIZED_NO_HISTORY_DATA(2),
	// 未同步包含历史数据
	NOT_SYNCHRONIZED_HISTORY_DATA(3),
	// 未曾落地
	NOT_PERSISTENT(4);

	private int _value;
	
	private static Map<Integer, SyncStatusEnum> map;

	/**
	 * 构造方法
	 * @param value
	 */
	private SyncStatusEnum(int value) {
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
	public static SyncStatusEnum valueOf(int value) throws EnumException {
		SyncStatusEnum type = null;
		if(map==null){
			SyncStatusEnum[] types = SyncStatusEnum.values();
			for(SyncStatusEnum tmp : types){
				
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
