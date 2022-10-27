package com.eurlanda.datashire.enumeration;

import com.eurlanda.datashire.utility.EnumException;

import java.util.HashMap;
import java.util.Map;

/**
 * 树节点类型
 * @author chunhua.zhao
 * @version 1.0
 * @created 10-一月-2014 9:22:24
 */
public enum MetadataNodeType {
	FOLDER(0),
	DATABASE(1),
	TABLE(2),
	TABLE_COLUMN(3),
	FILE_COLUMN(4),
	WEB_COLUMN(5),
	FILE(6),
	WEB(7),
	DATAFOLDER(8);
	private int _value;
	
	private static Map<Integer, MetadataNodeType> map;

	/**
	 * 构造方法
	 * 
	 * @param value
	 */
	private MetadataNodeType(int value) {
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
	
    public static MetadataNodeType parse(int t) {
        for (MetadataNodeType result : values()) {
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
	public static MetadataNodeType valueOf(int value) throws EnumException {
		MetadataNodeType type = null;
		if (map == null) {
			map = new HashMap<Integer, MetadataNodeType>();
			MetadataNodeType[] types = MetadataNodeType.values();
			for (MetadataNodeType tmp : types) {
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