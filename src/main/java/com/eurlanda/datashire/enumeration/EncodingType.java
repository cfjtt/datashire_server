package com.eurlanda.datashire.enumeration;

import com.eurlanda.datashire.utility.EnumException;

import java.util.HashMap;
import java.util.Map;

/**
 * 编码类型枚举类
 * @author lei.bin
 *
 */
public enum EncodingType {
	UTF8(0), 
	ASCII(1), 
	GBK(2), 
	UNICODE(3),
	GB2312(4), 
	BIG5(5);
	
    private int _value;
	
	private static Map<Integer, EncodingType> map;

	/**
	 * 构造方法
	 * 
	 * @param value
	 */
	private EncodingType(int value) {
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
	
    public static EncodingType parse(int t) {
        for (EncodingType result : values()) {
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
	public static EncodingType valueOf(int value) throws EnumException {
		EncodingType type = null;
		if (map == null) {
			map = new HashMap<Integer, EncodingType>();
			EncodingType[] types = EncodingType.values();
			for (EncodingType tmp : types) {
				map.put(tmp.value(), tmp);
			}
		}
		type = map.get(value);
		if (type == null) {
			throw new EnumException();
		}
		return type;
	}

	/**
	 * 枚举值转换为对应编码字符串
	 * 
	 * @return
	 */
	public static String toFtpEncoding(int value) {
		if (0 == value) {
			return "UTF-8";
		} else if (1 == value) {
			return "ASCII";
		} else if (2 == value) {
			return "GBK";
		} else if (3 == value) {
			return "UNICODE";
		} else if (4 == value) {
			return "GB2312";
		} else if (5 == value) {
			return "BIG5";
		}
		return null;
	}

	/**
	 * 枚举值转换为对应编码字符串
	 *
	 * @return
	 */
	public String toFtpEncoding() {
		int value = this.value();
		if (0 == value) {
			return "UTF-8";
		} else if (1 == value) {
			return "ASCII";
		} else if (2 == value) {
			return "GBK";
		} else if (3 == value) {
			return "UNICODE";
		} else if (4 == value) {
			return "GB2312";
		} else if (5 == value) {
			return "BIG5";
		}
		return null;
	}
}
