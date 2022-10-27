package com.eurlanda.datashire.enumeration;

import com.eurlanda.datashire.utility.EnumException;

import java.util.HashMap;
import java.util.Map;


/**
 * 文件类型枚举类，文件类型抽取Squid中的文件类型属性使用
 * @author lei.bin
 *
 */
public enum ExtractFileType {
	CSV(0),
	TXT(1),
	DOC(2),
	DOCX(3),
	XLSX(4),
	XLS(5),
	PDF(6),
	LOG(7),
	LCK(8),
	XML(9),
	XSD(10),
	DTD(11);

	private int _value;

	private static Map<Integer, ExtractFileType> map;

	/**
	 * 构造方法
	 * 
	 * @param value
	 */
	private ExtractFileType(int value) {
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

    public static boolean isTxt(ExtractFileType ft){
        return ft == ExtractFileType.TXT || ft == ExtractFileType.CSV;
    }

    public static boolean isExcel(ExtractFileType ft){
        return ft == ExtractFileType.XLS || ft == ExtractFileType.XLSX;
    }

    public static boolean isWord(ExtractFileType ft){
        return ft == ExtractFileType.DOC || ft == ExtractFileType.DOCX;
    }

    public static boolean isOffice(ExtractFileType ft){
        return ft == ExtractFileType.DOC || ft == ExtractFileType.DOCX || ft == ExtractFileType.XLS || ft == ExtractFileType.XLSX;
    }

	public static ExtractFileType parse(int t) {
		for (ExtractFileType result : values()) {
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
	public static ExtractFileType valueOf(int value) throws EnumException {
		ExtractFileType type = null;
		if (map == null) {
			map = new HashMap<Integer, ExtractFileType>();
			ExtractFileType[] types = ExtractFileType.values();
			for (ExtractFileType tmp : types) {
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
