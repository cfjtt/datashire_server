package com.eurlanda.datashire.enumeration;

import com.eurlanda.datashire.utility.EnumException;

import java.util.HashMap;
import java.util.Map;


/**
 * 文件类型枚举类，文件类型抽取Squid中的文件类型属性使用(不要删，引擎会用到)
 * @author lei.bin
 *
 */
public enum FileType {
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

	private static Map<Integer, FileType> map;

	/**
	 * 构造方法
	 *
	 * @param value
	 */
	private FileType(int value) {
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

    public static boolean isTxt(FileType ft){
        return ft == FileType.TXT || ft == FileType.CSV;
    }

    public static boolean isExcel(FileType ft){
        return ft == FileType.XLS || ft == FileType.XLSX;
    }

    public static boolean isWord(FileType ft){
        return ft == FileType.DOC || ft == FileType.DOCX;
    }

    public static boolean isOffice(FileType ft){
        return ft == FileType.DOC || ft == FileType.DOCX || ft == FileType.XLS || ft == FileType.XLSX;
    }

	public static FileType parse(int t) {
		for (FileType result : values()) {
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
	public static FileType valueOf(int value) throws EnumException {
		FileType type = null;
		if (map == null) {
			map = new HashMap<Integer, FileType>();
			FileType[] types = FileType.values();
			for (FileType tmp : types) {
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
