package com.eurlanda.datashire.enumeration;

import com.eurlanda.datashire.utility.EnumException;

import java.util.HashMap;
import java.util.Map;

/**
 * 文件格式的枚举
 * Created by Administrator on 2016/9/14.
 */
public enum FileFormatTypeEnum {

    TEXTFILE(1),

    SEQUENCEFILE(2),

    PARQUETFILE(3),

    AVRODATAFILE(4),

    CSVFILE(5),

    JSONFILE(6),

    ORCFILE(7);

    private int _value;
    private static Map<Integer, FileFormatTypeEnum> map;

    public static FileFormatTypeEnum parse(int t) {
        for (FileFormatTypeEnum result : values()) {
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
    public static FileFormatTypeEnum valueOf(int value) throws EnumException {
        FileFormatTypeEnum type = null;
        if (map == null) {
            map = new HashMap<Integer, FileFormatTypeEnum>();
            FileFormatTypeEnum[] types = FileFormatTypeEnum.values();
            for (FileFormatTypeEnum tmp : types) {
                map.put(tmp.value(), tmp);
            }
        }
        type = map.get(value);
        if (type == null) {
            throw new EnumException();
        }
        return type;
    }

    private FileFormatTypeEnum(int value) {
        _value = value;
    }

    public int value() {
        return _value;
    }

    public void set_value(int _value) {
        this._value = _value;
    }
}
