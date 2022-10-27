package com.eurlanda.datashire.enumeration;

import com.eurlanda.datashire.utility.EnumException;

import java.util.HashMap;
import java.util.Map;

public enum CollationType {
    UNKNOWN(0),
    SQL_Latin1_General_Cp437_CS_AS_KI_WI(1),
    SQL_Latin1_General_Cp437_CI_AS_KI_WI(2),
    SQL_Latin1_General_Pref_Cp437_CI_AS_KI_WI(3),
    SQL_Latin1_General_Cp437_CI_AI_KI_WI(4),
    SQL_Latin1_General_Cp437_BIN(5),
    SQL_Latin1_General_Pref_Cp850_CI_AS_KI_WI(6),
    SQL_1xCompat_Cp850_CI_AS_KI_WI(7),
    SQL_Latin1_General_Cp1_CS_AS_KI_WI(8),
    SQL_Latin1_General_Cp1_CI_AS_KI_WI(9);
    
    
    private int _value;
    
    private static Map<Integer, CollationType> map;

    /**
     * 构造方法
     * 
     * @param value
     */
    private CollationType(int value) {
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
    public static CollationType valueOf(int value) throws EnumException {
        CollationType type = null;
        if (map == null) {
            map = new HashMap<Integer, CollationType>();
            CollationType[] types = CollationType.values();
            for (CollationType tmp : types) {
                map.put(tmp.value(), tmp);
            }
        }
        type = map.get(value);
        if (type == null) {
            throw new EnumException();
        }
        return type;
    }
    
    public static CollationType parse(int t) {
        for (CollationType result : values()) {
            if (result.value() == t) {
                return result;
            }
        }
        return null;
    }
}
