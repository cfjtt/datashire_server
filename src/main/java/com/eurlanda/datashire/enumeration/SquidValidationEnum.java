package com.eurlanda.datashire.enumeration;

import com.eurlanda.datashire.utility.EnumException;

import java.util.HashMap;
import java.util.Map;

public enum SquidValidationEnum {

    SQUID_NAME(1),
    JOIN_CONDITION(2);
    
    
    private int _value;
    
    private static Map<Integer, SquidValidationEnum> map;

    /**
     * 构造方法
     * @param value
     */
    private SquidValidationEnum(int value) {
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
    public static SquidValidationEnum valueOf(int value) throws EnumException {
        SquidValidationEnum type = null;
        if(map==null){
            map = new HashMap<Integer, SquidValidationEnum>();
            SquidValidationEnum[] types = SquidValidationEnum.values();
            for(SquidValidationEnum tmp : types){
                
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
