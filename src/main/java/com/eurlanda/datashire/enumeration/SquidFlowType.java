package com.eurlanda.datashire.enumeration;

import com.eurlanda.datashire.utility.EnumException;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yhc on 1/25/2016.
 */
public enum SquidFlowType {
    BATCH(0),
    FLOW(1);
    private int _value;
    private static Map<Integer, SquidFlowType> map;
    /**
     * 构造方法
     *
     * @param value
     */
    private SquidFlowType(int value) {
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
    public static SquidFlowType valueOf(int value) throws EnumException {
        SquidFlowType type = null;
        if (map == null) {
            map = new HashMap<Integer, SquidFlowType>();
            SquidFlowType[] types = SquidFlowType.values();
            for (SquidFlowType tmp : types) {
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
