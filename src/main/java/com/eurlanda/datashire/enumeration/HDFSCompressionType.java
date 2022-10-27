package com.eurlanda.datashire.enumeration;

import com.eurlanda.datashire.utility.EnumException;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yhc on 1/27/2016.
 */
public enum HDFSCompressionType {

    NOCOMPRESS(0, "_no_compress_"),
    BZIP(1, "bzip2"),
    GZIP(2, "gzip"),
    LZ4(3, "lz4"),
    SNAPPY(4, "snappy"),
    DEFLATE(5, "deflate");
    // 数据库保存的枚举数字
    private int _value;
    // 对应的压缩算法
    private String _codec;
    private static Map<Integer, HDFSCompressionType> map;
    /**
     * 构造方法
     *
     * @param value
     * @param codec
     */
    HDFSCompressionType(int value, String codec) {
        this._value = value;
        this._codec = codec;
    }

    /**
     * 得到枚举值
     *
     * @return
     */
    public int value() {
        return _value;
    }

    public String codec() {
        return this._codec;
    }
    /**
     * 从int到enum的转换函数
     *
     * @param value
     * @return
     * @throws Exception
     */
    public static HDFSCompressionType valueOf(int value) throws EnumException {
        HDFSCompressionType type = null;
        if (map == null) {
            map = new HashMap<Integer, HDFSCompressionType>();
            HDFSCompressionType[] types = HDFSCompressionType.values();
            for (HDFSCompressionType tmp : types) {
                map.put(tmp.value(), tmp);
            }
        }
        type = map.get(value);
        if (type == null) {
            throw new EnumException("不存在该枚举类型");
        }
        return type;
    }
}
