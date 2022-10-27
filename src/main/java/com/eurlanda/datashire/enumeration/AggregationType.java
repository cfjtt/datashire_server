package com.eurlanda.datashire.enumeration;

public enum AggregationType {
	//COUNT(-2),AVG(-1),SUM(1),GROUP(0),MAX(2),MIN(3);
    AVG(1),
    MIN(2),
    SUM(3),
    COUNT(4),
    STDEV(5),
    STDEVP(6),
    MAX(7),
    FIRST_VALUE(8),
    LAST_VALUE(9),
    STRING_SUM(10);
	// GROUP 表示分组字段，不是聚合类型
	private int value;
	
	private AggregationType(int v) {
		value = v;
	}
	
	public int value() {
		return value;
	}
	
	public int set(int _v) {
		return value = _v;
	}
	
    public static AggregationType parse(int t) {
        for (AggregationType result : values()) {
            if (result.value() == t) {
                return result;
            }
        }
        return null;
    }
    
/*    public static AggregationType parse(String name) {
    	AggregationType e =  Enum.valueOf(AggregationType.class, name);
    	return e == null ? GROUP : e;
    }*/
    
}
