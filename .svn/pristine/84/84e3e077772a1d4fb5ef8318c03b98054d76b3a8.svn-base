package com.eurlanda.datashire.enumeration;

public enum SortTypeEnum {
	ASC(1),
	DEAC(2);
	
	private int value;
	
	private SortTypeEnum(int v) {
		value = v;
	}
	
	public int value() {
		return value;
	}
	
    public static SortTypeEnum parse(int t) {
        for (SortTypeEnum result : values()) {
            if (result.value() == t) {
                return result;
            }
        }
        return null;
    }
    
    public static SortTypeEnum parse(String name) {
    	SortTypeEnum sys = Enum.valueOf(SortTypeEnum.class, name);
    	return sys == null ? ASC : DEAC;
    }
}
