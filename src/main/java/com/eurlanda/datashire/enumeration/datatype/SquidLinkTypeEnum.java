package com.eurlanda.datashire.enumeration.datatype;

public enum SquidLinkTypeEnum {
	NORMAL(0),
	EXCEPTION(1);
	
	private int value;
	
	private SquidLinkTypeEnum(int v) {
		value = v;
	}
	
	public int value() {
		return value;
	}
	
    public static SquidLinkTypeEnum parse(int t) {
        for (SquidLinkTypeEnum result : values()) {
            if (result.value() == t) {
                return result;
            }
        }
        return null;
    }
    
    public static SquidLinkTypeEnum parse(String name) {
    	SquidLinkTypeEnum sys = Enum.valueOf(SquidLinkTypeEnum.class, name);
    	return sys == null ? NORMAL : sys;
    }
}
