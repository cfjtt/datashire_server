package com.eurlanda.datashire.enumeration.datatype;

public enum DeBugStatusEnum {
	READY(1),		//就绪
    RUNNING(2),		//运行中
    COMPLETED(3),	//完毕
    WARNING(4),		//警告
    ERROR(5),		//错误
    PAUSE(6);		//暂停
	
	private int value;
	
	private DeBugStatusEnum(int v) {
		value = v;
	}
	
	public int value() {
		return value;
	}
	
    public static DeBugStatusEnum parse(int t) {
        for (DeBugStatusEnum result : values()) {
            if (result.value() == t) {
                return result;
            }
        }
        return null;
    }
    
    public static DeBugStatusEnum parse(String name) {
    	DeBugStatusEnum sys = Enum.valueOf(DeBugStatusEnum.class, name);
    	return sys == null ? READY : sys;
    }
}
