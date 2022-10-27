package com.eurlanda.datashire.enumeration;

/** DML:数据操纵语言，SQL的分类之一，命令使用户能够查询数据库以及操作已有数据库中的数据的计算机语言 */
public enum DMLType {
	
	INSERT(1), UPDATE(2), DELETE(3), SELECT(4);
	
	private int value;
	
	private DMLType(int v) {
		value = v;
	}
	
	public int value() {
		return value;
	}
	
    public static DMLType parse(int t) {
        for (DMLType result : values()) {
            if (result.value() == t) {
                return result;
            }
        }
        return null;
    }
    
    public static DMLType parse(String name) {
    	return (DMLType)Enum.valueOf(DMLType.class, name);
    }
    
}
