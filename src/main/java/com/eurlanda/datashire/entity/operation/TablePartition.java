package com.eurlanda.datashire.entity.operation;

/**
 * 定义 table 的partition属性
 * 
 * @author eurlanda
 * @version 1.0
 * @created 16-八月-2013 16:48:59
 */
public class TablePartition {
	
	/**
	 * 键列
	 */
	private String keyColumn;
	
	/***
	 * 构造器
	 */
	public TablePartition() {

	}

	public void finalize() throws Throwable {

	}
	
	/**
	 * 获得键列
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@return
	 */
	public String getKeyColumn() {
		return keyColumn;
	}

	/**
	 * 设置
	 * @param newVal
	 */
	public void setKeyColumn(String newVal) {
		keyColumn = newVal;
	}

}