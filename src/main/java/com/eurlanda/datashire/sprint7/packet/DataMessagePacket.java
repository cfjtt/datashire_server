package com.eurlanda.datashire.sprint7.packet;

/**
 * 返回列表数据包
 * @author Fumin
 *
 */
public class DataMessagePacket<T> extends BasicMessagePacket{

	/**
	 * 数据数组
	 */
	private T[] dataList;
	
	/**
	 * 构造器
	 */
	public DataMessagePacket(){
		
	}

	public T[] getDataList() {
		return dataList;
	}

	public void setDataList(T[] dataList) {
		this.dataList = dataList;
	}
	
}
