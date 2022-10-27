package com.eurlanda.datashire.entity.operation;
/**
 * 导出\导入的数据传输时的接口对象
 * @author yi.zhou
 */


public class FlowUnit{
	//任务传输的总次数
	private int count;
	//任务传输的序号（序号从1开始）
	private Integer index;
	//任务的数据体（数据保存格式为XML，同时进行压缩处理，当数据量比较大时，进行数据分割）
	private byte[] data;
	
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	public Integer getIndex() {
		return index;
	}
	public void setIndex(Integer index) {
		this.index = index;
	}
	public byte[] getData() {
		return data;
	}
	public void setData(byte[] data) {
		this.data = data;
	}
}
