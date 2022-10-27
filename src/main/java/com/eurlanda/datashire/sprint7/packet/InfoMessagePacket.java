package com.eurlanda.datashire.sprint7.packet;

public class InfoMessagePacket <T> extends BasicMessagePacket {

	/**
	 * info消息
	 */
	private T info;
	
	/**
	 * 构造器
	 */
	public InfoMessagePacket(){
		super();
	}
	
	public InfoMessagePacket(int code){
		this.setCode(code);
	}
	
	public T getInfo() {
		return info;
	}

	public void setInfo(T info) {
		this.info = info;
	}
	
}
