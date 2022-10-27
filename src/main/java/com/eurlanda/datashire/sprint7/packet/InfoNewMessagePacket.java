package com.eurlanda.datashire.sprint7.packet;

import com.eurlanda.datashire.utility.JsonUtil;

public class InfoNewMessagePacket <T> extends BasicMessagePacket {

	/**
	 * info消息
	 */
	private Object info;



	public InfoNewMessagePacket(Object info, int code) {
		super();
		this.info = info;
		this.setCode(code);
	}

	/**
	 * 构造器
	 */
	public InfoNewMessagePacket(){
		super();
	}
	
	public InfoNewMessagePacket(int code){
		this.setCode(code);
	}

    public String toJsonString() {
        return JsonUtil.object2Json(this);
    }

	public Object getInfo() {
		return info;
	}

	public void setInfo(Object info) {
		this.info = info;
	}
}
