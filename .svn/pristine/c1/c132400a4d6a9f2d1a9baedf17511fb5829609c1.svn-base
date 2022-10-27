package com.eurlanda.datashire.sprint7.packet;

import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.utility.MessageCode;
/**
 * 待发送的消息。
 * @date 2014-4-18
 * @author jiwei.zhang
 *
 */
public class BasicMessagePacket {
	private int code;
	public DSObjectType type;
	private String desc;
	private String token;

	public String getToken() {
		return token;
	}

	public void setToken(String token) {
		this.token = token;
	}

	/**
	 * 构造器
	 */
	public BasicMessagePacket(){
		this.setCode(MessageCode.SUCCESS.value());
	}

	public DSObjectType getType() {
		return type;
	}

	public void setType(DSObjectType type) {
		this.type = type;
	}

	public int getCode() {
		return code;
	}

	public void setCode(int code) {
		this.code = code;
	}

	public String getDesc() {
		return desc;
	}

	public void setDesc(String desc) {
		this.desc = desc;
	}
}
