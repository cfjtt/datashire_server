package com.eurlanda.datashire.socket.protocol;

import com.eurlanda.datashire.enumeration.DSObjectType;

/**
 * SOCKET返回数据包
 * @author Fumin
 *
 */
public class ProtocolData {
	
	/**
	 * 状态
	 */
	private String status;
	
	/**
	 * 数据
	 */
	private String data;
	
	private DSObjectType type = DSObjectType.UNKNOWN;
	
	public ProtocolData(){
		this.status = ResponseStatus.SUCCESS_CALL.toString();
		
	}
	
	public ProtocolData(String status, String data){
		this.status = status;
		this.data = data;
	}
	
	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getData() {
		return data;
	}

	public void setData(String data) {
		this.data = data;
	}

	public DSObjectType getType() {
		return type;
	}

	public void setType(DSObjectType type) {
		this.type = type;
	}
	
}
