package com.eurlanda.datashire.exception;

import com.eurlanda.datashire.utility.MessageCode;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * 系统错误异常	
 * @date 2014-1-8
 * @author jiwei.zhang
 *
 */
public class SystemErrorException extends RuntimeException {
	private static final long serialVersionUID = 1L;
	private MessageCode code;
	private String msg;
	/**
	 * 构造错误异常
	 * @param code 消息编号
	 */
	public SystemErrorException(MessageCode code) {
		super();
		this.code = code;
	}
	/**
	 * 构造错误异常
	 * @param code 消息编号
	 * @param msg 消息
	 */
	public SystemErrorException(MessageCode code, String msg) {
		super(msg);
		this.code = code;
		this.msg = msg;
	}
	/**
	 * 构造错误异常
	 * @param code 消息编号
	 * @param msg 消息
	 */
	public SystemErrorException(MessageCode code, Throwable e) {
		super(e);
		this.code = code;
		this.msg = exception2String(e);
	}
	public MessageCode getCode() {
		return code;
	}
	public void setCode(MessageCode code) {
		this.code = code;
	}
	public String getMsg() {
		return msg;
	}
	public void setMsg(String msg) {
		this.msg = msg;
	}
	public String exception2String(Throwable e){
		if(e==null) return null;
		StringWriter writer = new StringWriter();
		PrintWriter pw = new PrintWriter(writer);
		e.printStackTrace(pw);
		try {
			pw.close();
			writer.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		return writer.toString();
	}
	@Override
	public String toString() {
		return "SystemErrorException [code=" + code + ", msg=" + msg + "]";
	}
	
}
