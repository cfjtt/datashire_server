package com.eurlanda.datashire.socket.protocol;

import com.eurlanda.datashire.enumeration.DMLType;


/**
 * 
 * 通讯接口参数配置 Superuser具有一切操作权限(根据登陆账号[用户名]判断即可)
 * 
 * @author Administrator
 * 
 */
public class ServiceConf {
	/*
	 * 方法名
	 */
	private String methodName;
	/*
	 * 入参类型
	 */
	private Class argType;
	/*
	 * 权限操作
	 */
	private PrivilegeConf p;

	/** 是否需要向客户端发送响应内容，默认需要 */
	private boolean sendResponse = true;
	
	/** 是否需要替换包头的key，默认不需要 */
	private boolean replaceKey = false;
	
	
	/**
	 * 通讯接口参数配置(一个接口对应一个权限)
	 * 
	 * @param methodName
	 *            service方法名称
	 * @param argType
	 *            service方法参数类型（入参，目前只有两种：Void.TYPE / String.class）
	 * @param p
	 *            对象类型和操作类型(null表示该接口不做权限验证，直接调用service业务逻辑)
	 */
	public ServiceConf(String methodName, Class argType, PrivilegeConf p) {
		this.methodName=methodName;
		this.argType=argType;
	    this.p=	p;
	}

	/**
	 * 通讯接口参数配置
	 * 		前台入参为json序列化字符串;且不用权限验证
	 * @param methodName
	 */
	public ServiceConf(String methodName) {
		this.methodName=methodName;
		this.argType=String.class;
	}
	
	/**
	 * 通讯接口参数配置(一个接口对应一个权限)
	 * 
	 * @param methodName
	 *            service方法名称
	 * @param argType
	 *            service方法参数类型（入参，目前只有两种：Void.TYPE / String.class）
	 * @param p
	 *            对象类型和操作类型(null表示该接口不做权限验证，直接调用service业务逻辑)
	 * @param sendResponse
	 *            是否需要向客户端发送响应内容.如果不需要，则不会向客户端发送业务层处理结果.
	 */
	public ServiceConf(String methodName, Class argType, PrivilegeConf p, boolean sendResponse) {
		this.methodName=methodName;
		this.argType=argType;
	    this.p=	p;
	    this.sendResponse = sendResponse;
	}
	
	/**
	 * 通讯接口参数配置(一个接口对应一个权限)
	 * 
	 * @param methodName
	 *            service方法名称
	 * @param argType
	 *            service方法参数类型（入参，目前只有两种：Void.TYPE / String.class）
	 * @param p
	 *            对象类型和操作类型(null表示该接口不做权限验证，直接调用service业务逻辑)
	 * @param sendResponse
	 *            是否需要向客户端发送响应内容.如果不需要，则不会向客户端发送业务层处理结果.
	 * @param replaceKey
	 *            是否需要替换包头的key，默认不需要.
	 */
	public ServiceConf(String methodName, Class argType, PrivilegeConf p, boolean sendResponse, boolean replaceKey) {
		this.methodName=methodName;
		this.argType=argType;
	    this.p=	p;
	    this.sendResponse = sendResponse;
	    this.replaceKey = replaceKey;
	}
	
	/** 通用删除，对象类型从消息包中获取 */
	public ServiceConf(String methodName, Class argType) {
		new PrivilegeConf(DMLType.DELETE); // this.set(...);
		this.methodName=methodName;
		this.argType=argType;
	}

	/**
	 * 一个接口涉及多权限 (保留接口，暂未涉及)
	 * 
	 * @param methodName
	 *            service方法名称
	 * @param argType
	 *            service方法参数类型（入参，目前只有两种：空或者一个字符串）
	 * @param p
	 *            对象类型和操作类型
	 * @param r
	 *            ANY 表示只要满足任意一种权限即可, ALL表示要同时满足所有权限
	 */
	ServiceConf(String methodName, Class argType, PrivilegeConf[] p, Required r) {
	}


	public String getMethodName() {
		return methodName;
	}

	public void setMethodName(String methodName) {
		this.methodName = methodName;
	}

	public Class getArgType() {
		return argType;
	}

	public void setArgType(Class argType) {
		this.argType = argType;
	}

	public PrivilegeConf getP() {
		return p;
	}

	public void setP(PrivilegeConf p) {
		this.p = p;
	}

	public boolean isSendResponse() {
		return sendResponse;
	}

	public void setSendResponse(boolean sendResponse) {
		this.sendResponse = sendResponse;
	}

	public boolean isReplaceKey() {
		return replaceKey;
	}

	public void setReplaceKey(boolean replaceKey) {
		this.replaceKey = replaceKey;
	}

}
