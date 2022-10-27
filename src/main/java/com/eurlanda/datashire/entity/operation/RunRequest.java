package com.eurlanda.datashire.entity.operation;

/**
 * 请求转换实体类
 * 
 * @author Fumin
 * 
 */
public class RunRequest {

	/**
	 * token信息
	 */
	private String token;

	/**
	 * SquidFlowId
	 */
	private Integer squidFlowId;

	/**
	 * 是否为运行模式
	 */
	private Integer isRunMode;

	/**
	 * 构造器
	 */
	public RunRequest() {
		this.isRunMode = 1;
	}

	public String getToken() {
		return token;
	}

	public void setToken(String token) {
		this.token = token;
	}

	public Integer getSquidFlowId() {
		return squidFlowId;
	}

	public void setSquidFlowId(Integer squidFlowId) {
		this.squidFlowId = squidFlowId;
	}

	public Integer getIsRunMode() {
		return isRunMode;
	}

	public void setIsRunMode(Integer isRunMode) {
		this.isRunMode = isRunMode;
	}
}
