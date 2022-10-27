package com.eurlanda.datashire.entity.operation;

public class GetRowsFromDataSetPar  {
	/**
	 * token
	 */
	private String token;
	
	/**
	 * SquidFlowId
	 */
	private Integer squidFlowId;
	
	/**
	 * squidID 
	 */
	private Integer squidId;
	
	/**
	 * 项目ID
	 */
	private Integer projectId;
	
	/**
	 * 是否推送
	 */
	private Integer autRefresh;
	
	/**
	 * 构造器
	 */
	public GetRowsFromDataSetPar() {
		super();
	}

	/**
	 * 构造器
	 * @param token
	 * @param squidFlowId
	 * @param squidId
	 * @param projectId
	 * @param autRefresh
	 */
	public GetRowsFromDataSetPar(String token, Integer squidFlowId, Integer squidId,
			Integer projectId, Integer autRefresh) {
		this.token = token;
		this.squidFlowId = squidFlowId;
		this.squidId = squidId;
		this.projectId = projectId;
		this.autRefresh = autRefresh;
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

	public Integer getSquidId() {
		return squidId;
	}

	public void setSquidId(Integer squidId) {
		this.squidId = squidId;
	}

	public Integer getProjectId() {
		return projectId;
	}

	public void setProjectId(Integer projectId) {
		this.projectId = projectId;
	}

	public Integer getAutRefresh() {
		return autRefresh;
	}

	public void setAutRefresh(Integer autRefresh) {
		this.autRefresh = autRefresh;
	}
	
	
}
