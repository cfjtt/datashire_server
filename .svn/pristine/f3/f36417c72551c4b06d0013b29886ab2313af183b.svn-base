package com.eurlanda.datashire.entity.operation;


/**
 * squid运行时的特性指定,如是否向客户端自动推送数据，推送的频率等。
 * 该对象和用户登录成功后所获得的token绑定。同一个squid，不同的用户，该对象取值可以不同。
 * 
 * <p>
 * Title : 
 * </p>
 * <p>
 * Description: 
 * </p>
 * <p>Cheryl 2013-7-26
 * </p>
 * <p>
 * update :Cheryl2013-7-26
 * </p>
 * <p>
 * Department :  JAVA后端研发部
 * </p>
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司
 * </p>
 */
public class SquidRunTimeProperties {

	/**
	 * SquidFlowId
	 */
	private Integer squidflow_id;
	
	/**
	 * squidID 
	 */
	private Integer squid_id;
	/**
	 * 每一次推送的最大记录个数
	 */
	private int batchRowNumber;
	
	/**
	 * 自动向客户端推送数据，默认为false
	 */
	private boolean isAutoRefreshData;
	
	/**
	 * 是否自动向客户端推送squid状态，默认为false
	 */
	private boolean isAutoRefreshStatus;
	
	/**
	 * 所有推送的最大记录个数
	 */
	private int maxRowNumber;
	
	/**
	 * 两次推送时间间隔（单位：毫秒）
	 */
	private int refreshInterval=5;
	
	/**
	 * locale可以在不同范围内指定，包括：repository, project,
	 *  squidflow,squid和column。squidUILocale仅仅规范特定用
	 */
	private Locale squidUILocale;
	
	/**
	 * 命令号
	 */
	private String token;

	public SquidRunTimeProperties(){

	}

	public void finalize() throws Throwable {

	}

	public int getBatchRowNumber() {
		return batchRowNumber;
	}

	public void setBatchRowNumber(int batchRowNumber) {
		this.batchRowNumber = batchRowNumber;
	}

	public boolean isAutoRefreshData() {
		return isAutoRefreshData;
	}

	public void setAutoRefreshData(boolean isAutoRefreshData) {
		this.isAutoRefreshData = isAutoRefreshData;
	}

	public boolean isAutoRefreshStatus() {
		return isAutoRefreshStatus;
	}

	public void setAutoRefreshStatus(boolean isAutoRefreshStatus) {
		this.isAutoRefreshStatus = isAutoRefreshStatus;
	}

	public int getMaxRowNumber() {
		return maxRowNumber;
	}

	public void setMaxRowNumber(int maxRowNumber) {
		this.maxRowNumber = maxRowNumber;
	}

	public int getRefreshInterval() {
		return refreshInterval;
	}

	public void setRefreshInterval(int refreshInterval) {
		this.refreshInterval = refreshInterval;
	}

	public Locale getSquidUILocale() {
		return squidUILocale;
	}

	public void setSquidUILocale(Locale squidUILocale) {
		this.squidUILocale = squidUILocale;
	}

	public String getToken() {
		return token;
	}

	public void setToken(String token) {
		this.token = token;
	}

	public Integer getSquidflow_id() {
		return squidflow_id;
	}

	public void setSquidflow_id(Integer squidflow_id) {
		this.squidflow_id = squidflow_id;
	}

	public Integer getSquid_id() {
		return squid_id;
	}

	public void setSquid_id(Integer squid_id) {
		this.squid_id = squid_id;
	}

}