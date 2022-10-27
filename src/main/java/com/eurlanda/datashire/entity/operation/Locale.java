package com.eurlanda.datashire.entity.operation;
/**
 * locale可以在不同范围内指定，包括：repository, project, squidflow,squid和column。
 * 
 * <p>
 * Title : 
 * </p>
 * <p>
 * Description: 
 * </p>
 * <p>
 * Author :Cheryl 2013-7-26
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
public class Locale {

	/**
	 * 日期格式
	 */
	private String lcDate;
	
	/**
	 *货币格式
	 */
	private String lcMoney;
	
	/**
	 * 时间格式
	 */
	private String lcTime;

	public Locale(){

	}

	public void finalize() throws Throwable {

	}

	/**
	 * 得到日期格式
	 */
	public String getLcDate(){
		return lcDate;
	}

	/**
	 * 得到钱的格式
	 */
	public String getLcMoney(){
		return lcMoney;
	}

	/**
	 * 得到时间格式
	 */
	public String getLcTime(){
		return lcTime;
	}

	/**
	 * 设置日期格式
	 * 
	 * @param newVal
	 */
	public void setLcDate(String newVal){
		lcDate = newVal;
	}

	/**
	 * 设置钱的格式
	 * 
	 * @param newVal
	 */
	public void setLcMoney(String newVal){
		lcMoney = newVal;
	}

	/**
	 * 设置时间格式
	 * 
	 * @param newVal
	 */
	public void setLcTime(String newVal){
		lcTime = newVal;
	}

}