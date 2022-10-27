package com.eurlanda.datashire.entity.operation;

import com.eurlanda.datashire.enumeration.DataStatusEnum;
/**
 * 
 * 
 * <p>
 * Title : 
 * </p>
 * <p>
 * Description: 
 * </p>
 * <p>
 * Author :赵春花 2013-9-10
 * </p>
 * <p>
 * update :赵春花 2013-9-10
 * </p>
 * <p>
 * Department :  JAVA后端研发部
 * </p>
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司
 * </p>
 */
public class DataEntity {

	/**
	 * 属性名称
	 */
	private String attributeName;
	/**
	 * 属性值
	 */
	private Object value;
	/**
	 * 属性类型
	 */
	private DataStatusEnum dataType;

	public DataEntity(){
	}
	
	/**
	 * 构造器
	 * @param attributeName
	 * @param value
	 * @param dataType
	 */
	public DataEntity(String attributeName, DataStatusEnum dataType, Object value){
		this.attributeName = attributeName;
		this.dataType = dataType;
		this.value = value;
	}

	/**
	 * 属性名称
	 */
	public String getAttributeName(){
		return attributeName;
	}

	/**
	 * 属性名称
	 * 
	 * @param newVal
	 */
	public void setAttributeName(String newVal){
		attributeName = newVal;
	}

	/**
	 * 属性名称
	 */
	public Object getValue(){
		return this.value;
	}

	/**
	 * 属性名称
	 * 
	 * @param newVal
	 */
	public void setValue(Object newVal){
		this.value =newVal;
	}

	/**
	 * 属性类型
	 */
	public DataStatusEnum getDataType(){
		return dataType;
	}

	/**
	 * 属性类型
	 * 
	 * @param newVal
	 */
	public void setDataType(DataStatusEnum newVal){
		dataType = newVal;
	}

	@Override
	public String toString() {
		return "DataEntity [attributeName=" + attributeName + ", value="
				+ value + ", dataType=" + dataType + "]";
	}

}