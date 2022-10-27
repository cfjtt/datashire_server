package com.eurlanda.datashire.entity.operation;

import com.eurlanda.datashire.entity.DataSquid;

import java.util.List;
import java.util.Map;

/**
 * 
 * SquidDataSet
 * <p>
 * Title : 
 * </p>
 * <p>
 * Description: 
 * </p>
 * <p>
 * Author :赵春花 2013-9-5
 * </p>
 * <p>
 * update :赵春花 2013-9-5
 * </p>
 * <p>
 * Department :  JAVA后端研发部
 * </p>
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司
 * </p>
 */
public class SquidDataSet {

	private Map<String, String> columnList;
	private int dataCount;
	private List<Map<String, String>> dataList;
	/**
	 * 该DataSet所描述的目标squid
	 */
	private DataSquid target_squid;
	
	//public DataSquid m_DataSquid;
	///public DBSourceSquid m_DBSourceSquid;
	
	public Map<String, String> getColumnList() {
		return columnList;
	}
	public void setColumnList(Map<String, String> columnList) {
		this.columnList = columnList;
	}
	public int getDataCount() {
		return dataCount;
	}
	public void setDataCount(int dataCount) {
		this.dataCount = dataCount;
	}
	public List<Map<String, String>> getDataList() {
		return dataList;
	}
	public void setDataList(List<Map<String, String>> dataList) {
		this.dataList = dataList;
	}
	public DataSquid getTarget_squid() {
		return target_squid;
	}
	public void setTarget_squid(DataSquid target_squid) {
		this.target_squid = target_squid;
	}

}