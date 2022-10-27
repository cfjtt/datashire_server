package com.eurlanda.datashire.entity.operation;

import java.util.List;
import java.util.Map;

/**
 * 返回数据结果集
 * @author Cheryl
 *
 */
public class ResultSets {
	//列的类型信息集合
	private Map<String, String> columnList;
	//数据信息集合
	private  List<Map<String, String>> dataList;
	//数据总数
	private int dataCount; //多余的  = dataList.size 
	
	public ResultSets() {
	}

	public ResultSets(Map<String, String> columnList, List<Map<String, String>> dataList, int count) {
		this.columnList = columnList;
		this.dataList = dataList;
		this.dataCount = count;
	}

	public Map<String, String> getColumnList() {
		return columnList;
	}

	public void setColumnList(Map<String, String> columnList) {
		this.columnList = columnList;
	}

	public List<Map<String, String>> getDataList() {
		return dataList;
	}

	public void setDataList(List<Map<String, String>> dataList) {
		this.dataList = dataList;
	}

	public int getDataCount() {
		return dataCount;
	}

	public void setDataCount(int dataCount) {
		this.dataCount = dataCount;
	}

}
