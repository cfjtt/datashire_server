package com.eurlanda.datashire.entity.operation;

import com.eurlanda.datashire.entity.Column;

import java.util.List;


/**
 * 关系型数据集（表结构）
 * @author Charyl
 * @version 1.0
 * @created 29-May-2013 10:17:14 AM
 */
public class DataSet {

	/**
	 * dataset的列名
	 */
	private List<Column> columns;
	
	/**
	 * how many columns?
	 */
	public int columnCount = 0;
	
	/**
	 * how many rows?
	 */
	public int rowCount = 0;
	
	/**
	 * data store of this dataset.  a cell of this collection could be a value of any
	 * type
	 */
	private List<List<Object>> cells;

	/**
	 * 构造器
	 */
	public DataSet(){

	}

	public void finalize() throws Throwable {

	}
	
	public DataSet(List<Column> columns, int columnCount, int rowCount,
			List<List<Object>> cells) {
		super();
		this.columns = columns;
		this.columnCount = columnCount;
		this.rowCount = rowCount;
		this.cells = cells;
	}

	/**
	 * dataset的列名
	 */
	public List<Column> getColumns(){
		return columns;
	}

	/**
	 * dataset的列名
	 * 
	 * @param newVal
	 */
	public void setColumns(List<Column> newVal){
		columns = newVal;
	}

	public int getColumnCount() {
		return columnCount;
	}

	public void setColumnCount(int columnCount) {
		this.columnCount = columnCount;
	}

	public int getRowCount() {
		return rowCount;
	}

	public void setRowCount(int rowCount) {
		this.rowCount = rowCount;
	}

	public List<List<Object>> getCells() {
		return cells;
	}

	public void setCells(List<List<Object>> cells) {
		this.cells = cells;
	}

}
