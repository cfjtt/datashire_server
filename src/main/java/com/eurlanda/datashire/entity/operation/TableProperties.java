package com.eurlanda.datashire.entity.operation;


/**
 * 创建table时所指定的一些高级属性设置。
 * 
 * @author eurlanda
 * @version 1.0
 * @created 16-八月-2013 16:49:01
 */
public class TableProperties {
	
	/**
	 * 文件组
	 */
	private TableFileGroup fileGroup;
	
	/**
	 * 分割
	 */
	private TablePartition partition;
	
	/**
	 * 获得文件组
	 * @return fileGroup
	 */
	public TableFileGroup getFileGroup() {
		return fileGroup;
	}
	
	/**
	 * 设置文件组
	 * @param fileGroup
	 */
	public void setFileGroup(TableFileGroup fileGroup) {
		this.fileGroup = fileGroup;
	}
	
	/**
	 * 获得分割
	 * @return partition
	 */
	public TablePartition getPartition() {
		return partition;
	}
	
	/**
	 * 设置分割
	 * @param partition
	 */
	public void setPartition(TablePartition partition) {
		this.partition = partition;
	}

    

}