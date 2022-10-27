package com.eurlanda.datashire.utility.objectsql;
/**
 * sql排序
 * @date 2014-1-10
 * @author jiwei.zhang
 *
 */
public class SQLOrder {
	private String orderType;
	private String orderBy;
	/**
	 * 用指定的字段名升序排序
	 * @date 2014-1-10
	 * @author jiwei.zhang
	 * @param fieldName 待排序的字段名
	 * @return
	 */
	public static SQLOrder asc(String fieldName){
		SQLOrder order = new SQLOrder();
		order.orderBy=fieldName;
		order.orderType="asc";
		return order;
	}
	/**
	 * 用指定的字段名降序排序 
	 * @date 2014-1-10
	 * @author jiwei.zhang
	 * @param fieldName 待排序的字段名
	 * @return
	 */
	public static SQLOrder desc(String fieldName){
		SQLOrder order = new SQLOrder();
		order.orderBy=fieldName;
		order.orderType="desc";
		return order;
	}
	@Override
	public String toString() {
		return "SQLOrder [orderType=" + orderType + ", orderBy=" + orderBy + "]";
	}
	public String generateSQL(){
		return this.orderBy +" "+this.orderType;
	}
}
