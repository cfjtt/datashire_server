package com.eurlanda.datashire.utility.objectsql;

import com.eurlanda.datashire.enumeration.DataBaseType;

import java.util.ArrayList;
import java.util.List;

/**
 * 基于对象的SQL语句。实现与数据库无关的查询。
 * 
 * @date 2014-1-10
 * @author jiwei.zhang
 * 
 */
public abstract class ObjectSQL {
	protected String tableName;
	protected List<SQLCondition> condationList = new ArrayList<SQLCondition>();


	protected List<SQLOrder> orderList = new ArrayList<SQLOrder>();
	protected DataBaseType database;

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public DataBaseType getDatabase() {
		return database;
	}

	public void setDatabase(DataBaseType database) {
		this.database = database;
	}

	/**
	 * 用指定的表明构造一个ObjctSQL
	 * 
	 * @param tableName
	 *            表名
	 */
	public ObjectSQL(String tableName) {
		this.tableName = tableName;
	}

	/**
	 * 添加一个排序字段，可以添加多个。
	 * 
	 * @date 2014-1-10
	 * @author jiwei.zhang
	 * @param order
	 */
	public void addOrder(SQLOrder order) {
		if (order != null) {
			this.orderList.add(order);
		}
	}

	/**
	 * 添加一个查询条件，可以添加多个。
	 * 
	 * @date 2014-1-10
	 * @author jiwei.zhang
	 * @param condation
	 *            条件
	 */
	public void addCondation(SQLCondition condation) {
		if (condation != null) {
			this.condationList.add(condation);
		}
	}

	protected String generateConditionSQL() {
		String ret = "";
		if (this.condationList.size() > 0) {
			ret += "\nwhere ";
			for (SQLCondition con : this.condationList) {
				ret += con.generateConditionSql() + "\n\t and ";
			}
			ret = ret.replaceAll(" and $", "");
		}
		return ret;
	}

	/**
	 * 生成sql
	 * 
	 * @date 2014-1-11
	 * @author jiwei.zhang
	 * @param database
	 *            指定数据库类型
	 * @return
	 */
	protected abstract String generateSQL();

	/**
	 * 创建排序sql.
	 * 
	 * @date 2014-1-11
	 * @author jiwei.zhang
	 * @return
	 */
	protected String generateOrderSql() {
		String ret = "";
		if (orderList.size() > 0) {
			ret += "order by ";
			for (SQLOrder order : orderList) {
				ret += order.generateSQL() + ",";
			}
		}
		ret = ret.replaceAll(",$", "");
		return ret;
	}

	/**
	 * 生成sql语句，如果未指定数据库会抛异常，使用之前请确保设置了数据库
	 * 
	 * @date 2014-1-13
	 * @author jiwei.zhang
	 * @return 具体数据库的sql语句
	 */
	public String createSQL() {
		if (this.database == null) {
			throw new RuntimeException("请先指定数据库！");
		}
		return this.generateSQL();
	}

	/**
	 * 将java类型翻译成sql字符串，转换规则为booean->0/1
	 * 
	 * @date 2014-1-11
	 * @author jiwei.zhang
	 * @param val
	 * @return
	 */
	public static String convertVal(Object val) {

		if (val instanceof java.lang.Number) {
			return val.toString();
		} else if (val instanceof java.lang.Boolean) {
			return ((Boolean) val) == true ? "1" : "0";
		} else {
			return "'" + val.toString() + "'";
		}
	}

}
