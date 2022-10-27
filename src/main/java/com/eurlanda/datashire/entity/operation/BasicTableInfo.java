package com.eurlanda.datashire.entity.operation;

import com.eurlanda.datashire.enumeration.DataBaseType;
import java.util.ArrayList;
import java.util.List;

/**
 * 数据库表基础信息，用于DBAdapter
 * @date 2014-1-10
 * @author jiwei.zhang
 *
 */
public class BasicTableInfo {
	private String tableName;
	private String catalog;
	private String scheme;
	private String tableSpace;
	private String tableGroup;
	private String comment;
	private String keyspace;
	private String cluster;
	private Integer recordCount;
	private DataBaseType dbType;
	private boolean isPersistTable;
	private List<TablePrimaryKey> primaryKeyList = new ArrayList<TablePrimaryKey>();
	private List<TableIndex> tableIndexList = new ArrayList<TableIndex>();
	private List<TableForeignKey> foreignKeyList=new ArrayList<TableForeignKey>();
	private List<ColumnInfo> columnList;

	public String getKeyspace() {
		return keyspace;
	}

	public void setKeyspace(String keyspace) {
		this.keyspace = keyspace;
	}

	public String getCluster() {
		return cluster;
	}

	public void setCluster(String cluster) {
		this.cluster = cluster;
	}

	public Integer getRecordCount() {
		return recordCount;
	}

	public void setRecordCount(Integer recordCount) {
		this.recordCount = recordCount;
	}

	public List<TablePrimaryKey> getPrimaryKeyList() {
		return primaryKeyList;
	}

	public void setPrimaryKeyList(List<TablePrimaryKey> primaryKeyList) {
		this.primaryKeyList = primaryKeyList;
	}

	public List<TableIndex> getTableIndexList() {
		return tableIndexList;
	}

	public void setTableIndexList(List<TableIndex> tableIndexList) {
		this.tableIndexList = tableIndexList;
	}

	public List<TableForeignKey> getForeignKeyList() {
		return foreignKeyList;
	}

	public void setForeignKeyList(List<TableForeignKey> foreignKeyList) {
		this.foreignKeyList = foreignKeyList;
	}

	public String getScheme() {
		return scheme;
	}

	public void setScheme(String scheme) {
		this.scheme = scheme;
	}

	public String getComment() {
		return comment;
	}

	public void setComment(String comment) {
		this.comment = comment;
	}

	public String getCatalog() {
		return catalog;
	}

	public void setCatalog(String catalog) {
		this.catalog = catalog;
	}

	public String getTableSpace() {
		return tableSpace;
	}

	public void setTableSpace(String tableSpace) {
		this.tableSpace = tableSpace;
	}

	public String getTableGroup() {
		return tableGroup;
	}

	public void setTableGroup(String tableGroup) {
		this.tableGroup = tableGroup;
	}

	public List<ColumnInfo> getColumnList() {
		return columnList;
	}

	public void setColumnList(List<ColumnInfo> columnList) {
		this.columnList = columnList;
	}

	
	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	/**
	 * 用表名创建TableInfo
	 * @param tableName 表名
	 */
	public BasicTableInfo(String tableName) {
		super();
		this.tableName = tableName;
		this.isPersistTable = true;
	}

	public BasicTableInfo() {
	}
	@Override
	public String toString() {
		return "BasicTableInfo [tableName=" + tableName + "]";
	}

	public DataBaseType getDbType() {
		return dbType;
	}

	public void setDbType(DataBaseType dbType) {
		this.dbType = dbType;
	}

	public boolean isPersistTable() {
		return isPersistTable;
	}

	public void setPersistTable(boolean isPersistTable) {
		this.isPersistTable = isPersistTable;
	}
	
}
