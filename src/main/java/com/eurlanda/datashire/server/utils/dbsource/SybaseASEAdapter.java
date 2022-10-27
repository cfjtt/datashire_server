package com.eurlanda.datashire.server.utils.dbsource;

import com.eurlanda.datashire.exception.SystemErrorException;
import com.eurlanda.datashire.server.utils.entity.DBConnectionInfo;
import com.eurlanda.datashire.server.utils.entity.operation.BasicTableInfo;
import com.eurlanda.datashire.server.utils.entity.operation.ColumnInfo;
import com.eurlanda.datashire.server.utils.entity.operation.TableForeignKey;
import com.eurlanda.datashire.server.utils.entity.operation.TableIndex;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.objectsql.TemplateParser;

/**
 * sybase数据库
 * @date 2014-2-11
 * @author jiwei.zhang
 *
 */
public class SybaseASEAdapter extends AbsDBAdapter implements INewDBAdapter {
	/**
	 * 构造一个sybase数据库操作工具。
	 * @param info
	 */
	public SybaseASEAdapter(DBConnectionInfo info) {
		super(info);
	}
	@Override
	protected String buildColumnSql(ColumnInfo col) {
		String column_template = "${colName} ${colType} ${nullable} ${auto_increment}";
		TemplateParser parser = new TemplateParser();
		parser.addParam("colName", col.getName());
		
		//String typeStr = DataTypeManager.decode(DataBaseType.MYSQL, col.getSystemDataType());
		String typeStr = col.getSystemDatatype().name();
		switch (col.getSystemDatatype()) {
		case NVARCHAR:
			parser.addParam("colType", typeStr+"("+col.getLength()+")");
			break;
		default:
			parser.addParam("colType", typeStr);
		}
		if (col.isNullable()&& !col.isIdentity()) {
			parser.addParam("nullable", "NULL");
		} else {
			parser.addParam("nullable", "");
		}
		if (col.isIdentity()) {
			parser.addParam("auto_increment", "identity"); 
		}
		String sql= parser.parseTemplate(column_template);
		return sql;
	}

	@Override
	public void createTable(BasicTableInfo table) {
		try{
			String tableName = table.getTableName();
			String sql = "create table " + table.getTableName();
			if (table == null || table.getColumnList() == null || table.getColumnList().size() == 0) {
				throw new SystemErrorException(MessageCode.ERR_CREATE_TABLE, "创建表失败，表名错误或者列为空。");
			}
			sql += "(";
			for (ColumnInfo col : table.getColumnList()) {
				sql += "\n\t" + this.buildColumnSql(col) + ",";
				if (col.isPrimary()) {
					String keyName = "pk_" + table.getTableName() + "_" + col.getName();
					sql += "\n\tCONSTRAINT " + keyName + " PRIMARY KEY (" + col.getName() + "),";
				}
			}
			sql = sql.replaceFirst(",$", "") + "\n\t)";
			logger.debug("建表:" + sql);
			this.jdbcTemplate.update(sql);
	
			// 外键
			for (TableForeignKey foreignKey : table.getForeignKeyList()) {
				this.addForeignKey(tableName, foreignKey);
			}
			// 索引
			for (TableIndex key : table.getTableIndexList()) {
				this.addIndex(table.getTableName(), key);
			}
		}catch(Exception e){
			e.printStackTrace();
		}
	}

}