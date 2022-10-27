package com.eurlanda.datashire.adapter.db;

import cn.com.jsoft.jframe.utils.jdbc.ConnectionCallback;
import com.eurlanda.datashire.adapter.datatype.DataTypeConvertor;
import com.eurlanda.datashire.entity.Column;
import com.eurlanda.datashire.entity.DBConnectionInfo;
import com.eurlanda.datashire.entity.SquidIndexes;
import com.eurlanda.datashire.entity.operation.BasicTableInfo;
import com.eurlanda.datashire.entity.operation.ColumnInfo;
import com.eurlanda.datashire.entity.operation.TableForeignKey;
import com.eurlanda.datashire.entity.operation.TableIndex;
import com.eurlanda.datashire.enumeration.DataBaseType;
import com.eurlanda.datashire.enumeration.datatype.SystemDatatype;
import com.eurlanda.datashire.exception.SystemErrorException;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import com.eurlanda.datashire.utility.StringUtils;
import com.eurlanda.datashire.utility.SysConf;
import com.eurlanda.datashire.utility.objectsql.TemplateParser;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Hbase数据库
 * @author yi.zhou
 *
 */
public class HanaAdapter  extends AbsDBAdapter implements INewDBAdapter {
	
	public HanaAdapter(DBConnectionInfo info) {
		super(info);
	}

	/**
	 * 初始化 hbase链接
	 * @return
	 */
	public static DBConnectionInfo getHanaConnection(){
    	DBConnectionInfo dbInfo = new DBConnectionInfo();
    	dbInfo.setDbType(DataBaseType.HANA);
    	dbInfo.setHost(SysConf.getValue("hana.host"));
    	dbInfo.setPort(Integer.parseInt(SysConf.getValue("hana.port")));
    	dbInfo.setUserName("System");
    	dbInfo.setPassword("Datacvg123");
    	dbInfo.setConnectionString("jdbc:sap://"+dbInfo.getHost()+":"+dbInfo.getPort()+"?reconnect=true");
		return dbInfo;
    }
	
	@Override
	protected String buildColumnSql(ColumnInfo col) {
		String column_template = "${colName} ${colType} ${nullable} ${auto_increment}";
		TemplateParser parser = new TemplateParser();
		parser.addParam("colName", "\""+col.getName().toUpperCase()+"\"");
		
		//String typeStr = DataTypeManager.decode(DataBaseType.HBASE_PHOENIX, col.getSystemDatatype());
		//String typeStr = col.getSystemDatatype().name();
		/*switch (col.getSystemDatatype()) {
		case NVARCHAR:
			parser.addParam("colType", typeStr+"("+col.getLength()+")");
			break;
		default:
			parser.addParam("colType", typeStr);
		}*/
		String typeStr = DataTypeConvertor.getOutTypeByColumn(DataBaseType.HANA.value(), col);
		//System.out.println(typeStr);
		parser.addParam("colType", typeStr);
		if (col.isNullable()&& !col.isIdentity()) {
			parser.addParam("nullable", "NULL");
		} else if (col.isPrimary() && !col.isNullable()) {
			parser.addParam("nullable", "NOT NULL");
		}
		if (col.isIdentity()) {
			parser.addParam("auto_increment", "generated by default as identity"); 
		}
		String sql= parser.parseTemplate(column_template);
		return sql;
	}
	
	/**
	 * 查询索引
	 * @param tableName
	 * @param DatabaseName
	 * @return
	 * @author bo.dang
	 */
	@Override
	public List<SquidIndexes> getTableIndexes(final String tableName,String dataBaseName) {
		final List<SquidIndexes> squidIndexList = new ArrayList<SquidIndexes>();
		final String[] SchemaAndName=this.getSchemaAndName(tableName);
		this.jdbcTemplate.execute(new ConnectionCallback<Object>() {
			@Override
			public Object doSomething(Connection conn) {
				ResultSet rs = null;
				try {
					// 主键
					Set<String> primaryKeys = new HashSet<String>();
					if(null!=SchemaAndName&&SchemaAndName.length==2)
					{
						rs = conn.getMetaData().getPrimaryKeys(null, SchemaAndName[0], SchemaAndName[1]);
					}else
					{
						rs = conn.getMetaData().getPrimaryKeys(null, null, tableName);
					}
					while (rs.next()) {
						primaryKeys.add(rs.getString("COLUMN_NAME"));
					}
					closeResultSet(rs);
                    if(null!=SchemaAndName&&SchemaAndName.length==2)
                    {
                    	rs = conn.getMetaData().getIndexInfo(null, SchemaAndName[0], SchemaAndName[1], false, false);
                    	//rs = conn.getMetaData().getColumns(null, SchemaAndName[0], SchemaAndName[1], null);
                    }else
                    {
                    	rs = conn.getMetaData().getIndexInfo(null, null, tableName, false, false);
                    	//rs = conn.getMetaData().getColumns(null, null, tableName, null);
                    }
                    SquidIndexes squidIndexes = null;
                    int count = 0;
                    Integer ordinal_position = 0;
                    String columnName = null;
                    String indexName = null;
					while (rs.next()) {
						
						// 索引名
						indexName = rs.getString("INDEX_NAME");
						if(StringUtils.isNull(indexName)){
							continue;
						}
						count ++;
						indexName = indexName.toLowerCase();
						if(1 == count){
							squidIndexes = new SquidIndexes();
							squidIndexes.setIndex_name(indexName);
						}
						
						if(count > 1 && !squidIndexes.getIndex_name().equals(indexName)){
							squidIndexList.add(squidIndexes);
							squidIndexes = new SquidIndexes();
							squidIndexes.setIndex_name(indexName);
						}
						ordinal_position = rs.getInt("ORDINAL_POSITION");
						columnName = rs.getString("COLUMN_NAME").split(":")[1].toLowerCase();
						if(columnName.equals("_datashire_guid_hbase_pk")){
							continue;
						}
						setColumnName(ordinal_position, columnName, squidIndexes);
					}
					if(StringUtils.isNotNull(squidIndexes)){
						squidIndexList.add(squidIndexes);
					}
					closeResultSet(rs);
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					closeResultSet(rs);
				}
				return squidIndexList;
			}
		});
		return squidIndexList;
	}
/*	@Override
	public List<ColumnInfo> getTableColumns(final String tableName, final String databaseName) {
		final List<ColumnInfo> columnList = new ArrayList<ColumnInfo>();
		this.jdbcTemplate.execute(new ConnectionCallback<Object>() {
			@Override
			public Object doSomething(Connection conn) {
				ResultSet rs = null;
				try {
					// 主键
					Set<String> primaryKeys = new HashSet<String>();
					System.out.println("表名为："+tableName);
					rs = conn.getMetaData().getPrimaryKeys(null, null, tableName);
					while (rs.next()) {
						primaryKeys.add(rs.getString("COLUMN_NAME"));
					}
					closeResultSet(rs);
					
					rs = conn.getMetaData().getColumns(null, null, tableName, null);
					while (rs.next()) {
						ColumnInfo column = new ColumnInfo();
						column.setComment("");
						column.setOrderNumber(rs.getInt("ORDINAL_POSITION"));
						column.setName(rs.getString("COLUMN_NAME"));
						String typeName = rs.getString("TYPE_NAME");
						// column.setIdentity(rs.getBoolean("IS_AUTOINCREMENT"));
						column.setTypeName(typeName);
						//column.setSystemDatatype(SystemDatatype.parse(typeName));
						column.setDbBaseDatatype(DbBaseDatatype.parse(typeName));
						column.setTableName(rs.getString("TABLE_NAME"));
						column.setLength(rs.getInt("COLUMN_SIZE"));
						//column.setNullable(rs.getBoolean("IS_NULLABLE"));
						column.setNullable(true);
						if (primaryKeys.contains(column.getName())) {
							column.setPrimary(true);
						} else {
							column.setPrimary(false);
						}
						columnList.add(column);
					}
					closeResultSet(rs);
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					closeResultSet(rs);
				}
				return null;
			}
		});
		return columnList;
	}*/
	
	@Override
	public void createTable(BasicTableInfo table, ReturnValue out) {
		this.beginTransaction();
		try{
			List<String> list = new ArrayList<String>();
			//List<String> uList = new ArrayList<String>();
			String tableName = table.getTableName().toUpperCase();
			String sql = "create table " + tableName;
			if (table == null || table.getColumnList() == null || table.getColumnList().size() == 0) {
				throw new SystemErrorException(MessageCode.ERR_CREATE_TABLE, "创建表失败，表名错误或者列为空。");
			}
			if (tableName.contains(".")){
				String schema = tableName.split("\\.")[0];
			    int count = this.queryForList("select * from SYS.SCHEMAS where schema_name='"+schema+"'").size();
			    if (count==0){
			    	this.executeUpdate("CREATE SCHEMA "+schema+" ");
			    }
			}
			sql += "(";
			for (ColumnInfo col : table.getColumnList()) {
				sql += "\n\t" + this.buildColumnSql(col) + ",";
				if (col.isPrimary()) {
					list.add(col.getName());
				}else if (col.isUniqueness()) {
					//uList.add(col.getName());
				}
			}
			//不存在主键，默认加上 _DATASHIRE_GUID_HBASE_PK
			/*if (list.size()==0){
				ColumnInfo defaultInfo = new ColumnInfo();
				defaultInfo.setSystemDatatype(SystemDatatype.NVARCHAR);
				defaultInfo.setName("_DATASHIRE_GUID_HBASE_PK");
				defaultInfo.setLength(256);
				defaultInfo.setPrimary(true);
				defaultInfo.setNullable(false);
				defaultInfo.setTableName(tableName.toUpperCase());
				sql += "\n\t" + this.buildColumnSql(defaultInfo) + ",";
				list.add(defaultInfo.getName());
			}*/
			//sql = sql.replaceFirst(",$", "");
			if (list.size()>0){
				String tempName = "";
				for (String key : list) {
					tempName += ",\"" + key.toUpperCase() + "\"";
				}
				if (tempName!=""){
					tempName = tempName.substring(1);
				}
				sql += "\n\t PRIMARY KEY (" + tempName + "),";
			}
			/*if (uList.size()>0){
				String keyName = "uq_" + tableName + "_temp";
				String tempName = "";
				for (String key : uList) {
					tempName += "," + initColumnNameByDbType(table.getDbType().value(), key) + "";
				}
				if (tempName!=""){
					tempName = tempName.substring(1);
				}
				sql += "\n\tCONSTRAINT " + keyName + " UNIQUE (" + tempName + "),";
			}*/
			sql = sql.replaceFirst(",$", "");
			sql += "\n\t)";
			
			logger.debug("建表:" + sql);
			System.out.println(sql);
			this.jdbcTemplate.update(sql);
	
			// 外键
			for (TableForeignKey foreignKey : table.getForeignKeyList()) {
				this.addForeignKey(tableName, foreignKey);
			}
			// 索引
			for (TableIndex key : table.getTableIndexList()) {
				this.addIndex(tableName, key);
			}
			this.commit();
		}catch(Exception e){
			e.printStackTrace();
			this.rollback();
			out.setMessageCode(MessageCode.SQL_ERROR);
		}
	}
	
	/**
	 * 引擎进行隐式落地时调用的方法，生成最终的创建落地表sql脚本语句
	 * @param tableName
	 * @param columns
	 * @return
	 */
	public static String getTableCreateSqlByColumn(String tableName, List<Column> columns){
		try {
			List<String> list = new ArrayList<String>();
			String sql = "create table " + tableName.toUpperCase();
			if (columns == null || columns.size() == 0) {
				throw new SystemErrorException(MessageCode.ERR_CREATE_TABLE, "创建表失败，表名错误或者列为空。");
			}
			sql += "(";
			for (Column col : columns) {
				sql += "\n\t" + bulidColunnSql(col) + ",";
				if (col.isIsPK()) {
					list.add(col.getName());
				}
			}
			//不存在主键，默认加上 _DATASHIRE_GUID_HBASE_PK
			if (list.size()==0){
				Column defaultInfo = new Column();
				defaultInfo.setData_type(SystemDatatype.NVARCHAR.value());
				defaultInfo.setName("_DATASHIRE_GUID_HBASE_PK");
				defaultInfo.setLength(256);
				defaultInfo.setIsPK(true);
				defaultInfo.setNullable(false);
				sql += "\n\t" + bulidColunnSql(defaultInfo) + ",";
				list.add(defaultInfo.getName());
			}
			sql = sql.replaceFirst(",$", "");
			if (list.size()>0){
				String keyName = "pk_" + tableName.toUpperCase().replace(".", "_") + "_temp";
				String tempName = "";
				for (String key : list) {
					tempName += ",\"" + key.toUpperCase() + "\"";
				}
				if (tempName!=""){
					tempName = tempName.substring(1);
				}
				sql += "\n\tCONSTRAINT " + keyName + " PRIMARY KEY (" + tempName + "),";
			}
			sql = sql.replaceFirst(",$", "");
			sql += "\n\t)";
			System.out.println(sql);
			return sql;
		} catch (Exception e) {
			e.printStackTrace();
		} 
		return "";
	}
	
	private static String bulidColunnSql(Column col){
		String column_template = "${colName} ${colType} ${nullable} ${auto_increment}";
		TemplateParser parser = new TemplateParser();
		parser.addParam("colName", "\""+col.getName().toUpperCase()+"\"");
		ColumnInfo info = DataTypeConvertor.getColumnInfoByColumn(col);
		String typeStr = DataTypeConvertor.getOutTypeByColumn(DataBaseType.HANA.value(), info);
		//System.out.println(typeStr);
		parser.addParam("colType", typeStr);
		if (col.isNullable()) {
			parser.addParam("nullable", "NULL");
		} else if (col.isIsPK() && !col.isNullable()) {
			parser.addParam("nullable", "NOT NULL");
		}
		String sql= parser.parseTemplate(column_template);
		return sql;
	}
	
	public static void main(String[] args) {
		DBConnectionInfo dbs = new DBConnectionInfo();
		dbs.setDbName("SYSTEM");
		//dbs.setDbName("eurlanda");
		dbs.setDbType(DataBaseType.HANA);
		dbs.setHost("192.168.137.219:30115");
		dbs.setPort(0);
		dbs.setUserName("SYSTEM");
		dbs.setPassword("Datacvg123");
		try {
			INewDBAdapter adaoterSource= AdapterDataSourceManager.getAdapter(dbs);
			String sql = "insert into template_table(id";
			for (int i = 1; i < 16; i++) {
				sql += ",col_"+i;
			}
			sql += ") values(3,311111111,2234,3,3,3234.21,323.222,323.11111,32.2,3,?,'2015-03-30','2015-03-30',?,'zxc','zxc')";
			int cnt =  adaoterSource.executeUpdate(sql, new Timestamp(new Date().getTime()), new Time(new Date().getTime()));
			adaoterSource.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void ss(){
		/*drop table template_table;
		create table template_table(
		    id int,
		    col_1  bigint,
		    col_2  integer,
		    col_3  smallint,
		    col_4  tinyint,
		    col_5  decimal(18,2),
		    col_6  double,
		    col_7  float,
		    col_8  SMALLDECIMAL,
		    col_9  REAL,
		    col_10 TIMESTAMP,
		    col_11 DATE,
		    col_12 SECONDDATE,
		    col_13 TIME,
		    col_14 nvarchar(100),
		    col_15 varchar(100)
		);*/
		
		DBConnectionInfo dbs = new DBConnectionInfo();
		dbs.setDbName("SYSTEM");
		//dbs.setDbName("eurlanda");
		dbs.setDbType(DataBaseType.HANA);
		dbs.setHost("192.168.137.219:30115");
		dbs.setPort(0);
		dbs.setUserName("SYSTEM");
		dbs.setPassword("Datacvg123");
		try {
			INewDBAdapter adaoterSource= AdapterDataSourceManager.getAdapter(dbs);
			String sql = "insert into template_table(id";
			for (int i = 1; i < 16; i++) {
				sql += ",col_"+i;
			}
			sql += ") values(3,311111111,2234,3,3,3234.21,323.222,323.11111,32.2,3,?,'2015-03-30','2015-03-30',?,'zxc','zxc')";
			int cnt =  adaoterSource.executeUpdate(sql, new Timestamp(new Date().getTime()), new Time(new Date().getTime()));
			adaoterSource.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void dd(){
		DBConnectionInfo dbs = new DBConnectionInfo();
		dbs.setDbName("SYSTEM");
		//dbs.setDbName("eurlanda");
		dbs.setDbType(DataBaseType.HANA);
		dbs.setHost("192.168.137.219:30115");
		dbs.setPort(0);
		dbs.setUserName("SYSTEM");
		dbs.setPassword("Datacvg123");
		try {
			INewDBAdapter adaoterSource= AdapterDataSourceManager.getAdapter(dbs);
			String sql = "select * from template_table_conn";
			 adaoterSource.queryForList(sql);
			
			//sql = "insert into template_table_conn(id";
			for (int i = 1; i < 6; i++) {
				sql += ",col_"+i;
			}
			sql += ") values(1,nul,?,?,?,?)";
			int cnt =  adaoterSource.executeUpdate(sql, 
					new Timestamp(new Date().getTime()), 
					new Time(new Date().getTime()));
			//NClob clob = new 
			
			adaoterSource.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}