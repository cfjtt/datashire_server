package com.eurlanda.datashire.server.utils.dbsource;

import cn.com.jsoft.jframe.utils.jdbc.JDBCTemplate;
import com.eurlanda.datashire.enumeration.DataBaseType;
import com.eurlanda.datashire.enumeration.datatype.SystemDatatype;
import com.eurlanda.datashire.server.utils.entity.DBConnectionInfo;
import com.eurlanda.datashire.server.utils.entity.operation.BasicTableInfo;
import com.eurlanda.datashire.server.utils.entity.operation.ColumnInfo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 连接Postgresql的Data Store的处理类
 * Postgresql安装在服务器4上
 * @author bo.dang
 * @date 2014-5-5
 */
public class PGsqlAdapter extends AbsDBAdapter implements INewDBAdapter  {
	
	public PGsqlAdapter(DBConnectionInfo info) {
		super(info);
		// TODO Auto-generated constructor stub
	}
	
	@Override
	public List<BasicTableInfo> getAllTables(final String filter) {
		String sql ="select tablename from pg_tables where schemaname='public'";
		List<Map<String, Object>> results = this.jdbcTemplate.queryForList(sql,null);
		List<BasicTableInfo> tableList = new ArrayList<BasicTableInfo>();
		String constraintsSQL = "select t.*," +
				"fk.column_name as source_column,ref.column_name as ref_column,ref.table_name as ref_table " +
				"from user_constraints t " +
				"left join user_cons_columns fk on  t.constraint_name=fk.constraint_name " +
				"left join user_cons_columns ref on  t.r_constraint_name=ref.constraint_name  " +
				"where table_name=?";
		String indexSQL = "select t.*,fk.column_name as source_column from user_indexes t  left join user_cons_columns fk on  t.index_name=fk.constraint_name where t.table_name=?";
		
		for(Map<String,Object> tableMap:results){		// 表
			String tableName = tableMap.get("TABLENAME").toString();
			BasicTableInfo tableInfo = new BasicTableInfo(tableName);
			tableInfo.setRecordCount(super.getRecordCount(tableName));
			/*List<Map<String,Object>> constraintsList = this.jdbcTemplate.queryForList(constraintsSQL, tableName);
			//List<Map<String,Object>> indexList = this.jdbcTemplate.queryForList(indexSQL, tableName);
			for(Map<String,Object> constraints:constraintsList){
				String constraintType = constraints.get("CONSTRAINT_TYPE").toString();
				if(constraintType.equals("P")){
					TablePrimaryKey pk = new TablePrimaryKey();
					pk.setKeyName(constraints.get("CONSTRAINT_NAME").toString());
					pk.setTableName(tableName);
					pk.setSourceColumn(constraints.get("SOURCE_COLUMN").toString());
					tableInfo.getPrimaryKeyList().add(pk);
				}else if(constraintType.equals("R")){
					TableForeignKey fk = new TableForeignKey();
					fk.setKeyName(constraints.get("CONSTRAINT_NAME").toString());
					fk.setTableName(tableName);
					fk.setOnDelete(constraints.get("DELETE_RULE").toString());
					fk.setSourceColumn(constraints.get("SOURCE_COLUMN").toString());
					fk.setReferenceColumn(constraints.get("REF_COLUMN").toString());
					fk.setReferenceTable(constraints.get("REF_TABLE").toString());
					fk.setOrderType(constraints.get("DELETE_RULE").toString());
					tableInfo.getForeignKeyList().add(fk);
				}
			}
 			for(Map<String,Object> index:indexList){
				TableIndex idx = new TableIndex();
				idx.setTableName(tableName);
				idx.setIndexName(index.get("INDEX_NAME").toString());
				idx.setColumnName(index.get("SOURCE_COLUMN").toString());
				idx.setOrderType(index.get("INDEX_TYPE").toString());
				idx.setUnique(index.get("UNIQUENESS").toString());
				tableInfo.getTableIndexList().add(idx);
			}*/
			
			//tableInfo.setRecordCount((Integer)tableMap.get("COLCOUNT"));
			tableList.add(tableInfo);
		}
		return tableList;
	}
	@Override
	public List<ColumnInfo> getTableColumns(final String tableName, String DatabaseName) {
		List<ColumnInfo> columnList = new ArrayList<ColumnInfo>();
		// Postgresql对大小写有区分
		String colsql = "select ordinal_position, column_name, column_default, is_nullable, data_type from information_schema.columns where table_schema='public' and table_name=?";
		
		List<Map<String,Object>> colList = this.jdbcTemplate.queryForList(colsql,null, tableName.toLowerCase());
		
		for(Map<String,Object> col: colList){
			ColumnInfo column = new ColumnInfo();
			column.setTableName(tableName);
			
			column.setOrderNumber((Integer)col.get("ORDINAL_POSITION"));
			column.setName(col.get("COLUMN_NAME").toString());
			//column.setComment((String) col.get("REMARKS"));
			String typeName = col.get("DATA_TYPE").toString();
			//column.setSystemDataType(DataTypeManager.encodeSystemDataType(DataBaseType.POSTGRESQL, typeName));
			column.setSystemDatatype(SystemDatatype.parse(typeName));
			column.setTypeName(typeName);
			//column.setLength(((Integer)col.get("LENGTH")));
			column.setNullable(Boolean.parseBoolean(col.get("IS_NULLABLE").toString()));
			column.setDefaultValue(col.get("COLUMN_DEFAULT")==null? null:col.get("COLUMN_DEFAULT").toString());
			columnList.add(column);
		}
		return columnList;
		
		//return columnList;
	}
	
	/**
	 * 
	 * @param args
	 * @return 
	 * @author bo.dang
	 * @date 2014-5-5
	 */
	public static void main(String[] args) {
		try {
			Class.forName("org.postgresql.Driver").newInstance();

			String url = "jdbc:postgresql://127.0.0.1:5432/postgres";
			String user = "postgres";
			String password = "sa";
			Connection conn = DriverManager.getConnection(url, user, password);
			if(null != conn){
				System.out.println("DB connected successfully! ");
			}
			String sql ="select tablename from pg_tables where schemaname='public' ";
			
		/*	JdbcTemplate jd = new JdbcTemplate();
			jd.queryforlist*/
			PreparedStatement ps = conn.prepareStatement(sql);
			//PreparedStatement ps = conn.prepareStatement("select * from DS_SOURCE_TABLE ");
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				System.out.println("tablename = " + rs.getString("tablename"));
/*				System.out.println("id = " + rs.getString("id"));
				System.out.println("table_name = " + rs.getString("table_name"));
				System.out.println("source_squid_id = " + rs.getString("source_squid_id"));
				System.out.println("is_extracted = " + rs.getString("is_extracted"));*/
			}
			conn.close();
			DBConnectionInfo dbs = new DBConnectionInfo();
			dbs.setHost("127.0.0.1");
			dbs.setPort(5432);
			dbs.setUserName("postgres");
			dbs.setPassword("sa");
			dbs.setDbName("postgres");
			dbs.setDbType(DataBaseType.POSTGRESQL);
			JDBCTemplate jdbc = new JDBCTemplate(new PGsqlAdapter(dbs));
			System.out.println(jdbc.queryForList(sql));
		} catch (Exception sqle) {
			System.out.print(sqle);

		}
	}
}
