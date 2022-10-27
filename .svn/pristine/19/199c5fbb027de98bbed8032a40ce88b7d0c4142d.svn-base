package com.eurlanda.datashire.adapter.db;

import cn.com.jsoft.jframe.utils.jdbc.ConnectionCallback;
import com.eurlanda.datashire.entity.DBConnectionInfo;
import com.eurlanda.datashire.entity.operation.ColumnInfo;
import com.eurlanda.datashire.enumeration.datatype.SystemDatatype;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
/**
 * ACCESS数据库adapter
 * @author lei.bin
 *
 */
public class AccessAdapter extends AbsDBAdapter implements INewDBAdapter {
	public AccessAdapter(DBConnectionInfo info) {
		super(info);
		// TODO Auto-generated constructor stub
	}

/*	@Override
	public List<BasicTableInfo> getAllTables(String filter) {
		// TODO Auto-generated method stub
		DatabaseMetaData  dbmd=Conn.getMetaData(); 
		String sql="select Name from MSysObjects where type=1";
		
		return super.getAllTables(filter);
	}*/

	@Override
	public List<ColumnInfo> getTableColumns(final String tableName,String DatabaseName) {
		final List<ColumnInfo> columnList = new ArrayList<ColumnInfo>();
		super.jdbcTemplate.execute(new ConnectionCallback<Object>() {
			@Override
			public Object doSomething(Connection conn) {
				ResultSet rs = null;
				try {
					DatabaseMetaData dbmt = conn.getMetaData();
					rs = dbmt.getColumns(null,null, tableName, "%");//access
					while(rs.next()){ 
						ColumnInfo column = new ColumnInfo();
						column.setComment(rs.getString("REMARKS"));
						column.setOrderNumber(rs.getInt("ORDINAL_POSITION"));
						column.setName(rs.getString("COLUMN_NAME")) ;//字段名称（列名）
						String typeName = rs.getString("TYPE_NAME");//字段类型
						column.setTypeName(typeName);
						column.setSystemDatatype(SystemDatatype.parse(typeName));
						column.setTableName(rs.getString("TABLE_NAME"));
						column.setLength(rs.getInt("COLUMN_SIZE"));
						//column.setNullable(rs.getBoolean("IS_NULLABLE"));
						columnList.add(column);
				/*		 //获得所有列的数目及实际列数 
						int columnCount=data.getColumnCount(); 
						//获得指定列的列名 
						String columnName = data.getColumnName(i); 
						//获得指定列的列值 
						String columnValue = rs.getString(i); 
						//获得指定列的数据类型 
						int columnType=data.getColumnType(i); 
						//获得指定列的数据类型名 
						String columnTypeName=data.getColumnTypeName(i); 
						//所在的Catalog名字 
						String catalogName=data.getCatalogName(i); 
						//对应数据类型的类 
						String columnClassName=data.getColumnClassName(i); 
						//在数据库中类型的最大字符个数 
						int columnDisplaySize=data.getColumnDisplaySize(i); 
						//默认的列的标题 
						String columnLabel=data.getColumnLabel(i); 
						//获得列的模式 
						String schemaName=data.getSchemaName(i); */
						}
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					closeResultSet(rs);
				}
				return null;
			}
		});
		return columnList;
	
	}
   
}
