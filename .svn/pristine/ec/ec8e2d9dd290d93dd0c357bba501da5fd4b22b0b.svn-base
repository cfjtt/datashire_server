package com.eurlanda.datashire.adapter.db;

import com.eurlanda.datashire.entity.DBConnectionInfo;
import com.eurlanda.datashire.entity.operation.ColumnInfo;
import com.eurlanda.datashire.enumeration.DataBaseType;
import com.eurlanda.datashire.enumeration.datatype.DbBaseDatatype;
import com.eurlanda.datashire.utility.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * adapter for mysql
 * 
 * CREATE TABLE `gc`.`new_table` ( `id` INT NOT NULL AUTO_INCREMENT , `name`
 * VARCHAR(45) NULL , `address` VARCHAR(45) NULL , `addDate` VARCHAR(45) NULL ,
 * PRIMARY KEY (`id`) , UNIQUE INDEX `id_UNIQUE` (`id` ASC) );
 * 
 * @author eurlanda
 * @version 1.0
 * @updated 19-八月-2013 8:50:25
 */
public class MysqlAdapter extends AbsDBAdapter {
	public MysqlAdapter(DBConnectionInfo info) {
		super(info);
	}
	
	@Override
	public List<ColumnInfo> getTableColumns(final String tableName,String DataBaseName) {
		List<ColumnInfo> columnList = new ArrayList<ColumnInfo>();
		//String[] schemaAndName = this.getSchemaAndName(tableName);
		String tbNm = tableName;
		//String schema = "";
	    /*if (schemaAndName!=null&&schemaAndName.length==2){
			tbNm = schemaAndName[1];
			schema = schemaAndName[0];
		}*/
		String colsql = "select * from INFORMATION_SCHEMA.COLUMNS where table_name = '"+tbNm+"'";
		if (StringUtils.isNotBlank(DataBaseName)){
			colsql += " and table_schema='"+DataBaseName+"'";
		}
		List<Map<String,Object>> colList = this.jdbcTemplate.queryForList(colsql,null);
		String pkSql="select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS where table_name = '"+tbNm+"' and column_key='PRI'";
		if(StringUtils.isNotBlank(DataBaseName))
		{
			pkSql += " and table_schema='"+DataBaseName+"'";
		}
		List<Map<String,Object>> pkList = this.jdbcTemplate.queryForList(pkSql,null);
		String uniqueSql="select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS where table_name = '"+tbNm+"' and column_key='UNI'";
		if(StringUtils.isNotBlank(DataBaseName))
		{
			uniqueSql += " and table_schema='"+DataBaseName+"'";
		}
		List<Map<String,Object>> uniqueList = this.jdbcTemplate.queryForList(uniqueSql,null);
		for(Map<String,Object> col: colList){
			ColumnInfo column = new ColumnInfo();
			column.setTableName(tableName);
			//column.setComment(col.get("CONNATION_NAME")+"");
			column.setOrderNumber(StringUtils.isNull(col.get("ORDINAL_POSITION"))?0:Integer.parseInt(col.get("ORDINAL_POSITION")+""));
			column.setName(col.get("COLUMN_NAME").toString());
			String typeName = col.get("DATA_TYPE").toString();
			//column.setSystemDataType(DataTypeManager.encodeSystemDataType(DataBaseType.ORACLE, typeName));
			column.setDbBaseDatatype(DbBaseDatatype.parse(typeName));
			column.setTypeName(typeName);
			//column.setLength(col.get("CHARACTER_OCTET_LENGTH")==null?0:Integer.parseInt(col.get("CHARACTER_OCTET_LENGTH")+""));
			if (StringUtils.isNotNull(col.get("CHARACTER_MAXIMUM_LENGTH"))){
				Long lg = Long.parseLong(col.get("CHARACTER_MAXIMUM_LENGTH")+"");
				if (lg>Long.parseLong(Integer.MAX_VALUE+"")){
					column.setLength(-1);
				}else{
					column.setLength(StringUtils.isNull(col.get("CHARACTER_MAXIMUM_LENGTH"))?0:Integer.parseInt(col.get("CHARACTER_MAXIMUM_LENGTH")+""));
				}
			}else{
				column.setLength(0);
			}
			column.setPrecision(StringUtils.isNull(col.get("NUMERIC_PRECISION"))?0:Integer.parseInt(col.get("NUMERIC_PRECISION")+""));
			column.setScale(StringUtils.isNull(col.get("NUMERIC_SCALE"))?0:Integer.parseInt(col.get("NUMERIC_SCALE")+""));
			//column.setNullable(col.get("IS_NULLABLE").toString().equals("YES")?true:false);
			column.setNullable(true);
			for(Map<String,Object> pk:pkList)
			{
				if(col.get("COLUMN_NAME").toString().equals(pk.get("COLUMN_NAME").toString()))
				{
					column.setPrimary(true);
					break;
				}
			}
			for(Map<String,Object> unique:uniqueList)
			{
				if(col.get("COLUMN_NAME").toString().equals(unique.get("COLUMN_NAME").toString()))
				{
					column.setUniqueness(true);
					break;
				}
			}
			columnList.add(column);
		}
		return columnList;
	}
	
	/**
	 * 获取schema和tableName
	 * @param tableName
	 * @return
	 */
	protected String[] getSchemaAndName(String tableName)
	{
		String[] SchemaAndName=null;
		if(tableName.indexOf(".")>0)
		{
			SchemaAndName=tableName.split("\\.");
		}
		return SchemaAndName;
	}
	
	public static void main(String[] args) {
		DBConnectionInfo dbs = new DBConnectionInfo();
		dbs.setDbName("BizTestDB");
		//dbs.setDbName("eurlanda");
		dbs.setDbType(DataBaseType.MYSQL);
		dbs.setHost("192.168.137.4:3306");
		dbs.setPort(0);
		dbs.setUserName("root");
		dbs.setPassword("root");
		try {
			INewDBAdapter adaoterSource= AdapterDataSourceManager.getAdapter(dbs);
			List<Map<String, Object>> list = adaoterSource.queryForList("select * from s_table1"); 
			if (list!=null&&list.size()>0){
				for (Map<String, Object> map : list) {
					System.out.println(map);
				}
			}
			adaoterSource.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}