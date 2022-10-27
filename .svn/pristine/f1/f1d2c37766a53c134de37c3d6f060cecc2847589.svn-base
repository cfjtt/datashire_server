package com.eurlanda.datashire.server.utils.dbsource;

import com.eurlanda.datashire.adapter.DataTypeManager;
import com.eurlanda.datashire.server.utils.entity.DBConnectionInfo;
import com.eurlanda.datashire.entity.SquidIndexes;
import com.eurlanda.datashire.server.utils.entity.operation.ColumnInfo;
import com.eurlanda.datashire.enumeration.DataBaseType;
import com.eurlanda.datashire.enumeration.datatype.DbBaseDatatype;
import com.eurlanda.datashire.utility.StringUtils;
import com.eurlanda.datashire.utility.objectsql.TemplateParser;

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
public class NewSqlServerAdapter extends AbsDBAdapter implements INewDBAdapter {
	public NewSqlServerAdapter(DBConnectionInfo info) {
		super(info);
	}
	@Override
	protected String buildColumnSql(ColumnInfo col) {
		String column_template = "${colName} ${colType} ${nullable} ${auto_increment}";
		TemplateParser parser = new TemplateParser();
		parser.addParam("colName", "["+col.getName()+"]");
		
		//String typeStr = DataTypeManager.decode(DataBaseType.SQLSERVER, col.getSystemDataType());
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
			parser.addParam("nullable", "NOT NULL");
		}
		if (col.isIdentity()) {
			parser.addParam("auto_increment", "IDENTITY"); 
		}
		String sql= parser.parseTemplate(column_template);
		return sql;
	}
	
	@Override
	public List<ColumnInfo> getTableColumns(final String tableName,String DatabaseName) {
		List<ColumnInfo> columnList = new ArrayList<ColumnInfo>();
		String[] schemaAndName = this.getSchemaAndName(tableName);
		String tbNm = tableName;
		String schema = "";
		if (schemaAndName!=null&&schemaAndName.length==2){
			tbNm = schemaAndName[1];
			schema = schemaAndName[0];
		}
		/*String colsql = "select t1.*,t2.name as Data_Type from sys.columns as t1 " +
				"inner join sys.types as t2 on t1.system_type_id=t2.system_type_id and t1.user_type_id=t2.user_type_id " +
				"where object_id in (select object_id from sys.objects where name='"+tbNm+"')";*/
		String colsql = "select * from [INFORMATION_SCHEMA].[COLUMNS] where table_name = '"+tbNm+"'";
		if (schema!=""){
			colsql += " and table_schema='"+schema+"'";
		}
		String pkSql="SELECT tab.name as TABLENAME,idx.name as IDXNAME,col.name as COLUMNNAME FROM  sys.indexes idx   JOIN sys.index_columns idxCol     ON (idx.object_id = idxCol.object_id    AND idx.index_id = idxCol.index_id     AND idx.is_primary_key = 1) JOIN sys.tables tab  ON (idx.object_id = tab.object_id)  JOIN sys.columns col  ON (idx.object_id = col.object_id  AND idxCol.column_id = col.column_id)  where tab.name='"+tbNm+"'";
		String uniqueSql="SELECT  tab.name AS TABLENAME, idx.name AS IDXNAME, col.name AS COLUMNNAME FROM sys.indexes idx  JOIN sys.index_columns idxCol    ON (idx.object_id = idxCol.object_id    AND idx.index_id = idxCol.index_id     AND idx.is_unique_constraint = 1)  JOIN sys.tables tab  ON (idx.object_id = tab.object_id) JOIN sys.columns col  ON (idx.object_id = col.object_id  AND idxCol.column_id = col.column_id)  where tab.name='"+tbNm+"'";
		List<Map<String,Object>> colList = this.jdbcTemplate.queryForList(colsql,null);
		List<Map<String,Object>> pkList = this.jdbcTemplate.queryForList(pkSql,null);
		List<Map<String,Object>> uniqueList = this.jdbcTemplate.queryForList(uniqueSql,null);
		
		for(Map<String,Object> col: colList){
			ColumnInfo column = new ColumnInfo();
			column.setTableName(tableName);
			//column.setComment(col.get("CONNATION_NAME")+"");
			column.setOrderNumber(col.get("ORDINAL_POSITION")==null?0:Integer.parseInt(col.get("ORDINAL_POSITION")+""));
			column.setName(col.get("COLUMN_NAME").toString());
			String typeName = col.get("DATA_TYPE").toString();
			//column.setSystemDataType(DataTypeManager.encodeSystemDataType(DataBaseType.ORACLE, typeName));
			column.setSystemDatatype(DataTypeManager.encodeSystemDataType(DataBaseType.SQLSERVER, typeName));
			column.setDbBaseDatatype(DbBaseDatatype.parse(typeName));
			column.setTypeName(typeName);
			//column.setLength(col.get("CHARACTER_OCTET_LENGTH")==null?0:Integer.parseInt(col.get("CHARACTER_OCTET_LENGTH")+""));
			column.setLength(StringUtils.isNull(col.get("CHARACTER_MAXIMUM_LENGTH"))?0:Integer.parseInt(col.get("CHARACTER_MAXIMUM_LENGTH")+""));
			column.setPrecision(StringUtils.isNull(col.get("NUMERIC_PRECISION"))?0:Integer.parseInt(col.get("NUMERIC_PRECISION")+""));
			column.setScale(StringUtils.isNull(col.get("NUMERIC_SCALE"))?0:Integer.parseInt(col.get("NUMERIC_SCALE")+""));
			if (col.get("IS_NULLABLE")!=null){
				column.setNullable(col.get("IS_NULLABLE").toString().equals("YES")?true:false);
			}else{
				column.setNullable(false);
			}
			//column.setNullable(true);
			//主键
			for(Map<String,Object> pk: pkList)
			{
				if(col.get("COLUMN_NAME").toString().equals(pk.get("COLUMNNAME").toString()))  
				{
					column.setPrimary(true);
					break;
				}
			}
			//是否唯一约束
			for(Map<String,Object> unique: uniqueList)
			{
				if(col.get("COLUMN_NAME").toString().equals(unique.get("COLUMNNAME").toString()))  
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
	
	@Override
	public void deleteIndex(String tableName, String indexName) {
		String temp = "DROP INDEX ${indexName} ON ${tableName}";
		TemplateParser parser = new TemplateParser();
		parser.addParam("tableName", tableName);
		parser.addParam("indexName", indexName);
		String sql = parser.parseTemplate(temp);
		logger.info("删除表索引:" + sql);
		this.jdbcTemplate.update(sql);
	}
	
    /**
     * 查询Sql server表的索引
     */
	@Override
	public List<SquidIndexes> selectTableIndex(String tableName){
		String indexSql = "SELECT                                                                                       "
				+ "TableId=O.[object_id],                                                                                 "
				+ "TableName=O.Name,                                                                                      "
				+ "IndexId=ISNULL(KC.[object_id],IDX.index_id),                                                           "
				+ "IndexName=IDX.Name,                                                                                    "
				+ "IndexType=ISNULL(KC.type_desc,'Index'),                                                                "
				+ "Index_Column_id=IDXC.index_column_id,                                                                  "
				+ "ColumnID=C.Column_id,                                                                                  "
				+ "ColumnName=C.Name,                                                                                     "
				+ "Sort=CASE INDEXKEY_PROPERTY(IDXC.[object_id],IDXC.index_id,IDXC.index_column_id,'IsDescending')        "
				+ "WHEN 1 THEN 'DESC' WHEN 0 THEN 'ASC' ELSE '' END,                                                      "
				+ "PrimaryKey=CASE WHEN IDX.is_primary_key=1 THEN N'√'ELSE N'' END,                                       "
				+ "[UQIQUE]=CASE WHEN IDX.is_unique=1 THEN N'√'ELSE N'' END,                                              "
				+ "Ignore_dup_key=CASE WHEN IDX.ignore_dup_key=1 THEN N'√'ELSE N'' END,                                   "
				+ "Disabled=CASE WHEN IDX.is_disabled=1 THEN N'√'ELSE N'' END,                                            "
				+ "Fill_factor=IDX.fill_factor,                                                                           "
				+ "Padded=CASE WHEN IDX.is_padded=1 THEN N'√'ELSE N'' END                                                 "
				+ "FROM sys.indexes IDX                                                                                   "
				+ "INNER JOIN sys.index_columns IDXC                                                                      "
				+ "ON IDX.[object_id]=IDXC.[object_id]                                                                    "
				+ "AND IDX.index_id=IDXC.index_id                                                                         "
				+ "LEFT JOIN sys.key_constraints KC                                                                       "
				+ "ON IDX.[object_id]=KC.[parent_object_id]                                                               "
				+ "AND IDX.index_id=KC.unique_index_id                                                                    "
				+ "INNER JOIN sys.objects O                                                                               "
				+ "ON O.[object_id]=IDX.[object_id]                                                                       "
				+ "INNER JOIN sys.columns C                                                                               "
				+ "ON O.[object_id]=C.[object_id]                                                                         "
				+ "AND O.type='U'                                                                                         "
				+ "AND O.is_ms_shipped=0                                                                                  "
				+ "AND IDXC.Column_id=C.Column_id  where O.name='" + tableName + "'                                       ";
		// 取得Index
		List<Map<String,Object>> indexList = this.jdbcTemplate.queryForList(indexSql,null);
		if(StringUtils.isNull(indexList) || indexList.isEmpty()){
			return null;
		}
		List<SquidIndexes> squidIndexList = new ArrayList<SquidIndexes>();
		SquidIndexes squidIndexes = null;
		Map<String, Object> indexMap = null;
		Integer indexId = 0;
		Integer index_column_id = 0;
		for (int i = 0; i < indexList.size(); i++) {
			indexMap = indexList.get(i);
			indexId = Integer.parseInt(indexMap.get("INDEXID").toString());
			if(i == 0){
				squidIndexes = new SquidIndexes();
				squidIndexes.setIndex_name(indexMap.get("INDEXNAME").toString());
			}
			if(i > 1 && !indexId.equals(Integer.parseInt(indexList.get(i-1).get("INDEXID").toString()))){
				squidIndexList.add(squidIndexes);
				squidIndexes = new SquidIndexes();
				squidIndexes.setIndex_name(indexMap.get("INDEXNAME").toString());
			}
			index_column_id = Integer.parseInt(indexMap.get("INDEX_COLUMN_ID").toString());
//			squidIndexes.setColumn_name_1(indexList.get(i).get("COLUMNNAME").toString());
			setColumnName(index_column_id, indexMap.get("COLUMNNAME").toString(), squidIndexes);
			if(i == indexList.size() - 1){
				squidIndexList.add(squidIndexes);
			}
		}
		return squidIndexList;
	}
	
	/**
	 * 设置columnName
	 * @param index_column_id
	 * @param columnName
	 * @param squidIndexes
	 * @return
	 * @author bo.dang
	 */
	public SquidIndexes setColumnName(Integer index_column_id, String columnName, SquidIndexes squidIndexes){
		switch(index_column_id){
		case 1:
			squidIndexes.setColumn_name_1(columnName);
			break;
		case 2:
			squidIndexes.setColumn_name_2(columnName);
			break;
		case 3:
			squidIndexes.setColumn_name_3(columnName);
			break;
		case 4:
			squidIndexes.setColumn_name_4(columnName);
			break;
		case 5:
			squidIndexes.setColumn_name_5(columnName);
			break;
		case 6:
			squidIndexes.setColumn_name_6(columnName);
			break;
		case 7:
			squidIndexes.setColumn_name_7(columnName);
			break;
		case 8:
			squidIndexes.setColumn_name_8(columnName);
			break;
		case 9:
			squidIndexes.setColumn_name_9(columnName);
			break;
		case 10:
			squidIndexes.setColumn_name_10(columnName);
			break;
		
		}
		return squidIndexes;
	}
	
	public static void main(String[] args) {
		DBConnectionInfo dbs = new DBConnectionInfo();
		dbs.setDbName("test2");
		//dbs.setDbName("eurlanda");
		dbs.setDbType(DataBaseType.SQLSERVER);
		dbs.setHost("192.168.137.1\\aaaa:1433");
		dbs.setPort(0);
		dbs.setUserName("sa");
		dbs.setPassword("squiding@eurlanda");
		try {
			INewDBAdapter adaoterSource= AdapterDataSourceManager.getAdapter(dbs);
			List<Map<String, Object>> list = adaoterSource.queryForList("select * from sys.objects"); 
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