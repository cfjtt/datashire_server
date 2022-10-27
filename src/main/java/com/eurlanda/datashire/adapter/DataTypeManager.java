package com.eurlanda.datashire.adapter;

import com.eurlanda.datashire.enumeration.DataBaseType;
import com.eurlanda.datashire.enumeration.datatype.HbaseDatatype;
import com.eurlanda.datashire.enumeration.datatype.MysqlDatatype;
import com.eurlanda.datashire.enumeration.datatype.OracleDataType;
import com.eurlanda.datashire.enumeration.datatype.SqlServerDataType;
import com.eurlanda.datashire.enumeration.datatype.SystemDatatype;
import com.eurlanda.datashire.utility.StringUtils;
import org.apache.log4j.Logger;

import java.text.MessageFormat;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * DataShire数据类型映射
 * 
 * 1. 系统内部数据类型转换规则:
	 * 		SystemDatatype + DatabaseType + SourceDatatype
	 * 
	 * eg. MySQL.BIGINT (对应SystemDatatype.BIGINT): 
	 * 转换：MysqlDatatype.BIGINT.value()+DataTypeEnum.MYSQL.value()*100+SystemDatatype.BIGINT.value()*10000=10301
	 * 还原：DataTypeEnum = 10301%10000/100, SourceDatatype = 10301%100, SystemDatatype = 10301%10000

   2. 数据类型名称
		sqlserver: SELECT NAME FROM sys.systypes
		mysql:  SELECT DATA_TYPE   FROM information_schema.COLUMNS
		oracle: SELECT COLUMN_NAME FROM USER_TAB_COLUMNS
		
   3. source column保存原始数据类型和原始数据库类型以及系统内部自定义类型；column和reference column只保存系统内部类型
		
 * @author dang.lu 2014.1.11
 *
 */
public class DataTypeManager {
	private static final Logger logger = Logger.getLogger(DataTypeManager.class);
	// 数据类型映射 key：系统内部类型，value：sqlserver2012/mysql5/oracle11g ...
	private static Map<String, String> SQL2012Type = new TreeMap<String, String>();
	private static Map<String, String> MySQL5Type = new TreeMap<String, String>();
	private static Map<String, String> ORA11GType = new TreeMap<String, String>();
	private static Map<String, String> HbasePhoenixType = new TreeMap<String, String>();
	
	// key:databaseType+dataType, value:systemDataType
	private static Map<String, String> DBDataTypeMap = new TreeMap<String, String>();
	
	
	/**
	 * 外部数据库数据类型转换为系统内部类型
	 * 
	 * @param databaseType
	 * @param dataType 数据类型名称
	 * 				mysql:  SELECT DATA_TYPE   FROM information_schema.COLUMNS
	 * 				oracle: SELECT COLUMN_NAME FROM USER_TAB_COLUMNS
	 * 				sqlserver: SELECT NAME FROM sys.systypes
	 * @return
	 */
	public static int encode(DataBaseType databaseType, String dataType){
		logger.debug("[数据类型映射] dataType: "+dataType+", databaseType: "+databaseType);
		if(StringUtils.isNotNull(dataType)){
			dataType = dataType.trim().toUpperCase();
			SystemDatatype sys = null;
			try {
				sys = SystemDatatype.parse(dataType);
			} catch (IllegalArgumentException e) { // 优先匹配类型名称一致的（1对1约定优于配置）
			}
			if(sys==null || sys==SystemDatatype.UNKNOWN){
				String typeName = DBDataTypeMap.get(databaseType.name().concat("_").concat(dataType));
				if(StringUtils.isNotNull(typeName)) sys = SystemDatatype.parse(typeName);
			}
			if(sys!=null && sys!=SystemDatatype.UNKNOWN){
				int type = sys.value()*10000 + databaseType.value()*100;
				switch(databaseType){
					case SQLSERVER: return type + SqlServerDataType.parse(dataType).value();
					case MYSQL: 	return type + MysqlDatatype.parse(dataType).value();
					case ORACLE: 	return type + 0; //TODO
					// ...
					default: logger.warn("dataType: "+dataType+", unknow databaseType: "+databaseType); return 0;
				}
			}else{
				logger.warn("unsupported dataType: "+dataType+", databaseType: "+databaseType);
			}
		}
		logger.warn("null type: "+dataType+", databaseType: "+databaseType);
		return 0;
	}
	
	public static SystemDatatype encodeSystemDataType(DataBaseType databaseType, String dataType){
		logger.trace("[数据类型映射] dataType: "+dataType+", databaseType: "+databaseType);
		SystemDatatype sys = null;
		if(StringUtils.isNotNull(dataType)){
			dataType = dataType.trim().toUpperCase();
			try {
				sys = SystemDatatype.parse(dataType);
			} catch (IllegalArgumentException e) { // 优先匹配类型名称一致的（1对1约定优于配置）
			}
			if(sys==null || sys==SystemDatatype.UNKNOWN){
				String typeName = DBDataTypeMap.get(databaseType.name().concat("_").concat(dataType));
				if(StringUtils.isNotNull(typeName)) sys = SystemDatatype.parse(typeName);
			}
		}
		return sys==null ? SystemDatatype.UNKNOWN : sys;
	}

	/**
	 * 系统内部统一使用自定义的数据类型
	 * @param type
	 * @return
	 */
	public static int decode(int type){
		return type/10000;
	}
	
	/**
	 * 内部数据类型转成默认外部数据类型
	 * 	默认转成第一个外部数据类型
	 * 	用户自定义为OTHER类型又没有及时更改的，不能落地
	 * 
	 * @param databaseType
	 * @param type
	 * @return
	 */
	public static String decode(DataBaseType databaseType, SystemDatatype sys){
		
		//type = decode(type);
		if(databaseType==null || sys==null || sys==SystemDatatype.UNKNOWN){
			throw new RuntimeException(MessageFormat.format("unknow data type:{2}, cannot convert to target database:{1}", databaseType, sys));
		}
		Enum<?> e = null;
		switch(databaseType){
			case SQLSERVER:
				e = SqlServerDataType.parse(sys.name());
				return e!=SqlServerDataType.UNKNOWN?e.name():SQL2012Type.get(sys.name()).split(",")[0];
			case MYSQL: 	
				e = MysqlDatatype.parse(sys.name());
				return e!=MysqlDatatype.UNKNOWN?e.name():MySQL5Type.get(sys.name()).split(",")[0];
			case HBASE_PHOENIX:
				e = HbaseDatatype.parse(sys.name());
				return e!=HbaseDatatype.UNKNOWN?e.name():HbasePhoenixType.get(sys.name()).split(",")[0];
			case ORACLE:
				e = OracleDataType.parse(sys.name());
				return e!=OracleDataType.UNKNOWN?e.name():ORA11GType.get(sys.name()).split(",")[0];
			default: return null;
		}
	}
	public static String decode(DataBaseType databaseType, int type){
		logger.debug("[数据类型还原] dataType: "+type+", databaseType: "+databaseType);
		SystemDatatype sys = SystemDatatype.parse(type);
		return decode(databaseType,sys);
	}
	
	
	static{
		init(); // 初始化配置
		autoMpping(); // source->extract,外部系统数据类型自动映射为系统自定义的数据类型
	}
	private static void autoMpping(){
		for(String sysType : SQL2012Type.keySet()){
			for(String dataType : SQL2012Type.get(sysType).split(",")){
				DBDataTypeMap.put(DataBaseType.SQLSERVER.name().concat("_").concat(dataType), sysType);
			}
		}
		for(String sysType : MySQL5Type.keySet()){
			for(String dataType : MySQL5Type.get(sysType).split(",")){
				DBDataTypeMap.put(DataBaseType.MYSQL.name().concat("_").concat(dataType), sysType);
			}
		}
		for(String sysType : ORA11GType.keySet()){
			for(String dataType : ORA11GType.get(sysType).split(",")){
				DBDataTypeMap.put(DataBaseType.ORACLE.name().concat("_").concat(dataType), sysType);
			}
		}
		for(String sysType : HbasePhoenixType.keySet()){
			for(String dataType : HbasePhoenixType.get(sysType).split(",")){
				DBDataTypeMap.put(DataBaseType.HBASE_PHOENIX.name().concat("_").concat(dataType), sysType);
			}
		}
	}
	private static void init(){ // 以下代码由excel函数生成（请参考：数据类型映射关系表.xls）
		SQL2012Type.put("VARBINARY","IMAGE");
		SQL2012Type.put("DOUBLE","NUMERIC");
		SQL2012Type.put("FLOAT","MONEY");
		SQL2012Type.put("REAL","SMALLMONEY");
		SQL2012Type.put("TIMESTAMP","DATETIME,DATETIME2,DATETIMEOFFSET,SMALLDATETIME");
		SQL2012Type.put("CHAR","UNIQUEIDENTIFIER");
		SQL2012Type.put("VARCHAR","HIERARCHYID,SYSNAME");
		SQL2012Type.put("INT","INT IDENTITY");
		
		/*SQL2012Type.put("BIGINT", "BIGINT,TIMESTAMP,ROWVERSOIN");
		SQL2012Type.put("INT", "INT");
		SQL2012Type.put("SMALLINT", "SMALLINT");
		SQL2012Type.put("TINYINT", "TINYINT");
		SQL2012Type.put("BIT", "BIT");
		SQL2012Type.put("DECIMAL", "DECIMAL,NUMERIC,MONEY,SMALLMONEY");
		SQL2012Type.put("FLOAT", "FLOAT,REAL");
		SQL2012Type.put("DATETIME", "DATETIME,SMALLDATETIME,DATE,DATETIMEOFFSET,DATETIME2");
		SQL2012Type.put("TIME", "TIME");
		SQL2012Type.put("NCHAR", "NCHAR,CHAR,UNIQUEIDENTIFIER");
		SQL2012Type.put("NVARCHAR", "NVARCHAR,VARCHAR,XML");
		SQL2012Type.put("BINARY", "BINARY");
		SQL2012Type.put("VARBINARY", "VARBINARY,IMAGE,TEXT,NTEXT,GEOMETRY,GEOGRAPHY,HIERARCHYID");*/
		
		MySQL5Type.put("BINARY","BLOB,LONGBLOB,TINYBLOB");
		MySQL5Type.put("VARBINARY","BLOB,LONGBLOB,TINYBLOB");
		MySQL5Type.put("REAL","FLOAT");
		MySQL5Type.put("BIT","BOOL,BOOLEAN");
		MySQL5Type.put("DATE","DATETIME,YEAR");
		MySQL5Type.put("TIMESTAMP","DATETIME");
		MySQL5Type.put("INT","MEDIUMINT");
		MySQL5Type.put("TEXT","LONGTEXT,MEDIUMTEXT");
		MySQL5Type.put("NTEXT","TEXT");
		
		ORA11GType.put("BINARY","BLOB");
		ORA11GType.put("VARBINARY","LONG RAW,RAW");
		ORA11GType.put("DECIMAL","NUMBER,BINARY_FLOAT,BINARY_DOUBLE");
		ORA11GType.put("DOUBLE","NUMBER");
		ORA11GType.put("BIT","CHAR(1)");
		ORA11GType.put("TIME","TIMESTAMP(8)");
		ORA11GType.put("BIGINT","NUMBER");
		ORA11GType.put("INT","NUMBER,INTEGER");
		ORA11GType.put("SMALLINT","NUMBER");
		ORA11GType.put("TINYINT","NUMBER");
		ORA11GType.put("VARCHAR","VARCHAR2,LONG");
		ORA11GType.put("NVARCHAR","NVARCHAR2");
		ORA11GType.put("TEXT","CLOB");
		ORA11GType.put("NTEXT","NCLOB");
		
		HbasePhoenixType.put("BIGINT", "BIGINT");
		HbasePhoenixType.put("INT", "INTEGER");
		HbasePhoenixType.put("FLOAT", "FLOAT");
		HbasePhoenixType.put("DOUBLE", "DOUBLE");
		HbasePhoenixType.put("TINYINT", "TINYINT");
		HbasePhoenixType.put("DECIMAL", "DECIMAL");
		HbasePhoenixType.put("VARCHAR", "VARCHAR,NVARCHAR");
		HbasePhoenixType.put("BIT", "BOOLEAN");
		HbasePhoenixType.put("CHAR", "CHAR,NCHAR");
		HbasePhoenixType.put("TIME", "TIME");
		HbasePhoenixType.put("TIMESTAMP","UNSIGNED_TIMESTAMP");
		HbasePhoenixType.put("BINARY", "BINARY");
		HbasePhoenixType.put("SMALLINT", "NUMBER");
		HbasePhoenixType.put("DATE", "DATE");
		HbasePhoenixType.put("VARBINARY", "VARBINARY");
	}
	public static void main(String[] args) {
		//print(DBDataTypeMap, true);
		
		/*StringBuffer buffer = new StringBuffer();
		for(Enum e : SqlServerDataType.values()){
			buffer.append("\r\n").append(DataBaseType.SQLSERVER+"\t"+encode(DataBaseType.SQLSERVER, e.name())+"\t"+e.name());
		}
		for(Enum e : MysqlDatatype.values()){
			buffer.append("\r\n").append(DataBaseType.MYSQL+"\t"+encode(DataBaseType.MYSQL, e.name())+"\t"+e.name());
		}
		System.out.println(buffer.toString());*/
	}
	public static void print(Map map, boolean showIndex){
		if(map==null) return;
		StringBuffer buffer = new StringBuffer();
		 Iterator it = map.keySet().iterator();
		 Object tmp=null;
		 int index=1;
		 while(it.hasNext()){
			 tmp = it.next();
			if(showIndex) buffer.append(index+++"\t"+tmp+"\t\t\t"+map.get(tmp)+"\r\n");
			else buffer.append(tmp+"\t"+map.get(tmp)+"\r\n");
		 }
		 System.out.println(buffer.toString());
	}
}

