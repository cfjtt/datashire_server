package com.eurlanda.datashire.utility;



import com.eurlanda.datashire.enumeration.DataBaseType;

/**
 * SQL处理工具类
 * 
 * @author dang.lu 2013-10-1
 *
 */
public class SQLUtils {

	/**
	 * 系统内部数据类型转换规则:
	 * 		SystemDatatype + DatabaseType + SourceDatatype
	 * 
	 * eg. MySQL.BIGINT (对应SystemDatatype.BIGINT): 
	 * 转换：MysqlDatatype.BIGINT.value()+DataTypeEnum.MYSQL.value()*100+SystemDatatype.BIGINT.value()*10000=10301
	 * 还原：DataTypeEnum = 10301%10000/100, SourceDatatype = 10301%100, SystemDatatype = 10301%10000
	 * 
	 * @param databaseType
	 * 			数据库类型枚举
	 * @param dataType
	 * 			数据类型名称
	 * 				sqlserver: SELECT NAME FROM sys.systypes
	 * 				mysql:  SELECT DATA_TYPE   FROM information_schema.COLUMNS
	 * 				oracle: SELECT COLUMN_NAME FROM USER_TAB_COLUMNS
	 * 				...
	 * @return
	 * @see DataTypeManager
	 */
	private static int encodeDataType(DataBaseType databaseType, String dataType){
		return 0;
	}
	
	
	  /**
	   * 生成分页查询SQL语句(支持SQLServer所有版本？效率？)
	   * @param currPage 当前页码
	   * @param pageSize 每页显示的记录数
	   * @param tableName 表名
	   * @param pk 主键名
	   * @param cols 要显示的列名（*或其他）
	   * @return
	   */
	    public static final String pageSQLServer(int currPage, int pageSize, 
	    		String tableName, String pk, String cols
	            /*, String condition, String order, boolean isDesc*/){
	    	StringBuilder str= new StringBuilder();
	        if(currPage == 1){
	           str.append("SELECT TOP ")
	           .append(pageSize).append(" ")
	           .append(cols)
	           .append(" FROM ")
	           .append(tableName);
	        }else{
	           str.append("SELECT TOP ")
	           .append(pageSize).append(" ")
	           .append(cols)
	           .append(" FROM ")
	           .append(tableName)
	           .append(" WHERE ")
	           .append(pk)
	           .append(" >=(SELECT MAX(T.")
	           .append(pk).append(") FROM (SELECT TOP ")
	           .append(pageSize*(currPage-1)+1).append(" ")
	           .append(pk)
	           .append(" FROM ")
	           .append(tableName)
	           .append(") AS T)");
	        }
	        return str.toString();
	    }
	    /**
		 * sqlserver filter *号转换%号
		 * @param filter
		 * @return
		 */
		public static String sqlserverFilter(String filter) {
			String filterOut = filter.replaceAll("\\*", "%");
			filterOut=filterOut.replaceAll("\\?","_");
			return filterOut;
		}
}
