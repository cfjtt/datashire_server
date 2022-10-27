package com.eurlanda.datashire.adapter;

import com.eurlanda.datashire.entity.Column;
import com.eurlanda.datashire.entity.DbSquid;
import com.eurlanda.datashire.entity.SourceTable;
import com.eurlanda.datashire.entity.operation.ColumnInfo;
import com.eurlanda.datashire.entity.operation.ResultSets;
import com.eurlanda.datashire.entity.operation.WhereCondition;
import com.eurlanda.datashire.utility.CommonConsts;
import com.eurlanda.datashire.utility.DbUtils;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import com.eurlanda.datashire.utility.SQLUtils;
import com.eurlanda.datashire.utility.StringUtils;
import org.apache.log4j.Logger;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 *HsqlDbAdapter 
 *针对Hsql数据库的增删改操作
 * <p>
 * Title : 
 * </p>
 * <p>
 * Description: 
 * </p>
 * <p>
 * Author :赵春花 2013-8-26
 * </p>
 * <p>
 * update :赵春花2013-8-26
 * </p>
 * <p>
 * Department :  JAVA后端研发部
 * </p>
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司
 * </p>
 */
public class HsqlDbAdapter extends AbsStatementSqlAdapter {

	static Logger logger = Logger.getLogger(HsqlDbAdapter.class);

	public HsqlDbAdapter(DbSquid dataSource) {
		super(dataSource);
	}

	/**
	 *  创建表（落地创建表结构）
	 */
	public boolean createTable(String tableName, List<Column> columns, ReturnValue out) {
		boolean create = false;
		String column = null;//包装要插入的列名
		StringBuffer columnList = new StringBuffer();
		//拼接列的信息
		for (Column eColumn : columns) {
			columnList.append(eColumn.getName()).append(" ");
			columnList.append(eColumn.getData_type()).append(" ");
			columnList.append(eColumn.isNullable()?" NULL":"  NOT NULL ");
			columnList.append(" ,");
		}
		column = columnList.substring(0,columnList.length()-1);//截取
		StringBuffer sql = new StringBuffer();
		sql.append("CREATE TABLE ");
		sql.append(tableName);
		sql.append("( ");
		sql.append( column);
		sql.append(" ) ");
		//执行SQL
		if(this.executeAll(sql.toString(), out)>0){
			create = true;
		}else{
			create = false;
		}
		return create;
	}

	public void dropTable(String name, ReturnValue out) {
		if(name==null||name.equals("")){
			out.setMessageCode(MessageCode.ERR_TABLENAME_NULL);
			return;
		}
		StringBuffer sqlbBuffer = new StringBuffer();
		sqlbBuffer.append(" DROP TABLE ");
		sqlbBuffer.append(name);
		executeAll(sqlbBuffer.toString(), out);
	}

	/**
	 * 查询
	 */
	public List query(String sql, ReturnValue out) {
		//记录日
		logger.debug(String.format("query—sql=%s", sql));
		//定义集合接收
		//调用查询方法
		try {
			List<Map<String, Object>> dataList = super.selectAll(sql, out);
			//判断是否有数据
			if(dataList==null||dataList.size()==0){
			//异常记录
			out.setMessageCode(MessageCode.NODATA);
			return null;
			}
			//记录日志
			logger.debug(String.format("query—sql=%s;result=%s", sql, dataList));
			//返回结果
			return dataList;
			} catch (Exception e) {
			//异常记录
			out.setMessageCode(MessageCode.SQL_ERROR);
			//日志记录
			logger.error("Exception",e);
			}
		return null;
	}

	/**
	 * 根据
	 */
	public List<Map<String, Object>> query(List<String> columns, String tableName, List<WhereCondition> whereCond,
			ReturnValue out) {
		if(StringUtils.isNull(tableName)){ //表名为空
			out.setMessageCode(MessageCode.ERR_TABLENAME_NULL);
			return null;
		}
		StringBuffer sqlbBuffer = new StringBuffer();// 拼接查询语句
		StringBuffer conditions = new StringBuffer();// 包装条件项
		List<Map<String, Object>> dataList = null;// 接收查询结果
		//查看是否有条件
		if (whereCond!=null&&whereCond.size()>0) {
			// 条件拼接方法
			bindCondition(whereCond, conditions);
			sqlbBuffer.append(" SELECT ");
			sqlbBuffer.append(this.bindColumns(columns));
			sqlbBuffer.append(" FROM ");
			sqlbBuffer.append(tableName);
			sqlbBuffer.append(" WHERE 1=1 ");
			sqlbBuffer.append(conditions);
			String sql = sqlbBuffer.toString();
			dataList = super.selectAll(sqlbBuffer.toString(), out);// 接收查询结果（传入查询值）
		} else {
			sqlbBuffer.append(" SELECT ");
			sqlbBuffer.append(this.bindColumns(columns));
			sqlbBuffer.append(" FROM ");
			sqlbBuffer.append(tableName);	
			String sql = sqlbBuffer.toString();
			dataList = super.selectAll(sqlbBuffer.toString(), out);// 接收查询结果
		}
		logger.debug(sqlbBuffer.toString());
		if(dataList==null||dataList.size()==0){
			//异常记录
			out.setMessageCode(MessageCode.NODATA);
			return null;
		}else{
			out.setMessageCode(MessageCode.SUCCESS);
		}
		return dataList;
	}

	
	 /**
	  * 执行Sql
	  */
	public int executeSQL(String sql, ReturnValue value) {
		logger.debug(String.format("executeSQL-sql", sql));
		return this.executeAll(sql, value);
	}


	public Map<String, Object> queryRecord(List<String> columns, String tableName, List<WhereCondition> whereCond,
			ReturnValue out) {
		logger.debug(String.format("queryOneRecord_columns=%s,tableName=%s,whereCond=%s", columns,
				tableName,whereCond));
		try {
			//调用查询方法
			List<Map<String, Object>>  resultList = this.query(columns, tableName, whereCond, out);
			//判断是否获得数据
			if(resultList==null||resultList.size()==0){
				//没有数据
				out.setMessageCode(MessageCode.NODATA);
				//返回
				return null;
			}
			//得到结果是否唯一
		//	if(resultList.size()==1){
				//获得数据返回
				Map<String, Object>  result = resultList.get(0);
				return result;
			//}else{
				//异常处理
				//out.setMessageCode(MessageCode.DATA_IS_NOT_THE_ONLY);
				//返回结果
			//	return null;
			//}
					
		} catch (Exception e) {
			//异常记录
			out.setMessageCode(MessageCode.SQL_ERROR);
			//日志
			logger.error("Exception",e);
		}
		//返回结果
		return null;
	}

	public Map<String, Object> queryOneRecord(List<String> columns, String name, ReturnValue out) {
		logger.debug(String.format("queryOneRecord_columns=%s,tableName=%s", columns,name));
		try {
			if(name==null||name.equals("")){
				//异常记录
				out.setMessageCode(MessageCode.ERR_TABLENAME_NULL);
				return null;
			}
			Map<String, Object> map =  this.queryRecord(columns, name, null, out);// 调用方法
			//判断是否获得数据
			if(map==null||map.size()==0){
				//没有数据
				out.setMessageCode(MessageCode.NODATA);
				return null;
			}
			//返回结果
			return map;
		} catch (Exception e) {
			//异常记录
			out.setMessageCode(MessageCode.SQL_ERROR);
			//日志记录
			logger.error("Exception",e);
		}
		return null;
	}

	public List<ColumnInfo> getColums(String name, ReturnValue out) {
		// TODO Auto-generated method stub
		return null;
	}

	public SourceTable[] getTables(ReturnValue out,String filter) {
		// TODO Auto-generated method stub
		return null;
	}

	public ResultSets queryResultSets(List<String> columns, String tableName, List<WhereCondition> whereCond,
			ReturnValue out) {
		// TODO Auto-generated method stub
		return null;
	}

	public ResultSets selectResultSets(String sql, ReturnValue out) {
		// TODO Auto-generated method stub
		return null;
	}

	public ResultSets selectResultSets(String sql, Object[] objects, ReturnValue out) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean createRepository(ReturnValue value) {
		boolean create = false;
		String sql = StringUtils.toString(CommonConsts.Repository_SQL_PATH, "UTF-8");
		try {
			for (String string : sql.split(";")) {
				if(StringUtils.isNull(string)) continue;
				if(super.executeAll(string.trim(), value)>0){
					create = true;
				}else{
					create = false;
					//退出循环
					break;
				}
			}
		} catch (Exception e) {
			 value.setMessageCode(MessageCode.ERR_URL);
			logger.error("Exception",e);
		}
		return create;
	}

	/** 获取指定表的所有列名、列类型 */
	private Map<String, String> getAllColumnInfo(String tableName) throws SQLException {
		Map<String, String> columnType = new HashMap<String, String>();
		ResultSet rs = null;
		ResultSetMetaData md = null;
		Statement st = null;
		try{
			st = connection.createStatement();
			rs = st.executeQuery("select top 0 * from "+tableName);
			md = rs.getMetaData();
			for (int i=md.getColumnCount(); i>=1; i--) {
				columnType.put(md.getColumnName(i), md.getColumnTypeName(i));
			}
		}finally{
			DbUtils.closeQuietly(null, st, rs);
		}
		return columnType;
	}
	/** 获取指定表的总记录数 */
	private int getTotalCount(String tableName) throws SQLException {
		int totalCount = 0;
		ResultSet rs = null;
		Statement st = null;
		try{
			st = connection.createStatement();
			rs = st.executeQuery("select count(*) from "+tableName);
			if(rs!=null && rs.next()){
				totalCount = rs.getInt(1);
			}
		}finally{
			DbUtils.closeQuietly(null, st, rs);
		}
		return totalCount;
	}
	
	/**
	 * 获取指定表数据
	 * @param currPage
	 * @param pageSize
	 * @param tableName
	 * @return
	 */
	public ResultSets queryAll(int currPage, int pageSize, String tableName) {
		if (connection==null/* || connection.isClosed() */|| tableName==null) return null;
		ResultSets resultSets = new ResultSets();
		ResultSet rt = null; //接收结果
		Statement pt = null;
		String sql = null;
		Map<String, String> columnType = null;
		Iterator<String> col = null;
		String pk = "ID"; // 默认主键
		String cols = "*"; // 要查询的列名 (TODO 将*转为所有列)
		String tmpStr = null;
		int colCnt = 0; // 列数
		try {
			columnType = getAllColumnInfo(tableName);
			colCnt = columnType.size();
			resultSets.setColumnList(columnType);
			resultSets.setDataCount(getTotalCount(tableName));
			col = columnType.keySet().iterator();
			while(col.hasNext()){
				tmpStr = col.next();
				if(!pk.equalsIgnoreCase(tmpStr)){
					pk = tmpStr;
				}
				break;
			}
			// hsqldb 也支持SQL Server分页？
			sql = SQLUtils.pageSQLServer(currPage, pageSize, tableName, pk, cols);
			logger.debug("QueryAll SQL: "+sql);
			pt = connection.createStatement();
			rt = pt.executeQuery(sql);
			if(rt!=null){
				List<Map<String, String>> list = new ArrayList<Map<String, String>>(pageSize);
				Map<String, String> map = null;
				ResultSetMetaData md = rt.getMetaData(); 
				int i;
				while (rt.next()) {
					map = new HashMap<String, String>(colCnt);
					for (i=1; i<=colCnt; i++) { // 是否考虑日期格式、BLOB、CLOB？
						map.put(md.getColumnName(i), rt.getString(i));
					}
					list.add(map);
				}
				resultSets.setDataList(list);
			}
		} catch (Exception e) {
			logger.error("QueryAll Err", e);
		}finally{
			DbUtils.closeQuietly(null, pt, rt);
		}
		return resultSets;
	}
}