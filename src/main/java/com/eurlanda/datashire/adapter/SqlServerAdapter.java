package com.eurlanda.datashire.adapter;

import com.eurlanda.datashire.entity.Column;
import com.eurlanda.datashire.entity.DbSquid;
import com.eurlanda.datashire.entity.SourceTable;
import com.eurlanda.datashire.entity.operation.ColumnInfo;
import com.eurlanda.datashire.entity.operation.ResultSets;
import com.eurlanda.datashire.entity.operation.WhereCondition;
import com.eurlanda.datashire.enumeration.datatype.SqlServerDataType;
import com.eurlanda.datashire.enumeration.datatype.SystemDatatype;
import com.eurlanda.datashire.utility.DbUtils;
import com.eurlanda.datashire.utility.EnumException;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import com.eurlanda.datashire.utility.SQLUtils;
import com.eurlanda.datashire.utility.StringUtils;
import org.apache.log4j.Logger;

import java.sql.PreparedStatement;
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
 * adapter for SQL Server
 * 
 * @author eurlanda
 * @version 1.0
 * @updated 19-八月-2013 8:50:25
 */
public class SqlServerAdapter extends AbsSqlAdapter {
	static Logger logger = Logger.getLogger(SqlServerAdapter.class);
	public SqlServerAdapter(DbSquid dataSource) {
		super(dataSource);
	}

	/**
	 * 从当前位置删除一个table（对象被移走，并非单单数据）
	 * 
	 * @param tableName
	 *            要移走的table的名字
	 * @param ret
	 */
	public void dropTable(String tableName, ReturnValue ret) {
		if (tableName == null || tableName.equals("")) {
			ret.setMessageCode(MessageCode.ERR_TABLENAME_NULL);
			return;
		}
		StringBuffer sqlbBuffer = new StringBuffer();
		sqlbBuffer.append(" DROP TABLE ");
		sqlbBuffer.append(tableName);
		executeAll(sqlbBuffer.toString(), ret);
	}

	/**
	 * 
	 * 创建简单的表结构
	 * 调用此方法前确保表名不为空
	 * columns对象集合不为空
	 */
	public boolean createTable(String tableName, List<Column> columns, ReturnValue out) {
		boolean create = false;
		String column = null;//包装要插入的列名
		StringBuffer columnList = new StringBuffer();
		//拼接列的信息
		for (Column eColumn : columns) {
			logger.debug("name="+eColumn.getName()
					+", type="+eColumn.getData_type()
					+", len="+eColumn.getLength()
					+", precision="+eColumn.getPrecision()
					+", null="+eColumn.isNullable());
			// 发现重复列告警？
			try {
				SqlServerDataType type = SqlServerDataType.valueOf(eColumn.getData_type());
				columnList.append(eColumn.getName()).append(" ");
				//TODO 有很多情况不能指定length (SQLServerException: 第 1 个列、参数或变量: 不能对数据类型 real 指定列宽。)
				if(false && eColumn.getLength()>0 && eColumn.getData_type()!=SqlServerDataType.INT.value()
						&& eColumn.getData_type()!=SqlServerDataType.SMALLINT.value()){
					columnList.append(type.toString()).append("("+eColumn.getLength()+") ");
				}else{
					columnList.append(type.toString()).append(" ");
				}
				columnList.append(eColumn.isNullable()?" NULL":"  NOT NULL");
				columnList.append(",");
			} catch (EnumException e) {
				//异常处理
				out.setMessageCode(MessageCode.ERR_ENUM);
				logger.warn("美剧找不到异常！", e);
			}
		}
		column = columnList.substring(0,columnList.length()-1);//截取
		StringBuffer sql = new StringBuffer();
		sql.append("CREATE TABLE ");
		sql.append(tableName);
		sql.append("( ");
		sql.append( column);
		sql.append(" ) ");
		
		//执行SQL
		if(super.executeAll(sql.toString(), out)>0){
			create = true;
			//创建成功
			out.setMessageCode(MessageCode.SUCCESS);
		}else{
			create = false;
		}
		logger.debug(String.format("createTable_Return", create));
		return create;
	}

	/**
	 * 根据语句查询
	 * 调用此方法钱保证Sql语句不为空
	 */
	public List<Map<String, Object>> query(String sql, ReturnValue out) {
		logger.debug(String.format("query_sq1", sql));
		// 记录日志
		logger.debug(String.format("query—sql=%s", sql));
		// 定义集合接收
		// 调用查询方法
		try {
			List<Map<String, Object>> dataList = super.selectAll(sql, out);
			logger.debug(String.format("query_Return", dataList));
			// 判断是否有数据
			if (dataList == null || dataList.size() == 0) {
				// 异常记录
				out.setMessageCode(MessageCode.NODATA);
				return null;
			}
			// 记录日志
			logger.debug(String.format("query—sql=%s;result=%s", sql, dataList));
			// 返回结果
			return dataList;
		} catch (Exception e) {
			// 异常记录
			out.setMessageCode(MessageCode.SQL_ERROR);
			// 日志记录
			logger.error("Exception",e);
		}
		logger.debug("query_Return null");
		return null;
	}

	/**
	 * 根据传入条件 拼接语句查询
	 * 调用方法前确保TableName不为空
	 * 
	 * 
	 */
	public List<Map<String, Object>> query(List<String> columns, String tableName, List<WhereCondition> whereCond,
			ReturnValue out) {
		logger.debug(String.format("query-columns=%s;tableName=%s;whereCone=%s", columns, tableName, whereCond));
		StringBuffer sqlbBuffer = new StringBuffer();// 拼接查询语句
		StringBuffer conditions = new StringBuffer();// 包装条件项
		List<Object> str = new ArrayList<Object>();// 值
		List<String> datas = new ArrayList<String>();// 接收数据
		List<Map<String, Object>> dataList = new ArrayList<Map<String, Object>>();// 接收查询结果
		// 查看是否有条件
		if (whereCond != null && whereCond.size() > 0) {
			// 条件拼接方法
			bindCondition(whereCond, conditions, str);
			if (str == null || str.size() == 0) {
				// 异常处理
				out.setMessageCode(MessageCode.ERR_CONDITION_BIND);
				return null;
			}
			Object[] objects = new Object[str.size()];// 参数数组
			str.toArray(objects);
			sqlbBuffer.append(" SELECT ");
			sqlbBuffer.append(this.bindColumns(columns));
			sqlbBuffer.append(" FROM ");
			sqlbBuffer.append(tableName);
			sqlbBuffer.append(" WHERE 1=1 ");
			sqlbBuffer.append(conditions);
			dataList = super.selectAll(sqlbBuffer.toString(), objects, out);// 接收查询结果（传入查询值）

		} else {
			sqlbBuffer.setLength(0);// 清空语句
			sqlbBuffer.append(" SELECT ");
			sqlbBuffer.append(this.bindColumns(columns));
			sqlbBuffer.append(" FROM ");
			sqlbBuffer.append(tableName);

			dataList = super.selectAll(sqlbBuffer.toString(), out);// 接收查询结果
		}
		if (dataList == null || dataList.size() == 0) {
			// 异常记录
			out.setMessageCode(MessageCode.NODATA);
			return null;
		}
		// 没有传入列的情况下
		if (datas == null || datas.size() == 0) {

		}
		logger.debug(String.format("queryOneRecord-columns=%s;tableName=%s;whereCone=%s;result=%s", columns, tableName,
				whereCond, dataList));

		return dataList;
	}

	/**
	 * 执行语句
	 */
	public int executeSQL(String sql, ReturnValue value) {
		return super.executeAll(sql, value);
	}

	/**
	 * 查询记录
	 */
	public Map<String, Object> queryRecord(List<String> columns, String tableName, List<WhereCondition> whereCond,
			ReturnValue out) {
		logger.debug(String.format("queryOneRecord_columns=%s,tableName=%s,whereCond=%s", columns,
				tableName,whereCond));
		try {
			//调用查询方法
			List<Map<String, Object>>  resultList = this.query(columns, tableName, whereCond, out);
			int rowCnt = resultList==null ? -1:resultList.size();
			//得到结果是否唯一
			if(rowCnt==1){ //获得数据返回
				return resultList.get(0);
			}
			//判断是否获得数据
			else if(rowCnt<=0){
				//没有数据
				out.setMessageCode(MessageCode.NODATA);
				//返回
				return null;
			}
			else{
				//异常处理 (直接取第一个，记一下warn日志就好了)
				out.setMessageCode(MessageCode.DATA_IS_NOT_THE_ONLY);
				//返回结果
				return null;
			}
					
		} catch (Exception e) {
			//异常记录
			out.setMessageCode(MessageCode.SQL_ERROR);
			//日志
			logger.error("Exception",e);
		}
		//返回结果
		return null;
	}

	/**
	 * 查询记录
	 * 调用方法前确保TableName不为空
	 */
	public Map<String, Object> queryOneRecord(List<String> columns, String name, ReturnValue out) {
		logger.debug(String.format("queryOneRecord_columns=%s,tableName=%s", columns,name));
		try {
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

	/**
	 * 查询列信息
	 * 调用方法前确认表没不为空
	 * @deprecated 建议一次获取所有表及其列定义（或者方法参数增加adapter，以方便调用函数决定是否在同一个数据库连接中获取相关表及其列定义）
	 */
	public List<ColumnInfo> getColums(String name, ReturnValue out) {
		StringBuffer sqlsBuffer = new StringBuffer(1024);
		sqlsBuffer.append("SELECT d.name  tablename,");
		sqlsBuffer.append("  a.name columnname,");
		sqlsBuffer.append("b.name tp,");
		sqlsBuffer.append(" CASE WHEN EXISTS (SELECT 1 FROM   " +
				"sysobjects WHERE  xtype = 'PK' AND name IN (SELECT " +
				"name FROM   sysindexes WHERE  indid IN (SELECT indid FROM   sysindexkeys  WHERE  id = a.id " +
				"AND colid = a.colid))) THEN 'true'  ELSE 'false'  END  pk,");
		sqlsBuffer.append(" CASE  WHEN EXISTS (SELECT 1 FROM   sysobjects" +
				"  WHERE  xtype = 'UQ' AND name IN (SELECT name FROM   sysindexes" +
				"  WHERE  indid IN (SELECT indid FROM   sysindexkeys " +
				"  WHERE  id = a.id AND colid = a.colid))) THEN 'true' ELSE 'false' END uq,");
		sqlsBuffer.append(" a.length length,");
		sqlsBuffer.append(" Columnproperty(a.id,a.name,'PRECISION') size,");
		sqlsBuffer.append(" Isnull(Columnproperty(a.id,a.name,'Scale'),0) identiy,");
		sqlsBuffer.append(" CASE  WHEN a.isnullable = 1 THEN 'true' ELSE 'false' END isNull,");
		sqlsBuffer.append(" Isnull(e.text,'null') df ");
		sqlsBuffer.append("FROM   syscolumns a LEFT JOIN systypes b " +
				" ON a.xusertype = b.xusertype INNER JOIN sysobjects d " +
				" ON a.id = d.id AND d.xtype = 'U' AND d.name <> 'dtproperties' " +
				" LEFT JOIN syscomments e ON a.cdefault = e.id WHERE d.name=?");
		// TODO 如果不同数据库实例下存在相同表名会有问题
		if(StringUtils.isNotNull(name) && name.indexOf(".")!=-1){
			name = name.substring(name.indexOf(".")+1, name.length());
		}
		logger.debug(name+"\t"+sqlsBuffer.toString());
		List<Map<String, Object>> dataList = super.selectAll(sqlsBuffer.toString(), new Object[]{name}, out);
		// 获得查询的结果
		if(dataList==null||dataList.size()==0){
			//异常记录
			out.setMessageCode(MessageCode.NODATA);
			return null;
		}
		//判断是否得到数据
		List<ColumnInfo> columnInfo = new ArrayList<ColumnInfo>(dataList.size());// 创建列的实体类
		for (Map<String, Object> map : dataList) {
			columnInfo.add(this.toColumnInfo(map));// 绑定列的数据
		}
		return columnInfo;
	}

	/**
	 * 转换成列实体类
	 * @param map 集合
	 * @return ColumnInfo
	 */
	private ColumnInfo toColumnInfo(Map<String, Object> map) {
		ColumnInfo columnInfo = new ColumnInfo();
		columnInfo.setName(map.get("columnname").toString());// 列名
		/*int dataType = 0;
		String type = map.get("tp").toString();
		if(type.equals("varchar")||type.equals("nvarchar")||type.equals("geography")||type.equals("uniqueidentifier")){
			dataType = DataStatusEnum.STRING.value();
		}else if(type.equals("int")){
			dataType = DataStatusEnum.INT.value();
		}else if(type.equals("datetime")|| type.equals("data")){
			dataType = DataStatusEnum.DATE.value();
		}else if(type.equals("money")||type.equals("smallmoney")){
			dataType = DataStatusEnum.MONEY.value();
		}else if(type.equals("tinyint")){
			dataType = DataStatusEnum.BYTE.value();
		}else if(type.equals("smallint")){
			dataType = DataStatusEnum.SHORT.value();
		}else if(type.equals("bigint")){
			dataType = DataStatusEnum.LONG.value();
		}else if(type.equals("real")){
			dataType = DataStatusEnum.FLOAT.value();
		}else if(type.equals("float")){
			dataType = DataStatusEnum.DOUBLE.value();
		}else if(type.equals("double")){
			dataType = DataStatusEnum.DOUBLE.value();
		}else if(type.equals("binary")){
			dataType = DataStatusEnum.BYTE.value();
		}else if(type.equals("bigint")){
			dataType = DataStatusEnum.BYTE.value();
		}else if(type.equals("xml")){
			dataType = DataStatusEnum.XML.value();
		}else if(type.equals("bit")||type.equals("char")||type.equals("nchar")){
			dataType = DataStatusEnum.BOOLEAN.value();
		}else{
			dataType = DataStatusEnum.STRING.value();
		}*/
		//columnInfo.setDbBaseDatatype(DataTypeManager.encodeSystemDataType(DataBaseType.SQLSERVER, map.get("tp").toString()));// 列类型
		columnInfo.setSystemDatatype(SystemDatatype.parse(map.get("tp")+""));
		columnInfo.setPrimary(Boolean.parseBoolean(map.get("pk").toString()));// 是否为主键
		columnInfo.setUniqueness(Boolean.parseBoolean(map.get("uq").toString()));// 是否唯一
		// line.setForeign(Integer.parseInt(map.get(5).toString()));// 是否外键
		columnInfo.setLength(Integer.parseInt(map.get("length").toString()));// 长度
		// columnInfo.setPbytes(Long.parseLong(map.get(7).toString()));// 占位节
		columnInfo.setPrecision(Integer.parseInt(map.get("size").toString()));// 精度
		columnInfo.setNullable(Boolean.parseBoolean(map.get("isNull").toString()));// 是否为空
		columnInfo.setIdentity(Boolean.parseBoolean(map.get("identiy").toString()));// 是否标识
		columnInfo.setDefaultValue(map.get("df").toString());// 默认值
		
		logger.trace("[数据类型映射]\t"+map.get("tp")+"\t"+columnInfo.getSystemDatatype());
		return columnInfo;
	}

	/**
	 * 获得所有表名
	 * @param out
	 * @param filter 过滤条件
	 * @return
	 */
	public SourceTable[] getTables(ReturnValue out,String filter) {
		SourceTable[] basicTableInfos = null;
		List<Map<String, Object>> dataList=null;
		try {
			if(org.apache.commons.lang.StringUtils.isNotBlank(filter))
			{
				String filterOut=SQLUtils.sqlserverFilter(filter);
				dataList=super.selectAll("SELECT TABLE_NAME,TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES  WHERE TABLE_TYPE='BASE TABLE' and TABLE_NAME like '%"+filterOut+"%'", null, out);
			}else
			{
			 dataList = super.selectAll("SELECT TABLE_NAME,TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES  WHERE TABLE_TYPE='BASE TABLE'", null, out);// 获得查询的结果
			}
			
			//判断数据是否为空
			if(dataList==null||dataList.isEmpty()){
				out.setMessageCode(MessageCode.NODATA);
				return null;
			}
			basicTableInfos = new SourceTable[dataList.size()];// 定义数组接收数据
			//遍历存储数据
			for (int i=0; i<dataList.size(); i++) {
				basicTableInfos[i] = new SourceTable();// 创建表实体类
				basicTableInfos[i].setTableName(
						dataList.get(i).get("TABLE_SCHEMA").toString()+ "." + 
						dataList.get(i).get("TABLE_NAME").toString());// 得到表名
			}
		} catch (Exception e) {
			//异常记录
			out.setMessageCode(MessageCode.SQL_ERROR);
			//日志记录
			logger.error("Exception", e);
		}
		return basicTableInfos;
	}

	/**
	 * 获得表数据
	 */
	public ResultSets queryResultSets(List<String> columns, String tableName, List<WhereCondition> whereCond,
			ReturnValue out) {

		logger.debug(String.format("query-columns=%s;tableName=%s;whereCone=%s", columns,
				tableName, whereCond));
		if(tableName==null || tableName.equals("")){
			//表名为空
			out.setMessageCode(MessageCode.ERR_TABLENAME_NULL);
			return null;
		}
		StringBuffer sqlbBuffer = new StringBuffer();// 拼接查询语句
		StringBuffer conditions = new StringBuffer();// 包装条件项
		List<Object> str = new ArrayList<Object>();// 值
		List<String> datas = new ArrayList<String>();// 接收数据
		ResultSets dataList = null;// 接收查询结果
		//1、列数据处理
		//TODO
		columnsData(tableName,out,datas,columns);
		//查看是否有条件
		if (whereCond!=null&&whereCond.size()>0) {
			// 条件拼接方法
			bindCondition(whereCond, conditions, str);
			if(str==null||str.size()==0){
				//异常处理
				out.setMessageCode(MessageCode.ERR_CONDITION_BIND);
				return null;
			}
			Object[] objects = new Object[str.size()];// 参数数组
			str.toArray(objects);
			sqlbBuffer.append(" SELECT ");
			sqlbBuffer.append(this.bindColumns(columns));
			sqlbBuffer.append(" FROM ");
			sqlbBuffer.append(tableName);
			sqlbBuffer.append(" WHERE 1=1 ");
			sqlbBuffer.append(conditions);
			dataList  = selectResultSets(sqlbBuffer.toString(), objects, out);// 接收查询结果（传入查询值）
			
		} else {
			sqlbBuffer.setLength(0);// 清空语句
			sqlbBuffer.append(" SELECT ");
			sqlbBuffer.append(this.bindColumns(columns));
			sqlbBuffer.append(" FROM ");
			sqlbBuffer.append(tableName);
			
			dataList = selectResultSets(sqlbBuffer.toString(), out);// 接收查询结果
		}
		if(dataList==null||dataList.getDataList().size()==0){
			//异常记录
			out.setMessageCode(MessageCode.NODATA);
			return null;
		}
		logger
		.debug(String
				.format(
						"queryOneRecord-columns=%s;tableName=%s;whereCone=%s;result=%s",
						columns, tableName, whereCond, dataList));
		
		return dataList;
	
	}

	public ResultSets selectResultSets(String sql, ReturnValue out) {
		return this.selectResultSets(sql, null, out);
	}

	public ResultSets selectResultSets(String sql, Object[] objects, ReturnValue out) {
		//记录日志
		logger.debug(String.format("selectResultSets-sql=%s;", sql));
		ResultSet rt = null; //接收结果
		PreparedStatement pt = null;
		if (connection != null) {//数据库不为空的情况
			try {
				//放入sql
				pt = connection.prepareStatement(sql);
				//绑定参数
				bind(pt,objects,out);
				//执行查询并接收结果
				rt = pt.executeQuery();
				if(rt==null){
					out.setMessageCode(MessageCode.SQL_ERROR);
					return null;
				}
				//定义集合接收
				ResultSets resultSets = new ResultSets();
				//接收数据
				List<Map<String, String>> list = new ArrayList<Map<String, String>>();
				//接收列数据
				Map<String, String> columnType = new HashMap<String, String>();
				//循环读取封装数据
				while (rt.next()) {
					Map<String, String> map = new HashMap<String, String>();
					for (int i = 1; i <= rt.getMetaData().getColumnCount(); i++) {
						columnType.put(rt.getMetaData().getColumnName(i), rt.getMetaData().getColumnTypeName(i));
						map.put(rt.getMetaData().getColumnName(i), rt.getObject(i)==null?null: rt.getObject(i).toString());//存储结果
					}
					list.add(map);
				}
				resultSets.setColumnList(columnType);
				resultSets.setDataList(list);
				return resultSets;
			} catch (Exception e) {//异常处理
				//异常返回
				out.setMessageCode(MessageCode.SQL_ERROR);
				//记录错误日志
				logger.error("SQLException",e);
			} finally{
				DbUtils.closeQuietly(null, pt, rt);
			}
		}
		//记录日志
		logger.debug(String.format("selectAll-sql=%s;", sql));
		//返回结果
		return null;
	}
	/**
	 * 列数据处理方法
	 * @param tableName表名
	 * @param out 异常结果返回
	 * @param datas 数据集合
	 * @param result 结果
	 * @param columns列集合
	 */
	private void columnsData(String tableName, ReturnValue out,
			List<String> datas,  List<String> columns) {
		if(tableName==null||tableName.equals("")){
			out.setMessageCode(MessageCode.ERR_TABLENAME_NULL);
			return;
		}
		Object[] objects = new Object[1];// 参数数组
		objects[0] = tableName;
	}
	
	/**
	 * 绑定
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param pt
	 *@param object
	 *@param value
	 */
	private void bind(PreparedStatement pt, Object[] object, ReturnValue value) {
		// 记录日志
		logger.debug(String.format("bind-pt=%s;", pt));
		if (object != null && object.length > 0) {
			// 循环绑定
			for (int i = 0; i < object.length; i++) {
				try {
					pt.setObject(i + 1, object[i]);
				} catch (SQLException e) {
					// 异常处理
					value.setMessageCode(MessageCode.SQL_ERROR);
					// 错误日志记录
					logger.error("SQLException", e);
				}
			}
		}
	}

	@Override
	public boolean createRepository(ReturnValue value) {
		// TODO Auto-generated method stub
		return false;
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