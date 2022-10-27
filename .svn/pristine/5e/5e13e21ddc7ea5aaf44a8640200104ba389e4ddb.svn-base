package com.eurlanda.datashire.adapter;

import com.eurlanda.datashire.entity.DbSquid;
import com.eurlanda.datashire.entity.operation.DataEntity;
import com.eurlanda.datashire.entity.operation.WhereCondition;
import com.eurlanda.datashire.enumeration.MatchTypeEnum;
import com.eurlanda.datashire.utility.DbUtils;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 读写传统关系型数据库的父类，如SQL SERVER, ORACLE, DB2, MYSQL, HYSQLDB 等
 * 
 * @author eurlanda
 * @version 1.0
 * @created 19-八月-2013 9:35:22
 */
abstract class AbsSqlAdapter extends DataSourceManager implements IDBAdapter {

    static Logger logger = Logger.getLogger(AbsSqlAdapter.class);

	//accessHandle可以是一个jdbc Connection对象，
    //或者一个File操作句柄 完全是为了方便内部method调用有关数据存取的系统函数，
    //从DataStoreConnection获取得到
    protected Connection connection;

    protected  AbsSqlAdapter(DbSquid dataStore) {
        super(dataStore);
        this.connection = super.openAdapter();
    }

	/**
	 * 取得当前connection.
	 */
	public Connection getConnection() {
		if(this.defaultAutoCommit){
			this.connection= super.getDBCPConnection();
		}
		return this.connection;
	}

	public void setConnection(Connection connection) {
		this.connection = connection;
	}

	/**
	 * <p>
	 * 作用描述：提交事务，关闭一个连接
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 * 
	 * @return
	 */
	public void commitAdapter() {
		if(connection==null)
			return;
		try {
			connection.commit();
		} catch (SQLException e) {
			logger.debug("commit connection err!", e);
			// 事物回滚
			try {
				connection.rollback();
			} catch (SQLException e1) {
				logger.debug("rollback con err!", e1);
			}
		} finally {
			try { 
				connection.close();
			} catch (SQLException e2) {
				logger.debug("close con err!", e2);
			}
		}
		logger.debug("[closeAdapter] sqlAdapter is closed.");
	}
	public void closeAdapter() {
		if(connection==null)
			return;
		//closeConnectionLog(connection);
		try {
			connection.close();
		} catch (SQLException e2) {
			logger.debug("close con err!", e2);
		}
		logger.debug("[closeAdapter] sqlAdapter is closed.");
	}
	
	public void rollback()  throws SQLException{
		if(connection!=null) connection.rollback();
		logger.debug("[rollback] sqlAdapter is rollback.");
	}
	/**
	 * <p>
	 * 作用描述：重置连接
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 * 
	 * @return
	 */
	public boolean resetAdapter() {
		try {
			if (connection==null||connection.isClosed()) {
				this.connection = getDBCPConnection();
			}
		} catch (SQLException e) {
			logger.error("链接重置失败...", e);
		}
		logger.debug("[重置连接] "+connection);
		return connection!=null;
	}
	
	public boolean isValid(int timeout) throws SQLException{
		return connection!=null;
	}
	
	/**
	 * 绑定条件
	 * 
	 * @param whereCond
	 *            条件
	 * @param conditions
	 *            值
	 * @param str
	 *            结果
	 */
	protected void bindCondition(List<WhereCondition> whereCondList, StringBuffer conditions, List<Object> str) {
		if (whereCondList == null || whereCondList.size() == 0) {
			return;
		}
		// 循环封装条件
		for (WhereCondition whereCondition : whereCondList) {// 条件
			if (whereCondition.getMatchType() == MatchTypeEnum.LIKE) {
				conditions.append(" AND ");
				conditions.append(whereCondition.getAttributeName());
				conditions.append(" LIKE ?   ");
				if (whereCondition.getValue() == null) {
					return;
				}
				str.add(whereCondition.getValue().toString());// 值
			} else if (whereCondition.getMatchType() == MatchTypeEnum.EQ) {
				conditions.append(" AND ");
				conditions.append(whereCondition.getAttributeName());
				conditions.append("= ?  ");
				if (whereCondition.getValue() == null) {
					return;
				}
				str.add(whereCondition.getValue().toString());// 值
			} else if (whereCondition.getMatchType() == MatchTypeEnum.GT) {
				conditions.append(" AND ");
				conditions.append(whereCondition.getAttributeName());
				conditions.append("> ? ");
				if (whereCondition.getValue() == null) {
					return;
				}
				str.add(whereCondition.getValue().toString());// 值
			} else if (whereCondition.getMatchType() == MatchTypeEnum.GTE) {
				conditions.append(" AND ");
				conditions.append(whereCondition.getAttributeName());
				conditions.append(">= ?  ");
				if (whereCondition.getValue() == null) {
					return;
				}
				str.add(whereCondition.getValue().toString());// 值
			} else if (whereCondition.getMatchType() == MatchTypeEnum.LT) {
				conditions.append(" AND ");
				conditions.append(whereCondition.getAttributeName());
				conditions.append(" < ?  ");
				if (whereCondition.getValue() == null) {
					return;
				}
				str.add(whereCondition.getValue().toString());// 值
			} else if (whereCondition.getMatchType() == MatchTypeEnum.LTE) {
				conditions.append(" AND ");
				conditions.append(whereCondition.getAttributeName());
				conditions.append(" <= ? ");
				if (whereCondition.getValue() == null) {
					return;
				}
				str.add(whereCondition.getValue().toString());// 值
			} else if (whereCondition.getMatchType() == MatchTypeEnum.NQE) {
				conditions.append(" AND ");
				conditions.append(whereCondition.getAttributeName());
				conditions.append(" <> ? ");
				if (whereCondition.getValue() == null) {
					return;
				}
				str.add(whereCondition.getValue().toString());// 值
			}
		}
	}

	/**
	 * 新增数据操作
	 * 
	 * @param tableName
	 *            表名
	 * @param paramValue
	 *            表字段数据包
	 * @param out
	 *            异常返回结果
	 */
	public boolean insert(String tableName, List<DataEntity> paramValue, ReturnValue out) {
		logger.debug(String.format("insert-tableName=%s;paramValue=%s;", tableName, paramValue));
		boolean result = false;
		if (tableName == null || tableName.equals("")) {
			// 异常处理
			out.setMessageCode(MessageCode.ERR_TABLENAME_NULL);
			return result;
		}
		StringBuffer columns = new StringBuffer();// 包装要插入的列名
		StringBuffer datas = new StringBuffer();// 包装要插入列的值
		String column;// 接收要插入的列名
		String data;// 接收要插入的列数据
		StringBuffer sqlbBuffer = new StringBuffer();// 语句
		if (paramValue != null && paramValue.size() > 0) {// 插入项有值的情况下
			try {
				Object[] objects = new Object[paramValue.size()];// 接收数据值
				int i = 0;
				for (DataEntity dataEntity : paramValue) {
					columns.append(dataEntity.getAttributeName());// 添加列
					columns.append(",");
					datas.append("?,");// 添加列数据
					objects[i] = dataEntity.getValue();

					i++;
				}
				column = columns.substring(0, columns.length() - 1);// 截取
				data = datas.substring(0, datas.length() - 1);// 截取
				sqlbBuffer.append("INSERT INTO ");
				sqlbBuffer.append(tableName);
				sqlbBuffer.append("(");
				sqlbBuffer.append(column);
				sqlbBuffer.append(")");
				sqlbBuffer.append(" VALUES (");
				sqlbBuffer.append(data);
				sqlbBuffer.append(")");
				String sql = sqlbBuffer.toString();
				logger.debug(String.format("insert-sql", sql));
				if (this.executeAll(sql, objects, out) > 0) {
					// 插入结果
					result = true;// 成功
				} else {
					result = false;// 失败
					out.setMessageCode(MessageCode.INSERT_ERROR);// 新增失败
				}
			} catch (Exception e) {
				out.setMessageCode(MessageCode.INSERT_ERROR);// 新增失败
				logger.error("Exception", e);
			}
		} else {
			out.setMessageCode(MessageCode.NODATA);// 没有数据
			return false;
		}
		logger.debug(String.format("insert-tableName=%s;paramValue-size()=%s;result=%s", tableName, paramValue.size(),
				result));
		return result;

	}

	/**
	 * 删除数据操作
	 * 
	 * @param tableName
	 *            表名
	 * @param conditions
	 *            条件数据包
	 * @param returnValue
	 */
	@SuppressWarnings("unused")
	public boolean delete(String tableName, List<WhereCondition> whereCondList, ReturnValue out) {
		logger.debug(String.format("delete-tableName=%s;whereCond=%s", tableName, whereCondList));
		boolean result = false;// 返回值
		if (tableName == null || tableName.equals("")) {
			// 异常处理
			out.setMessageCode(MessageCode.ERR_TABLENAME_NULL);
			return result;
		}
		StringBuffer sqlbBuffer = new StringBuffer();// 拼接查询语句
		StringBuffer conditions = new StringBuffer();// 包装条件项
		List<Object> str = new ArrayList<Object>();
		Object[] objects = null;
		if (whereCondList != null && whereCondList.size() > 0) {// 有条件
			bindCondition(whereCondList, conditions, str);// 条件处理方法
			if (str == null || str.size() == 0) {
				// 异常处理
				out.setMessageCode(MessageCode.ERR_CONDITION_BIND);
				return false;
			}
			objects = new Object[str.size()];
			str.toArray(objects);
			// 拼装sql
			sqlbBuffer.append(" DELETE ");
			sqlbBuffer.append(tableName);
			sqlbBuffer.append(" WHERE 1=1 ");
			sqlbBuffer.append(conditions);

		} else {
			sqlbBuffer.setLength(0);// 清空
			// 无条件
			sqlbBuffer.append(" DELETE ");
			sqlbBuffer.append(tableName);
		}
		int i = executeAll(sqlbBuffer.toString(), objects, out);
		if (i > 0) {
			// 数据操作成功
			out.setMessageCode(MessageCode.SUCCESS);
			result = true;
		} else {
			// 数据操作失败
			out.setMessageCode(MessageCode.ERR_DELETE);
			result = false;
		}
		// 日志记录
		logger.debug(String.format("delete-sql=%s", sqlbBuffer.toString()));
		logger.debug(String.format("delete-tableName=%s;whereCondresult=%s", tableName, whereCondList, result));
		// 结果返回
		return result;
	}

	/**
	 * 更新数据操作
	 * 
	 * @param tableName
	 *            表名
	 * @param paramValue
	 *            表字段数据包（修改的字段）
	 * @param whereCond
	 *            条件数据包
	 * @param out
	 *            异常结果返回
	 */
	@SuppressWarnings("unused")
	public boolean update(String tableName, List<DataEntity> paramValue, List<WhereCondition> whereCondList,
			ReturnValue out) {
		logger.debug(String.format("update-tableName=%s;whereCond=%s;paramValue=%s;", tableName, whereCondList,
				paramValue));
		boolean result = false;
		if (tableName == null || tableName.equals("")) {
			// 异常处理
			out.setMessageCode(MessageCode.ERR_TABLENAME_NULL);
			return result;
		}
		StringBuffer sqlbBuffer = new StringBuffer();// 拼接查询语句
		StringBuffer parameters = new StringBuffer();// 包装参数项
		StringBuffer conditions = new StringBuffer();// 包装条件项
		List<Object> str = new ArrayList<Object>();
		Object[] objects;
		String parameter;// 参数项
		String sql;// 语句
		if (paramValue != null && paramValue.size() > 0) {// 修改项有值的情况下
			for (DataEntity dataEntity : paramValue) {// 修改项
				parameters.append(dataEntity.getAttributeName());// 拼接修改项
				parameters.append("=?,");// 拼接修改项
				str.add(dataEntity.getValue());// 值
			}
			parameter = parameters.substring(0, parameters.length() - 1);// 截取
			if (whereCondList != null && whereCondList.size() > 0) {
				objects = new Object[paramValue.size() + whereCondList.size()];
				// 有条件的情况
				bindCondition(whereCondList, conditions, str);// 条件处理方法
				str.toArray(objects);// 转换成数组
				sqlbBuffer.append("UPDATE ");
				sqlbBuffer.append(tableName);
				sqlbBuffer.append(" SET ");
				sqlbBuffer.append(parameter);
				sqlbBuffer.append(" WHERE 1=1 ");
				sqlbBuffer.append(conditions);
			} else {
				sqlbBuffer.setLength(0);// 清空
				// 无条件
				objects = new Object[paramValue.size()];
				str.toArray(objects);// 转换成数组
				sqlbBuffer.append("UPDATE ");
				sqlbBuffer.append(tableName);
				sqlbBuffer.append(" SET ");
				sqlbBuffer.append(parameter);
			}
			sql = sqlbBuffer.toString();
			// 判断执行是否成功
			if (this.executeAll(sql, objects, out) > 0) {
				result = true;
			} else {
				out.setMessageCode(MessageCode.UPDATE_ERROR);// 修改失败
				result = false;
			}
		} else {
			out.setMessageCode(MessageCode.NODATA);// 参数项为空
			return false;
		}
		logger.debug(String.format("update-tableName=%s;whereCond-size()=%s;paramValue-size()=%s;result=%s", tableName,
				paramValue, paramValue, result));
		return result;

	}

	/**
	 * 通用修改无参
	 * 
	 * @param sql
	 *            语句
	 * @param value
	 *            异常处理返回
	 * @return
	 */
	public int executeAll(String sql, ReturnValue value) {
		logger.debug(String.format("executeAll", sql));
		// 调用有参方法
		int result = this.executeAll(sql, null, value);
		return result;
	}

	/**
	 * 通用操作 sql sql语句 objecets 条件 value 异常处理
	 */
	public int executeAll(String sql, Object[] objects, ReturnValue value) {
		logger.debug(String.format("updateAll-sql=%s;", sql));
		int result = 0;// 接收结果
		PreparedStatement pt = null;
		try {
			if(connection==null || connection.isClosed()){
				logger.warn("connection is null or closed! try to get a new connection!");
				connection = super.openAdapter();
				logger.info("new connection="+connection);
			}
			pt = connection.prepareStatement(sql);
			// 绑定参数
			this.bind(pt, objects, value);
			// 执行操作
			pt.executeUpdate();
			result = 1;
		} catch (SQLException e) {
			result = 0;
			// 抛出异常
			value.setMessageCode(MessageCode.SQL_ERROR);
			// 记录错误日志
			logger.error("SQLException", e);
		}finally{
			if(pt!=null)
			try {
				pt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		logger.debug(String.format("updateAll-sql=%s;result=%s", sql, result));
		// 返回结果
		return result;
	}

	/**
	 * 绑定参数
	 * 
	 * @param object
	 */
	private void bind(PreparedStatement pt, Object[] object, ReturnValue value) {
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

	/**
	 * 返回单值
	 * 
	 * @return
	 */
	public String selectsingle(String sql, Object[] objects, ReturnValue value) {
		logger.debug(String.format("selectsingle-sql=%s;", sql));
		String data = "";
		ResultSet rt = null; // 接收结果
		PreparedStatement pt = null;
		try {
			if(connection==null || connection.isClosed()){
				logger.warn("connection is null or closed! try to get a new connection!");
				connection = super.openAdapter();
				logger.info("new connection="+connection);
			}
			// 放入sql
			pt = connection.prepareStatement(sql);
			// 绑定参数
			this.bind(pt, objects, value);
			// 执行查询
			rt = pt.executeQuery();
			// 循环封装数据
			while (rt.next()) {
				// for (int i = 1; i <= rt.getMetaData().getColumnCount(); i++) {
				data = rt.getObject(1).toString();
				// }
			}
		} catch (SQLException e) {
			// 异常处理
			value.setMessageCode(MessageCode.SQL_ERROR);
			// 错误日志记录
			logger.error("SQLException", e);
		}finally{
			if(pt!=null)
				try {
					pt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			if(rt!=null)
				try {
					rt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
		}
		// 日志记录
		logger.debug(String.format("selectsingle-sql=%s;result=%s", sql, data));
		// 返回结果
		return data;
	}

	/**
	 * 通用查询 sql sql语句
	 * 
	 * @return list集合返回
	 */
	public List<Map<String, Object>> selectAll(String sql, Object[] objects, ReturnValue value) {
		// 记录日志
		logger.debug(String.format("selectAll-sql=%s;", sql));
		ResultSet rt = null; // 接收结果
		PreparedStatement pt = null;
		try {
			if(connection==null || connection.isClosed()){
				logger.warn("connection is null or closed! try to get a new connection!");
				connection = super.openAdapter();
				logger.info("new connection="+connection);
			}
			// 放入sql
			pt = connection.prepareStatement(sql);
			// 绑定参数
			this.bind(pt, objects, value);
			// 执行查询并接收结果
			rt = pt.executeQuery();
			if (rt == null) {
				value.setMessageCode(MessageCode.SQL_ERROR);
				return null;
			}
			// 定义集合接收
			List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
			// 循环读取封装数据
			while (rt.next()) {
				Map<String, Object> map = new HashMap<String, Object>();
				for (int i = 1; i <= rt.getMetaData().getColumnCount(); i++) {
					map.put(rt.getMetaData().getColumnName(i), rt.getObject(i));// 存储结果
				}
				list.add(map);
			}
			return list;
		} catch (SQLException e) {// 异常处理
			// 异常返回
			value.setMessageCode(MessageCode.SQL_ERROR);
			// 记录错误日志
			logger.error("SQLException", e);
		}finally{
			DbUtils.closeQuietly(null, pt, rt);
		}
		// 记录日志
		logger.debug(String.format("selectAll-sql=%s;", sql));
		// 返回结果
		return null;
	}

	/**
	 * 通用查询无参
	 * 
	 * @param sql
	 * @param value
	 * @return
	 */
	public List<Map<String, Object>> selectAll(String sql, ReturnValue value) {
		// 调用有参方法
		List<Map<String, Object>> list = this.selectAll(sql, null, value);
		// 返回结果
		return list;
	}

	/**
	 * <p>
	 * 作用描述： 批量修改
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 * 
	 * @param tableName
	 * @param paramValue
	 * @param whereCondList
	 * @param out
	 * @return
	 */
	public boolean updatBatch(String tableName, List<List<DataEntity>> paramValue,
			List<List<WhereCondition>> whereCondList, ReturnValue out) {
		logger.debug(String.format("updatBatch", tableName));
		boolean result = false;
		if (tableName == null || tableName.equals("")) {
			// 异常处理
			out.setMessageCode(MessageCode.ERR_TABLENAME_NULL);
			return result;
		}
		ArrayList<Object> arrayList = new ArrayList<Object>();
		if (paramValue == null || paramValue.size() == 0) {
			return false;
		}
		StringBuffer sqlbBuffer = new StringBuffer();// 拼接查询语句
		List<Object> str = new ArrayList<Object>();
		for (int i = 0; i < paramValue.size(); i++) {

			StringBuffer parameters = new StringBuffer();// 包装参数项
			StringBuffer conditions = new StringBuffer();// 包装条件项
			String parameter;// 参数项
			if (paramValue.get(i) != null && paramValue.get(i).size() > 0) {// 修改项有值的情况下
				for (DataEntity dataEntity : paramValue.get(i)) {// 修改项
					parameters.append(dataEntity.getAttributeName());// 拼接修改项
					parameters.append("=?,");// 拼接修改项
					arrayList.add(dataEntity.getValue());// 值
					str.add(dataEntity.getValue());
				}
				parameter = parameters.substring(0, parameters.length() - 1);// 截取
				if (whereCondList != null && whereCondList.get(i).size() > 0) {
					// 有条件的情况
					bindCondition(whereCondList.get(i), conditions, str);// 条件处理方法
					sqlbBuffer.append(" UPDATE ");
					sqlbBuffer.append(tableName);
					sqlbBuffer.append(" SET ");
					sqlbBuffer.append(parameter);
					sqlbBuffer.append(" WHERE 1=1 ");
					sqlbBuffer.append(conditions);
					sqlbBuffer.append(";");
				} else {
					// 无条件
					sqlbBuffer.append(" UPDATE ");
					sqlbBuffer.append(tableName);
					sqlbBuffer.append(" SET ");
					sqlbBuffer.append(parameter);
					sqlbBuffer.append(";");
				}
			} else {
				return false;
			}

		}
		// 转换成数组
		Object[] objects = new Object[str.size()];
		str.toArray(objects);
		// 判断执行是否成功
		if (this.executeAll(sqlbBuffer.toString(), objects, out) > 0) {
			result = true;
		} else {
			out.setMessageCode(MessageCode.UPDATE_ERROR);// 修改失败
			result = false;
		}
		return result;

	}

	/***
	 * 
	 * <p>
	 * 作用描述： 批量新增
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 * 
	 * @param tableName
	 * @param paramList
	 * @param out
	 * @return
	 */
	public boolean createBatch(String tableName, List<List<DataEntity>> paramList, ReturnValue out) {
		logger.debug(String.format("crateBatch-tableName=%s", tableName));
		boolean result = false;
		// 判断表名是否为空
		if (tableName == null || tableName.equals("")) {
			out.setMessageCode(MessageCode.ERR_TABLENAME_NULL);
			return result;
		}
		// 判断实体类对象是否为空
		if (paramList == null || paramList.size() == 0) {
			// 参数项为空
			out.setMessageCode(MessageCode.NODATA);
			return result;
		}
		StringBuffer sqlbBuffer = new StringBuffer();// 语句
		ArrayList<Object> value = new ArrayList<Object>();// 条件
		for (int i = 0; i < paramList.size(); i++) {
			StringBuffer columns = new StringBuffer();// 包装要插入的列名
			StringBuffer datas = new StringBuffer();// 包装要插入列的值
			String column;// 接收要插入的列名
			String data;// 接收要插入的列数据
			if (paramList.get(i) != null && paramList.get(i).size() > 0) {// 插入项有值的情况下
				try {
					for (DataEntity dataEntity : paramList.get(i)) {
						columns.append(dataEntity.getAttributeName());// 添加列
						columns.append(",");
						datas.append("?,");// 添加列数据
						value.add(dataEntity.getValue());
					}
					column = columns.substring(0, columns.length() - 1);// 截取
					data = datas.substring(0, datas.length() - 1);// 截取
					sqlbBuffer.append("INSERT INTO ");
					sqlbBuffer.append(tableName);
					sqlbBuffer.append("(");
					sqlbBuffer.append(column);
					sqlbBuffer.append(")");
					sqlbBuffer.append(" VALUES (");
					sqlbBuffer.append(data);
					sqlbBuffer.append("); ");
				} catch (Exception e) {
					out.setMessageCode(MessageCode.INSERT_ERROR);// 新增失败
					logger.error("Exception", e);
				}
			} else {
				out.setMessageCode(MessageCode.NODATA);// 没有数据
				return false;
			}
		}
		Object[] objects = null;
		if (value != null && value.size() > 0) {
			objects = new Object[value.size()];
			value.toArray(objects);
		}
		logger.debug(String.format("insert-sql", sqlbBuffer.toString()));
		if (this.executeAll(sqlbBuffer.toString(), objects, out) > 0) {
			// 插入结果
			result = true;// 成功
		} else {
			result = false;// 失败
			out.setMessageCode(MessageCode.INSERT_ERROR);// 新增失败
		}
		return result;
	}

	/**
	 * 多事务处理删除
	 * 
	 * @return
	 */
	public boolean deleteBatch(List<String> tableNames, List<List<WhereCondition>> whereCondList, ReturnValue out) {
		logger.debug(String.format("deleteBatch-tableNames=%s", tableNames));
		boolean result = false;// 返回值
		if (tableNames == null || tableNames.equals("")) {
			// 异常处理
			// out.setMessageCode(MessageCode.ERR_TABLENAME_NULL);
			return result;
		}
		StringBuffer sqlbBuffer = new StringBuffer();// 拼接查询语句
		List<Object> str = new ArrayList<Object>();// 接收条件值
		// 循环拼接条件
		for (int i = 0; i < tableNames.size(); i++) {
			if (whereCondList != null && whereCondList.get(i) != null) {
				StringBuffer conditions = new StringBuffer();// 包装条件项
				bindCondition(whereCondList.get(i), conditions, str);// 条件处理方法
				sqlbBuffer.append(" DELETE ");
				sqlbBuffer.append(tableNames.get(i));
				sqlbBuffer.append(" WHERE 1=1 ");
				sqlbBuffer.append(conditions);
				sqlbBuffer.append(" ; ");

			} else {
				sqlbBuffer.append(" DELETE ");
				sqlbBuffer.append(tableNames.get(i));
				sqlbBuffer.append(" WHERE 1=1 ");
				sqlbBuffer.append(" ; ");
			}

		}
		Object[] objects = null;
		if (str != null) {
			objects = new Object[str.size()];
		}
		str.toArray(objects);
		int i = executeAll(sqlbBuffer.toString(), objects, out);
		if (i > 0) {
			// 数据操作成功
			out.setMessageCode(MessageCode.SUCCESS);
			result = true;
		} else {
			// 数据操作失败
			out.setMessageCode(MessageCode.ERR_DELETE);
			result = false;
		}
		// 日志记录
		logger.debug(String.format("delete-sql=%s", sqlbBuffer.toString()));
		logger.debug(String.format("deleteBatch-tableNames=%s;whereCond=%s", tableNames, whereCondList));
		// 结果返回
		return result;
	}

	/**
	 * 绑定列
	 * 
	 * @param columnList
	 *            列集合
	 * @return String
	 */
	public String bindColumns(List<String> columnList) {
		StringBuffer column = new StringBuffer();
		String columns = "";
		if (columnList != null && columnList.size() > 0) {// 列集合数据大于0 的情况下
			for (int i = 0; i < columnList.size(); i++) {
				column.append(columnList.get(i));
				column.append(",");
			}
			columns = column.substring(0, column.length() - 1);// 截取
		} else {
			// 如果没有查询所有
			columns = "*";
		}
		return columns;
	}

}