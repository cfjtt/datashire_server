package com.eurlanda.datashire.sprint7.plug;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IDBAdapter;
import com.eurlanda.datashire.entity.operation.WhereCondition;
import com.eurlanda.datashire.enumeration.DataStatusEnum;
import com.eurlanda.datashire.enumeration.MatchTypeEnum;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import com.eurlanda.datashire.utility.StringUtils;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * 对数据库的操作类，校验的具体内容为ds前缀的表(不包含ds_sys系统表),枚举类型的表，以及具有主外键约束的表
 * 
 * @version 1.0
 * @author lei.bin
 * @created 2013-09-17
 */
public class RepositoryValidationPlug {
	/**
	 * 对RepositoryValidationPlug记录日志
	 */
	static Logger logger = Logger.getLogger(RepositoryValidationPlug.class);
	private String DBName;
	IDBAdapter adapter=null;
	public RepositoryValidationPlug(String DBName) {
		this.DBName=DBName;
		adapter = DataAdapterFactory.newInstance().getAdapter(DBName);
	}
	

	/**
	 * 获取数据库中所有的DS前缀的表
	 * @param tableName 表名
	 * @param out 返回结果代码
	 * @return map对象
	 */
	public Map<String, Object> getAllDSTable(String tableName, ReturnValue out) {
		//定义一个map
		Map<String, Object> columnsValue = null;
		// 封装查询条件
		try {
			List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
			WhereCondition condition = new WhereCondition();
			condition.setAttributeName("TABLE_NAME");
			condition.setDataType(DataStatusEnum.STRING);
			condition.setMatchType(MatchTypeEnum.EQ);
			condition.setValue("'" + tableName + "'");
			whereCondList.add(condition);
			logger.debug("whereCondList====" + whereCondList);
			logger.debug("adapter的值===========" + adapter);
			columnsValue = adapter.queryRecord(null,
					"information_schema.tables", whereCondList, out);

		} catch (Exception e) {
			logger.debug("getAllDSTable",e);
			out.setMessageCode(MessageCode.NODATA);
		}finally
		{
			adapter.closeAdapter();
		}

		return columnsValue;
	}

	/**
	 * 获取所有枚举表中的内容
	 * @param tableName 表名
	 * @param out  返回结果代码
	 * @return list结果
	 */
	public List<Map<String, Object>> getEnumTableValue(String tableName,
			ReturnValue out) {
		logger.debug("具体的枚举校验进来了");
		List<Map<String, Object>> columnsValue = null;
		// 封装查询条件
		try {
			List<String> columns = new ArrayList<String>();
			//columns.add("ID");
			columns.add("CODE");
			columns.add("DESCRIPTION");
			columnsValue = adapter.query(columns, tableName, null, out);
			logger.debug(String.format(
					"getEnumTableValue  by tableName(%s)=====", columnsValue));
		} catch (Exception e) {
			logger.error("getEnumTableValue is error", e);
			out.setMessageCode(MessageCode.NODATA);
		}finally
		{
			adapter.closeAdapter();
		}
		return columnsValue;
	}

	/**
	 * 获取所有具有外键的表的记录
	 * 
	 * @param tableName 表名
	 * @param out 结果返回代码
	 * @return 表集合
	 */
	public List getForeignKeyValue(String tableName, ReturnValue out) {
		List list = null;
		try {
			String sql = this.getSql(tableName);
			logger.debug("sql语句为==" + sql);
			if (StringUtils.isNotNull(sql)) {
				list = adapter.query(sql, out);
			}
		} catch (Exception e) {
			logger.error("getForeignKeyValue", e);
			out.setMessageCode(MessageCode.SQL_ERROR);
		}finally
		{
			adapter.closeAdapter();
		}
		return list;
	}

	/**
	 * 根据表名获取sql执行语句
	 * 
	 * @param tableName 表名
	 * @param out 返回结果代码
	 * @return sql语句
	 */
	public String getSql(String tableName) {
		throw new RuntimeException("不应该被使用的函数");
	}
}
