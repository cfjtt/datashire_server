package com.eurlanda.datashire.utility.objectsql;

import cn.com.jsoft.jframe.utils.CollectionUtils;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * sql查询
 * 
 * @date 2014-1-11
 * @author jiwei.zhang
 * 
 */
public class CopyOfSelectSQL extends ObjectSQL {
	private Logger log = Logger.getLogger(CopyOfSelectSQL.class);
	private Collection<String> fields = new ArrayList<String>(); // 使用的列。
	private Integer maxCount;
	private static final String oracle_template = "select ${columns} from ${table} ${where} ${order}";
	private static final String mysql_template = "select ${columns} from ${table} ${where} ${order} ${limit}";
	private static final String sqlserver_template = "select ${top} ${columns} from ${table} ${where} ${order}";
	private static final String hive_template ="select ${columns} from ${table} ${where} ${order} ${limit}";
	/**
	 * 查询指定的表
	 * 
	 * @param tableName
	 *            表名
	 */
	public CopyOfSelectSQL(String tableName) {
		super(tableName);
	}

	/**
	 * 取得查询结果数量
	 * 
	 * @date 2014-1-13
	 * @author jiwei.zhang
	 * @return
	 */
	public Integer getMaxCount() {
		return maxCount;
	}

	/**
	 * 设置查询结果数量
	 * 
	 * @date 2014-1-13
	 * @author jiwei.zhang
	 * @param maxCount
	 */
	public void setMaxCount(Integer maxCount) {
		this.maxCount = maxCount;
	}

	/**
	 * 查询指定的表和列。
	 * 
	 * @param tableName
	 *            表名
	 * @param fields
	 *            列名，多个列以逗号分隔。
	 */
	public CopyOfSelectSQL(String tableName, String fields) {
		super(tableName);
		this.fields = Arrays.asList(fields.split(","));
	}

	/**
	 * 查询指定的表和列。
	 * 
	 * @param tableName
	 *            表名
	 * @param fields
	 *            列名集合
	 */
	public CopyOfSelectSQL(String tableName, Collection<String> fields) {
		super(tableName);
		this.fields.addAll(fields);
	}

	public String getFields() {
		return CollectionUtils.join(this.fields);
	}

	/**
	 * 设置待查询的列名。
	 * 
	 * @date 2014-1-11
	 * @author jiwei.zhang
	 * @param fields
	 *            列名
	 */
	public void setFields(String fields) {
		this.fields = Arrays.asList(fields.split(","));
	}

	public void addField(String field) {
		this.fields.add(field);
	}

	/**
	 * 
	 private static final String
	 * oracle_template="select ${columns} from ${table} ${where} ${order}";
	 * private static final String mysql_template=
	 * "select ${columns} from ${table} ${where} ${order} ${limit}"; private
	 * static final String sqlserver_template=
	 * "select ${top} ${columns} from ${table} ${where} ${order}";
	 */
	@Override
	public String generateSQL() {
		Map<String, Object> params = new HashMap<String, Object>();
		String tableAliasName = "t";
		
		String template = null;

		switch (super.database) {
		case MYSQL:
			if (this.maxCount != null) {
				params.put("limit", " limit " + (this.maxCount - 1));
			}
			template = mysql_template;
			break;
		case HIVE:
			if (this.maxCount != null) {
				params.put("limit", " limit " + (this.maxCount - 1));
			}
			template = hive_template;
			break;
		case SQLSERVER:
			if (this.maxCount != null) {
				params.put("top", " top " + this.maxCount);
			}
			template = sqlserver_template;
			break;
		case ORACLE:
			if (this.maxCount != null) {
				super.condationList.add(SQLCondition.lte("rownum", this.maxCount));
			}
			//this.fields.add(tableAliasName+".rowid");
			template = oracle_template;
			break;
		case DB2:
			break;
		default:
		}
		if (this.fields.size() == 0) {
			this.fields.add("*");
		}
		String columns = "";
		// 设置别名
		for (String x : this.fields) {
			columns += tableAliasName + "." + x + ",";
		}
		columns = columns.replaceAll(",$", "");
		params.put("columns", columns);
		params.put("table", this.tableName + " t");
		params.put("where", super.generateConditionSQL());
		params.put("order", super.generateOrderSql());
		String sql = parseTemplate(template, params);
		log.debug(sql);
		return sql;
	}

	/**
	 * 解析模板。
	 * 
	 * @date 2014-1-13
	 * @author jiwei.zhang
	 * @param template
	 * @param params
	 * @return
	 */
	private static String parseTemplate(String template, Map<String, Object> params) {
		if (template == null)
			return null;
		Pattern patten = Pattern.compile("\\$\\{(.*?)\\}");
		Matcher matcher = patten.matcher(template);
		String ret = template;
		while (matcher.find()) {
			String var = matcher.group(1);
			Object val = params.get(var);
			if (val == null)
				val = "";
			ret = ret.replace(matcher.group(), val.toString());
		}
		return ret;
	}

	/**
	 * 使用JPA实体类自动构造select. 暂时不受支持
	 * 
	 * @date 2014-1-11
	 * @author jiwei.zhang
	 * @param entity
	 * @return
	 */
	public static CopyOfSelectSQL fromEntity(Class entity) {
		return null;
	}

	public String toString() {
		return this.generateSQL();
	}

	public static void main(String[] args) {
		String template = "select ${columns} from ${table} t1  ${where} ${order}";
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("columns", "a,b,c");
		String ret = parseTemplate(template, params);
		System.out.println(ret);
	}
}
