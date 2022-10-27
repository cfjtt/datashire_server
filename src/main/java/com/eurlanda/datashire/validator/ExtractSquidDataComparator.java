package com.eurlanda.datashire.validator;

import cn.com.jsoft.jframe.utils.CollectionUtils;
import cn.com.jsoft.jframe.utils.DateTimeUtils;
import cn.com.jsoft.jframe.utils.FileUtils;
import com.eurlanda.datashire.adapter.HyperSQLManager;
import com.eurlanda.datashire.adapter.db.AdapterDataSourceManager;
import com.eurlanda.datashire.adapter.db.INewDBAdapter;
import com.eurlanda.datashire.entity.DBConnectionInfo;
import com.eurlanda.datashire.entity.DBSourceTable;
import com.eurlanda.datashire.entity.DbSquid;
import com.eurlanda.datashire.entity.ExtractSquid;
import com.eurlanda.datashire.entity.ReferenceColumn;
import com.eurlanda.datashire.enumeration.DataBaseType;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.enumeration.datatype.DbBaseDatatype;

import javax.sql.DataSource;
import java.io.File;
import java.lang.reflect.Array;
import java.sql.Connection;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

/**
 * ExtractSquid抽取数据校验器，用来检验抽取的数据是否和原来的数据一致
 * 
 * @author Gene
 * 
 */
public class ExtractSquidDataComparator {
	private static Map<String, DataSource> dbs = new HashMap<String, DataSource>();
	private static String repositoryName = null;
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");

	public static class MyLogger {
		private StringBuilder sb = new StringBuilder();

		public void info(String msg) {
			info(msg, null);
		}

		public void info(String msg, Object... params) {
			if (params != null && msg != null) {
				for (Object p : params)
					msg = msg.replaceFirst("\\{\\}", p.toString());
			}
			msg = DateTimeUtils.currentDateTime() + " INFO " + msg + "\n";
			sb.append(msg);
			System.out.print(msg);
		}

		public void error(String msg) {
			error(msg, null);
		}

		public void error(String msg, Object... params) {
			if (params != null && msg != null) {
				for (Object p : params)
					msg = msg.replaceFirst("\\{\\}", p.toString());
			}
			msg = DateTimeUtils.currentDateTime() + " ERROR " + msg + "\n";
			sb.append(msg);
			System.err.print(msg);
		}

		public String toString() {
			return sb.toString();
		}
	}

	public static void main(String[] args) {
		Scanner s = new Scanner(System.in);

		while (true) {
			System.out.println("比较squid请输入RepositoryName SquidFlowId,如prod 294；比较squidFlow请输入repositoryName squidId s,如prod 23 s");
			String line = s.nextLine();
			if (line.equals("exit"))
				break;
			String[] params = line.split("\\s");
			Integer squidFlowId = null;

			if (params.length == 1) { // 省略 repName
				squidFlowId = Integer.parseInt(params[0]);
			} else if (params.length >= 2) {
				repositoryName = params[0];
				squidFlowId = Integer.parseInt(params[1]);
			}

			if (repositoryName != null && squidFlowId != null) {
				if (params.length == 3) { // 比较 squidFlow.

					try {
						compareSquidFlow(repositoryName, squidFlowId);
					} catch (Exception e) {
						e.printStackTrace();
						System.err.println("数据检验过程中出现错误，请重试！");
					}
				} else {
					try { // 比较squid.
						compareExtractSquid(repositoryName, squidFlowId);
					} catch (Exception e) {
						e.printStackTrace();
						System.err.println("数据检验过程中出现错误，请重试！");
					}
				}
			} else {
				System.out.println("数据输入不合法，格式为:仓库名称  squidFlowId [SquidFlow]");
			}
			System.out.println();
		}
	}

	private static void compareSquidFlow(String repositoryName, Integer sfId) throws Exception {
		DataSource ds = getHsqlDbConnection(repositoryName);
		// 取出所有extractSquid
		List<ExtractSquid> successedSquids = new ArrayList<ExtractSquid>();
		List<ExtractSquid> failedSquids = new ArrayList<ExtractSquid>();
		String sql = "select * from DS_SQUID where squid_type_id=" + SquidTypeEnum.EXTRACT.value() + " and SQUID_FLOW_ID =  " + sfId;
		List<ExtractSquid> extractSquidList = HyperSQLManager.query2List(ds.getConnection(), false, sql, null, ExtractSquid.class);
		for (ExtractSquid es : extractSquidList) {
			boolean ret = false;
			try {
				ret = compareExtractSquid(repositoryName, es);
				if (ret) {
					successedSquids.add(es);
				} else {
					failedSquids.add(es);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
		System.out.println("-----------------------已经校验" + extractSquidList.size() + "个ExtractSquid-----------------------\n成功的squids:"+successedSquids.size());
		for (ExtractSquid es : successedSquids) {
			System.out.println("squidId:" + es.getId() + " name:" + es.getName() + " 校验成功！");
		}
		System.out.println("\n失败的 squids:"+failedSquids.size());
		for (ExtractSquid es : failedSquids) {
			System.out.println("squidId:" + es.getId() + " name:" + es.getName() + " 校验失败！");
		}

	}

	private static DataSource getHsqlDbConnection(String repositoryName) throws Exception {
		if (!dbs.containsKey(repositoryName)) {
            throw new RuntimeException("有事找叶昊辰......,2016-4-25, 修改dbcpDatasource 为 druid datasource");
            /**
			BasicDataSource dbcp = new BasicDataSource();
			dbcp.setUsername("sa");
			dbcp.setPassword("");
			dbcp.setDriverClassName("org.hsqldb.jdbcDriver");
			dbcp.setUrl("jdbc:hsqldb:hsql://192.168.137.2:9092/" + repositoryName);
			dbs.put(repositoryName, dbcp);
			return dbcp;
             */
		} else {
			return dbs.get(repositoryName);
		}
	}

	private static INewDBAdapter getDbcp(DbSquid dbinfo) throws Exception {
		DBConnectionInfo info = DBConnectionInfo.fromDBSquid(dbinfo);
		return AdapterDataSourceManager.getAdapter(info);
	}

	/**
	 * 比较一个extractSquid.
	 * 
	 * @param repositoryName
	 * @param squidId
	 * @throws Exception
	 */
	private static boolean compareExtractSquid(String repositoryName, Integer squidId) throws Exception {
		DataSource ds = getHsqlDbConnection(repositoryName);
		// 取出所有extractSquid
		String sql = "select * from DS_SQUID where id =  " + squidId + "squid_type_id=" + SquidTypeEnum.EXTRACT.value();
		List<ExtractSquid> extractSquidList = HyperSQLManager.query2List(ds.getConnection(), false, sql, null, ExtractSquid.class);
		return compareExtractSquid(repositoryName, extractSquidList.get(0));
	}
	/**
	 * 按数据库修饰字段，比如sqlserver 用[]， mysql用``
	 * @param squid
	 * @param fname
	 * @return
	 */
	private static String decorateField(DbSquid squid,String name){
		DataBaseType dbt = DataBaseType.parse(squid.getDb_type());
		switch(dbt){
		 case MYSQL:
             return '`' + name + '`';
         case SQLSERVER:
             return '[' + name + ']';
         case ORACLE:
             return '"' + name + '"';
         case HBASE_PHOENIX:
             return '"' + name.toUpperCase() + '"';
         default:
             return name;
		}
	}
	/**
	 * 判断该字段类型是否能被排序。
	 * @param type
	 * @return
	 */
	private static boolean validateOrderField(DbBaseDatatype type){
		switch (type) {
		case XML:
		case GEOGRAPHY:
			
			return false;

		default:
			return true;
		}
	}
	/**
	 * 比较extractSquid数据。
	 * 
	 * @param repositoryName
	 *            仓库名称
	 * @param squidId
	 *            extractSquid id.
	 * @throws Exception
	 */
	private static boolean compareExtractSquid(String repositoryName, ExtractSquid squid) throws Exception {
		MyLogger log = new MyLogger();
		boolean success = true;
		// 取squid定义。
		log.info("正在校验 SquidId[{}],SquidIName:[{}],RepositoryName:[{}]", squid.getId(), squid.getName(), repositoryName);
		DataSource ds = getHsqlDbConnection(repositoryName);
		Connection hsqlConn = ds.getConnection();

		List<ReferenceColumn> refs = HyperSQLManager.query2List(hsqlConn, false, "select * from DS_REFERENCE_COLUMN where REFERENCE_SQUID_ID=" + squid.getId(), null, ReferenceColumn.class);

		DBSourceTable sourceTable = HyperSQLManager.query2List(hsqlConn, false, "select * from ds_source_table where id=" + squid.getSource_table_id(), null, DBSourceTable.class).get(0);

		DbSquid dbSquid = HyperSQLManager.query2List(hsqlConn, false, "select * from DS_SQUID where id=" + sourceTable.getSource_squid_id(), null, DbSquid.class).get(0);
		List<DbSquid> fallSquids = HyperSQLManager.query2List(hsqlConn, false, "select * from DS_SQUID where id=" + squid.getDestination_squid_id(), null, DbSquid.class);
		hsqlConn.close();
		DbSquid fallSquid = null;
		if (fallSquids.size() > 0) {
			fallSquid = fallSquids.get(0);
		} else {
			log.error("此squid{}未落地，跳过处理", squid.getId());
			throw new RuntimeException();
		}

		StringBuilder sb = new StringBuilder();
		String fromTableName = sourceTable.getTable_name();
		String toTableName = squid.getTable_name();
		String dbA = dbSquid.getDb_name();
		String dbB = fallSquid.getDb_name();

		log.info("正在获取数据A连接");
		INewDBAdapter jdbcA = getDbcp(dbSquid); // sbSquid adapter.

		log.info("正在获取数据B连接");
		INewDBAdapter jdbcB = getDbcp(fallSquid);

		List<String> headers = new ArrayList<String>();
		
		List<String> colA = new ArrayList<String>();
		List<String> colB = new ArrayList<String>();
		List<String> orderA = new ArrayList<String>();
		List<String> orderB = new ArrayList<String>();
		
		for (ReferenceColumn rc : refs) {
			DbBaseDatatype type = DbBaseDatatype.parse(rc.getData_type());
			
			// 构造字段。
			
			String ca = decorateField(dbSquid, rc.getName());
			String cb = decorateField(fallSquid, rc.getName());
			
			headers.add(rc.getName().toUpperCase());
			colA.add(ca);
			colB.add(cb);
			
			if(validateOrderField(type)){
				orderA.add(ca);
				orderB.add(cb);
			}
			

		}
		
		List<Map<String, Object>> retA = null;
		List<Map<String, Object>> retB = null;

		try {
			String sqla = "select " + CollectionUtils.join(colA) + " from " + fromTableName + " order by " +CollectionUtils.join(orderA);
			log.info("load data A:" + sqla);
			retA = jdbcA.queryForList(sqla);
		} catch (Exception e) {
			log.error(dbA + "库，读取表" + fromTableName + "数据失败！" + e.getMessage());
			success = false;
		}
		try {
			String sqla = "select " + CollectionUtils.join(colB) + " from " + toTableName + " order by " + CollectionUtils.join(orderB);
			log.info("load data B:" + sqla);
			retB = jdbcB.queryForList(sqla);
		} catch (Exception e) {
			log.error(dbB + "库，读取表" + toTableName + "数据失败！" + e.getMessage());
			success = false;
		}

		if (retA.size() != retB.size()) {
			log.error(String.format("两表数据量不同%s表：%s条，%s表：%s条。", fromTableName, retA.size(), toTableName, retB.size()));
			success = false;
		}

	

		for (String key : headers) {
			sb.append(key + "\t");
		}
		sb.append("\n");
		// 逐行比较。
		int count = 0;
		if (success) {
			for (int i = 0; i < retA.size(); i++) {
				count++;

				Map<String, Object> mapA = retA.get(i);
				Map<String, Object> mapB = retB.get(i);

				// 对比所有出错的列，并记录。
				Set<String> errHeader = new HashSet<String>();
				for (String key : headers) {
					boolean colfg = true;
					String A = str(mapA.get(key));
					String B = str(mapB.get(key));
					if (A == null && B == null) {

					} else if (A != null && B != null) {
						if (A.getClass().isArray() && B.getClass().isArray()) { // array比较
							if (Array.getLength(A) != Array.getLength(B)) {
								colfg = false;
							}
						} else if (!A.equals(B)) {
							colfg = false;
						}

					} else {
						colfg = false;
					}

					if (!colfg) {
						log.error(String.format("字段%s,值匹配失败：%s=[%s] %s=[%s]", key, fromTableName, A, toTableName, B));
						errHeader.add(key);
					}
				}
				if (errHeader.size() > 0) {
					success = false;
					for (String key : headers) {
						String A = str(mapA.get(key));
						if (errHeader.contains(key)) {
							sb.append("[" + A + "]" + "\t");
						} else {
							sb.append(A + "\t");
						}
					}
					sb.append("\n");
					for (String key : headers) {
						String B = str(mapB.get(key));
						if (errHeader.contains(key)) {
							sb.append("[" + B + "]" + "\t");
						} else {
							sb.append(B + "\t");
						}
					}
					sb.append("\n\n");
				}
			}
		}

		if (retA.size() != count) {
			log.error("数据总量不同：" + retA.size() + "，已检验" + count + "行");
			success = false;
		}

		if (!success) {
			File target = new File(repositoryName + "_" + squid.getId() + "_compare_" + fromTableName + "_" + toTableName + ".txt");
			log.error("数据校验失败\n请查看数据文件：" + target);
			FileUtils.writeFile(target.getPath(), log.toString() + " ================数据行==========\n" + sb.toString());
		} else {
			log.info(">>> 数据总量" + retA.size() + "，已检验" + count + "行，数据校验成功！");
		}
		return success;
	}

    private static String str(Object obj) {
    	if(obj == null) return null;
    	
        if(obj instanceof Timestamp) {
            return sdf.format(new Date(((Timestamp)obj).getTime()));
        } else if(obj instanceof Time) {
            return ((Time)obj).getTime() + "";
        }else if(obj instanceof Boolean){
        	if((Boolean) obj){
        		return "1";
        	}else{
        		return "0";
        	}
        } else {
            return obj.toString();
        }
    }

}
