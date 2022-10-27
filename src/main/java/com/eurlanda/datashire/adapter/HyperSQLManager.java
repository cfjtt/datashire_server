package com.eurlanda.datashire.adapter;

import com.eurlanda.datashire.adapter.db.AbsDBAdapter;
import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;
import com.eurlanda.datashire.entity.Column;
import com.eurlanda.datashire.entity.DbSquid;
import com.eurlanda.datashire.entity.operation.BeyondSquidException;
import com.eurlanda.datashire.entity.operation.ResultSets;
import com.eurlanda.datashire.enumeration.DMLType;
import com.eurlanda.datashire.exception.SystemErrorException;
import com.eurlanda.datashire.socket.ServerEndPoint;
import com.eurlanda.datashire.utility.AnnotationHelper;
import com.eurlanda.datashire.utility.CommonConsts;
import com.eurlanda.datashire.utility.DatabaseException;
import com.eurlanda.datashire.utility.DbUtils;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.StringUtils;
import org.apache.commons.collections.map.CaseInsensitiveMap;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.lang.reflect.Field;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * HyperSQL内存数据库操作处理类
 *   系统权限库数据增删查改
 *   
 * @author dang.lu 2013-10-2
 *
 */
public class HyperSQLManager extends DataSourceManager implements IRelationalDataManager {
// A class with too many methods is probably a good suspect for refactoring, 
// in order to reduce its complexity and find a way to have more fine grained objects.
	
	private static final Logger logger = LogManager.getLogger(HyperSQLManager.class);
	/** 数据库连接，仅供数据更新使用（纯查询类业务连接建议即时开启、关闭） */
	private Connection connection;
	/** 列的基本信息缓存（必须数据结构不会动态变更）*/
	private static final Map<String, Map<String, Column>> ColumnMetaDataCache = new HashMap<String, Map<String, Column>>();
	
	public HyperSQLManager(DbSquid dataStore) {
		super(dataStore);
	}

	public void openSession(){
		openSession(false);
	}
	
	public void openSession(boolean withSession){
		super.setDefaultAutoCommit(false);
		setConnection(super.openAdapter());
	}
	
	public synchronized Connection getConnection(){
	    return this.connection;
	}
	
	public synchronized void setConnection(Connection connection){
		this.connection = connection;
	}
	
	public synchronized void closeSession(){
		//closeConnectionLog(getConnection());
		DbUtils.commitAndCloseQuietly(getConnection());
	}
	
	@Override
	public void commit()  throws SQLException{
		if(getConnection()!=null&&!getConnection().isClosed()) getConnection().commit();
	}

	@Override
	public void rollback()  throws SQLException{
		if(getConnection()!=null&&!getConnection().isClosed()) getConnection().rollback();
	}


	/**
	 * 执行insert操作，返回自增主键
	 * @param sql
	 * @param params
	 * @return 自增主键
	 * @throws SQLException
	 */
	public int insertObjReturnId(String sql, List<Object> params) throws SQLException {
		if(StringUtils.isNull(sql)){
			logger.warn("sql is null !");
			return 0;
		}else{
			logger.debug(MessageFormat.format("sql: {0}, params: {1}", sql, params));
			if(getConnection()==null||getConnection().isClosed()){
				logger.error("Connection is null or closed !");
				return 0;
			}
		}
		PreparedStatement pt = null;
		try {
			logger.debug("执行sql语句:"+sql);
			//System.out.println(sql);
			pt = getConnection().prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			if (!(getConnection().getMetaData() instanceof org.apache.phoenix.jdbc.PhoenixDatabaseMetaData)){
				pt.setQueryTimeout(ServerEndPoint.querytimeout);
			}
			if(params!=null){
				for(int i=0; i<params.size(); i++){
					pt.setObject(i+1, params.get(i));
				}
			}
			//logger.info(MessageFormat.format("sql: {0}, params: {1}", sql, params));
			int rows = pt.executeUpdate();
			logger.debug("被影响的行数" + rows);

			int id = 0;
			ResultSet rs = pt.getGeneratedKeys();
			if (rs.next())
				id = rs.getInt(1);
			return id;
		}finally{
			DbUtils.closeQuietly(pt);
			//important! getConnection() 需要手工关闭
		}
	}

	/**
	 * 执行更新语句
	 * @param sql
	 * @param params
	 * @return
	 * @throws SQLException
	 */
	public int execute(String sql, List<Object> params) throws SQLException {
		if(StringUtils.isNull(sql)){
			logger.warn("sql is null !");
			return 0;
		}else{
			logger.debug(MessageFormat.format("sql: {0}, params: {1}", sql, params));
			if(getConnection()==null||getConnection().isClosed()){
				logger.error("Connection is null or closed !");
				return 0;
			}
		}
		PreparedStatement pt = null;
		try {
			logger.debug("执行sql语句:"+sql);
			//System.out.println(sql);
			pt = getConnection().prepareStatement(sql);
			if (!(getConnection().getMetaData() instanceof org.apache.phoenix.jdbc.PhoenixDatabaseMetaData)){
				pt.setQueryTimeout(ServerEndPoint.querytimeout);
			}
			if(params!=null){
				for(int i=0; i<params.size(); i++){
					pt.setObject(i+1, params.get(i));
				}
			}
			//logger.info(MessageFormat.format("sql: {0}, params: {1}", sql, params));
			return pt.executeUpdate();
		}finally{
			DbUtils.closeQuietly(pt);
			//important! getConnection() 需要手工关闭
		}
	}
	
	public int execute(String sql) throws DatabaseException{
		if(StringUtils.isNotNull(sql)){
			logger.debug(MessageFormat.format("sql: {0}", sql));
			if(getConnection()==null){
				logger.error("getConnection() is null");
				return 0;
			}
			Statement pt = null;
			try {
				if(getConnection().isClosed()){
					logger.error("getConnection() closed");
					return 0;
				}
				pt = getConnection().createStatement();
				if (!(getConnection().getMetaData() instanceof org.apache.phoenix.jdbc.PhoenixDatabaseMetaData)){
					pt.setQueryTimeout(ServerEndPoint.querytimeout);
				}

				return pt.executeUpdate(sql);
			}catch(SQLException e){
				logger.error("update failed:\t"+sql, e);
				throw new DatabaseException("database update failed" + sql, e);
			}finally{
				DbUtils.closeQuietly(pt); //important! getConnection() 需要手工关闭
			}
		}else{
			logger.warn("sql is null:\t"+sql);
			return 0;
		}
	}
	
	/**
	 * 执行查询语句
	 * @param sql
	 * @param params
	 * @return List<Map<列名, 列值>>
	 * @throws SQLException
	 */
	public List<Map<String, Object>> query2List(String sql, List<Object> params) throws SQLException{
		return query2List(getDBCPConnection(), true, sql, params);
	}
	
	public List<Map<String, Object>> query2List(boolean inSession, String sql, List<Object> params) throws SQLException{
		return inSession?query2List(this.getConnection(), false, sql, params):query2List(getDBCPConnection(), true, sql, params);
	}
	
	public List<Map<String, Object>> query2List4(boolean inSession, String sql, List<Object> params) throws SQLException{
	        return inSession?query2List4(this.getConnection(), false, sql, params):query2List4(getDBCPConnection(), true, sql, params);
	    }
	   
	public Map<String, Object> query2Object(boolean inSession, String sql, List<Object> params) throws SQLException{
		return inSession?query2Object(this.getConnection(), false, sql, params):query2Object(getDBCPConnection(), true, sql, params);
	}
	
	public Map<String, Object> query2Object4(boolean inSession, String sql, List<Object> params) throws SQLException{
		return inSession?query2Object4(this.getConnection(), false, sql, params):query2Object4(getDBCPConnection(), true, sql, params);
	}
	
	public Map<String, Object> query2Object(String sql, List<Object> params) throws SQLException{
		return query2Object(getDBCPConnection(), true, sql, params);
	}
	
	public static Map<String, Object> query2Object(Connection conn, boolean closeCon, String sql, List<Object> params) throws SQLException{
		try{
			List<Map<String, Object>> list = query2List(conn, closeCon, sql, params);
			if(list!=null && !list.isEmpty() && list.get(0)!=null){
				return list.get(0);
			}
		}finally{
			if(closeCon)DbUtils.closeQuietly(conn);
		}
		return null;
	}
	
	public static Map<String, Object> query2Object4(Connection conn, boolean closeCon, String sql, List<Object> params) throws SQLException{
		try{
			List<Map<String, Object>> list = query2List4(conn, closeCon, sql, params);
			if(list!=null && !list.isEmpty() && list.get(0)!=null){
				return list.get(0);
			}
		}finally{
			if(closeCon)DbUtils.closeQuietly(conn);
		}
		return null;
	}
	/** 仅供内部使用同一个事务时使用(conn需要手工关闭) */
	public static List<Map<String, Object>> query2List(Connection conn, boolean closeCon, String sql, List<Object> params) throws SQLException{
		logger.debug(MessageFormat.format("[Query As List]\tsql: {0}, params: {1}", sql, params));
		if(StringUtils.isNull(sql)){
			logger.warn("sql is null !");
			return null;
		}
		if(conn==null||conn.isClosed()){
			logger.error("Connection is null or closed !");
			return null;
		}
		ResultSetMetaData md = null; 
		ResultSet rs=null;
		PreparedStatement pt=null;
		List<Map<String, Object>> list = null;
		Map<String, Object> map=null;
		try {
			//pt = conn.prepareStatement(sql);
			pt = (PreparedStatement) conn.prepareStatement(sql,ResultSet.TYPE_FORWARD_ONLY,  
                    ResultSet.CONCUR_READ_ONLY);
			if(params!=null){
				for(int i=0; i<params.size(); i++){
					pt.setObject(i+1, params.get(i));
				}
			}
			if (!(conn.getMetaData() instanceof org.apache.phoenix.jdbc.PhoenixDatabaseMetaData)){
				pt.setQueryTimeout(ServerEndPoint.querytimeout);
				//pt.setFetchSize(10000);
				//pt.setFetchDirection(ResultSet.FETCH_REVERSE);
			}
			//System.out.println(sql);
			rs = pt.executeQuery();
			if(rs!=null){
				list = new ArrayList<Map<String, Object>>();
				md = rs.getMetaData();
				int cols = md.getColumnCount();
				while(rs.next()){
					map = new HashMap<String, Object>();
					for(int i=1; i<=cols; i++){
						map.put(md.getColumnLabel(i).toUpperCase(), rs.getObject(i));
					}
					list.add(map);
				}
			}
			logger.debug("[Query As List]\t"+list);
			return list;
		}finally{
			DbUtils.closeQuietly(closeCon?conn:null, pt, rs);
		}
	}

	/**
	 * 
	 * @param conn
	 * @param closeCon
	 * @param sql
	 * @param params
	 * @return
	 * @throws SQLException
	 * @author bo.dang
	 * @date 2014年7月7日
	 */
    public static List<Map<String, Object>> query2List4(Connection conn, boolean closeCon, String sql, List<Object> params) throws SQLException{
        logger.debug(MessageFormat.format("[Query As List]\tsql: {0}, params: {1}", sql, params));
        if(StringUtils.isNull(sql)){
            logger.warn("sql is null !");
            return null;
        }
        if(conn==null||conn.isClosed()){
            logger.error("Connection is null or closed !");
            return null;
        }
        ResultSetMetaData md = null; 
        ResultSet rs=null;
        PreparedStatement pt=null;
        List<Map<String, Object>> list = null;
        Map<String, Object> map=null;
        try {
            //pt = conn.prepareStatement(sql);
            pt = (PreparedStatement) conn.prepareStatement(sql,ResultSet.TYPE_FORWARD_ONLY,  
                    ResultSet.CONCUR_READ_ONLY);
            if(params!=null){
                for(int i=0; i<params.size(); i++){
                    pt.setObject(i+1, params.get(i));
                }
            }
            if (!(conn.getMetaData() instanceof org.apache.phoenix.jdbc.PhoenixDatabaseMetaData)){
                pt.setQueryTimeout(ServerEndPoint.querytimeout);
                //pt.setFetchSize(10000);
                //pt.setFetchDirection(ResultSet.FETCH_REVERSE);
            }
            //System.out.println(sql);
            rs = pt.executeQuery();
            if(rs!=null){
                list = new ArrayList<Map<String, Object>>();
                md = rs.getMetaData();
                int cols = md.getColumnCount();
                while(rs.next()){
                    map = new HashMap<String, Object>();
                    for(int i=1; i<=cols; i++){
//                        map.put(md.getColumnName(i), rs.getObject(i));
                        map.put(md.getColumnLabel(i), rs.getObject(i));
                    }
                    list.add(map);
                }
            }
            logger.debug("[Query As List]\t"+list);
            return list;
        }finally{
            DbUtils.closeQuietly(closeCon?conn:null, pt, rs);
        }
    }
	/**
	 * 查看指定表的所有列信息: 
	 * 		列名、类型、是否自增等
	 * @param tableName
	 * @return
	 * @throws SQLException
	 */
	private Map<String, Column> getColumnMetaData(Connection conn, String tableName) throws SQLException{
		if(StringUtils.isNull(tableName)){
			return null;
		}else if(ColumnMetaDataCache.containsKey(tableName)){
			return ColumnMetaDataCache.get(tableName);
		}
		// 不查数据，只看列信息
		String sql="SELECT * FROM "+tableName+" WHERE 1=2";
		ResultSet rs=null;
		Statement pt=null;
		ResultSetMetaData md = null; 
		pt = conn.createStatement();
		rs = pt.executeQuery(sql);
		md = rs.getMetaData();
		int cols = md.getColumnCount();
		Map<String, Column> ret = new HashMap<String, Column>();
		for (int i=1; i<=cols; i++) {
			ret.put(md.getColumnName(i).toUpperCase(), new Column(
						md.getColumnName(i),
						md.getColumnType(i),
						md.isAutoIncrement(i)
					));
			if(md.isAutoIncrement(i)){
				ret.put(CommonConsts.AutoIncrementColumn, null);
			}
		}
		ColumnMetaDataCache.put(tableName, ret);
		DbUtils.closeQuietly(null, pt, rs);
		return ret;
	}
	
	public Map<String, Column> getColumnMetaData(String tableName) throws SQLException{
		if(StringUtils.isNull(tableName)){
			return null;
		}
		Connection conn = super.getDBCPConnection();
		try{
			return getColumnMetaData(conn, tableName);
		}finally{
			DbUtils.closeQuietly(conn);
		}
	}
	
	public static <T>List<T> query2List(Connection conn, boolean closeCon, String sql, List<String> params, Class<T> c){
		logger.debug(MessageFormat.format("sql: {0}, params: {1}", sql, params));
		ResultSetMetaData md = null; 
		ResultSet rs=null;
		PreparedStatement pt=null;
		// 返回业务对象
		List<T> ret = null;
		// 数据库一条记录<列名，列值>
		Map<String, Object> map=null;
		try {
			//pt = conn.prepareStatement(sql);
			//System.out.println(sql);
			pt = (PreparedStatement) conn.prepareStatement(sql,ResultSet.TYPE_FORWARD_ONLY,  
                    ResultSet.CONCUR_READ_ONLY);
			if(params!=null){
				for(int i=0; i<params.size(); i++){
					pt.setObject(i+1, params.get(i));
				}
			}
			if (!(conn.getMetaData() instanceof org.apache.phoenix.jdbc.PhoenixDatabaseMetaData)){
				pt.setQueryTimeout(ServerEndPoint.querytimeout);
				//pt.setFetchSize(10000);
				//pt.setFetchDirection(ResultSet.FETCH_REVERSE);
			}
			//System.out.println(sql+" > obj : "+ params==null?"":params.toString());
			//logger.info(MessageFormat.format("sql: {0}, params: {1}", sql, params));
			////pt.setFetchSize(10000);
			rs = pt.executeQuery();
			if(rs!=null){
				ret = new ArrayList<T>();
				md = rs.getMetaData();
				int cols = md.getColumnCount();
				int i=1;
				while(rs.next()){
					//map = new HashMap<String, Object>(cols, DEFAULT_LOAD_FACTOR);
					map = new CaseInsensitiveMap();
					for(i=1; i<=cols; i++){
						map.put(md.getColumnLabel(i), rs.getObject(i));
					}
					ret.add(AnnotationHelper.result2Object(map, c));
				}
			}
			return ret;
		}catch (Exception e) {
			logger.error("查询解析异常", e);
			return null;
		}finally{
			DbUtils.closeQuietly(closeCon?conn:null, pt, rs);
		}
	}
	
	public <T>List<T> query2List(String sql, List<String> params, Class<T> c){
		Connection conn;
		if(StringUtils.isNull(sql)){
			logger.debug("sql is null !");
			return null;
		}else{
			logger.debug(MessageFormat.format("sql: {0}, params: {1}", sql, params));
			conn = super.getDBCPConnection();
			try {
				if(conn==null||conn.isClosed()){
					logger.error("Connection is null or closed !");
					return null;
				}
			} catch (SQLException e) {
				logger.error("Connection is closed !", e);
			}
		}
		return query2List(conn, true, sql, params, c);
	}
	
	public <T> T query2Object(Map<String, String> params, Class<T> c){
		List<T> list = query2List(params, c);
		if(list!=null && !list.isEmpty()){
			return list.get(0);
		}
		return null;
	}
	
	public <T> T query2Object(boolean inSession, Map<String, Object> params, Class<T> c){
		List<T> list = query2List2(inSession, params, c);
		if(list!=null && !list.isEmpty()){
			return list.get(0);
		}
		return null;
	}
	
	public <T> T query2Object2(boolean inSession, Map<String, String> params, Class<T> c){
		List<T> list = query2List(inSession, params, c);
		if(list!=null && !list.isEmpty()){
			return list.get(0);
		}
		return null;
	}
	
	public <T>List<T> query2List(boolean inSession, Map<String, String> params, Class<T> c){	
		String [] tm = AnnotationHelper.getTableName(c);
		StringBuffer b = new StringBuffer(100);
		b.append("SELECT * FROM ").append(tm[tm.length-1]);
		List<String> p = new ArrayList<String>();
		if(params!=null && !params.isEmpty()){
			b.append(" WHERE ");
			Iterator<String> it = params.keySet().iterator();
			int i=1;
			while(it.hasNext()){
				String key = it.next();
				p.add(params.get(key));
				b.append(key).append("=?");
				if(i++!=params.size()){ // to avoid "where 1=1"
					b.append(" AND ");
				}
			}
		}
		return inSession?query2List(this.getConnection(), false, b.toString(), p, c):query2List(b.toString(), p, c);
	}
	
	public <T>List<T> query2List3(boolean inSession, Map<String, String> params, Class<T> c){	
		String [] tm = AnnotationHelper.getTableName(c);
		StringBuffer b = new StringBuffer(100);
		b.append("SELECT * FROM ").append(tm[tm.length-1]);
		List<String> p = new ArrayList<String>();
		if(params!=null && !params.isEmpty()){
			b.append(" WHERE ");
			Iterator<String> it = params.keySet().iterator();
			int i=1;
			while(it.hasNext()){
				String key = it.next();
				p.add(params.get(key));
				b.append(key).append("=?");
				if(i++!=params.size()){ // to avoid "where 1=1"
					b.append(" AND ");
				}
			}
		}
		return inSession?query2List(this.getConnection(), true, b.toString(), p, c):query2List(b.toString(), p, c);
	}
	
	public <T>List<T> query2List2(boolean inSession, Map<String, Object> params, Class<T> c){
		String [] tm = AnnotationHelper.getTableName(c);
		StringBuffer b = new StringBuffer(100);
		b.append("SELECT * FROM ").append(tm[tm.length-1]);
		List<String> p = new ArrayList<String>();
		if(params!=null && !params.isEmpty()){
			b.append(" WHERE ");
			Iterator<String> it = params.keySet().iterator();
			int i=1;
			while(it.hasNext()){
				String key = it.next();
				Object val = params.get(key);
				if(val==null){
					continue;
				}
				else if(val instanceof List){
					List list = (List)val;
					if(list!=null && !list.isEmpty()){
						b.append(key).append(" in (");
						for(int k=0; k<list.size(); k++){
							if(list.get(k) instanceof String){
								b.append('\'').append(list.get(k)).append('\'');
							}else{
								b.append(list.get(k));
							}
							if(k!=list.size()-1){
								b.append(',');
							}
						}
						b.append(')');
					}
				}
				else/* if(val instanceof String)*/{
					p.add(String.valueOf(val));
					b.append(key).append("=?");
				}
				if(i++!=params.size()){ // to avoid "where 1=1"
					b.append(" AND ");
				}
			}
		}

		return inSession?(this.getConnection()==null?null:query2List(this.getConnection(), false, b.toString(), p, c)):query2List(b.toString(), p, c);
	}
	
	public <T>List<T> query2List(Map<String, String> params, Class<T> c){
		return query2List(false, params, c);
	}
	
	public int delete(Map<String, String> params, String tableName) throws SQLException{
		StringBuffer b = new StringBuffer(100);
		b.append("DELETE FROM ").append(tableName);
		List p = new ArrayList();
		if(params!=null && !params.isEmpty()){
			b.append(" WHERE ");
			Iterator<String> it = params.keySet().iterator();
			int i=1;
			while(it.hasNext()){
				String key = it.next();
				p.add(params.get(key));
				b.append(key).append("=?");
				if(i++!=params.size()){
					b.append(" AND ");
				}
			}
		}else{
			logger.warn(MessageFormat.format("[DELETE ALL] sql: {0}", b.toString()));
			b.setLength(0);
			b.append("TRUNCATE TABLE ").append(tableName);
		}
		return execute(b.toString(), p);
	}
	
	public <T> int delete(Map<String, String> params, Class<T> c) throws SQLException{
		String[] tableName = AnnotationHelper.getTableName(c);
		if(tableName!=null){
			return delete(params, tableName[tableName.length-1]);
		}
		return 0;
	}

	/**
	 * 根据表名删除
	 */
	public int deleteByTableName(Map<String, String> params, String tableName)
			throws SQLException {
		StringBuffer b = new StringBuffer(100);
		b.append("DELETE FROM ").append(tableName);
		List p = new ArrayList();
		if(params!=null && !params.isEmpty()){
			b.append(" WHERE ");
			Iterator<String> it = params.keySet().iterator();
			int i=1;
			while(it.hasNext()){
				String key = it.next();
				p.add(params.get(key));
				b.append(key).append("=?");
				if(i++!=params.size()){
					b.append(" AND ");
				}
			}
			//logger.debug(MessageFormat.format("sql: {0}, params: {1}", b.toString(), p));
		}else{
			logger.warn(MessageFormat.format("[DELETE ALL] sql: {0}", b.toString()));
		}
		return execute(b.toString(), p);
	}

	/**
	 * 支持实体类和表对应关系一对多，支持复合主键
	 * @param obj 实体类（需要JDK1.4以上注解支持）
	 * @return
	 * @throws SQLException
     */
	public int insert2(Object obj) throws SQLException{
		if(obj==null){
			return -1;
		}
		int pk=0;
		
		// 注意：注解会继承
		String[] tableName=null;
//		TableMapping m = obj.getClass().getAnnotation(TableMapping.class);
//		if(m!=null){
//			tableName = new String[]{obj.getClass().getAnnotation(TableMapping.class).name()};
//		}
		MultitableMapping tm = obj.getClass().getAnnotation(MultitableMapping.class);
		if(tm!=null){
			tableName = tm.name();
		}
		if (tableName!=null&&tableName[0].equals("DS_SQUID")){
			if(CommonConsts.MaxSquidCount != -99) {
				String sqlstr = "select COUNT(*) as CNTID from DS_SQUID ";
				Map<String, Object> map = query2Object(true, sqlstr, null);
				if (map != null && map.containsKey("CNTID")) {
					int cntId = Integer.parseInt(map.get("CNTID") + "");
					if (CommonConsts.MaxSquidCount <= cntId) {
						throw new BeyondSquidException("squid count Beyond the limit");
					}
				}
			}
		}
		logger.debug(MessageFormat.format("save {0}, insert into {1}", obj.getClass().getSimpleName(), Arrays.toString(tableName)));
		try{
			for(int i=0; i<tableName.length; i++){
				/*if (tableName[i].equals("DS_SQUID")){
					String sqlstr = "select COUNT(*) as CNTID from ds_squid ";
					Map<String, Object> map = query2Object(true, sqlstr, null);
					if (map!=null&&map.containsKey("CNTID")){
						int cntId = Integer.parseInt(map.get("CNTID")+"");
						if (CommonConsts.MaxSquidCount<cntId){
							throw new RuntimeException("squid count Beyond the limit");
						}
					}
				}*/

				Map<String, Column> c = this.getColumnMetaData(getConnection(), tableName[i]);
				List<Column> list = getUpdatedColumns(obj, obj.getClass(), c, tableName[i], DMLType.INSERT);
				if(pk!=0){
					list.add(new Column("ID", pk));
				}
				pk = insert(tableName[i], list);
				// 如果表里含有自增列，则将自增列的值查出来
//				if(c.containsKey(CommonConsts.AutoIncrementColumn)){
//					String sql = null;
//					// 兼容没有key的表
//					if(c == null || c.get("KEY") == null || StringUtils.isNull(c.get("KEY").getValue())){
//						sql = "SELECT MAX(ID) AS ID FROM "+tableName[i];
//					}else{
//						sql = "SELECT MAX(ID) AS ID FROM "+tableName[i]+" WHERE 1=1 ";
//						sql += " and "+ AbsDBAdapter.initColumnNameByDbType(super.dataStore.getDb_type(), "KEY")+"='"+c.get("KEY").getValue()+"'";
//					}
//					List<Map<String, Object>> list2 = query2List(getConnection(), false, sql, null);
//					if(list2!=null && !list2.isEmpty() && list2.get(0)!=null){
//						pk = list2.get(0).get("ID")==null?0:Integer.parseInt(list2.get(0).get("ID").toString(), 10);
//					}
//				}
			}
		}catch(Exception e){
			logger.error(MessageFormat.format("[INSERT-MAPPING-ERROR] {0}", obj), e);
			throw new SQLException(e);
		}
		return pk;
	}
	
	/** 支持实体类和表对应关系一对多，支持复合主键 */
	public int insert2(Object obj, boolean isParms) throws SQLException{
		if(obj==null){
			return -1;
		}
		int pk=0;
		// 注意：注解会继承
		String[] tableName=null;
//		TableMapping m = obj.getClass().getAnnotation(TableMapping.class);
//		if(m!=null){
//			tableName = new String[]{obj.getClass().getAnnotation(TableMapping.class).name()};
//		}
		MultitableMapping tm = obj.getClass().getAnnotation(MultitableMapping.class);
		if(tm!=null){
			tableName = tm.name();
		}
		
		logger.debug(MessageFormat.format("save {0}, insert into {1}", obj.getClass().getSimpleName(), Arrays.toString(tableName)));
		try{
			for(int i=0; i<tableName.length; i++){
				Map<String, Column> c = this.getColumnMetaData(getConnection(), tableName[i]);
				List<Column> list = getUpdatedColumns(obj, obj.getClass(), c, tableName[i], DMLType.INSERT);
				if(pk!=0){
					list.add(new Column("ID", pk));
				}
				pk = insert(tableName[i], list);
				// 如果表里含有自增列，则将自增列的值查出来
//				if(c.containsKey(CommonConsts.AutoIncrementColumn)){
//					String sql = null;
//					// 兼容没有key的表
//					if(c == null || c.get("KEY") == null || StringUtils.isNull(c.get("KEY").getValue())){
//						sql = "SELECT MAX(ID) AS ID FROM "+tableName[i];
//					}else{
//						sql = "SELECT MAX(ID) AS ID FROM "+tableName[i]+" WHERE 1=1 ";
//						sql += " and "+ AbsDBAdapter.initColumnNameByDbType(super.dataStore.getDb_type(), "KEY")+"='"+c.get("KEY").getValue()+"'";
//					}
//					List<Map<String, Object>> list2 =
//							query2List(getConnection(), false, sql, null);
//					if(list2!=null && !list2.isEmpty() && list2.get(0)!=null){
//						pk = Integer.parseInt(list2.get(0).get("ID").toString(), 10);
//					}
//				}
			}
		}catch(Exception e){
			logger.error(MessageFormat.format("[INSERT-MAPPING-ERROR] {0}", obj), e);
		}
		return pk;
	}

	/**
	 * 更新数据库中表的个别字段(单表操作)
	 * @param map
	 * @return
	 * @throws SQLException
     */
	public boolean updateSomeColumn(Map<String,Object> map) throws SQLException, ClassNotFoundException, NoSuchFieldException {
		String sql="update";
		String condition="where";
		StringBuffer sbfSql=new StringBuffer(sql);
		StringBuffer sbfCondition=new StringBuffer(condition);
		Class<?> value=(Class<?>)map.get("tableName");
		//遍历Map集合，拼接成sql语句
		for(Map.Entry<String,Object> entry:map.entrySet())
			if ("tableName".toUpperCase().equals(entry.getKey().toUpperCase())) {
				MultitableMapping tm = value.getAnnotation(MultitableMapping.class);
				String tableName = tm.name()[0];
				sbfSql.insert(sql.indexOf("update") + 6, " " + tableName + " set");
			} else if ("id".toUpperCase().equals(entry.getKey().toUpperCase())) {
				//id不能为空
				if (entry.getValue() == null) {
					return false;
				}
				sbfCondition.append(" id=" + entry.getValue());

			} else {
				//获得指定的属性的类型
				Field field=value.getDeclaredField(entry.getKey().toLowerCase());
				String typeName = field.getType().getName();
				if("java.lang.String".equals(typeName)){
					//更新为当前系统的时间
					if("last_logon_date".equals(field.getName().toLowerCase())){
						sbfSql.append(" " + entry.getKey() + "= " + entry.getValue() + " ,");
					} else {
						sbfSql.append(" " + entry.getKey() + "= '" + entry.getValue() + "' ,");
					}

				} else {
					sbfSql.append(" " + entry.getKey() + "= " + entry.getValue() + " ,");
				}

			}
		sql=sbfSql.toString().substring(0,sbfSql.toString().lastIndexOf(","))+" "+sbfCondition.toString();
		System.out.println("执行sql语句: "+sql);
		try{
			int n=this.execute(sql);
			if(n>0){
				return true;
			} else {
				return false;
			}
		}catch(Exception e){
			logger.error(MessageCode.ERR_UPDATEUSER);
			return false;
		}
	}
	public boolean update2(Object obj) throws SQLException{
		return update2(obj, null);
	}
	
	public boolean update2(Object obj, List<String> otherColumns) throws SQLException{
		if(obj==null){
			return false;
		}
		MultitableMapping tm = obj.getClass().getAnnotation(MultitableMapping.class);
		String[] tableName = tm.name();//tm==null ? 
				// 兼容以前单表映射
				//new String[]{obj.getClass().getAnnotation(TableMapping.class).name()}:
				//tm.name();
		logger.debug(MessageFormat.format("save {0}, update {1}", obj.getClass().getSimpleName(), Arrays.toString(tableName)));
		int updatedCnt=0;
		try{
			for(int i=0; i<tableName.length; i++){
				Map<String, Column> c = this.getColumnMetaData(getConnection(), tableName[i]);
				List<Column> list = getUpdatedColumns(obj, obj.getClass(), c, tableName[i], DMLType.UPDATE);
				updatedCnt += update(tableName[i], list, otherColumns);
				logger.debug(MessageFormat.format("{0} rows updated, current table {1}", updatedCnt, tableName[i]));
			}
		}catch(Exception e){
			logger.error(MessageFormat.format("[UPDATE-MAPPING-ERROR] {0}", obj), e);
			return false;
		}
		return updatedCnt>=0?true:false;
	}

	/**
	 * 返回自增主键的插入方法
	 * @param tableName
	 * @param list
	 * @return 自增主键
	 * @throws DatabaseException
	 * @throws SQLException
	 */
	public int insert(String tableName, List<Column> list) throws DatabaseException, SQLException{
		if(StringUtils.isNull(tableName) || list==null || list.isEmpty()){
			return -1;
		}
		logger.debug(MessageFormat.format("新增数据：{0}\t{1}", tableName, list));
		StringBuffer colName = new StringBuffer(100);
		StringBuffer colValue = new StringBuffer(100);
		List<Object> parmas = new ArrayList<Object>();
		for(int i=0; i<list.size(); i++){
			colName.append(AbsDBAdapter.initColumnNameByDbType(super.dataStore.getDb_type(), list.get(i).getName()));
			if(list.get(i).getValue()==null){
				colValue.append("null");
			}else{
				colValue.append("?");
				parmas.add(list.get(i).getValue());
			}
			if(i!=list.size()-1){
				colName.append(',');
				colValue.append(',');
			}
		}
		//直接返回自增主键了
		return this.insertObjReturnId(MessageFormat.format(
				"INSERT  INTO {0}({1}) VALUES({2})",
				tableName, colName.toString(), colValue.toString()), parmas);
	}

	public int update(String tableName, List<Column> list) throws DatabaseException, SQLException{
		return update(tableName, list, null);
	}
	
	public int update(String tableName, List<Column> list, List<String> otherColumns) throws DatabaseException, SQLException{
		if(StringUtils.isNull(tableName) || list==null || list.isEmpty()){
			return -1;
		}
		logger.debug(MessageFormat.format("更新数据：{0}\t{1}", tableName, list));
		StringBuffer cols = new StringBuffer(100);
		StringBuffer where = new StringBuffer(100);
		List<Object> parmas = new ArrayList<Object>();
		where.append(" 1=1 ");
		for(int i=0; i<list.size(); i++){
			if(list.get(i).isIsPK()||(otherColumns!=null&&otherColumns.contains(list.get(i).getName()))){
				where.append(" and ");
				String colName = AbsDBAdapter.initColumnNameByDbType(super.dataStore.getDb_type(), list.get(i).getName());
				where.append(colName).append('=')
					 .append("'").append(list.get(i).getValue()).append("'");
			}else{
				String colName = AbsDBAdapter.initColumnNameByDbType(super.dataStore.getDb_type(), list.get(i).getName());
				cols.append(colName).append('=');
				if(list.get(i).getValue()==null){
					cols.append("null");
				}else{
					cols.append("?");
					parmas.add(list.get(i).getValue());
				}
				cols.append(',');
			}
			//where.append(',');
		}
		String sql = MessageFormat.format(
				"UPDATE {0} SET {1} WHERE {2}",
				tableName, cols.toString(), where.toString()).replaceAll(", WHERE", " WHERE");
		//System.out.println(sql);
		return cols.length()==0?0 : this.execute(sql, parmas);
	}
	
	public List<Column> getUpdatedColumns(Object obj, Class c, Map<String, Column> columnMap, String tableName, DMLType t) throws SQLException, IllegalArgumentException, IllegalAccessException{
		List<Column> list = new ArrayList<Column>();
		Object o=null;
		ColumnMpping cm=null;
		Field[] f = c.getDeclaredFields();
		String cmName=null;
		boolean isInsert = t == DMLType.INSERT; // 是否新增 （否则更新）
		for(int i=f.length-1; i>=0; i--){
			cm = f[i].getAnnotation(ColumnMpping.class);
// 1. Ignore		
			// 没有注解的忽略
			if(cm==null||cm.type()==Types.NULL) continue;
			cmName=cm.name().toUpperCase();
			//过滤掉 last_value,因为last——value使用一个新的接口，单独更新
			if(!isInsert && "LAST_VALUE".equals(cmName)){
				continue;
			}
			// 表结构里没有的列忽略
			if(!columnMap.containsKey(cmName)) continue;
			// 自增列新增时忽略
			if(isInsert && columnMap.get(cmName).isIsAutoIncrement()) continue; 
			f[i].setAccessible(true);
			o=f[i].get(obj);
			if(isInsert){
				// 值为空(且可空)的新增时也忽略
				if(o==null&&cm.nullable()&&StringUtils.isNull(cm.valueReg())){
					continue;
				}
				// id是默认值0的也忽略
				if("ID".equals(cmName) && "0".equals(o==null?"1":o.toString())){
					continue;
				}
			}
			logger.trace(MessageFormat.format("{2} {0}\t{1}", cm.name(), o, t));
// 2. Validate
			// 非空字段不能为空
			if(!cm.nullable() && o==null){
				throw new RuntimeException(MessageFormat.format("{0}.{1} can't be null !", tableName, cm.name()));
			}
			// 属性值超出精度
			if(o!=null && cm.type()==Types.VARCHAR && cm.precision()!=1 && o.toString().trim().length()>cm.precision()){
				throw new RuntimeException(MessageFormat.format("{0}.{1}: {2} too large({3})!", tableName, cm.name(), o.toString(), cm.precision()));
			}
// 3. Mapper			
			if(o!=null){
				// 类型转换 char(1) <-> boolean
				if(Boolean.TYPE.equals(f[i].getType())&&cm.type()==Types.CHAR&&cm.precision()==1){
					list.add(new Column(cm.name(), Boolean.TRUE.equals(o)?'Y':'N'));
				}
				// 数字类型值是0(且要求数据是大于0)的置空
				else if(cm.type()==Types.INTEGER&&Integer.TYPE.equals(f[i].getType())&&"0".equals(o.toString())&&">=1".equals(cm.valueReg())){
					list.add(new Column(cm.name(), null));
					//  if(!cm.nullable()) 抛出异常
				}
				// 更新时，根据主键值更新
				else if(!isInsert && 
						(columnMap.get(cmName).isIsAutoIncrement()
								|| "ID".equals(columnMap.get(cmName).getName()))){
					list.add(new Column(cm.name(), o, true));
					columnMap.get(cmName).setValue(o);
				}
				else{
					list.add(new Column(cm.name(), o));
					columnMap.get(cmName).setValue(o);
				}
			}
			// 值为空(且可空)的更新时置空
			else if(!isInsert && o==null&&cm.nullable()&&StringUtils.isNull(cm.valueReg())){
				list.add(new Column(cm.name(), null));
			}
		}
		if(c!=null && c.getSuperclass()!=null){
			list.addAll(getUpdatedColumns(obj, c.getSuperclass(), columnMap, tableName, t));
		}
		return list;
	}
	
	public List<Column> getUpdatedColumnsForMap(Map<String, Object> map, Class c, Map<String, Column> columnMap, String tableName, DMLType t) throws Exception{
		List<Column> list = new ArrayList<Column>();
		Object o=null;
		ColumnMpping cm=null;
		Field[] f = c.getDeclaredFields();
		String cmName=null;
		boolean isInsert = t == DMLType.INSERT; // 是否新增 （否则更新）
		for(int i=f.length-1; i>=0; i--){
			cm = f[i].getAnnotation(ColumnMpping.class);
// 1. Ignore		
			// 没有注解的忽略
			if(cm==null||cm.type()==Types.NULL) continue;
			cmName=cm.name().toUpperCase();
			// 表结构里没有的列忽略
			if(!columnMap.containsKey(cmName)) continue;
			// 自增列新增时忽略
			if(isInsert && columnMap.get(cmName).isIsAutoIncrement()) continue; 
			f[i].setAccessible(true);
			o=map.get(cmName);
			if(isInsert){
				// 值为空(且可空)的新增时也忽略
				if(o==null&&cm.nullable()&&StringUtils.isNull(cm.valueReg())){
					continue;
				}
				// id是默认值0的也忽略
				if("ID".equals(cmName) && "0".equals(o==null?"1":o.toString())){
					continue;
				}
			}
			logger.trace(MessageFormat.format("{2} {0}\t{1}", cm.name(), o, t));
// 2. Validate
			// 非空字段不能为空
			if(!cm.nullable() && o==null){
				throw new RuntimeException(MessageFormat.format("{0}.{1} can't be null !", tableName, cm.name()));
			}
			// 属性值超出精度
			if(o!=null && cm.type()==Types.VARCHAR && o.toString().trim().length()>cm.precision()){
				throw new RuntimeException(MessageFormat.format("{0}.{1}: {2} too large({3})!", tableName, cm.name(), o.toString(), cm.precision()));
			}
// 3. Mapper			
			if(o!=null){
				// 类型转换 char(1) <-> boolean
				if(Boolean.TYPE.equals(f[i].getType())&&cm.type()==Types.CHAR&&cm.precision()==1){
					list.add(new Column(cm.name(), Boolean.TRUE.equals(o)?'Y':'N'));
				}
				// 数字类型值是0(且要求数据是大于0)的置空
				else if(cm.type()==Types.INTEGER&&Integer.TYPE.equals(f[i].getType())&&"0".equals(o.toString())&&">=1".equals(cm.valueReg())){
					list.add(new Column(cm.name(), null));
					//  if(!cm.nullable()) 抛出异常
				}
				// 更新时，根据主键值更新
				else if(!isInsert && 
						(columnMap.get(cmName).isIsAutoIncrement()
								|| "ID".equals(columnMap.get(cmName).getName()))){
					list.add(new Column(cm.name(), o, true));
					columnMap.get(cmName).setValue(o);
				}
				else{
					list.add(new Column(cm.name(), o));
					columnMap.get(cmName).setValue(o);
				}
			}
			// 值为空(且可空)的更新时置空
			else if(!isInsert && o==null&&cm.nullable()&&StringUtils.isNull(cm.valueReg())){
				list.add(new Column(cm.name(), null));
			}
		}
		return list;
	}
	
	@Override
	public int executeBatch(String sql, List<List<Object>> params) throws SQLException {
	  	long startTime = System.currentTimeMillis();
	  	int size = params==null?0:params.size();
	  	logger.debug("Execute Batch ["+size+"]\t"+sql);
	    if(size==0 || sql==null || getConnection()==null) return -1;
	    PreparedStatement stm = null;
		List data=null;
		int cnt=0;
        try {
        	stm = getConnection().prepareStatement(sql);
        	if (!(getConnection().getMetaData() instanceof org.apache.phoenix.jdbc.PhoenixDatabaseMetaData)){
        		stm.setQueryTimeout(ServerEndPoint.querytimeout);
			}
			for(int i=size-1; i>=0; i--){
				data = params.get(i);
				if(data!=null){
					 for(int k=0; k<data.size(); k++){
						 stm.setObject(k+1, data.get(k));
					 }
					stm.addBatch();
				    if(i%200==0){
				    	stm.executeBatch();
				    	cnt+=stm.getUpdateCount();
				    }
				}
			}
			if(size>200 && size%cnt!=0){
				stm.executeBatch();
				cnt+=stm.getUpdateCount();
			}
			logger.debug("Update Count: "+size+" / "+cnt+", Column Count: "+data.size()+" / "+(sql.split("[?]").length-1)+", Time Used: "+(System.currentTimeMillis()-startTime)+" ms.");
			return cnt;
		}finally{
			DbUtils.closeQuietly(stm);
		}
	}

	//@Override
	public ResultSets queryAll(int currPage, int pageSize, String tableName) {
		return null;
	}

	@Override
	public <T> List<T> query2List(boolean inSession, String sql,
			List<String> params, Class<T> c) {
		return inSession?query2List(this.getConnection(), false, sql, params, c):query2List(sql, params, c);
	}
	
	@Override
	public <T> T query2Object(boolean inSession, String sql,
            List<String> params, Class<T> c){
	        List<T> ret = inSession?query2List(this.getConnection(), false, sql, params, c):query2List(sql, params, c);
	        T object = null;
	        if(StringUtils.isNotNull(ret) && !ret.isEmpty()){
	            object = ret.get(0);
	        }
	        return object;
	}
	
	@Override
	public boolean createRepository() {
		boolean create = false;
		String sql = StringUtils.toString(CommonConsts.Repository_SQL_PATH, "UTF-8");
		try {
			for (String string : sql.split(";")) {
				if(StringUtils.isNull(string)) continue;
				if(this.executeAll(string.trim(),null)>0){
					create = true;
				}else{
					create = false;
					//退出循环
					break;
				}
			}
		} catch (Exception e) {
			logger.error("Exception",e);
			throw new SystemErrorException(MessageCode.ERR_URL);
		}
		return create;
	}
	/**
	 * 通用操作 sql sql语句 objecets 条件 value 异常处理
	 */
	public int executeAll(String sql, Object[] objects) {
		logger.debug(String.format("updateAll-sql=%s;", sql));
		int result = 0;// 接收结果
		PreparedStatement pt = null;
		try {
			if(getConnection()==null || getConnection().isClosed()){
				logger.warn("getConnection() is null or closed! try to get a new getConnection()!");
				setConnection(super.openAdapter());
				logger.info("new getConnection()="+getConnection());
			}
			pt = getConnection().prepareStatement(sql);
			// 绑定参数
			this.bind(pt, objects);
			// 执行操作
			pt.executeUpdate();
			result = 1;
		} catch (SQLException e) {
			result = 0;
			// 抛出异常
			// 记录错误日志
			logger.error("SQLException", e);
			throw new SystemErrorException(MessageCode.SQL_ERROR);
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
	private void bind(PreparedStatement pt, Object[] object) {
		if (object != null && object.length > 0) {
			// 循环绑定
			for (int i = 0; i < object.length; i++) {
				try {
					pt.setObject(i + 1, object[i]);
				} catch (SQLException e) {
					// 错误日志记录
					logger.error("SQLException", e);
					throw new SystemErrorException(MessageCode.SQL_ERROR);
				}
			}
		}
	}
	
	/**
	 * 创建索引
	 * @param indexName
	 * @author bo.dang
	 * @date 2014年5月19日
	 */
	@SuppressWarnings("unused")
    public int insertIndex(String indexName, String tableName, int indexType, List<Column> columnList){
        logger.debug(MessageFormat.format("新增数据：{0}\t{1}", tableName, columnList));
        StringBuffer colName = new StringBuffer(100);
        colName.append("(");
        for(int i=0; i<columnList.size(); i++){
            if(columnList.get(i).getName() != null && i != columnList.size()-1){
                colName.append(columnList.get(i).getName()).append(",");
            }
            if(i==columnList.size()-1){
                colName.append(columnList.get(i).getName()).append(")");
            }
        }
        
	    //String sql = "create index" + indexName + "on tableName (" + columnName, columnName, columnName)";
	    String sql = "create index " + indexName + " on " + tableName + colName.toString();
	    int indexId = 0;
	    try {
             indexId = execute(sql);
        } catch (DatabaseException e) {
            // TODO Auto-generated catch block
            // 错误日志记录
            logger.error("创建索引失败", e);
        }
	    return indexId;
	}
	
	/**
	 * 
	 * @param indexName
	 * @param tableName
	 * @return
	 * @author bo.dang
	 * @date 2014年5月20日
	 */
	public int dropIndex(String indexName, String tableName){
//	    String sql = "drop index " + indexName + " on " + tableName;
	    String sql = "drop index " + indexName + " if exists";
	    int index = 0;
        try {
            index = execute(sql);
        } catch (DatabaseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
	    return index;
	}
	
	/**
	 * 调用存储过程
	 * @param inSession
	 * @param str
	 * @return
	 * @author bo.dang
	 * @date 2014年6月5日
	 */
	public CallableStatement prepareCall(boolean inSession, String str){
          return inSession?prepareCall(this.getConnection(), str):prepareCall(getDBCPConnection(), str);
	}
	
	/**
	 * 调用存储过程
	 * @param conn
	 * @param str
	 * @return
	 * @author bo.dang
	 * @date 2014年6月5日
	 */
	public CallableStatement prepareCall(Connection conn, String str){
	       CallableStatement call = null;
	        try {
	            call = conn.prepareCall(str);
	        } catch (SQLException e) {
	            // TODO Auto-generated catch block
	            e.printStackTrace();
	        }
	        
	        return call;
	}
	
	
	   /**
	    * 验证SQL的语法
	    * @param sql
	    * @return
	    * @throws DatabaseException
	    * @author bo.dang
	    * @date 2014年6月4日
	    */
	   public int validateSQL(String sql) throws DatabaseException{
	        if(StringUtils.isNotNull(sql)){
	            logger.debug(MessageFormat.format("sql: {0}", sql));
	            if(getConnection()==null){
	                logger.error("getConnection() is null");
	                return 0;
	            }
	            Statement pt = null;
	            try {
	                if(getConnection().isClosed()){
	                    logger.error("getConnection() closed");
	                    return 0;
	                }
/*	                pt = getConnection().createStatement();
	                System.out.println(sql);*/
	                //SQLCondition
	                PreparedStatement ps = getConnection().prepareStatement("SET PARSEONLY ON");
	                ps.execute();
	                ps = getConnection().prepareStatement(sql);
	                ps.execute();
	                return pt.executeUpdate(sql);
	            }catch(SQLException e){
	                logger.error("update failed:\t"+sql, e);
	                throw new DatabaseException("database update failed", e);
	            }finally{
	                DbUtils.closeQuietly(pt); //important! getConnection() 需要手工关闭
	            }
	        }else{
	            logger.warn("sql is null:\t"+sql);
	            return 0;
	        }
	    }
}
