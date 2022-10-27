package com.eurlanda.datashire.sprint7.service.squidflow;

import cn.com.jsoft.jframe.utils.ValidateUtils;
import cn.com.jsoft.jframe.utils.jdbc.IConnectionProvider;
import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.adapter.db.AdapterDataSourceManager;
import com.eurlanda.datashire.adapter.db.INewDBAdapter;
import com.eurlanda.datashire.adapter.db.JDBCTemplate;
import com.eurlanda.datashire.entity.Column;
import com.eurlanda.datashire.entity.DBConnectionInfo;
import com.eurlanda.datashire.entity.DataSquid;
import com.eurlanda.datashire.entity.DbSquid;
import com.eurlanda.datashire.entity.NOSQLConnectionSquid;
import com.eurlanda.datashire.entity.Squid;
import com.eurlanda.datashire.entity.operation.BasicTableInfo;
import com.eurlanda.datashire.entity.operation.ColumnInfo;
import com.eurlanda.datashire.enumeration.DataBaseType;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.enumeration.datatype.SystemDatatype;
import com.eurlanda.datashire.exception.SystemErrorException;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import com.eurlanda.datashire.utility.SysConf;
import com.eurlanda.report.global.Global;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * 调度persist的处理类
 *  @author dzp
 *  Created by Eurlanda-dev on 2016/4/18.
 */
public class PersistManagerProcess implements IConnectionProvider {
    private static Logger logger = Logger.getLogger(ManagerSquidFlowProcess.class);// 记录日志
    protected JDBCTemplate jdbcTemplate = new JDBCTemplate(this);
    protected Connection connection = null;

    // token Repository的链接Connection对象（登录时候进行了保存）
    private String token;
    private String key;

    public PersistManagerProcess(String token, String key) {
        this.token = token;
        this.key = key;
    }


    /**
     * 作用描述:通过建表方式得到一条sql语句
     * @param currentSquid
     * @param out
     * @date 2016_04_19
     */
    public String getSQLForCreateTable(DataSquid currentSquid, ReturnValue out){
        DataAdapterFactory adapterFactory = null;
        IRelationalDataManager adapter3 = null;
        Map<String, Object> output = new HashMap<String, Object>();
        String sql="";
        try {
            adapterFactory = DataAdapterFactory.newInstance();
            adapter3 = DataAdapterFactory.getDefaultDataManager();
            adapter3.openSession();
            String tableName = currentSquid.getTable_name();
            // 验证表名
            String regex = "[a-zA-Z][_a-zA-Z0-9]*(\\.[a-zA-Z][_a-zA-Z0-9]*)?";
            Pattern pattern = Pattern.compile(regex);
            if (ValidateUtils.isEmpty(tableName)
                    || !pattern.matcher(tableName).matches()
                    || tableName.length() > 30) {
                out.setMessageCode(MessageCode.ERR_PERSISTTABLE_TABLE_NAME);
            }
            if (currentSquid.isIs_persisted()) {
                if (currentSquid.getDestination_squid_id() > 0) {
                    Squid dbSquid = adapter3.query2Object(true,
                            "select * from ds_squid ds where ds.id="
                                    + currentSquid.getDestination_squid_id(), null, DbSquid.class);
                    DBConnectionInfo dbs = null;
                    INewDBAdapter adaoterSource = null;
                    if (dbSquid.getSquid_type() == SquidTypeEnum.DBSOURCE.value()
                            || dbSquid.getSquid_type() == SquidTypeEnum.CLOUDDB.value()
                            || dbSquid.getSquid_type() == SquidTypeEnum.TRAININGDBSQUID.value()) {
                        String sqll = "select * from ds_squid where id=" + currentSquid.getDestination_squid_id();
                        DbSquid db = adapter3.query2Object(true, sqll, null, DbSquid.class);
                        if (db != null) {
                            // 获取数据源
                            dbs = DBConnectionInfo.fromDBSquid(db);
                            if((SysConf.getValue("cloud_host")+":3306").equals(dbs.getHost())
                                    || SysConf.getValue("train_db_host").equals(db.getHost())){
                                RepositoryService service = new RepositoryService();
                                dbs.setHost(service.getDBUrlBySquidFlowIdWithAutoware(currentSquid.getSquidflow_id()));
                            }
                            adaoterSource = AdapterDataSourceManager.getAdapter(dbs);
                            if (dbs.getDbType() == DataBaseType.HANA && !tableName.contains(".")) {
                                tableName = dbs.getDbName() + "." + tableName;
                            }
                        }
                    } else if (dbSquid.getSquid_type() == SquidTypeEnum.MONGODB.value()) {
                        String sql1 = "select * from ds_squid where id=" + currentSquid.getDestination_squid_id();
                        NOSQLConnectionSquid nosql = adapter3.query2Object(true, sql1, null,NOSQLConnectionSquid.class);
                        if (nosql != null) {
                            // 获取数据源
                            dbs = DBConnectionInfo.fromNOSQLSquid(nosql);
                            adaoterSource = AdapterDataSourceManager.getAdapter(dbs);
                        }
                    }
                    BasicTableInfo table = this.getBasicTableInfo(currentSquid);
                    if (table == null || table.getColumnList() == null || table.getColumnList().size() == 0) {
                        out.setMessageCode(MessageCode.ERR_COLUMN_EMPTY);
                    }
                    //将数据库类型添加到表中
                    table.setDbType(dbs.getDbType());
                    if (table.isPersistTable()) {
                        sql = adaoterSource.getSQLForTable(table,out);
                    } else {
                        out.setMessageCode(MessageCode.ERR_PERSISTTABLE_NOT_TABLENAME);
                    }
                }else {
                    out.setMessageCode(MessageCode.ERR_PERSISTTABLE_NOT_CHECKED);
                }
            }
        }
        catch (Exception e) {
            try {
                if (adapter3 != null)
                    adapter3.rollback();
            } catch (SQLException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            if(e instanceof SystemErrorException){
                out.setMessageCode(MessageCode.ERR_DATABASE_CONNECT);
            } else {
                out.setMessageCode(MessageCode.SQL_ERROR);
            }
            logger.error("getSQLForCreateTable is error",e);
        } finally {
            if (adapter3 != null)
                adapter3.closeSession();
        }
        return sql;
    }

    /**
     * 根据squid里面的信息得到一张表
     * @param currentSquid
     * @return
     * @date 2016_04_19
     */
    private BasicTableInfo getBasicTableInfo(DataSquid currentSquid) {
        String tableName = currentSquid.getTable_name();
        BasicTableInfo table = new BasicTableInfo(tableName);
        List<ColumnInfo> columnList = new ArrayList<ColumnInfo>();
        ColumnInfo e = null;
        List<Column> squidColumns = currentSquid.getColumns();
        if (squidColumns != null && squidColumns.size() > 0) {
            int i = 1;
            for (Column column : squidColumns) {
                if (currentSquid != null) {
                    e = new ColumnInfo();
                    e.setTableName(tableName);
                    e.setName(column.getName());
                    e.setIdentity(column.isIsAutoIncrement());
                    e.setPrimary(column.isIsPK());
                    e.setUniqueness(column.isIsUnique());
                    if (column.getData_type() == SystemDatatype.UNKNOWN.value()) {
                        table.setPersistTable(false);
                    }else{
                        table.setPersistTable(true);
                    }
                    e.setSystemDatatype(SystemDatatype.parse(column.getData_type()));
                    e.setTypeName(SystemDatatype.parse(column.getData_type()).toString());
                    e.setOrderNumber(i++);
                    e.setNullable(column.isNullable());
                    e.setLength(column.getLength());
                    e.setScale(column.getScale());
                    e.setPrecision(column.getPrecision());
                    columnList.add(e);
                }
            }
        }
        table.setColumnList(columnList);
        return table;
    }

    /**
     * 作用描述:通过sql语句创建表并更新返回表名
     * @param currentSquid
     * @param persist_sql
     * @param out
     * @date 2016_04_19
     */
    public String createTableBySQL(DataSquid currentSquid ,
                                   String persist_sql,
                                   ReturnValue out,List<Integer> list) throws Exception {
        DataAdapterFactory adapterFactory = DataAdapterFactory.newInstance();
        IRelationalDataManager adapter3 = DataAdapterFactory.getDefaultDataManager();
        String tableName = currentSquid.getTable_name();
        String returnTableName="";
        INewDBAdapter adaoterSource = null;
        DBConnectionInfo dbs = null;
        adapter3.openSession();
        try {
            if (currentSquid.isIs_persisted()) {
                if (currentSquid.getDestination_squid_id() > 0) {
                    Squid dbSquid = adapter3.query2Object(true,
                            "select * from ds_squid ds where ds.id="
                                    + currentSquid.getDestination_squid_id(), null, DbSquid.class);
                    if (dbSquid.getSquid_type() == SquidTypeEnum.DBSOURCE.value()
                            || dbSquid.getSquid_type() == SquidTypeEnum.CLOUDDB.value() || dbSquid.getSquid_type()==SquidTypeEnum.TRAININGDBSQUID.value()) {
                        String sqll = "select * from ds_squid where id=" + currentSquid.getDestination_squid_id();
                        DbSquid db = adapter3.query2Object(true, sqll, null, DbSquid.class);
                        if (db != null) {
                            // 获取数据源
                            dbs = DBConnectionInfo.fromDBSquid(db);
                            if((SysConf.getValue("cloud_host")+":3306").equals(dbs.getHost())
                                    || SysConf.getValue("train_db_host").equals(db.getHost())){
                                RepositoryService service = new RepositoryService();
                                dbs.setHost(service.getDBUrlBySquidFlowIdWithAutoware(currentSquid.getSquidflow_id()));
                            }
                            adaoterSource = AdapterDataSourceManager.getAdapter(dbs);
                            if (dbs.getDbType() == DataBaseType.HANA && !tableName.contains(".")) {
                                tableName = dbs.getDbName() + "." + tableName;
                            }
                        }
                    } else if (dbSquid.getSquid_type() == SquidTypeEnum.MONGODB.value()) {
                        String sql1 = "select * from ds_squid where id=" + currentSquid.getDestination_squid_id();
                        NOSQLConnectionSquid nosql = adapter3.query2Object(true, sql1, null, NOSQLConnectionSquid.class);
                        if (nosql != null) {
                            // 获取数据源
                            dbs = DBConnectionInfo.fromNOSQLSquid(nosql);
                            adaoterSource = AdapterDataSourceManager.getAdapter(dbs);
                        }
                    }
                    returnTableName = adaoterSource.createTable(persist_sql,out);
                    if (out.getMessageCode() != MessageCode.SUCCESS) {
                        out.setMessageCode(MessageCode.SUCCESS);
                        list.clear();
                        list.add(currentSquid.getId());
                    }
                    currentSquid.setTable_name(returnTableName);
                    adapter3.update2(currentSquid);

                } else {
                    out.setMessageCode(MessageCode.ERR_PERSISTTABLE_NOT_CHECKED);
                }
            }
        } catch (Exception e) {
            try {
                if (adapter3 != null)
                    adapter3.rollback();
            } catch (SQLException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            out.setMessageCode(MessageCode.SQL_ERROR);
            throw e;
        }finally {
            if (adapter3 != null)
                adapter3.closeSession();
        }
        return returnTableName;
    }

    /**
     * 查询一个列表
     * @date 2014-4-15
     * @param sql
     * @return
     */
    public List<Map<String, Object>> queryForList(String sql) {
        return this.jdbcTemplate.queryForList(sql, null);
    }
    /**
     * 执行更新操作
     */
    public int executeUpdate(String sql) {
        logger.debug("执行语句：" + sql);
        return this.jdbcTemplate.update(sql);
    }
    /**
     * 回滚事务
     * @date 2014_04_15
     */
    public void rollback() {
        try {
            this.connection.rollback();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    /**
     * 提交事务
     * @date 2014-04-15
     */
    public void commit() {
        try {
            this.connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Connection getConnection() throws Exception {
        return null;
    }

    public static String initColumnNameByDbType(int dbType, String columnName) {
        String temp = "";
        DataBaseType dataBaseType = DataBaseType.parse(dbType);
        switch (dataBaseType) {
            case SQLSERVER:
                temp = "[" + columnName + "]";
                break;
            case MYSQL:
                temp = "`" + columnName + "`";
                break;
            case ORACLE:
            case HBASE_PHOENIX:
            case DB2:
                temp = "\"" + columnName + "\"";
                break;
            default:
                temp = columnName;
                break;
        }
        return temp;
    }
}

