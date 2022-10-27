package com.eurlanda.datashire.server.utils.dbsource;

import cn.com.jsoft.jframe.utils.ValidateUtils;
import com.eurlanda.datashire.server.utils.dbsource.CassandraAdapter;
import com.eurlanda.datashire.adapter.db.*;
import com.eurlanda.datashire.server.utils.entity.DBConnectionInfo;
import com.eurlanda.datashire.enumeration.DataBaseType;
import com.eurlanda.datashire.exception.SystemErrorException;
import com.eurlanda.datashire.utility.MessageCode;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

/**
 * connection工厂
 *
 * @author jiwei.zhang
 * @date 2014-1-22
 */
public class AdapterDataSourceManager {
    private static final Logger logger = Logger.getLogger(AdapterDataSourceManager.class);

    /**
     * 根据连接定义，取得一个adapter,此方法获取的是新数据操作接口
     *
     * @param conInfo
     * @return 如果无法获取，则返回null
     * @date 2014-1-10
     * @author jiwei.zhang
     */
    public static INewDBAdapter getAdapter(DBConnectionInfo conInfo) throws Exception {
        if ((conInfo.getIsNoSql()==null|| !conInfo.getIsNoSql())&&conInfo.getDbType() == null || conInfo.getIsNoSql()!=null&&conInfo.getIsNoSql()&& conInfo.getNoSQLDataBaseType() == null) {
            throw new SystemErrorException(MessageCode.ERR_DS_DB_TYPE_Enum, "数据库类型未设置");
        }
        if (conInfo.getIsNoSql()!=null&&conInfo.getIsNoSql()) {
            switch (conInfo.getNoSQLDataBaseType()) {
                case MONGODB:
                    return new MongodbAdapter(conInfo);
            }
        } else {
            switch (conInfo.getDbType()) {
                case SQLSERVER:
                    return new NewSqlServerAdapter(conInfo);
                case MYSQL:
                    return new MysqlAdapter(conInfo);
                case HSQLDB:
                    //return new NewHsqlDbAdapter(conInfo);
                    return null;
                case INFORMIX:
                    return new InformixAdapter(conInfo);
                case HBASE_PHOENIX:
                    return new HbaseAdapter(conInfo);
                case DB2:
                    return new DB2Adapter(conInfo);
                case ORACLE:
                    return new OracleAdapter(conInfo);
                case SYBASE:
                    return new SybaseASEAdapter(conInfo);
                case POSTGRESQL:
                    return new PGsqlAdapter(conInfo);
                case MSACCESS:
                    return new AccessAdapter(conInfo);
                case HANA:
                    return new HanaAdapter(conInfo);
                case HIVE:
                    return new HiveAdapter(conInfo);
                case TERADATA:
                    return new TeradataAdapter(conInfo);
                case CASSANDRA:
                    return new CassandraAdapter(conInfo);
            }
        }
        return null;
    }

    /**
     * 根据连接信息创建一个connection.
     *
     * @param info 连接信息。
     * @return connection
     * @date 2014-1-22
     * @author jiwei.zhang
     */
    public static Connection createConnection(DBConnectionInfo info) throws Exception {
        Connection connection = null;
        JdbcConnectInfo connectInfo = buildConnectInfo(info);
        try {
            connection = connectInfo.getConnection();
        } catch (IOException e) {
            logger.error(e);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e);
            throw new SystemErrorException(MessageCode.ERR_CONNECTION_NULL, "创建connection对象失败");
        }
        //return new ProxyConnection(connection);
        return connection;
    }

    private static JdbcConnectInfo buildConnectInfo(DBConnectionInfo info) {
        String driverClassName = "";
        String jdbcURL = "";
        String tempStr = "";
        if (info.getPort() > 0 && !info.getHost().contains(":")) {
            tempStr = info.getHost() + ":" + info.getPort();
        } else {
            if (info.getDbType() == DataBaseType.ORACLE) {
                if (info.getHost().split(";").length >= 1) {
                    StringBuffer strbf = new StringBuffer();
                    strbf.append("(DESCRIPTION =");
                    for (String ss : info.getHost().split(";")) {
                        String host = ss.split(":")[0];
                        String port = ss.split(":")[1];
                        strbf.append("(ADDRESS = (PROTOCOL = TCP)(HOST =" + host + ")(PORT = " + port + "))");
                    }
                    strbf.append("(LOAD_BALANCE=yes)(CONNECT_DATA=(SERVER = DEDICATED)(SERVICE_NAME = " + info.getDbName() + ")))");
                    tempStr = strbf.toString();
                    System.out.println(tempStr);
                }
            } else {
                tempStr = info.getHost();
            }
        }
        switch (info.getDbType()) {
            case HSQLDB:
                driverClassName = "org.hsqldb.jdbcDriver";
                //jdbcURL = "jdbc:hsqldb:hsql:"+ info.getHost() + info.getDbName();
                jdbcURL = tempStr;
                break;
            case SQLSERVER:
                driverClassName = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
                //jdbcURL = "jdbc:sqlserver://" + info.getHost() + ":" + info.getPort() + ";DatabaseName=" + info.getDbName();
                jdbcURL = "jdbc:sqlserver://" + tempStr + ";DatabaseName=" + info.getDbName();
                break;
            case ORACLE:
                driverClassName = "oracle.jdbc.driver.OracleDriver";
                if (tempStr == "") {
                    jdbcURL = "jdbc:oracle:thin:@" + info.getHost() + ":" + info.getPort() + ":" + info.getDbName();
                } else {
                    jdbcURL = "jdbc:oracle:thin:@" + tempStr;
                }
                break;
            case INFORMIX:
                driverClassName = "com.informix.jdbc.IfxDriver";
                jdbcURL = "jdbc:informix-sqli://" + tempStr + "/" + info.getDbName() + ":informixserver=demoserver";
                break;
            case HBASE_PHOENIX:
                driverClassName = "org.apache.phoenix.jdbc.PhoenixDriver";
                //jdbcURL = "jdbc:phoenix:"+info.getHost()+":"+info.getPort()+"";
                jdbcURL = "jdbc:phoenix:" + tempStr + "";
                break;
            case DB2:
                driverClassName = "com.ibm.db2.jcc.DB2Driver";
                //jdbcURL = "jdbc:db2://" + info.getHost() + ":" + info.getPort() + "/" + info.getDbName();
                jdbcURL = "jdbc:db2://" + tempStr + "/" + info.getDbName();
                break;
            case MYSQL: // ?characterEncoding=utf8
                driverClassName = "com.mysql.jdbc.Driver";
                //jdbcURL = "jdbc:mysql://" + info.getHost() + ":" + info.getPort() + "/" + info.getDbName();da\
                jdbcURL = "jdbc:mysql://" + tempStr + "/" + info.getDbName();
                break;
            case SYBASE:
                driverClassName = "com.sybase.jdbc3.jdbc.SybDriver";
                //jdbcURL = "jdbc:sybase:Tds:" + info.getHost() + ":" + info.getPort() + "/" + info.getDbName();
                jdbcURL = "jdbc:sybase:Tds:" + tempStr + "/" + info.getDbName();
                break;
            case POSTGRESQL:
                driverClassName = "org.postgresql.Driver";
                //jdbcURL = "jdbc:postgresql://" + info.getHost() + ":" + info.getPort() + "/" + info.getDbName();
                jdbcURL = "jdbc:postgresql://" + tempStr + "/" + info.getDbName();
                break;
            case MSACCESS:
                driverClassName = "sun.jdbc.odbc.JdbcOdbcDriver";
                jdbcURL = "jdbc:odbc:DRIVER=Microsoft Access Driver (*.mdb, *.accdb);DBQ=" + info.getHost() + "";//D:/Access/test.accdb
                break;
            case HANA:
                driverClassName = "com.sap.db.jdbc.Driver";
                jdbcURL = "jdbc:sap://" + tempStr + "?reconnect=true";
                break;
            case HIVE:
                driverClassName="org.apache.hive.jdbc.HiveDriver";
                jdbcURL="jdbc:hive2://" + tempStr + "/" + info.getDbName();
                break;
            case TERADATA:
                driverClassName = "com.ncr.teradata.TeraDriver";
                jdbcURL = "jdbc:teradata://" + tempStr + "/TMODE=TERA,DATABASE=" + info.getDbName();
                break;
            default:
                logger.error("unknow data type: " + info.getDbType());
                break;
        }
        if (!ValidateUtils.isEmpty(info.getConnectionString())) {
            jdbcURL = info.getConnectionString();
        }
        return new JdbcConnectInfo(driverClassName, jdbcURL, info.getUserName(), info.getPassword());
    }


    /**
     * jdbc连接信息。
     *
     * @author jiwei.zhang
     * @date 2014-1-22
     */
    static class JdbcConnectInfo {
        private String driverName;
        private String connectionString;
        private String userName;
        private String password;

        public JdbcConnectInfo(String driverName, String connectionString, String userName, String password) {
            super();
            this.driverName = driverName;
            this.connectionString = connectionString;
            this.userName = userName;
            this.password = password;
        }

        public JdbcConnectInfo() {
        }

        ;

        /**
         * 创建一个连接。
         *
         * @return
         * @throws Exception
         * @date 2014-1-22
         * @author jiwei.zhang
         */
        public Connection getConnection() throws Exception {
            Connection conn = null;
            Class.forName(this.driverName);
            if (this.connectionString.indexOf("Access") > -1) {
                Properties pro = new Properties();
                pro.setProperty("charSet", "GB2312");
                conn = DriverManager.getConnection(this.connectionString, pro);
            } else {
                conn = DriverManager.getConnection(this.connectionString,
                        this.userName, this.password);
            }
            return conn;
        }
    }


}
