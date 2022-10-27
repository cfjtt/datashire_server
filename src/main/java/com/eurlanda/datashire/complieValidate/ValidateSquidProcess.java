package com.eurlanda.datashire.complieValidate;

import com.alibaba.fastjson.JSONArray;
import com.eurlanda.datashire.adapter.CassandraAdapter;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.adapter.db.AdapterDataSourceManager;
import com.eurlanda.datashire.adapter.db.INewDBAdapter;
import com.eurlanda.datashire.annotation.MultitableMapping;
import com.eurlanda.datashire.dao.ISquidDao;
import com.eurlanda.datashire.dao.ISquidLinkDao;
import com.eurlanda.datashire.dao.IVariableDao;
import com.eurlanda.datashire.dao.impl.SquidDaoImpl;
import com.eurlanda.datashire.dao.impl.SquidLinkDaoImpl;
import com.eurlanda.datashire.dao.impl.VariableDaoImpl;
import com.eurlanda.datashire.entity.*;
import com.eurlanda.datashire.entity.ReferenceColumn;
import com.eurlanda.datashire.entity.Repository;
import com.eurlanda.datashire.entity.SquidLink;
import com.eurlanda.datashire.entity.dest.DestCassandraColumn;
import com.eurlanda.datashire.entity.dest.*;
import com.eurlanda.datashire.entity.dest.DestHiveColumn;
import com.eurlanda.datashire.entity.operation.BasicTableInfo;
import com.eurlanda.datashire.enumeration.DataBaseType;
import com.eurlanda.datashire.enumeration.SchedulerLogStatus;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.enumeration.datatype.DbBaseDatatype;
import com.eurlanda.datashire.server.dao.ColumnDao;
import com.eurlanda.datashire.server.dao.SquidLinkDao;
import com.eurlanda.datashire.server.model.Column;
import com.eurlanda.datashire.server.model.*;
import com.eurlanda.datashire.server.service.PivotSquidService;
import com.eurlanda.datashire.server.utils.BeanUtil;
import com.eurlanda.datashire.server.utils.Constants;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.StringUtils;
import org.springframework.jdbc.core.JdbcTemplate;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 编译检查实现类
 * Created by Eurlanda-dev on 2016/8/9.
 */
public class ValidateSquidProcess {
    private static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(ValidateSquidProcess.class);// 记录日志

    public static List<DSVariable> getDSVariableListBySquidId(Squid squid, IRelationalDataManager adapter) {
        IVariableDao dao = new VariableDaoImpl(adapter);
        List<DSVariable> variables = null;
        try {
            variables = dao.getDSVariableByScope(2, squid.getId());
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return variables;
    }

    /**
     * 验证connection是否合法
     */
    public static boolean validateConnect(List<BuildInfo> buildInfoList, Map<String, Object> map, Squid squid, IRelationalDataManager adapter) {
        logger.info("编译检查:检查" + squid.getName() + "连接开始");
        Map<String, String> params = new HashMap<String, String>();
        params.put("ID", squid.getId() + "");
        boolean flag = true;
        if (SquidTypeEnum.DBSOURCE.value() == squid.getSquid_type()
                || SquidTypeEnum.CLOUDDB.value() == squid.getSquid_type()
                || SquidTypeEnum.TRAININGDBSQUID.value() == squid.getSquid_type()) {
            List<DbSquid> dbSquidList = adapter.query2List(params, DbSquid.class);
            for (DbSquid dbSquid : dbSquidList) {
                BuildInfo buildInfo = new BuildInfo(squid, map);
                /*如果是Teradata就不检查端口合法*/
                boolean checkPort = dbSquid.getDb_type() != DataBaseType.TERADATA.value();
                //检查IP地址和端口是否正确
                flag = checkIPAndPort(buildInfoList, map, squid, dbSquid.getHost(), checkPort);
                //数据库类型为空
                if (dbSquid.getDb_type() == 0) {
                    flag = false;
                    buildInfo = new BuildInfo(squid, map);
                    buildInfo.setMessageCode(MessageCode.ERR_DBSOURCE_TYPE_ISNULL.value());
                    buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                    buildInfoList.add(buildInfo);
                }
                //判断用户名等信息是否正确
                if (StringUtils.isEmpty(dbSquid.getUser_name())) {
                    flag = false;
                    buildInfo = new BuildInfo(squid, map);
                    buildInfo.setMessageCode(MessageCode.ERR_DBSOURCE_USERNAME_NULL.value());
                    buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                    buildInfoList.add(buildInfo);
                }
                if (StringUtils.isEmpty(dbSquid.getPassword())) {
                    flag = false;
                    buildInfo = new BuildInfo(squid, map);
                    buildInfo.setMessageCode(MessageCode.ERR_DBSOURCE_PWD_NULL.value());
                    buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                    buildInfoList.add(buildInfo);
                }
                if (StringUtils.isEmpty(dbSquid.getDb_name())) {
                    flag = false;
                    buildInfo = new BuildInfo(squid, map);
                    buildInfo.setMessageCode(MessageCode.ERR_DBSOURCE_DBNAME_NULL.value());
                    buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                    buildInfoList.add(buildInfo);
                }
                if (flag) {
                    //如果没有错误，检查连接
                    DBConnectionInfo dbs = new DBConnectionInfo();
                    dbs.setHost(dbSquid.getHost());
                    dbs.setPort(dbSquid.getPort());
                    dbs.setUserName(dbSquid.getUser_name());
                    dbs.setPassword(dbSquid.getPassword());
                    dbs.setDbName(dbSquid.getDb_name());
                    dbs.setDbType(DataBaseType.parse(dbSquid.getDb_type()));
                    try {
                        INewDBAdapter adaoterSource = AdapterDataSourceManager.getAdapter(dbs);
                    } catch (Exception e) {
                        e.printStackTrace();
                        flag = false;
                        buildInfo = new BuildInfo(squid, map);
                        buildInfo.setMessageCode(MessageCode.ERR_CONNECTION.value());
                        buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                        buildInfoList.add(buildInfo);
                    }

                }
            }
        } else if (SquidTypeEnum.MONGODB.value() == squid.getSquid_type()) {
            List<NOSQLConnectionSquid> nosqlConnectionLists = adapter.query2List(params, NOSQLConnectionSquid.class);
            for (NOSQLConnectionSquid nosqlConnection : nosqlConnectionLists) {
                BuildInfo buildInfo = new BuildInfo(squid, map);
                //判断ip和端口是否符合要求
                flag = checkIPAndPort(buildInfoList, map, squid, nosqlConnection.getHost());
                //判断连接连接信息是否正确
                if (StringUtils.isEmpty(nosqlConnection.getUser_name())) {
                    buildInfo = new BuildInfo(squid, map);
                    buildInfo.setMessageCode(MessageCode.ERR_MONGODB_USERNAME_NULL.value());
                    buildInfo.setInfoType(SchedulerLogStatus.WARNING.getValue());
                    buildInfoList.add(buildInfo);
                }
                if (StringUtils.isEmpty(nosqlConnection.getPassword())) {
                    buildInfo = new BuildInfo(squid, map);
                    buildInfo.setMessageCode(MessageCode.ERR_MONGODB_PWD_NULL.value());
                    buildInfo.setInfoType(SchedulerLogStatus.WARNING.getValue());
                    buildInfoList.add(buildInfo);
                }
                if (StringUtils.isEmpty(nosqlConnection.getDb_name())) {
                    flag = false;
                    buildInfo = new BuildInfo(squid, map);
                    buildInfo.setMessageCode(MessageCode.ERR_MONGODB_DBNAME_NULL.value());
                    buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                    buildInfoList.add(buildInfo);
                }
                if (flag) {
                    //测试连接
                }
            }

        } else if (SquidTypeEnum.HDFS.value() == squid.getSquid_type()
                || SquidTypeEnum.SOURCECLOUDFILE.value() == squid.getSquid_type() || SquidTypeEnum.TRAINNINGFILESQUID.value()==squid.getSquid_type()) {
            List<HdfsSquid> hdfsSquidList = adapter.query2List(params, HdfsSquid.class);
            for (HdfsSquid hdfsSquid : hdfsSquidList) {

                //判断ip和端口是否符合要求
                flag = checkIPAndPort(buildInfoList, map, squid, hdfsSquid.getHost());
                //判断用户名等其他信息是否正确
                //判断连接连接信息是否正确
                if (StringUtils.isEmpty(hdfsSquid.getUser_name())) {
                    BuildInfo buildInfo = new BuildInfo(squid, map);
                    buildInfo.setMessageCode(MessageCode.ERR_HDFS_USERNAME_NULL.value());
                    buildInfo.setInfoType(SchedulerLogStatus.WARNING.getValue());
                    buildInfoList.add(buildInfo);
                }
                if (StringUtils.isEmpty(hdfsSquid.getPassword())) {
                    BuildInfo buildInfo = new BuildInfo(squid, map);
                    buildInfo.setMessageCode(MessageCode.ERR_HDFS_PWD_NULL.value());
                    buildInfo.setInfoType(SchedulerLogStatus.WARNING.getValue());
                    buildInfoList.add(buildInfo);
                }
                if (StringUtils.isEmpty(hdfsSquid.getFile_path())) {
                    BuildInfo buildInfo = new BuildInfo(squid, map);
                    flag = false;
                    buildInfo = new BuildInfo(squid, map);
                    buildInfo.setMessageCode(MessageCode.ERR_HDFS_FILEPATH_NULL.value());
                    buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                    buildInfoList.add(buildInfo);
                }
                if (flag) {
                    //测试连接
                }
            }

        } else if (SquidTypeEnum.FTP.value() == squid.getSquid_type()) {
            List<FtpSquid> ftpSquidList = adapter.query2List(params, FtpSquid.class);
            for (FtpSquid ftpSquid : ftpSquidList) {
                BuildInfo buildInfo = new BuildInfo(squid, map);
                //判断ip和端口是否符合要求
                flag = checkIPAndPort(buildInfoList, map, squid, ftpSquid.getHost());
                //判断用户名等其他信息是否正确
                //判断连接连接信息是否正确
                if (StringUtils.isEmpty(ftpSquid.getUser_name())) {
                    buildInfo = new BuildInfo(squid, map);
                    buildInfo.setMessageCode(MessageCode.ERR_FTP_USERNAME_NULL.value());
                    buildInfo.setInfoType(SchedulerLogStatus.WARNING.getValue());
                    buildInfoList.add(buildInfo);
                }
                if (StringUtils.isEmpty(ftpSquid.getPassword())) {
                    buildInfo = new BuildInfo(squid, map);
                    buildInfo.setMessageCode(MessageCode.ERR_FTP_PWD_NULL.value());
                    buildInfo.setInfoType(SchedulerLogStatus.WARNING.getValue());
                    buildInfoList.add(buildInfo);
                }
                if (StringUtils.isEmpty(ftpSquid.getFile_path())) {
                    flag = false;
                    buildInfo = new BuildInfo(squid, map);
                    buildInfo.setMessageCode(MessageCode.ERR_FTP_FILEPATH_NULL.value());
                    buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                    buildInfoList.add(buildInfo);
                }
                if (flag) {
                    //测试连接
                }
            }

        } else if (SquidTypeEnum.FILEFOLDER.value() == squid.getSquid_type()) {
            List<FileFolderSquid> fileFolderSquidList = adapter.query2List(params, FileFolderSquid.class);
            for (FileFolderSquid fileFolderSquid : fileFolderSquidList) {
                BuildInfo buildInfo = new BuildInfo(squid, map);
                //判断ip和端口是否符合要求
                flag = checkIPAndPort(buildInfoList, map, squid, fileFolderSquid.getHost());
                //判断用户名等其他信息是否正确
                //判断连接连接信息是否正确
                if (StringUtils.isEmpty(fileFolderSquid.getUser_name())) {
                    buildInfo = new BuildInfo(squid, map);
                    buildInfo.setMessageCode(MessageCode.ERR_FILE_USERNAME_NULL.value());
                    buildInfo.setInfoType(SchedulerLogStatus.WARNING.getValue());
                    buildInfoList.add(buildInfo);
                }
                if (StringUtils.isEmpty(fileFolderSquid.getPassword())) {
                    buildInfo = new BuildInfo(squid, map);
                    buildInfo.setMessageCode(MessageCode.ERR_FILE_PWD_NULL.value());
                    buildInfo.setInfoType(SchedulerLogStatus.WARNING.getValue());
                    buildInfoList.add(buildInfo);
                }
                if (StringUtils.isEmpty(fileFolderSquid.getFile_path())) {
                    flag = false;
                    buildInfo = new BuildInfo(squid, map);
                    buildInfo.setMessageCode(MessageCode.ERR_FILE_FILEPATH_NULL.value());
                    buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                    buildInfoList.add(buildInfo);
                }
                if (flag) {
                    //测试连接
                }
            }

        } else if (SquidTypeEnum.HBASE.value() == squid.getSquid_type()) {
            List<HBaseConnectionSquid> hBaseConnectionSquidList = adapter.query2List(params, HBaseConnectionSquid.class);
            for (HBaseConnectionSquid hBaseConnectionSquid : hBaseConnectionSquidList) {
                //检查url是否符合要求
                flag = checkIPAndPort(buildInfoList, map, squid, hBaseConnectionSquid.getUrl());
            }
            if (flag) {
                //测试连接
            }
        } else if (SquidTypeEnum.KAFKA.value() == squid.getSquid_type()) {
            KafkaSquid kafkaSquid = (KafkaSquid) adapter.query2Object(params, KafkaSquid.class);
            flag = checkIPAndPort(buildInfoList, map, squid, kafkaSquid.getZkQuorum());
        } else if (SquidTypeEnum.HIVE.value() == squid.getSquid_type()) {
            List<SystemHiveConnectionSquid> dbSquidList = adapter.query2List(params, SystemHiveConnectionSquid.class);
            if (dbSquidList != null) {
                for (SystemHiveConnectionSquid dbSquid : dbSquidList)
                    if (StringUtils.isEmpty(dbSquid.getDb_name())) {
                        BuildInfo buildInfo = new BuildInfo(squid, map);
                        flag = false;
                        buildInfo = new BuildInfo(squid, map);
                        buildInfo.setMessageCode(MessageCode.ERR_HIVE_DATABASENAME_ISNULL.value());
                        buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                        buildInfoList.add(buildInfo);
                    }
            }
        } else if (SquidTypeEnum.CASSANDRA.value() == squid.getSquid_type()) {
            List<CassandraConnectionSquid> dbSquidList = adapter.query2List(params, CassandraConnectionSquid.class);
            if (dbSquidList != null) {
                for (CassandraConnectionSquid dbSquid : dbSquidList) {
                    if (StringUtils.isEmpty(dbSquid.getKeyspace())) {
                        BuildInfo buildInfo = new BuildInfo(squid, map);
                        flag = false;
                        buildInfo.setMessageCode(MessageCode.ERR_CASSANDRA_KEYSPACE_ISNULL.value());
                        buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                        buildInfoList.add(buildInfo);
                    }
                    if (StringUtils.isEmpty(dbSquid.getCluster())) {
                        BuildInfo buildInfo = new BuildInfo(squid, map);
                        flag = false;
                        buildInfo.setMessageCode(MessageCode.ERR_CASSANDRA_CLUSTER_ISNULL.value());
                        buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                        buildInfoList.add(buildInfo);
                    }
                    if (StringUtils.isEmpty(dbSquid.getHost())) {
                        BuildInfo buildInfo = new BuildInfo(squid, map);
                        flag = false;
                        buildInfo.setMessageCode(MessageCode.ERR_CASSANDRA_HOST_ISNULL.value());
                        buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                        buildInfoList.add(buildInfo);
                    }
                    if (dbSquid.getVerificationMode() == 1 && StringUtils.isEmpty(dbSquid.getUsername())) {
                        BuildInfo buildInfo = new BuildInfo(squid, map);
                        flag = false;
                        buildInfo.setMessageCode(MessageCode.ERR_CASSANDRA_USERNAME_ISNULL.value());
                        buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                        buildInfoList.add(buildInfo);
                    }
                    if (dbSquid.getVerificationMode() == 1 && StringUtils.isEmpty(dbSquid.getPassword())) {
                        BuildInfo buildInfo = new BuildInfo(squid, map);
                        flag = false;
                        buildInfo.setMessageCode(MessageCode.ERR_CASSANDRA_PASSWORD_ISNULL.value());
                        buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                        buildInfoList.add(buildInfo);
                    }
                }
            }
        }
        return flag;
    }

    /**
     * 判断IP和端口是否合法（支持域名，落地squid支持namespace,抽取squid的数据源也支持namespace）
     *
     * @param buildInfoList 编译信息实体对象
     * @param map
     * @param squid         当前Squid
     * @param IpAndPort     IP与端口
     * @return 是否合法
     */
    public static boolean checkIPAndPort(List<BuildInfo> buildInfoList, Map<String, Object> map, Squid squid, String IpAndPort) {
        return checkIPAndPort(buildInfoList, map, squid, IpAndPort, true);
    }

    /**
     * 判断IP和端口是否合法（支持域名，落地squid支持namespace,抽取squid的数据源也支持namespace）
     *
     * @param buildInfoList 编译信息实体对象
     * @param map
     * @param squid         当前Squid
     * @param IpAndPort     IP与端口
     * @param checkPort     是否检查端口合法
     * @return 是否合法
     */
    public static boolean checkIPAndPort(List<BuildInfo> buildInfoList, Map<String, Object> map, Squid squid, String IpAndPort, boolean checkPort) {
        boolean isIllegal = false;
        boolean isNull = false;
        String regex = "[0-9]+";
        //域名正则表达式
        String domainName = "([a-zA-Z]+[://]+)?[a-zA-Z0-9][a-zA-Z0-9-]{0,62}(\\.[a-zA-Z0-9][a-zA-Z0-9-]{0,62})+\\.?(:[a-zA-Z0-9]+)?";
        //简单的ip匹配
        String easyIpRegex = "[a-zA-Z0-9]+\\.[A-Za-z0-9]+\\.[A-Za-z0-9]+\\.[A-Za-z0-9]+(\\.:[a-zA-Z0-9]+)?";
        //详细的ip匹配
        String ipRegex = "((?:(?:25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d)))\\.){3}(?:25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d))))";
        BuildInfo buildInfo = new BuildInfo(squid, map);
        boolean flag = true;
        if (StringUtils.isNull(IpAndPort)) {
            isNull = true;
            flag = false;
            buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
            buildInfoList.add(buildInfo);
        } else if (IpAndPort.matches(easyIpRegex)) {
            String host = IpAndPort.split(":", -1)[0];
            String port = null;
            if (IpAndPort.indexOf(":") > 0) {
                port = IpAndPort.split(":", -1)[1];
            }
            //fix bug 5667：当DB Connection Squid在选中Teradata的时候不做端口号的判断
            if (checkPort) {
                if (StringUtils.isEmpty(port)) {
                    isIllegal = true;
                    flag = false;
                    if (squid.getSquid_type() != SquidTypeEnum.FILEFOLDER.value()
                            && squid.getSquid_type() != SquidTypeEnum.HDFS.value()) {
                        //IP端口不合法
                        buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                        buildInfoList.add(buildInfo);
                    }
                } else if (!port.matches(regex) || Integer.parseInt(port) < 0 || Integer.parseInt(port) > 65535 || !host.matches(ipRegex)) {
                    isIllegal = true;
                    flag = false;
                    buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                    buildInfoList.add(buildInfo);
                }
            }
        } else if (IpAndPort.matches(domainName)) {
            //这里表示的是域名
            if (IpAndPort.contains(":")) {
                String[] domain = IpAndPort.split(":", -1);
                if (domain.length == 3) {
                    //如果存在协议，协议是否正确
                    if (!("http".equals(domain[0]) || "https".equals(domain[0]))) {
                        flag = false;
                        isIllegal = true;
                       /* //IP端口不合法
                        buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                        buildInfoList.add(buildInfo);*/
                    }
                    if (domain[1].trim().indexOf("//") != 0) {
                        flag = false;
                        isIllegal = true;
                       /* //IP端口不合法
                        buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                        buildInfoList.add(buildInfo);*/
                    }
                    if (!domain[2].matches(regex) || (Integer.parseInt(domain[2]) < 0 || Integer.parseInt(domain[2]) > 65535)) {
                        flag = false;
                        isIllegal = true;
                    }
                } else if (domain.length == 2) {
                    if (domain[1].matches(regex)) {
                        if (Integer.parseInt(domain[1]) <= 0 && Integer.parseInt(domain[2]) > 65535) {
                            flag = false;
                            isIllegal = true;
                        }
                    } else {
                        if (!("http".equals(domain[0]) || "https".equals(domain[0]))) {
                            flag = false;
                            isIllegal = true;
                       /* //IP端口不合法
                        buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                        buildInfoList.add(buildInfo);*/
                        }
                        if (domain[1].trim().indexOf("//") != 0) {
                            flag = false;
                            isIllegal = true;
                        }
                    }
                }
                if (isIllegal && flag == false) {
                    buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                    buildInfoList.add(buildInfo);
                }
            }

        }
        if (isIllegal) {
            if (squid.getSquid_type() == SquidTypeEnum.DBSOURCE.value()
                    || squid.getSquid_type() == SquidTypeEnum.CLOUDDB.value()
                    || squid.getSquid_type() == SquidTypeEnum.TRAININGDBSQUID.value()) {
                buildInfo.setMessageCode(MessageCode.ERR_DBSOURCE_IP_ILLEGAL.value());
            } else if (squid.getSquid_type() == SquidTypeEnum.MONGODB.value()) {
                buildInfo.setMessageCode(MessageCode.ERR_MONGODB_IP_ILLEGAL.value());
            } else if (squid.getSquid_type() == SquidTypeEnum.HDFS.value()
                    || SquidTypeEnum.SOURCECLOUDFILE.value() == squid.getSquid_type()
                    || SquidTypeEnum.TRAINNINGFILESQUID.value() == squid.getSquid_type()) {
                buildInfo.setMessageCode(MessageCode.ERR_HDFS_IP_ILLEGAL.value());
            } else if (squid.getSquid_type() == SquidTypeEnum.FTP.value()) {
                buildInfo.setMessageCode(MessageCode.ERR_FTP_IP_ILLEGAL.value());
            } else if (squid.getSquid_type() == SquidTypeEnum.FILEFOLDER.value()) {
                buildInfo.setMessageCode(MessageCode.ERR_FILE_IP_ILLEGAL.value());
            } else if (squid.getSquid_type() == SquidTypeEnum.HBASE.value()) {
                buildInfo.setMessageCode(MessageCode.ERR_HABSE_IP_ILLEGAL.value());
            } else if (squid.getSquid_type() == SquidTypeEnum.DEST_HDFS.value()
                    || squid.getSquid_type() == SquidTypeEnum.DEST_IMPALA.value()
                    || squid.getSquid_type() == SquidTypeEnum.DESTES.value()
                    || squid.getSquid_type() == SquidTypeEnum.DESTCLOUDFILE.value()) {
                buildInfo.setMessageCode(MessageCode.ERR_DEST_SQUID_ILLEGAL.value());
            } else if (squid.getSquid_type() == SquidTypeEnum.KAFKA.value()) {
                buildInfo.setMessageCode(MessageCode.ERR_KAFKA_IP_ISLEEGAL.value());
            }
        }
        if (isNull) {
            if (squid.getSquid_type() == SquidTypeEnum.DBSOURCE.value()
                    || squid.getSquid_type() == SquidTypeEnum.CLOUDDB.value()
                    || squid.getSquid_type() == SquidTypeEnum.TRAININGDBSQUID.value()) {
                buildInfo.setMessageCode(MessageCode.ERR_DBSOURCE_IP_NULL.value());
            } else if (squid.getSquid_type() == SquidTypeEnum.MONGODB.value()) {
                buildInfo.setMessageCode(MessageCode.ERR_MONGODB_IP_NULL.value());
            } else if (squid.getSquid_type() == SquidTypeEnum.HDFS.value()
                    || SquidTypeEnum.SOURCECLOUDFILE.value() == squid.getSquid_type()
                    || SquidTypeEnum.TRAINNINGFILESQUID.value() == squid.getSquid_type()) {
                buildInfo.setMessageCode(MessageCode.ERR_HDFS_IP_NULL.value());
            } else if (squid.getSquid_type() == SquidTypeEnum.FTP.value()) {
                buildInfo.setMessageCode(MessageCode.ERR_FTP_IP_NULL.value());
            } else if (squid.getSquid_type() == SquidTypeEnum.FILEFOLDER.value()) {
                buildInfo.setMessageCode(MessageCode.ERR_FILE_IP_NULL.value());
            } else if (squid.getSquid_type() == SquidTypeEnum.HBASE.value()) {
                buildInfo.setMessageCode(MessageCode.ERR_HABSE_IP_NULL.value());
            } else if (squid.getSquid_type() == SquidTypeEnum.DEST_HDFS.value()
                    || squid.getSquid_type() == SquidTypeEnum.DEST_IMPALA.value()
                    || squid.getSquid_type() == SquidTypeEnum.DESTES.value()
                    || squid.getSquid_type() == SquidTypeEnum.DESTCLOUDFILE.value()) {
                buildInfo.setMessageCode(MessageCode.ERR_DEST_IPANDPORT_ISNULL.value());
            } else if (squid.getSquid_type() == SquidTypeEnum.KAFKA.value()) {
                buildInfo.setMessageCode(MessageCode.ERR_KAFKA_IP_ISNULL.value());
            }
        }
        return flag;
    }

    /**
     * 检查squid是否是孤立的squid
     * 编译状态检查时，所有squid都需要检查
     * 调度时候，编译状态检查,只要有错误的squid，就返回，无需继续向下检查
     */
    public static List<BuildInfo> validateSingleSquid(List<BuildInfo> buildInfoList, Map<String, Object> map, Squid squid, IRelationalDataManager adapter) {
        logger.info("编译检查:检查" + squid.getName() + "是否是孤立Squid开始");
        BuildInfo buildInfo = new BuildInfo(squid, map);
        /**
         * 判断当前squid是否有上游或者下游squid
         * 也就是查询squidLink是否有记录
         */
        //根据squidId，查询出SquidLink
        ISquidLinkDao squidLinkDao = new SquidLinkDaoImpl(adapter);
        /**
         * stage squid,dataming squid,exception squid,destination squid没有上游
         */
        Class<?> className = SquidTypeEnum.classOfValue(squid.getSquid_type());
        String name = className.getSimpleName();
        List<SquidLink> squidLinks = squidLinkDao.getSquidLinkBySquidIds(squid.getSquidflow_id(), squid.getId() + "");
        if (squidLinks.size() == 0 || squidLinks == null) {
            buildInfo.setMessageCode(MessageCode.SINGLE_SQUID.value());
            //只有当squid为connnection时，才报警告信息,其余的报错误信息
            if (squid.getSquid_type() == SquidTypeEnum.DBSOURCE.value()
                    || squid.getSquid_type() == SquidTypeEnum.MONGODB.value()
                    || squid.getSquid_type() == SquidTypeEnum.HDFS.value()
                    || SquidTypeEnum.SOURCECLOUDFILE.value() == squid.getSquid_type()
                    || squid.getSquid_type() == SquidTypeEnum.FTP.value()
                    || squid.getSquid_type() == SquidTypeEnum.FILEFOLDER.value()
                    || squid.getSquid_type() == SquidTypeEnum.HBASE.value()
                    || squid.getSquid_type() == SquidTypeEnum.KAFKA.value()
                    || squid.getSquid_type() == SquidTypeEnum.CLOUDDB.value()
                    || squid.getSquid_type() == SquidTypeEnum.TRAINNINGFILESQUID.value()
                    || squid.getSquid_type() == SquidTypeEnum.TRAININGDBSQUID.value()
                    || squid.getSquid_type() == SquidTypeEnum.HIVE.value()
                    || squid.getSquid_type() == SquidTypeEnum.CASSANDRA.value()) {
                buildInfo.setInfoType(SchedulerLogStatus.WARNING.getValue());
            } else {
                buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
            }
            buildInfoList.add(buildInfo);
            return buildInfoList;
        }
        //如果不是孤立的检查是否有上游
        if ("StageSquid".equals(name)
                || "StreamStageSquid".equals(name)
                || "ExceptionSquid".equals(name)
                || "DataMiningSquid".equals(name)
                || "DestESSquid".equals(name)
                || "DestHDFSSquid".equals(name)
                || "DestImpalaSquid".equals(name)) {
            try {
                List<SquidLink> squidLinkList = squidLinkDao.getFormSquidLinkBySquidId(squid.getId());
                if (squidLinkList == null || squidLinkList.size() == 0) {
                    buildInfo = new BuildInfo(squid, map);
                    int messagecode = 0;
                    switch (SquidTypeEnum.parse(squid.getSquid_type())) {
                        case STAGE:
                        case STREAM_STAGE:
                            messagecode = MessageCode.ERR_STAGEFORMSQUID_ISNULL.value();
                            break;
                        case LOGREG:
                        case NAIVEBAYES:
                        case SVM:
                        case KMEANS:
                        case ALS:
                        case LINEREG:
                        case RIDGEREG:
                        case QUANTIFY:
                        case DISCRETIZE:
                        case DECISIONTREE:
                        case LASSO:
                        case RANDOMFORESTCLASSIFIER:
                        case RANDOMFORESTREGRESSION:
                        case MULTILAYERPERCEPERONCLASSIFIER:
                        case NORMALIZER:
                        case PLS:
                        case ASSOCIATION_RULES:
                        case DECISIONTREEREGRESSION:
                        case DECISIONTREECLASSIFICATION:
                            messagecode = MessageCode.ERR_DATAMINGFROMSQUID_ISNULL.value();
                            break;
                        case EXCEPTION:
                            messagecode = MessageCode.ERR_EXCEPTIONFROMSQUID_ISNULL.value();
                            break;
                        case DEST_HDFS:
                        case DEST_IMPALA:
                        case DESTES:
                        case DESTCLOUDFILE:
                            messagecode = MessageCode.ERR_DESTSFROMSQUID_ISNULL.value();
                            break;
                    }
                    buildInfo.setMessageCode(messagecode);
                    buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                    buildInfoList.add(buildInfo);
                    return buildInfoList;
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return buildInfoList;

    }

    /**
     * 验证自定义squid信息是否正确
     *
     * @param buildInfoList
     * @param map
     * @param squid
     * @param adapter
     * @return
     */
    public static List<BuildInfo> validateUserDefinedSquidIsCorrect(List<BuildInfo> buildInfoList, Map<String, Object> map, Squid squid, IRelationalDataManager adapter) throws Exception {
        logger.info("编译状态:检查" + squid.getName() + "信息是否正确");
        if (squid.getSquid_type() == SquidTypeEnum.USERDEFINED.value()) {
            ISquidDao squidDao = new SquidDaoImpl(adapter);
            UserDefinedSquid definedSquid = squidDao.getSquidForCond(squid.getId(), UserDefinedSquid.class);
            //判断是否指定类
            if (StringUtils.isNull(definedSquid.getSelectClassName())) {
                BuildInfo buildInfo = new BuildInfo(squid, map);
                buildInfo.setMessageCode(MessageCode.USERDEFINED_CLASSNAME_ISNOT_SELECT.value());
                buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                buildInfoList.add(buildInfo);
            }
            //判断dataMapColumn是否制定入参
            Map<String, Object> paramMap = new HashMap<>();
            paramMap.put("squid_id", squid.getId());
            List<UserDefinedMappingColumn> userDefinedMappingColumnList = adapter.query2List2(true, paramMap, UserDefinedMappingColumn.class);
            if (userDefinedMappingColumnList != null) {
                for (UserDefinedMappingColumn mappingColumn : userDefinedMappingColumnList) {
                    if (mappingColumn.getColumn_id() == 0) {
                        BuildInfo buildInfo = new BuildInfo(squid, map);
                        buildInfo.setMessageCode(MessageCode.USERDEFINED_DATAMAPCOLUMN_NOT_HAVE_COLUMNID.value());
                        buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                        buildInfoList.add(buildInfo);
                        break;
                    }
                }
            }
            List<UserDefinedParameterColumn> parameterColumns = adapter.query2List2(true, paramMap, UserDefinedParameterColumn.class);
            if (parameterColumns != null) {
                for (UserDefinedParameterColumn parameterColumn : parameterColumns) {
                    if (StringUtils.isNull(parameterColumn.getValue())) {
                        BuildInfo buildInfo = new BuildInfo(squid, map);
                        buildInfo.setMessageCode(MessageCode.USERDEFINED_PARAMETER_VALUE_ISNULL.value());
                        buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                        buildInfoList.add(buildInfo);
                        break;
                    }
                }
            }

        }
        return buildInfoList;
    }

    /**
     * 校验statisticSquid信息是否正确
     *
     * @param buildInfoList
     * @param map
     * @param squid
     * @param adapter
     * @return
     */
    public static List<BuildInfo> validateStatisticSquidIsCorrect(List<BuildInfo> buildInfoList, Map<String, Object> map, Squid squid, IRelationalDataManager adapter) throws Exception {
        logger.info("编译状态:检查" + squid.getName() + "信息是否正确");
        if (squid.getSquid_type() == SquidTypeEnum.STATISTICS.value()) {
            ISquidDao squidDao = new SquidDaoImpl(adapter);
            StatisticsSquid statisticsSquid = squidDao.getSquidForCond(squid.getId(), StatisticsSquid.class);
            //判断dataMapColumn是否制定入参
            Map<String, Object> paramMap = new HashMap<>();
            paramMap.put("squid_id", squid.getId());
            List<StatisticsDataMapColumn> statisticsDataMapColumnList = adapter.query2List2(true, paramMap, StatisticsDataMapColumn.class);
            if (statisticsDataMapColumnList != null) {
                if ("OneWayANOVA".equals(statisticsSquid.getStatistics_name())) {
                    if (statisticsDataMapColumnList.size() < 2) {
                        BuildInfo buildInfo = new BuildInfo(squid, map);
                        buildInfo.setMessageCode(MessageCode.USERDEFINED_DATAMAPCOLUMN_NOT_HAVE_COLUMNID.value());
                        buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                        buildInfoList.add(buildInfo);
                    }
                }
                if (StringUtils.isNull(statisticsSquid.getStatistics_name())) {
                    BuildInfo buildInfo = new BuildInfo(squid, map);
                    buildInfo.setMessageCode(MessageCode.ERR_STATISTIC_NAME_IS_NULL.value());
                    buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                    buildInfoList.add(buildInfo);
                }
                for (StatisticsDataMapColumn mappingColumn : statisticsDataMapColumnList) {
                    if (mappingColumn.getColumn_id() == 0) {
                        BuildInfo buildInfo = new BuildInfo(squid, map);
                        buildInfo.setMessageCode(MessageCode.STATTISTICS_DATAMAPCOLUMN_NOT_HAVE_COLUMNID.value());
                        buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                        buildInfoList.add(buildInfo);
                        break;
                    }
                }
            }
            List<StatisticsParameterColumn> parameterColumns = adapter.query2List2(true, paramMap, StatisticsParameterColumn.class);
            if (parameterColumns != null) {
                for (StatisticsParameterColumn parameterColumn : parameterColumns) {
                    if (StringUtils.isNull(parameterColumn.getValue())) {
                        BuildInfo buildInfo = new BuildInfo(squid, map);
                        buildInfo.setMessageCode(MessageCode.STATISTICS_PARAMETER_VALUE_ISNULL.value());
                        buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                        buildInfoList.add(buildInfo);
                        break;
                    }
                }
            }
            //主成分数量要大于或等于1，且小于或等于PCAInput的列数
            if (statisticsDataMapColumnList != null && parameterColumns != null) {
                if ("PCA".equals(statisticsSquid.getStatistics_name()) && StringUtils.isNotEmpty(parameterColumns.get(0).getValue())) {
                    if (Integer.parseInt(parameterColumns.get(0).getValue()) < 1 || Integer.parseInt(parameterColumns.get(0).getValue()) > statisticsDataMapColumnList.size()) {
                        BuildInfo buildInfo = new BuildInfo(squid, map);
                        buildInfo.setMessageCode(MessageCode.STATISTICS_PARAMETER_PCA_VALUE_ISMAX.value());
                        buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                        buildInfoList.add(buildInfo);
                    }
                }
            }
        }
        return buildInfoList;
    }

    /**
     * 校验DataMiningSquid信息是否正确
     * @param buildInfoList
     * @param map
     * @param squid
     * @param adapter
     * @return
     * @throws Exception
     */
    public static List<BuildInfo> validateDataMiningSquidIsCorrect(List<BuildInfo> buildInfoList, Map<String, Object> map, Squid squid, IRelationalDataManager adapter) throws Exception {
        logger.info("编译状态:检查" + squid.getName() + "信息是否正确");
        ISquidDao squidDao = new SquidDaoImpl(adapter);
        DataMiningSquid dataMiningSquid = squidDao.getSquidForCond(squid.getId(), DataMiningSquid.class);
        if (dataMiningSquid != null && squid.getSquid_type() != SquidTypeEnum.DATAVIEW.value() && squid.getSquid_type() != SquidTypeEnum.COEFFICIENT.value()
                && squid.getSquid_type() != SquidTypeEnum.NORMALIZER.value()) {
            if (squid.getSquid_type() == SquidTypeEnum.MULTILAYERPERCEPERONCLASSIFIER.value()) {
                //MultilayerPerceptronClass网络层的值不是CSN格式
                if (!StringUtils.isNull(dataMiningSquid.getLayers()) && !StringUtils.isCSN(dataMiningSquid.getLayers())) {
                    BuildInfo buildInfo = new BuildInfo(squid, map);
                    buildInfo.setMessageCode(MessageCode.ERR_MultilayerPerceptron_Layers_ISNOT_CSN.value());
                    buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                    buildInfoList.add(buildInfo);
                }
                //MultilayerPerceptronClass初始权值的值不是CSN格式
                if (!StringUtils.isNull(dataMiningSquid.getInitialweights()) && !StringUtils.isDigital(dataMiningSquid.getInitialweights())) {
                    BuildInfo buildInfo = new BuildInfo(squid, map);
                    buildInfo.setMessageCode(MessageCode.ERR_MultilayerPerceptron_Initialweights_ISNOT_CSN.value());
                    buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                    buildInfoList.add(buildInfo);
                }
                //多层感知机分类中容许误差 Solver = l-bfgs 时>=0，Solver = gd 时在[0,1]
                if (!StringUtils.isNull(dataMiningSquid.getSolver())) {
                    if (dataMiningSquid.getSolver() == 2 && dataMiningSquid.getTolerance() < 0) {
                        BuildInfo buildInfo = new BuildInfo(squid, map);
                        buildInfo.setMessageCode(MessageCode.ERR_MultilayerPerceptron_Tolerance_IS_GreaterThan_Or_EqualToZero.value());
                        buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                        buildInfoList.add(buildInfo);
                    }
                    if (dataMiningSquid.getSolver() == 1 && (dataMiningSquid.getTolerance() < 0 || dataMiningSquid.getTolerance() > 1)) {
                        BuildInfo buildInfo = new BuildInfo(squid, map);
                        buildInfo.setMessageCode(MessageCode.ERR_MultilayerPerceptron_Tolerance_IS_Between_0and1.value());
                        buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                        buildInfoList.add(buildInfo);
                    }
                }
            }
            if (squid.getSquid_type() == SquidTypeEnum.RANDOMFORESTREGRESSION.value() || squid.getSquid_type() == SquidTypeEnum.RANDOMFORESTCLASSIFIER.value()) {
                //RANDOMFORESTREGRESSION的采样率要在0-1之间不包括0
                if (!StringUtils.isNull(dataMiningSquid.getSubsampling_rate()) && squid.getSquid_type() == SquidTypeEnum.RANDOMFORESTREGRESSION.value()
                        && (dataMiningSquid.getSubsampling_rate() <= 0.0 || dataMiningSquid.getSubsampling_rate() > 1)) {
                    BuildInfo buildInfo = new BuildInfo(squid, map);
                    buildInfo.setMessageCode(MessageCode.ERR_RandomforestRegression_SubsamplingRate_NotEqualTo_ZERO.value());
                    buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                    buildInfoList.add(buildInfo);
                }
                //RANDOMFORESTCLASSIFIER的采样率要在0-1之间不包括0
                if (!StringUtils.isNull(dataMiningSquid.getSubsampling_rate()) && squid.getSquid_type() == SquidTypeEnum.RANDOMFORESTCLASSIFIER.value()
                        && (dataMiningSquid.getSubsampling_rate() <= 0.0 || dataMiningSquid.getSubsampling_rate() > 1)) {
                    BuildInfo buildInfo = new BuildInfo(squid, map);
                    buildInfo.setMessageCode(MessageCode.ERR_RandomForestClassifier_SubsamplingRate_NotEqualTo_ZERO.value());
                    buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                    buildInfoList.add(buildInfo);
                }
                if (dataMiningSquid.getFeature_subset_strategy() == 5) {
                    //RandomforestRegression特征子集比例不能等于0
                    if (!StringUtils.isNull(dataMiningSquid.getFeature_subset_scale()) && squid.getSquid_type() == SquidTypeEnum.RANDOMFORESTREGRESSION.value()) {
                        if (dataMiningSquid.getFeature_subset_scale() <= 0.0) {
                            BuildInfo buildInfo = new BuildInfo(squid, map);
                            buildInfo.setMessageCode(MessageCode.ERR_RandomforestRegression_Feature_Subset_Scale_NotEqualTo_ZERO.value());
                            buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                            buildInfoList.add(buildInfo);
                        }
                        //RandomforestRegression特征子集比例大于1时要是整数
                        if (dataMiningSquid.getFeature_subset_scale() > 1.0) {
                            NumberFormat nf = NumberFormat.getInstance();
                            nf.setGroupingUsed(false);
                            String value = nf.format(dataMiningSquid.getFeature_subset_scale());
                            if(!StringUtils.isInteger(value)){
                                BuildInfo buildInfo = new BuildInfo(squid, map);
                                buildInfo.setMessageCode(MessageCode.ERR_RandomforestRegression_Feature_Subset_Scale_Not_INTEGER.value());
                                buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                                buildInfoList.add(buildInfo);
                            }
                        }
                    }
                    if (!StringUtils.isNull(dataMiningSquid.getFeature_subset_scale()) && squid.getSquid_type() == SquidTypeEnum.RANDOMFORESTCLASSIFIER.value()) {
                        //RandomForestClassifier特征子集比例不能等于0
                        if (dataMiningSquid.getFeature_subset_scale() <= 0.0) {
                            BuildInfo buildInfo = new BuildInfo(squid, map);
                            buildInfo.setMessageCode(MessageCode.ERR_RandomForestClassifier_Feature_Subset_Scale_NotEqualTo_ZERO.value());
                            buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                            buildInfoList.add(buildInfo);
                        }
                        //RandomForestClassifier特征子集比例大于1时要是整数
                        if (dataMiningSquid.getFeature_subset_scale() > 1.0) {
                            NumberFormat nf = NumberFormat.getInstance();
                            nf.setGroupingUsed(false);
                            String value = nf.format(dataMiningSquid.getFeature_subset_scale());
                            if(!StringUtils.isInteger(value)){
                                BuildInfo buildInfo = new BuildInfo(squid, map);
                                buildInfo.setMessageCode(MessageCode.ERR_RandomForestClassifier_Feature_Subset_Scale_Not_INTEGER.value());
                                buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                                buildInfoList.add(buildInfo);
                            }
                        }
                    }
                }
            }
            //步长的值要大于0
            if (dataMiningSquid.getSquid_type() == SquidTypeEnum.SVM.value() ||
                    dataMiningSquid.getSquid_type() == SquidTypeEnum.MULTILAYERPERCEPERONCLASSIFIER.value()) {
                if (!StringUtils.isNull(dataMiningSquid.getStep_size()) && dataMiningSquid.getStep_size() <= 0.0) {
                    BuildInfo buildInfo = new BuildInfo(squid, map);
                    buildInfo.setMessageCode(MessageCode.ERR_Step_size_NotEqualTo_ZERO.value());
                    buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                    buildInfoList.add(buildInfo);
                }
            }
            //每次最小数据量必须大于0 小于或等于1
            if (squid.getSquid_type() == SquidTypeEnum.SVM.value()) {
                if(!StringUtils.isNull(dataMiningSquid.getMin_batch_fraction())&&(dataMiningSquid.getMin_batch_fraction()<=0.0||dataMiningSquid.getMin_batch_fraction()>1.0)){
                    BuildInfo buildInfo = new BuildInfo(squid, map);
                    buildInfo.setMessageCode(MessageCode.ERR_Min_batch_fraction_IS_Between_0and1.value());
                    buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                    buildInfoList.add(buildInfo);
                }
            }

            //训练集比例必须大于0 小于等于100(关联规则和量化除外)
            if(squid.getSquid_type()!=SquidTypeEnum.QUANTIFY.value()||squid.getSquid_type()!=SquidTypeEnum.ASSOCIATION_RULES.value()){
                if(!StringUtils.isNull(dataMiningSquid.getTraining_percentage())&&(dataMiningSquid.getTraining_percentage()<=0.0||dataMiningSquid.getTraining_percentage()>100.0)){
                    BuildInfo buildInfo = new BuildInfo(squid, map);
                    buildInfo.setMessageCode(MessageCode.ERR_Training_Percentage_Greater_Than_0_Is_Less_Than_100.value());
                    buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                    buildInfoList.add(buildInfo);
                }
            }
            //最大深度必须大于等于0 小于或等于30
            if(squid.getSquid_type()!=SquidTypeEnum.RANDOMFORESTCLASSIFIER.value()||squid.getSquid_type()!=SquidTypeEnum.RANDOMFORESTREGRESSION.value()||
                    squid.getSquid_type()!=SquidTypeEnum.DECISIONTREE.value()){
                if(!StringUtils.isNull(dataMiningSquid.getMax_depth())&&(dataMiningSquid.getMax_depth()<0||dataMiningSquid.getMax_depth()>30)){
                    BuildInfo buildInfo = new BuildInfo(squid, map);
                    buildInfo.setMessageCode(MessageCode.ERR_Max_Depth_IS_Between_0and30.value());
                    buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                    buildInfoList.add(buildInfo);
                }
            }
            //并行数必须大于0  kmeansSquid 已经去掉了这个属性。
            if(squid.getSquid_type()==SquidTypeEnum.ALS.value()){
                if(!StringUtils.isNull(dataMiningSquid.getParallel_runs())&&dataMiningSquid.getParallel_runs()<=0){
                    BuildInfo buildInfo = new BuildInfo(squid, map);
                    buildInfo.setMessageCode(MessageCode.ERR_Parallel_Runs_IS_GreaterThanZero.value());
                    buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                    buildInfoList.add(buildInfo);
                }
            }
            //K值必须大于0
            if(squid.getSquid_type()==SquidTypeEnum.KMEANS.value()){
                if(!StringUtils.isNull(dataMiningSquid.getK())&&dataMiningSquid.getK()<2){
                    BuildInfo buildInfo = new BuildInfo(squid, map);
                    buildInfo.setMessageCode(MessageCode.ERR_K_IS_GreaterThan2.value());
                    buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                    buildInfoList.add(buildInfo);
                }
                //初始步数要大于0
                if(!StringUtils.isNull(dataMiningSquid.getInit_Steps()) && dataMiningSquid.getInit_Steps()<=0){
                    BuildInfo buildInfo = new BuildInfo(squid, map);
                    buildInfo.setMessageCode(MessageCode.ERR_KMEANS_InitSteps_GreaterThanZero.value());
                    buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                    buildInfoList.add(buildInfo);
                }
                //容许误差要大于或等于0
                if(!StringUtils.isNull(dataMiningSquid.getTolerance()) && dataMiningSquid.getTolerance()<0){
                    BuildInfo buildInfo = new BuildInfo(squid, map);
                    buildInfo.setMessageCode(MessageCode.ERR_KMEANS_tolerance_GreaterThanZero.value());
                    buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                    buildInfoList.add(buildInfo);
                }
            }
            //最大特征数必须大于等于2
            if(squid.getSquid_type() == SquidTypeEnum.RANDOMFORESTREGRESSION.value() || squid.getSquid_type() == SquidTypeEnum.RANDOMFORESTCLASSIFIER.value()||
                    squid.getSquid_type() == SquidTypeEnum.DECISIONTREE.value()) {
                if (!StringUtils.isNull(dataMiningSquid.getMax_bins())) {
                    if (dataMiningSquid.getMax_bins() < 2) {
                        BuildInfo buildInfo = new BuildInfo(squid, map);
                        buildInfo.setMessageCode(MessageCode.ERR_Max_Bins_Greater_Than_Or_Equal_To_2.value());
                        buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                        buildInfoList.add(buildInfo);
                    }
                }
            }
            //pls  X和Y标准化模型为空
            if(squid.getSquid_type() == SquidTypeEnum.PLS.value()){
                if(!StringUtils.isNull(dataMiningSquid.getX_model_squid_id())){
                    if(dataMiningSquid.getX_model_squid_id()==0){
                        BuildInfo buildInfo = new BuildInfo(squid, map);
                        buildInfo.setMessageCode(MessageCode.ERR_X_Model_ISNULL.value());
                        buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                        buildInfoList.add(buildInfo);
                    }
                }
                if(!StringUtils.isNull(dataMiningSquid.getY_model_squid_id())){
                    if(dataMiningSquid.getY_model_squid_id()==0){
                        BuildInfo buildInfo = new BuildInfo(squid, map);
                        buildInfo.setMessageCode(MessageCode.ERR_Y_Model_ISNULL.value());
                        buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                        buildInfoList.add(buildInfo);
                    }
                }
                if(!StringUtils.isNull(dataMiningSquid.getMax_integer_number())){
                    if(dataMiningSquid.getIteration_number()<1){
                        BuildInfo buildInfo = new BuildInfo(squid, map);
                        buildInfo.setMessageCode(MessageCode.ERR_Integer_Greater_Than_Or_Equal_To_One.value());
                        buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                        buildInfoList.add(buildInfo);
                    }
                }
            }
            //弹性网参数取值区间[0,1]（逻辑回归,线性回归）
            if (squid.getSquid_type() == SquidTypeEnum.LOGREG.value() || squid.getSquid_type() == SquidTypeEnum.LINEREG.value()) {
                if (dataMiningSquid.getElastic_net_param() < 0.0 || dataMiningSquid.getElastic_net_param() > 1.0) {
                    BuildInfo buildInfo = new BuildInfo(squid, map);
                    buildInfo.setMessageCode(MessageCode.ERR_Elastic_Net_Param_Between_0_and_1.value());
                    buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                    buildInfoList.add(buildInfo);
                }
            }
            //每个节点最小实例数要大于等于1(决策树回归,决策树分类)
            if (squid.getSquid_type() == SquidTypeEnum.DECISIONTREEREGRESSION.value() || squid.getSquid_type() == SquidTypeEnum.DECISIONTREECLASSIFICATION.value()) {
                if (dataMiningSquid.getMin_instances_per_node() < 1) {
                    BuildInfo buildInfo = new BuildInfo(squid, map);
                    buildInfo.setMessageCode(MessageCode.ERR_Min_Instances_Per_Node_Greater_Than_Or_Equal_To_1.value());
                    buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                    buildInfoList.add(buildInfo);
                }
            }
            if (squid.getSquid_type() == SquidTypeEnum.DECISIONTREEREGRESSION.value() || squid.getSquid_type() == SquidTypeEnum.RANDOMFORESTCLASSIFIER.value()
                    || squid.getSquid_type() == SquidTypeEnum.DECISIONTREECLASSIFICATION.value() || squid.getSquid_type() == SquidTypeEnum.RANDOMFORESTREGRESSION.value()) {
                //特征离散阈值要大于等于2(决策树回归,决策树分类,随机森林分类,随机森林回归)
                if (!StringUtils.isNull(dataMiningSquid.getMax_categories())) {
                    if (dataMiningSquid.getMax_categories() < 2) {
                        BuildInfo buildInfo = new BuildInfo(squid, map);
                        buildInfo.setMessageCode(MessageCode.ERR_Max_Categories_Greater_Than_Or_Equal_To_2.value());
                        buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                        buildInfoList.add(buildInfo);
                    }
                }
                //最大特征数要大于等于特征离散阈值(决策树回归,决策树分类,随机森林分类,随机森林回归)
                if (!StringUtils.isNull(dataMiningSquid.getMax_bins()) && !StringUtils.isNull(dataMiningSquid.getMax_categories())) {
                    if (dataMiningSquid.getMax_bins() < dataMiningSquid.getMax_categories()) {
                        BuildInfo buildInfo = new BuildInfo(squid, map);
                        buildInfo.setMessageCode(MessageCode.ERR_Max_Bins_Greater_Than_Or_Equal_To_Max_Categories.value());
                        buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                        buildInfoList.add(buildInfo);
                    }
                }

            }
            //分类阈值不能为空(逻辑回归,决策树分类,随机森林分类,朴素贝叶斯分类)
            if (squid.getSquid_type() == SquidTypeEnum.LOGREG.value()
                    || squid.getSquid_type() == SquidTypeEnum.RANDOMFORESTCLASSIFIER.value()
                    || squid.getSquid_type() == SquidTypeEnum.DECISIONTREECLASSIFICATION.value()
                    || squid.getSquid_type()==SquidTypeEnum.NAIVEBAYES.value()) {
                if (StringUtils.isNull(dataMiningSquid.getThreshold()) || !StringUtils.isCSN2(dataMiningSquid.getThreshold())) {
                    BuildInfo buildInfo = new BuildInfo(squid, map);
                    buildInfo.setMessageCode(MessageCode.ERR_Threshold_ISNOT_CSN.value());
                    buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                    buildInfoList.add(buildInfo);
                } else {
                    //分类阈值中只能有一个元素为0
                    if (!StringUtils.isNull(dataMiningSquid.getThreshold())) {
                        int count = 0;
                        String threshold = dataMiningSquid.getThreshold().replaceAll("\\'", "");
                        String[] thresholds = threshold.split(",");
                        for (String str : thresholds) {
                            if (count == 2) {
                                break;
                            }
                            //过滤掉小数
                            Pattern p = Pattern.compile("^(0|[1-9]\\d*)$");
                            Matcher m = p.matcher(str);
                            if (m.matches() && str.equals("0")) {
                                count++;
                            }
                        }
                        if (count == 2) {
                            BuildInfo buildInfo = new BuildInfo(squid, map);
                            buildInfo.setMessageCode(MessageCode.ERR_Threshold_Only_One_Is_0.value());
                            buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                            buildInfoList.add(buildInfo);
                        }
                    }
                }
            }
        }
        //NORMALIZER 量化区间的最大值要大于最小值
        if(dataMiningSquid!=null&&dataMiningSquid.getSquid_type()==SquidTypeEnum.NORMALIZER.value()&&dataMiningSquid.getMethod()==1){
            if(dataMiningSquid.getMax_value()<=dataMiningSquid.getMin_value()){
                BuildInfo buildInfo = new BuildInfo(squid, map);
                buildInfo.setMessageCode(MessageCode.ERR_Max_is_Greater_Than_Min.value());
                buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                buildInfoList.add(buildInfo);
            }
        }
        //迭代次数必须大于等于1
        if(squid.getSquid_type() == SquidTypeEnum.BISECTINGKMEANSSQUID.value()
                || squid.getSquid_type() == SquidTypeEnum.KMEANS.value()
                || squid.getSquid_type() == SquidTypeEnum.RIDGEREG.value()
                || squid.getSquid_type() == SquidTypeEnum.LINEREG.value()
                || squid.getSquid_type() == SquidTypeEnum.SVM.value()
                || squid.getSquid_type() == SquidTypeEnum.LOGREG.value()
                || squid.getSquid_type()==SquidTypeEnum.ALS.value()
                || squid.getSquid_type()==SquidTypeEnum.LASSO.value()
                || squid.getSquid_type()==SquidTypeEnum.MULTILAYERPERCEPERONCLASSIFIER.value()){
            if(dataMiningSquid.getIteration_number()<1){
                BuildInfo buildInfo = new BuildInfo(squid, map);
                buildInfo.setMessageCode(MessageCode.ERR__BISECTINGSQUID_ITERATION_NUMBER_GreaterThan1.value());
                buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                buildInfoList.add(buildInfo);
            }
        }

        //BisectingKMeansSquid 可分聚类最小样本数要大于0 K要大于等于2 迭代次数要大于等于1
        if(dataMiningSquid!=null && dataMiningSquid.getSquid_type()==SquidTypeEnum.BISECTINGKMEANSSQUID.value()){
            if(!StringUtils.isNull(dataMiningSquid.getK())&&dataMiningSquid.getK()<2){
                BuildInfo buildInfo = new BuildInfo(squid, map);
                buildInfo.setMessageCode(MessageCode.ERR_K_IS_GreaterThan2.value());
                buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                buildInfoList.add(buildInfo);
            }
            if(!StringUtils.isNull(dataMiningSquid.getMinDivisibleClusterSize()) && dataMiningSquid.getMinDivisibleClusterSize()<=0){
                BuildInfo buildInfo=new BuildInfo(squid,map);
                buildInfo.setMessageCode(MessageCode.ERR_minDivisibleClusterSize_IS_GreaterThanZero.value());
                buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                buildInfoList.add(buildInfo);
            }

        }

        return buildInfoList;
    }
    /**
     * 验证GroupTagSquid的sortColumn和TagColumn是否存在
     *
     * @param isConnect
     * @param buildInfoList
     * @param map
     * @param squid
     * @param adapter
     */
    public static List<BuildInfo> validateGroupTagIsExist(boolean isConnect, List<BuildInfo> buildInfoList, Map<String, Object> map, Squid squid, IRelationalDataManager adapter) throws Exception {
        logger.info("编译检查:检查 " + squid.getName() + " sortColumn,TagColumn是否存在");
        if (squid.getSquid_type() == SquidTypeEnum.GROUPTAGGING.value()) {
            ISquidDao squidDao = new SquidDaoImpl(adapter);
            DataSquid dataSquid = squidDao.getSquidForCond(squid.getId(), DataSquid.class);
            if (dataSquid != null) {
                if (StringUtils.isNull(dataSquid.getTaggingColumnIds())) {
                    BuildInfo buildInfo = new BuildInfo(squid, map);
                    buildInfo.setMessageCode(MessageCode.ERR_TAGSQUID_TAGCOLUMN_ISNULL.value());
                    buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                    buildInfoList.add(buildInfo);
                }
                if (StringUtils.isNull(dataSquid.getSortingColumnIds())) {
                    BuildInfo buildInfo = new BuildInfo(squid, map);
                    buildInfo.setMessageCode(MessageCode.ERR_TAGSQUID_SORTCOLUMN_ISNULL.value());
                    buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                    buildInfoList.add(buildInfo);
                }
            }
        }
        return buildInfoList;
    }

    /**
     *SamplingSquid 抽取百分比不能为空且大于0
     */
    public static List<BuildInfo> validateSamplingSquid(boolean isConnect, List<BuildInfo> buildInfoList, Map<String, Object> map, Squid squid, IRelationalDataManager adapter) throws Exception {
            if(squid.getSquid_type()==SquidTypeEnum.SAMPLINGSQUID.value()){
                ISquidDao squidDao = new SquidDaoImpl(adapter);
                SamplingSquid samplingSquid = squidDao.getSquidForCond(squid.getId(), SamplingSquid.class);
                if(samplingSquid!=null){
                    if(StringUtils.isNull(samplingSquid.getSamplingPercent()) ||  samplingSquid.getSamplingPercent()<=0.0 || samplingSquid.getSamplingPercent()>1.0){
                        BuildInfo buildInfo = new BuildInfo(squid, map);
                        buildInfo.setMessageCode(MessageCode.ERR_SAMPLINGSQUID_Percent_GreaterThanZero.value());
                        buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                        buildInfoList.add(buildInfo);
                    }
                }
            }
        return buildInfoList;
    }



    /**
     * 验证落地表名是否为空
     *
     * @param buildInfoList
     * @param map
     * @param squid
     * @param adapter
     * @return
     */
    public static List<BuildInfo> validateExtractName(boolean isConnect, List<BuildInfo> buildInfoList, Map<String, Object> map, Squid squid, IRelationalDataManager adapter) {
        logger.info("编译检查:检查 " + squid.getName() + " 落地表名是否为空");
        ISquidDao squidDao = new SquidDaoImpl(adapter);
        Class<?> className = SquidTypeEnum.classOfValue(squid.getSquid_type());
        try {
            //判断squid类型
            if (squid.getSquid_type() == SquidTypeEnum.DOC_EXTRACT.value()
                    || squid.getSquid_type() == SquidTypeEnum.KAFKAEXTRACT.value()
                    || squid.getSquid_type() == SquidTypeEnum.HBASEEXTRACT.value()) {
                DataSquid dataSquid = (DataSquid) squidDao.getSquidForCond(squid.getId(), className);
                if (dataSquid.isIs_persisted() && StringUtils.isEmpty(dataSquid.getTable_name())) {
                    BuildInfo buildInfo = new BuildInfo(squid, map);
                    buildInfo.setMessageCode(MessageCode.ERR_DATASQUID_TABLENAME_NULL.value());
                    buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                    buildInfoList.add(buildInfo);
                }
                //判断落地目标库或表已删除,判断落地sql是否正确
                //落地目标库为空，落地sql为空,只需校验落地表名是否重命名，当url测试连接没有问题时，才进行检测
                if (dataSquid.isIs_persisted()) {
                    boolean flag = true;
                    //检查落地目标库没有
                    if (dataSquid.getDestination_squid_id() == 0) {
                        flag = false;
                    } else {
                        //检查是否存在
                        Squid destsquid = squidDao.getSquidForCond(dataSquid.getDestination_squid_id(), Squid.class);
                        if (destsquid == null) {
                            flag = false;
                        }
                    }
                    if (!flag) {
                        BuildInfo buildInfo = new BuildInfo(squid, map);
                        buildInfo.setMessageCode(MessageCode.ERR_DESTSOURCE_ISNULL.value());
                        buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                        buildInfoList.add(buildInfo);
                    }
                }
                //判断kafka的group id是否为空
                if (squid.getSquid_type() == SquidTypeEnum.KAFKAEXTRACT.value()) {
                    KafkaExtractSquid kafkaExtractSquid = (KafkaExtractSquid) dataSquid;
                    if (StringUtils.isEmpty(kafkaExtractSquid.getGroup_id())) {
                        BuildInfo buildInfo = new BuildInfo(squid, map);
                        buildInfo.setMessageCode(MessageCode.ERR_KAFKA_EXTRACE_GROUPID_ISNULL.value());
                        buildInfo.setInfoType(SchedulerLogStatus.WARNING.getValue());
                        buildInfoList.add(buildInfo);
                    }


                }


            } else {
                //通过反射获取注解的类名，当类名中包含ds_squid的往下执行
                MultitableMapping tm = className.newInstance().getClass().getAnnotation(MultitableMapping.class);
                if (tm != null) {
                    String[] tableNames = tm.name();
                    for (String tableName : tableNames) {
                        if ("DS_SQUID".equals(tableName)) {
                            DataSquid dataSquid = squidDao.getSquidForCond(squid.getId(), DataSquid.class);
                            if (dataSquid != null) {
                                if (dataSquid.isIs_persisted() && StringUtils.isEmpty(dataSquid.getTable_name())) {
                                    BuildInfo buildInfo = new BuildInfo(squid, map);
                                    buildInfo.setMessageCode(MessageCode.ERR_DATASQUID_TABLENAME_NULL.value());
                                    buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                                    buildInfoList.add(buildInfo);
                                }
                                //判断落地目标库（Destination_squid_id!=0）,判断表名是否重名(squid.tableName),列名是否跟squid一致
                                if (dataSquid.isIs_persisted()) {
                                    boolean flag = true;
                                    if (dataSquid.getDestination_squid_id() == 0) {
                                        flag = false;
                                    } else {
                                        Squid destsquid = squidDao.getSquidForCond(dataSquid.getDestination_squid_id(), Squid.class);
                                        if (destsquid == null) {
                                            flag = false;
                                        }
                                    }
                                    if (!flag) {
                                        //查询落地对象是否存在
                                        BuildInfo buildInfo = new BuildInfo(squid, map);
                                        buildInfo.setMessageCode(MessageCode.ERR_DESTSOURCE_ISNULL.value());
                                        buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                                        buildInfoList.add(buildInfo);
                                    }
                                }
                                //校验语法是否正确
                                if (dataSquid.isIs_persisted() && (dataSquid.getPersist_sql() != null && dataSquid.getPersist_sql().length() > 0)) {
                                    String error = ValidateJoinProcess.validateSqlIsIllegal(dataSquid.getPersist_sql(), map);
                                    if (error != null) {
                                        BuildInfo buildInfo = new BuildInfo(squid, map);
                                        buildInfo.setMessageCode(MessageCode.ERR_DESTSQL_ISERROR.value());
                                        buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                                        buildInfoList.add(buildInfo);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            return buildInfoList;
        }

    }

    /**
     * 检查落地squid的ip是否合法
     */
    public static List<BuildInfo> validateDestSquid(List<BuildInfo> buildInfoList, Map<String, Object> map, Squid squid, IRelationalDataManager adapter) {
        logger.info("编译检查:检查落地Squid IP是否合法");
        if (SquidTypeEnum.isDestSquid(squid.getSquid_type())) {
            Map<String, String> params = new HashMap<String, String>();
            params.put("ID", squid.getId() + "");
            Class<?> className = SquidTypeEnum.classOfValue(squid.getSquid_type());
            try {
                Object obj = adapter.query2Object(params, className);
                if (squid.getSquid_type() != SquidTypeEnum.DESTCLOUDFILE.value()
                        && squid.getSquid_type() != SquidTypeEnum.DEST_HIVE.value()
                        && squid.getSquid_type() != SquidTypeEnum.DEST_CASSANDRA.value()) {
                    Method method = obj.getClass().getMethod("getHost", new Class[]{});
                    String host = method.invoke(obj, null) + "";
                    checkIPAndPort(buildInfoList, map, squid, host);
                }
                if (squid.getSquid_type() == SquidTypeEnum.DEST_HDFS.value() || squid.getSquid_type() == SquidTypeEnum.DESTCLOUDFILE.value()) {
                    DestHDFSSquid hdfsSquid = (DestHDFSSquid) obj;
                    String hdfs_path = hdfsSquid.getHdfs_path();
                    //只有当文件格式为text file的时候，才报文件路径为空
                    if (hdfs_path == null || hdfs_path.length() == 0) {
                        BuildInfo buildInfo = new BuildInfo(squid, map);
                        buildInfo.setMessageCode(MessageCode.ERR_DESTHDFS_ISNULL.value());
                        buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                        buildInfoList.add(buildInfo);
                    }

                    String delimiter = hdfsSquid.getColumn_delimiter();
                    if (hdfsSquid.getFile_formate() == 1) {
                        //允许为空格
                        if (delimiter==null || delimiter.length()==0) {
                            BuildInfo buildInfo = new BuildInfo(squid, map);
                            buildInfo.setMessageCode(MessageCode.ERR_DESTHDFS_DELIMITER_ISNULL.value());
                            buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                            buildInfoList.add(buildInfo);
                        }
                    }
                } else if (squid.getSquid_type() == SquidTypeEnum.DEST_IMPALA.value()) {
                    DestImpalaSquid destImpalaSquid = (DestImpalaSquid) obj;
                    String store_name = destImpalaSquid.getStore_name();
                    if (store_name == null || store_name.length() == 0) {
                        BuildInfo buildInfo = new BuildInfo(squid, map);
                        buildInfo.setMessageCode(MessageCode.ERR_IMPALA_STORENAME_ISNULL.value());
                        buildInfo.setInfoType(SchedulerLogStatus.WARNING.getValue());
                        buildInfoList.add(buildInfo);
                    }
                    String impala_table_name = destImpalaSquid.getImpala_table_name();
                    if (impala_table_name == null || impala_table_name.length() == 0) {
                        BuildInfo buildInfo = new BuildInfo(squid, map);
                        buildInfo.setMessageCode(MessageCode.ERR_IMPALA_TABLENAME_ISNULL.value());
                        buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                        buildInfoList.add(buildInfo);
                    }
                } else if (squid.getSquid_type() == SquidTypeEnum.DESTES.value()) {
                    DestESSquid destESSquid = (DestESSquid) obj;
                    if (StringUtils.isEmpty(destESSquid.getEsindex())) {
                        BuildInfo buildInfo = new BuildInfo(squid, map);
                        buildInfo.setMessageCode(MessageCode.ERR_DESTSINDEX_ISNULL.value());
                        buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                        buildInfoList.add(buildInfo);
                    }
                    if (StringUtils.isEmpty(destESSquid.getEstype())) {
                        BuildInfo buildInfo = new BuildInfo(squid, map);
                        buildInfo.setMessageCode(MessageCode.ERR_DESTTYPE_ISNULL.value());
                        buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                        buildInfoList.add(buildInfo);
                    }
                } else if (squid.getSquid_type() == SquidTypeEnum.DEST_HIVE.value()) {
                    DestHiveSquid destHive = (DestHiveSquid) obj;
                    if (StringUtils.isEmpty(destHive.getDb_name())) {
                        BuildInfo buildInfo = new BuildInfo(squid, map);
                        buildInfo.setMessageCode(MessageCode.ERR_DATABASENAME_ISNULL.value());
                        buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                        buildInfoList.add(buildInfo);
                    }
                    if (StringUtils.isEmpty(destHive.getTable_name())) {
                        BuildInfo buildInfo = new BuildInfo(squid, map);
                        buildInfo.setMessageCode(MessageCode.ERR_DEST_HIVE_TABLENAME_ISNULL.value());
                        buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                        buildInfoList.add(buildInfo);
                    }
                    //判断列是否设置
                    Map<String, Object> paramMap = new HashMap<>();
                    paramMap.put("squid_id", destHive.getId());
                    List<DestHiveColumn> destHiveColumns = adapter.query2List2(true, paramMap, DestHiveColumn.class);
                    if (destHiveColumns != null) {
                        for (DestHiveColumn hiveColumn : destHiveColumns) {
                            if (hiveColumn.getColumn_id() == 0) {
                                BuildInfo buildInfo = new BuildInfo(squid, map);
                                buildInfo.setMessageCode(MessageCode.ERR_DEST_HIVE_COLUMN_ISNOT_SET.value());
                                buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                                buildInfoList.add(buildInfo);
                                break;
                            }
                        }
                    } else {
                        BuildInfo buildInfo = new BuildInfo(squid, map);
                        buildInfo.setMessageCode(MessageCode.ERR_DEST_HIVE_COLUMN_ISNOT_SET.value());
                        buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                        buildInfoList.add(buildInfo);
                    }
                } else if (squid.getSquid_type() == SquidTypeEnum.DEST_CASSANDRA.value()) {
                    DestCassandraSquid destCassandra = (DestCassandraSquid) obj;
                    if (destCassandra.getDest_squid_id() == 0) {
                        BuildInfo buildInfo = new BuildInfo(squid, map);
                        buildInfo.setMessageCode(MessageCode.ERR_CASSANDRA_DATASOURCE_ISNULL.value());
                        buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                        buildInfoList.add(buildInfo);
                    }
                    if (StringUtils.isEmpty(destCassandra.getTable_name())) {
                        BuildInfo buildInfo = new BuildInfo(squid, map);
                        buildInfo.setMessageCode(MessageCode.ERR_DEST_CASSANDRA_TABLENAME_ISNULL.value());
                        buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                        buildInfoList.add(buildInfo);
                    }
                    if(!StringUtils.isEmpty(destCassandra.getTable_name())){
                        ISquidDao squidDao = new SquidDaoImpl(adapter);
                        CassandraConnectionSquid connectionSquid = squidDao.getSquidForCond(destCassandra.getDest_squid_id(), CassandraConnectionSquid.class);
                        if(connectionSquid!=null){
                            DBConnectionInfo dbs = CassandraAdapter.getCassandraConnection(connectionSquid);
                            INewDBAdapter cassandraAdapter = AdapterDataSourceManager.getAdapter(dbs);
                            List<BasicTableInfo> tableInfos = cassandraAdapter.getAllTables(destCassandra.getTable_name());
                            if(tableInfos==null||tableInfos.size()<=0){
                                BuildInfo buildInfo = new BuildInfo(squid, map);
                                buildInfo.setMessageCode(MessageCode.ERR_DEST_CASSANDRA_TABLENAME_ISEXIST.value());
                                buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                                buildInfoList.add(buildInfo);
                            }
                        }
                    }
                    //判断列是否设置
                    Map<String, Object> paramMap = new HashMap<>();
                    paramMap.put("squid_id", destCassandra.getId());
                    List<DestCassandraColumn> destCassandraColumns = adapter.query2List2(true, paramMap, DestCassandraColumn.class);
                    if (destCassandraColumns != null) {
                        for (DestCassandraColumn cassandraColumn : destCassandraColumns) {
                            if (cassandraColumn.getColumn_id() == 0&&cassandraColumn.getIs_primary_column()==1) {
                                BuildInfo buildInfo = new BuildInfo(squid, map);
                                buildInfo.setMessageCode(MessageCode.ERR_DEST_CASSANDRA_COLUMN_ISNOT_SET.value());
                                buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                                buildInfoList.add(buildInfo);
                                break;
                            }
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return buildInfoList;
    }

    /**
     * 增量抽取条件判断(设置增量抽取之后，增量抽取条件不能为空)
     * 范围:所有ExtractSquid
     * 判断数据格式为Delimited时，分隔符为空
     * 范围:DocExtractSquid
     * 判断数据格式为FixedLength时，字段长度为空
     * 范围:DocExtractSquid
     * 判断换行符不能为空
     * 范围:DocExtractSquid
     */
    public static List<BuildInfo> validateIncrementalCondition(List<BuildInfo> buildInfoList, Map<String, Object> map, Squid squid, IRelationalDataManager adapter) {
        logger.info("编译状态检查：增量抽取条件不能为空");
        Map<String, String> params = new HashMap<String, String>();
        params.put("ID", squid.getId() + "");
        Class<?> className = SquidTypeEnum.classOfValue(squid.getSquid_type());

        //判断增量
        if (SquidTypeEnum.isExtractBySquidType(squid.getSquid_type()) ) {
            DataSquid dataSquid = (DataSquid) adapter.query2Object(params, className);
            if (dataSquid != null && dataSquid.isIs_incremental()) {
                if (dataSquid.getIncremental_mode() == 0) {
                    BuildInfo buildInfo = new BuildInfo(squid, map);
                    buildInfo.setMessageCode(MessageCode.ERR_EXTRACT_INCREMENTAL_MODE_IS_NULL.value());
                    buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                    buildInfoList.add(buildInfo);
                }
                if (dataSquid.getCheck_column_id() == 0) {
                    BuildInfo buildInfo = new BuildInfo(squid, map);
                    buildInfo.setMessageCode(MessageCode.ERR_CHECK_COLUMN_IS_NULL.value());
                    buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                    buildInfoList.add(buildInfo);
                }
                //增量抽取时，判断检查列和最后值是否一致
                if(dataSquid.getCheck_column_id()!=0 && dataSquid.getLast_value()!=null && !"".equals(dataSquid.getLast_value())){
                    Map<String, String> columnParams = new HashMap<String, String>();
                    columnParams.put("COLUMN_ID", dataSquid.getCheck_column_id() + "");
                        //根据column_id 查询reference Column
                    ReferenceColumn referenceColumn=adapter.query2Object(columnParams,ReferenceColumn.class);
                    if(referenceColumn!=null){
                        //查询上游Squid
                        String sql = "select DB_TYPE_ID from ds_squid where id = (select from_squid_id from ds_squid_link where TO_SQUID_ID=" + dataSquid.getId() + ")";
                        Map<String,Object> resultMap = null;
                        try {
                            resultMap = adapter.query2Object(true,sql,null);
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                        boolean flag=checkType(Integer.parseInt(resultMap.get("DB_TYPE_ID")+""),referenceColumn.getData_type(), dataSquid.getLast_value());
                        if(!flag){
                            BuildInfo buildInfo = new BuildInfo(squid, map);
                            buildInfo.setMessageCode(MessageCode.ERR_INCREMENT_CHECK_COL_LASTVALUE.value());
                            buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                            buildInfoList.add(buildInfo);
                        }
                    }
                }




            }
            //判断合表时得数据是否正确
            if(dataSquid.getIsUnionTable()){
                //合表的表名设置方式不能为空
                if(dataSquid.getTableNameSettingType()==null){
                    BuildInfo buildInfo = new BuildInfo(squid, map);
                    buildInfo.setMessageCode(MessageCode.ERR_TABLENAME_SETTINGTYPE_IS_NULL.value());
                    buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                    buildInfoList.add(buildInfo);
                }
                //如果表名设置方式为sql,验证sql
                if(dataSquid.getTableNameSettingType()==1){
                    if(dataSquid.getTableNameSettingSql()!=null && !"".equals(dataSquid.getTableNameSettingSql())){
                        String error = ValidateJoinProcess.validateSqlIsIllegal(dataSquid.getTableNameSettingSql(), map);
                        if (error != null) {
                            BuildInfo buildInfo = new BuildInfo(squid, map);
                            buildInfo.setMessageCode(MessageCode.ERR_TABLENAME_SETTINGTYPE_SQL_IS_ERROR.value());
                            buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                            buildInfoList.add(buildInfo);
                        }
                    }else{
                        BuildInfo buildInfo = new BuildInfo(squid, map);
                        buildInfo.setMessageCode(MessageCode.ERR_TABLENAME_SETTINGTYPE_SQL_IS_NULL.value());
                        buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                        buildInfoList.add(buildInfo);
                    }
                }else if(dataSquid.getTableNameSettingType()==0){
                    if(dataSquid.getTableNameSetting()==null || "".equals(dataSquid.getTableNameSetting())){
                        BuildInfo buildInfo = new BuildInfo(squid, map);
                        buildInfo.setMessageCode(MessageCode.ERR_TABLENAME_SETTINGTYPE_JSON_IS_NULL.value());
                        buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                        buildInfoList.add(buildInfo);
                    }
                }

            }


            /*if (dataSquid != null && dataSquid.isIs_incremental() && StringUtils.isEmpty(dataSquid.getIncremental_expression())) {
                BuildInfo buildInfo = new BuildInfo(squid, map);
                buildInfo.setMessageCode(MessageCode.ERR_INCREMENTAL_NULL.value());
                buildInfo.setInfoType(SchedulerLogStatus.WARNING.getValue());
                buildInfoList.add(buildInfo);
            }*/
        }
        /*if (squid.getSquid_type() == DSObjectType.CASSANDRA_EXTRACT.value()) {
            DataSquid dataSquid = (DataSquid) adapter.query2Object(params, className);
            if (dataSquid != null && dataSquid.isIs_incremental()) {
                if (dataSquid.getIncremental_mode() == 0) {
                    BuildInfo buildInfo = new BuildInfo(squid, map);
                    buildInfo.setMessageCode(MessageCode.ERR_EXTRACT_INCREMENTAL_MODE_IS_NULL.value());
                    buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                    buildInfoList.add(buildInfo);
                }
                if (dataSquid.getCheck_column_id() == 0) {
                    BuildInfo buildInfo = new BuildInfo(squid, map);
                    buildInfo.setMessageCode(MessageCode.ERR_CHECK_COLUMN_IS_NULL.value());
                    buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                    buildInfoList.add(buildInfo);
                }
            }
        }*/
        if (SquidTypeEnum.DOC_EXTRACT.value() == squid.getSquid_type()) {
            DocExtractSquid docExtractSquid = (DocExtractSquid) adapter.query2Object(params, className);
            if (docExtractSquid.getRow_format() == 0 && (docExtractSquid.getDelimiter() == null || docExtractSquid.getDelimiter().length() == 0)) {
                BuildInfo buildInfo = new BuildInfo(squid, map);
                buildInfo.setMessageCode(MessageCode.ERR_DELIMITED_NULL.value());
                buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                buildInfoList.add(buildInfo);
            } else if (docExtractSquid.getRow_format() == 1 && (docExtractSquid.getField_length() <= 0)) {
                BuildInfo buildInfo = new BuildInfo(squid, map);
                buildInfo.setMessageCode(MessageCode.ERR_FIXEDLENGTH_null.value());
                buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                buildInfoList.add(buildInfo);
            }
            //换行符不能为空
            if (docExtractSquid.getRow_delimiter() == null || docExtractSquid.getRow_delimiter().length() == 0) {
                BuildInfo buildInfo = new BuildInfo(squid, map);
                buildInfo.setMessageCode(MessageCode.ERR_ROW_DELIMITED_NULL.value());
                buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                buildInfoList.add(buildInfo);
            }
        }
        //为xml_extract时，判断定义文件，元数据路径
        if (squid.getSquid_type() == SquidTypeEnum.XML_EXTRACT.value()) {
            DataSquid dataSquid = (DataSquid) adapter.query2Object(params, className);
            if (dataSquid.getXsd_dtd_file() == null || dataSquid.getXsd_dtd_file().length() == 0) {
                BuildInfo buildInfo = new BuildInfo(squid, map);
                buildInfo.setMessageCode(MessageCode.ERR_XMLEXTRACT_FILE_ISNULL.value());
                buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                buildInfoList.add(buildInfo);
            }
            if (dataSquid.getXsd_dtd_path() == null || dataSquid.getXsd_dtd_path().length() == 0) {
                BuildInfo buildInfo = new BuildInfo(squid, map);
                buildInfo.setMessageCode(MessageCode.ERR_XMLEXTRACT_FILEPATH_ISNULL.value());
                buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                buildInfoList.add(buildInfo);
            }
        }




        return buildInfoList;
    }

    /**
     * DataMinSquid 训练集比例小于50%
     */
    public static List<BuildInfo> validateDataMinTrain(List<BuildInfo> buildInfoList, Map<String, Object> map, Squid squid, IRelationalDataManager adapter) {
        logger.info("编译检查:检查训练集比例");
        Class<?> className = SquidTypeEnum.classOfValue(squid.getSquid_type());
        Map<String, String> params = new HashMap<String, String>();
        params.put("ID", squid.getId() + "");
        if ("DataMiningSquid".equals(className.getSimpleName())) {
            DataMiningSquid dataMiningSquid = (DataMiningSquid) adapter.query2Object(params, className);
            if (StringUtils.isNotNull(dataMiningSquid) &&dataMiningSquid.getSquid_type()!=SquidTypeEnum.NORMALIZER.value() && dataMiningSquid.getTraining_percentage() < 0.5) {
                BuildInfo buildInfo = new BuildInfo(squid, map);
                buildInfo.setMessageCode(MessageCode.ERR_TRAINPERC_LT.value());
                buildInfo.setInfoType(SchedulerLogStatus.WARNING.getValue());
                buildInfoList.add(buildInfo);
            }
        }
        return buildInfoList;
    }

    /**
     * 验证extractsquid的分片控制列是否为空
     * @param buildInfoList
     * @param map
     * @param squid
     * @param adapter
     * @return
     */
    public static List<BuildInfo> validateSpitCol(List<BuildInfo> buildInfoList, Map<String, Object> map, Squid squid, IRelationalDataManager adapter){
        logger.info("编译检查:校验分片控制列");
        //验证分片控制列是否为空
        if(SquidTypeEnum.EXTRACT.value()==squid.getSquid_type()){
            Map<String,Object> paramMap = new HashMap<>();
            paramMap.put("id",squid.getId());
            ExtractSquid extractSquid =  adapter.query2Object(true,paramMap,ExtractSquid.class);
            String splitCol = extractSquid.getSplit_col();
            if(StringUtils.isEmpty(splitCol) && extractSquid.getSplit_num()==0){
                BuildInfo buildInfo = new BuildInfo(squid, map);
                buildInfo.setMessageCode(MessageCode.ERR_SPLIT_COL_ISNULL.value());
                buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                buildInfoList.add(buildInfo);
            }
        }
        return buildInfoList;
    }

    /**
     * 校验trainfile和traindb是否允许使用
     * @param buildInfoList
     * @param map
     * @param squid
     * @param adapter
     * @return
     */
    public static List<BuildInfo> validateTrainFileorDBIsValid(List<BuildInfo> buildInfoList, Map<String, Object> map, Squid squid, IRelationalDataManager adapter) throws Exception {
        if(SquidTypeEnum.TRAININGDBSQUID.value() == squid.getSquid_type()
                || SquidTypeEnum.TRAINNINGFILESQUID.value() == squid.getSquid_type()){
            //查询出用户，查询出该用户所拥有的课程，判断当前tranfile和traindb是否有权限使用
            int repositoryId = Integer.parseInt(map.get("RepositoryId") + "");
            String sql = "select * from ds_sys_repository where id="+repositoryId;
            try {
                Repository repository = adapter.query2Object(true,sql,null,Repository.class);
                int userId = repository.getTeam_id();
                //查找出当前用户所拥有的课程
                JdbcTemplate cloudTemplate = (JdbcTemplate) Constants.context.getBean("cloudTemplate");
                sql = "select id from cloud_user where user_type=3";
                List<Map<String,Object>> userList = cloudTemplate.queryForList(sql);
                Map<String,Object> managerMap = userList.get(0);
                int managerId = Integer.parseInt(managerMap.get("id")+"");
                if(managerId == userId){
                    return buildInfoList;
                }
                sql = "select c.id from course c,course_user_collection cuc,cloud_user cu where c.id = cuc.courseId and cuc.userId = cu.id and cuc.userId="+userId;
                List<Map<String,Object>> courseList = cloudTemplate.queryForList(sql);
                ISquidDao squidDao = new SquidDaoImpl(adapter);
                if(courseList.size()>0){
                    //截取课程id
                    if(SquidTypeEnum.TRAINNINGFILESQUID.value() == squid.getSquid_type()){
                        TrainingFileSquid fileSquid = squidDao.getSquidForCond(squid.getId(),TrainingFileSquid.class);
                        String path = fileSquid.getFile_path();
                        if(path != null){
                            int courseId = Integer.parseInt(path.substring(path.lastIndexOf("c")+1));
                            boolean isValid = false;
                            for(Map<String,Object> courseMap : courseList){
                                if(Integer.parseInt(courseMap.get("id")+"")==courseId){
                                    isValid = true;
                                    break;
                                }
                            }
                            if(!isValid){
                                BuildInfo buildInfo = new BuildInfo(squid, map);
                                buildInfo.setMessageCode(MessageCode.ERR_TRAINSQUID_IS_VALID.value());
                                buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                                buildInfoList.add(buildInfo);
                            }
                        }
                    } else if(SquidTypeEnum.TRAININGDBSQUID.value() == squid.getSquid_type()){
                        TrainingDBSquid dbSquid = squidDao.getSquidForCond(squid.getId(),TrainingDBSquid.class);
                        String dbName = dbSquid.getDb_name();
                        if(dbName!=null){
                            int courseId = Integer.parseInt(dbName.substring(dbName.lastIndexOf("c")+1));
                            boolean isValid = false;
                            for(Map<String,Object> courseMap : courseList){
                                if(Integer.parseInt(courseMap.get("id")+"")==courseId){
                                    isValid = true;
                                    break;
                                }
                            }
                            if(!isValid){
                                BuildInfo buildInfo = new BuildInfo(squid, map);
                                buildInfo.setMessageCode(MessageCode.ERR_TRAINSQUID_IS_VALID.value());
                                buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                                buildInfoList.add(buildInfo);
                            }
                        }
                    }
                } else {
                    BuildInfo buildInfo = new BuildInfo(squid, map);
                    buildInfo.setMessageCode(MessageCode.ERR_TRAINSQUID_IS_VALID.value());
                    buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                    buildInfoList.add(buildInfo);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return buildInfoList;

    }
    /**
     * 验证增量抽取的时间类型是否正确
     * @param buildInfoList
     * @param map
     * @param squid
     * @param adapter
     * @return
     */
    public static List<BuildInfo> validateTimeFormat(List<BuildInfo> buildInfoList, Map<String, Object> map, Squid squid, IRelationalDataManager adapter){
        if(SquidTypeEnum.EXTRACT.value()==squid.getSquid_type()){
            Map<String,Object> paramMap = new HashMap<>();
            paramMap.put("id",squid.getId());
            ExtractSquid extractSquid =  adapter.query2Object(true,paramMap,ExtractSquid.class);
            if(extractSquid.isIs_incremental()) {
                String lastValue = extractSquid.getLast_value();
                //最后修改时间，数据库为sqlserver时，且类型为timestamp
                boolean isValid = validateCheckColIsValid(extractSquid,adapter,map,squid,buildInfoList);
                if(!isValid){
                    return buildInfoList;
                }

                if (StringUtils.isNotNull(lastValue)) {
                    //最后修改时间
                    if (extractSquid.getIncremental_mode() == 2) {
                        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        try {
                            format.parse(lastValue);
                        } catch (ParseException e) {
                            e.printStackTrace();
                            BuildInfo buildInfo = new BuildInfo(squid, map);
                            buildInfo.setMessageCode(MessageCode.ERR_INCREMENT_TIMEFORMAT_ERROR.value());
                            buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                            buildInfoList.add(buildInfo);
                        }
                    } else if (extractSquid.getIncremental_mode() == 1) {
                        int souceColumnId = extractSquid.getCheck_column_id();
                        String sql = "select data_type from ds_source_column where id=" + souceColumnId;
                        try {
                            Map<String, Object> dataTypeMap = adapter.query2Object(true, sql, null);
                            int data_type = StringUtils.isEmpty(dataTypeMap.get("DATA_TYPE") + "") ? 0 : Integer.parseInt(dataTypeMap.get("DATA_TYPE") + "");
                            if(data_type == DbBaseDatatype.TIMESTAMP.value()){
                                sql = "select DB_TYPE_ID from ds_squid where id = (select from_squid_id from ds_squid_link where TO_SQUID_ID="+extractSquid.getId()+")";
                                Map<String,Object> dbMap = adapter.query2Object(true,sql,null);
                                if(dbMap!=null){
                                    int dbType = Integer.parseInt(dbMap.get("DB_TYPE_ID")+"");
                                    if(dbType == DataBaseType.SQLSERVER.value()){
                                        return buildInfoList;
                                    }
                                }
                            }
                            String format = DbBaseDatatype.isTimeTypeToFormat(DbBaseDatatype.parse(data_type));
                            if (StringUtils.isNotNull(format)) {
                                SimpleDateFormat dateFormat = new SimpleDateFormat(format);
                                try {
                                    dateFormat.parse(lastValue);
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                    BuildInfo buildInfo = new BuildInfo(squid, map);
                                    buildInfo.setMessageCode(MessageCode.ERR_INCREMENT_TIMEFORMAT_ERROR.value());
                                    buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                                    buildInfoList.add(buildInfo);
                                }
                            }
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
        return buildInfoList;
    }

    public static boolean validateCheckColIsValid(ExtractSquid extractSquid,IRelationalDataManager adapter,Map<String, Object> map, Squid squid,List<BuildInfo> buildInfoList){
        boolean isValid = true;
        int souceColumnId = extractSquid.getCheck_column_id();
        String sql = "select data_type from ds_source_column where id=" + souceColumnId;
        try {
            Map<String, Object> vMap = adapter.query2Object(true, sql, null);
            int data_type = StringUtils.isEmpty(vMap.get("DATA_TYPE") + "") ? 0 : Integer.parseInt(vMap.get("DATA_TYPE") + "");
            sql = "select DB_TYPE_ID from ds_squid where id = (select from_squid_id from ds_squid_link where TO_SQUID_ID=" + extractSquid.getId() + ")";
            Map<String, Object> dbMap = adapter.query2Object(true, sql, null);
            int dbType = 0;
            if(dbMap!=null){
                dbType = Integer.parseInt(dbMap.get("DB_TYPE_ID") + "");
            }
            if(extractSquid.getIncremental_mode()==2) {
                //查找出上游的抽取squid中，db的类型
                if (data_type == DbBaseDatatype.TIMESTAMP.value() && dbType == DataBaseType.SQLSERVER.value()) {
                    isValid = false;
                    BuildInfo buildInfo = new BuildInfo(squid, map);
                    buildInfo.setMessageCode(MessageCode.ERR_INCREMENT_CHECK_COL_IS_VALID.value());
                    buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                    buildInfoList.add(buildInfo);
                }
            }


            switch (DbBaseDatatype.parse(data_type)){
                case IMAGE:
                case XML:
                case BINARY:
                case BFILE:
                case TEXT:
                case NCLOB:
                case BLOB:
                case LONGBLOB:
                case MEDIUMBLOB:
                case TINYBLOB:
                case CLOB:
                case RAW:
                case ROWID:
                case SDO_GEOMETRY:
                case SDO_TOPO_GEOMETRY:
                case UROWID:
                case XDBURITYPE:
                case DBURITYPE:
                case SDO_GEORASTER:
                case URITYPE:
                case XMLTYPE:
                case LONG:
                case GRAPHIC:
                case GEOGRAPHY:
                case DBCLOB:
                case GEOMETRY:
                case HIERARCHYID:
                case CHARFORBITDATA:
                case VARCHARFORBITDATA:
                case BIT:
                    isValid = false;
                    BuildInfo buildInfo = new BuildInfo(squid, map);
                    buildInfo.setMessageCode(MessageCode.ERR_INCREMENT_CHECK_COL_IS_VALID.value());
                    buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                    buildInfoList.add(buildInfo);
                    break;
                case BYTE:
                    if(dbType == DataBaseType.TERADATA.value()){
                        isValid = false;
                        BuildInfo info = new BuildInfo(squid, map);
                        info.setMessageCode(MessageCode.ERR_INCREMENT_CHECK_COL_IS_VALID.value());
                        info.setInfoType(SchedulerLogStatus.ERROR.getValue());
                        buildInfoList.add(info);
                    }
                    break;
            }

        } catch (Exception e){
            e.printStackTrace();
        }
        return isValid;
    }
    /**
     * 验证squid是否有效
     *
     * @param buildInfoList
     * @param map
     * @param squid
     * @param adapter
     * @return
     */
    public static List<BuildInfo> validateSquidIsAvail(List<BuildInfo> buildInfoList, Map<String, Object> map, Squid squid, IRelationalDataManager adapter) {

        if (squid.getDesign_status() == 1) {
            BuildInfo buildInfo = new BuildInfo(squid, map);
            buildInfo.setMessageCode(MessageCode.NOT_AVAIL_SQUID.value());
            buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
            buildInfoList.add(buildInfo);
        }
        return buildInfoList;
    }
    /**
     * 验证squid 下游连接的如果是samplingSquid 总的抽取百分比不能大于100
     */
    public static List<BuildInfo> validateToSquidSamplingSquidPercent(boolean isConnect, List<BuildInfo> buildInfoList, Map<String, Object> map, Squid squid, IRelationalDataManager adapter) throws Exception {
        try {
            if(squid!=null){
                ISquidDao squidDao = new SquidDaoImpl(adapter);
                //查询下游的squid
                List<Squid> squids=squidDao.getNextSquidsForSquidID(squid.getId(),Squid.class);
                List<Integer> sampSquidIds=new ArrayList<>();
                if(squids!=null && squids.size()>0){
                    for(Squid squid1:squids){
                        if(squid1.getSquid_type()==SquidTypeEnum.SAMPLINGSQUID.value()){
                            sampSquidIds.add(squid1.getId());
                        }
                    }
                    if(sampSquidIds!=null && sampSquidIds.size()>0){
                        String ids="";
                        for(Integer sampid:sampSquidIds){
                            ids+=sampid+",";
                        }
                        String sql="select sum(sampling_percent)as PERCENT from ds_squid where id in ";
                        ids=ids.substring(0,ids.length()-1);
                        sql+="("+ids+")";
                        Map<String,Object> map1=adapter.query2Object(true,sql,new ArrayList<>());
                        if(map1!=null && map1.size()>0){
                            Double percent=Double.valueOf(map1.get("PERCENT").toString());
                            if(percent>1.0){
                                BuildInfo buildInfo =new BuildInfo(squid,map);
                                buildInfo.setMessageCode(MessageCode.ERR_SAMPLINGSQUID_SumPercent_Greater100.value());
                                buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                                buildInfoList.add(buildInfo);
                            }
                        }
                    }
                }

            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return buildInfoList;
    }

    /**
     * 验证pivotSquid各类信息是否合理
     */
    public static List<BuildInfo> validateToSquidPivotSquid(boolean isConnect, List<BuildInfo> buildInfoList, Map<String, Object> map, Squid squid, IRelationalDataManager adapter) throws Exception {
        if((int)squid.getSquid_type() == (int) SquidTypeEnum.PIVOTSQUID.value()){
            Integer pivotSquidId = squid.getId();
            Map<String,Object> paramMap = new HashMap<>();
            paramMap.put("id",pivotSquidId);
            PivotSquid pivotSquid =  adapter.query2Object(true,paramMap,PivotSquid.class);
            Integer pivotColumnId = pivotSquid.getPivotColumnId();
            Integer valueColumnId = pivotSquid.getValueColumnId();
            String groupByColumnIds = pivotSquid.getGroupByColumnIds();
            Integer agreeType = pivotSquid.getAggregationType();
            String pivotValueStr = pivotSquid.getPivotColumnValue();
            List<String> pivotValue = JSONArray.parseArray(pivotValueStr,String.class);
            //验证上游column是否为空
            SquidLinkDao squidLinkDao = BeanUtil.getBean("squidLinkDao");

            com.eurlanda.datashire.server.model.SquidLink squidLink =  squidLinkDao.getSquidLinkListByToSquidId(pivotSquidId);
            if(squidLink ==  null){
                BuildInfo buildInfo = new BuildInfo(squid,map);
                buildInfo.setMessageCode(MessageCode.ERR_PIVOTSQUID_REFERENCE_COLUMN_IS_NULL.value());
                buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                buildInfoList.add(buildInfo);
                logger.error("ERR_PIVOTSQUID_REFERENCE_COLUMN_IS_NULL");
                return buildInfoList;
            }
            ColumnDao columnDao = BeanUtil.getBean("columnDao");
            List<Column> columns = columnDao.selectColumnBySquid_Id(squidLink.getFrom_squid_id());
            if(columns.size() < 1){
                BuildInfo buildInfo = new BuildInfo(squid,map);
                buildInfo.setMessageCode(MessageCode.ERR_PIVOTSQUID_REFERENCE_COLUMN_IS_NULL.value());
                buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                buildInfoList.add(buildInfo);
                logger.error("ERR_PIVOTSQUID_REFERENCE_COLUMN_IS_NULL");
                return buildInfoList;
            }
            //数据不完整
            if(agreeType == null|| agreeType.intValue() < 1 || agreeType.intValue() > 7){
                BuildInfo buildInfo =new BuildInfo(squid,map);
                buildInfo.setMessageCode(MessageCode.ERR_PIVOTSQUID_DATA_DEFECT_MSG.value());
                buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                buildInfoList.add(buildInfo);
                logger.error("ERR_PIVOTSQUID_DATA_DEFECT_MSG");
                return buildInfoList;
            }
            //分组列为空
            if(groupByColumnIds == null || "".equals(groupByColumnIds)){
                BuildInfo buildInfo =new BuildInfo(squid,map);
                buildInfo.setMessageCode(MessageCode.ERR_PIVOTSQUID_GROUP_BY_IDS_IS_NULL.value());
                buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                buildInfoList.add(buildInfo);
                logger.error("ERR_PIVOTSQUID_GROUP_BY_IDS_IS_NULL");
                return buildInfoList;
            }
            //pivotId为空
            if(pivotColumnId == null || (int)pivotColumnId == 0){
                BuildInfo buildInfo =new BuildInfo(squid,map);
                buildInfo.setMessageCode(MessageCode.ERR_PIVOTSQUID_PIVOT_COLUMN_ID_IS_NULL.value());
                buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                buildInfoList.add(buildInfo);
                logger.error("ERR_PIVOTSQUID_PIVOT_COLUMN_ID_IS_NULL");
                return buildInfoList;
            }
            //valueId为空
            if(valueColumnId == null || (int)valueColumnId == 0){
                BuildInfo buildInfo =new BuildInfo(squid,map);
                buildInfo.setMessageCode(MessageCode.ERR_PIVOTSQUID_VALUE_COLUMN_ID_IS_NULL.value());
                buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                buildInfoList.add(buildInfo);
                logger.error("ERR_PIVOTSQUID_VALUE_COLUMN_ID_IS_NULL");
                return buildInfoList;
            }
            //pivot列值为空
            if(pivotValue == null || pivotValue.size() < 1){
                BuildInfo buildInfo =new BuildInfo(squid,map);
                buildInfo.setMessageCode(MessageCode.ERR_PIVOTSQUID_PIVOT_VALUES_IS_NULL.value());
                buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                buildInfoList.add(buildInfo);
                logger.error("ERR_PIVOTSQUID_PIVOT_VALUES_IS_NULL");
                return buildInfoList;
            }

            //校验三种id是否有相同的
            if((""+valueColumnId).equals(pivotColumnId+"")){
                BuildInfo buildInfo =new BuildInfo(squid,map);
                buildInfo.setMessageCode(MessageCode.ERR_PIVOTSQUID_PIVOTCOLUMNID_EQUALS_VALUECOLUMNID.value());
                buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                buildInfoList.add(buildInfo);
                logger.error("ERR_PIVOTSQUID_PIVOTCOLUMNID_EQUALS_VALUECOLUMNID");
                return buildInfoList;
            }


            String[] groupByIdsList = groupByColumnIds.split(",");
            //校验生成的pivot列值与生成的column是否一致

            for (String groupId : groupByIdsList){
                if(groupId.equals("")){
                    BuildInfo buildInfo =new BuildInfo(squid,map);
                    buildInfo.setMessageCode(MessageCode.ERR_PIVOTSQUID_GROUP_BY_IDS_IS_NULL.value());
                    buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                    buildInfoList.add(buildInfo);
                    logger.error("ERR_PIVOTSQUID_GROUP_BY_IDS_IS_NULL");
                    return buildInfoList;
                }else {
                    if(groupId.equals(pivotColumnId+"")){
                        BuildInfo buildInfo =new BuildInfo(squid,map);
                        buildInfo.setMessageCode(MessageCode.ERR_PIVOTSQUID_GROUPBYIDS_CONTAIN_PIVOTCOLUMNID.value());
                        buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                        buildInfoList.add(buildInfo);
                        logger.error("ERR_PIVOTSQUID_GROUPBYIDS_CONTAIN_PIVOTCOLUMNID");
                        return buildInfoList;
                    }else if(groupId.equals(valueColumnId+"")){
                        BuildInfo buildInfo =new BuildInfo(squid,map);
                        buildInfo.setMessageCode(MessageCode.ERR_PIVOTSQUID_GROUPBYIDS_CONTAIN_VALUECOLUMNID.value());
                        buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                        buildInfoList.add(buildInfo);
                        logger.error("ERR_PIVOTSQUID_GROUPBYIDS_CONTAIN_VALUECOLUMNID");
                        return buildInfoList;
                    }
                }
            }
            //校验聚合方式与value列值是否兼容
            PivotSquidService pivotSquidService = BeanUtil.getBean("pivotSquidService");
            Map<String,Object> resultMapPivot = pivotSquidService.validatePivotValueIsCompatible(pivotSquid);
            if(!(boolean)resultMapPivot.get("flag")){
                BuildInfo buildInfo =new BuildInfo(squid,map);
                if((""+resultMapPivot.get("errorType")).equals("2")){
                    buildInfo.setMessageCode(MessageCode.ERR_PIVOTSQUID_PIVOT_COLUMN_ID_IS_NULL.value());
                    logger.error("ERR_PIVOTSQUID_PIVOT_COLUMN_ID_IS_NULL");
                }else {
                    buildInfo.setMessageCode(MessageCode.ERR_PIVOTSQUID_PIVOTCOLUMN_INCOMPATIBLE_MSG.value());
                    logger.error("ERR_PIVOTSQUID_PIVOTCOLUMN_INCOMPATIBLE_MSG");
                }
                buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                buildInfoList.add(buildInfo);
                return buildInfoList;
            }
            Map<String,Object> resultMap = pivotSquidService.validateAggregationTypeIsCompatibleValueType(pivotSquid);
            if(!(boolean)resultMap.get("flag")){
                BuildInfo buildInfo =new BuildInfo(squid,map);
                if("3".equals(resultMap.get("errorType")+"")){//value列为空
                    buildInfo.setMessageCode(MessageCode.ERR_PIVOTSQUID_VALUE_COLUMN_ID_IS_NULL.value());
                    logger.error("ERR_PIVOTSQUID_VALUE_COLUMN_ID_IS_NULL");
                }else {
                    buildInfo.setMessageCode(MessageCode.ERR_PIVOTSQUID_VALUECOLUMN_INCOMPATIBLE_MSG.value());
                    logger.error("ERR_PIVOTSQUID_VALUECOLUMN_INCOMPATIBLE_MSG");
                }

                buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                buildInfoList.add(buildInfo);
                return buildInfoList;
            }
            Map<String,Object> resultMap1 = pivotSquidService.validateColumnAndAgreeTypeIsCompatible(pivotSquid);
            if(!(boolean)resultMap1.get("flag")){
                BuildInfo buildInfo =new BuildInfo(squid,map);
                buildInfo.setMessageCode(MessageCode.ERR_PIVOTSQUID_VALUECOLUMN_INCOMPATIBLE_MSG.value());
                buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                buildInfoList.add(buildInfo);
                logger.error("ERR_PIVOTSQUID_VALUECOLUMN_INCOMPATIBLE_MSG");
                return buildInfoList;
            }
            Map<String,Object> resultMap2 = pivotSquidService.validateColumnAndGroupIdIsAgreement(pivotSquid);
            if(!(boolean)resultMap2.get("flag")){
                BuildInfo buildInfo =new BuildInfo(squid,map);
                buildInfo.setMessageCode(MessageCode.ERR_PIVOTSQUID_CHANGE_GROUP_BY_ID_IS_NOT_EQUALS_CREATE_COLUMN.value());
                buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                buildInfoList.add(buildInfo);
                logger.error("ERR_PIVOTSQUID_CHANGE_GROUP_BY_ID_IS_NOT_EQUALS_CREATE_COLUMN");
                return buildInfoList;
            }
            Map<String,Object> resultMap3 = pivotSquidService.validateValueColumnIsChange(pivotSquid);
            if(!(boolean)resultMap3.get("flag")){
                BuildInfo buildInfo =new BuildInfo(squid,map);
                buildInfo.setMessageCode(MessageCode.ERR_PIVOTSQUID_PIVOT__VALUE_COLUMN_IS_NOT_ACCORDANCE.value());
                buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                buildInfoList.add(buildInfo);
                logger.error("ERR_PIVOTSQUID_PIVOT__VALUE_COLUMN_IS_NOT_ACCORDANCE");
                return buildInfoList;
            }
        }
        return buildInfoList;
    }


    public  static boolean checkType(int dbType,Integer datatype,String value){
            boolean flag=true;
            DbBaseDatatype dbBaseDatatype=DbBaseDatatype.parse(datatype);
            DataBaseType dataBaseType=DataBaseType.parse(dbType);
            try{
                String format = "yyyy-MM-dd HH:mm:ss";
                if((dbBaseDatatype==DbBaseDatatype.DATE && dataBaseType!=DataBaseType.ORACLE)
                        || dbBaseDatatype == DbBaseDatatype.YEAR){
                    format = "yyyy-MM-dd";
                }

                DateFormat sdf = new SimpleDateFormat(format);
                switch (dbBaseDatatype){
                    case BIGINT:
                    case COUNTER:
                        BigDecimal decimal = new BigDecimal(value);
                        int i = decimal.scale();
                        if(i!=0){
                            throw new Exception();
                        }
                        //long longValue=Long.valueOf(value);
                        break;
                    case INT:
                    case TINYINT:
                    case INTEGER:
                    case SMALLINT:
                    case MEDIUMINT:
                    case UNSIGNED_INT:
                    case UNSIGNED_TINYINT:
                    case UNSIGNED_SMALLINT:
                    case BYTE:
                    case BYTEINT:
                       Integer intValue=Integer.parseInt(value);
                        break;
                    case DECIMAL:
                    case DOUBLE:
                    case MONEY:
                    case FLOAT:
                    case NUMERIC:
                    case REAL:
                    case SMALLMONEY:
                    case NUMBER:
                    case BINARY_DOUBLE:
                    case BINARY_FLOAT:
                    case UNSIGNED_FLOAT:
                    case UNSIGNED_DOUBLE:
                    case DECFLOAT:
                    case SMALLDECIMAL:
                    case VARINT:
                        BigDecimal doubleValue=BigDecimal.valueOf(Double.valueOf(value));
                        break;
                    case VARCHAR:
                    case VARCHAR2:
                    case NVARCHAR:
                    case NCHAR:
                    case CHAR:
                    case NTEXT:
                    case UNIQUEIDENTIFIER:
                    case LONGRAW:
                    case ROWVERSION:
                    case GEOMETRY:
                    case GEOGRAPHY:
                    case TINYTEXT:
                    case MEDIUMTEXT:
                    case LONGTEXT:
                    case ENUM:
                    case SET:
                    case CHARACTER:
                    case DBCLOB:
                    case VARGRAPHIC:
                    case GRAPHIC:
                    case SHORTTEXT:
                    case BINTEXT:
                    case ALPHANUM:
                    case STRING:
                    case ASCII:
                    case UUID:
                    case TIMEUUID:
                    case VARBYTE:
                        String strValue=String.valueOf(value);
                        break;
                    case DATETIME:
                    case TIME:
                    case UNSIGNED_TIME:
                        Time btime=new Time(sdf.parse(value).getTime());
                        break;
                    case INTERVAL:
                    case YEAR:
                    case DATE:
                    case DATETIME2:
                    case UNSIGNED_DATE:
                    case SECONDDATE:
                        Date date=sdf.parse(value);
                        break;
                    case BIT:
                    case BOOLEAN:
                        Boolean bool=Boolean.parseBoolean(value);
                        break;
                    case CSN:
                    case ARRAY:
                    case LIST:
                        JsonUtil.toGsonList(value,Object.class);
                        break;
                        //数组
                    case UNSIGNED_TIMESTAMP:
                        Timestamp tss=Timestamp.valueOf(value);
                        break;
                    case TIMESTAMP:
                        if(dataBaseType.value()==DataBaseType.SQLSERVER.value()){
                            Integer intValu=Integer.parseInt(value);
                        }else{
                            Timestamp ts=Timestamp.valueOf(value);
                        }
                        break;
                    case MAP:
                    case STRUCT:
                        JsonUtil.toHashMap(value);
                        break;
                }

            }catch (Exception e){
                flag=false;
            }
            return flag;
    }

}