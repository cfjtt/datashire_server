package com.eurlanda.datashire.server.utils.dbsource;

import com.eurlanda.datashire.server.utils.dbsource.AbsDBAdapter;
import com.eurlanda.datashire.server.utils.dbsource.INewDBAdapter;
import com.eurlanda.datashire.server.utils.entity.DBConnectionInfo;
import com.eurlanda.datashire.server.utils.entity.operation.BasicTableInfo;
import com.eurlanda.datashire.server.utils.entity.operation.ColumnInfo;
import com.eurlanda.datashire.enumeration.DataBaseType;
import com.eurlanda.datashire.enumeration.datatype.DbBaseDatatype;
import com.eurlanda.datashire.server.utils.entity.operation.ColumnInfo;
import com.eurlanda.datashire.utility.FilterUtil;
import com.eurlanda.datashire.utility.StringUtils;
import com.eurlanda.datashire.utility.SysConf;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * hive for adapter
 * Created by Eurlanda on 2017/3/22.
 */
public class HiveAdapter extends AbsDBAdapter implements INewDBAdapter {

    HiveConf hiveConf;
    HiveMetaStoreClient client;

    public HiveAdapter(DBConnectionInfo info) throws MetaException {
        super(info);
        try {
            hiveConf = new HiveConf();
            client = new HiveMetaStoreClient(hiveConf);
        } catch (MetaException e) {
            e.printStackTrace();
            throw e;
        } catch (TException e) {
            e.printStackTrace();
            throw e;
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    /**
     * 初始化 hive连接
     *
     * @return
     */
    public static DBConnectionInfo getHiveConnection() {
        DBConnectionInfo dbInfo = new DBConnectionInfo();
        dbInfo.setDbType(DataBaseType.HIVE);
        dbInfo.setHost(SysConf.getValue("hive.host") + ":" + SysConf.getValue("hive.port"));
        dbInfo.setPort(Integer.parseInt(SysConf.getValue("hive.port")));
        dbInfo.setUserName(SysConf.getValue("hive.username"));
        dbInfo.setPassword(SysConf.getValue("hive.password"));
        return dbInfo;
    }

    /**
     * 获取所有表的信息
     *
     * @param filter
     * @param schema
     * @return
     */
    @Override
    public List<BasicTableInfo> getAllTables(String filter, String schema) throws Exception {
        ResultSet result = null;
        List<BasicTableInfo> tables = new ArrayList<>();
        try {
            String sql = "show tables";
            List<String> tableNames = client.getAllTables(schema);
            if (tableNames != null) {
                FilterUtil filterUtil = new FilterUtil(filter);
                for (String tableName : tableNames) {
                    if (filterUtil.check(tableName)) {
                        BasicTableInfo basicTableInfo = new BasicTableInfo();
                        basicTableInfo.setTableName(tableName);
                        tables.add(basicTableInfo);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("获取hive表异常:" + e);
            throw e;
        } finally {
            try {
                if (result != null) {
                    result.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return tables;
    }

    /**
     * 获取表中列的信息
     *
     * @param tableName    表明
     * @param DatabaseName
     * @return
     */
    @Override
    public List<ColumnInfo> getTableColumns(String tableName, String DatabaseName) throws TException {
        List<ColumnInfo> columnInfoList = new ArrayList<>();
        Map<String, String> partitionList = new HashMap<>();
        ResultSet rs = null;
        try {
            Table table = client.getTable(DatabaseName, tableName);
            List<FieldSchema> cols = table.getSd().getCols();
            if (cols != null) {
                for (int i = 0; i < cols.size(); i++) {
                    FieldSchema colSchema = cols.get(i);
                    ColumnInfo columnInfo = new ColumnInfo();
                    columnInfo.setOrderNumber(i + 1);
                    columnInfo.setName(colSchema.getName());
                    String colType = colSchema.getType();
                    String regex="([A-Za-z0-9]+)(\\([0-9]+,?[0-9]*\\)).*";
                    Pattern pattern = Pattern.compile(regex);
                    Matcher matcher = pattern.matcher(colType);
                    int length = 0;
                    int presion = 0;
                    int scale = 0;
                    if(matcher.find()){
                        colType = matcher.group(1);
                        String[] lengths = matcher.group(2).replaceAll("\\(|\\)","").split(",");
                        if(lengths.length==1){
                            length=lengths[0].matches("[0-9]+")?Integer.parseInt(lengths[0]):0;
                            presion=0;
                            scale=0;
                        } else {
                            presion=lengths[0].matches("[0-9]+")?Integer.parseInt(lengths[0]):0;
                            scale=lengths[1].matches("[0-9]+")?Integer.parseInt(lengths[1]):0;
                        }

                    } else {
                        regex="([a-zA-Z0-9]+)<.*>.*";
                        pattern = Pattern.compile(regex);
                        matcher = pattern.matcher(colType);
                        if(matcher.find()){
                            colType=matcher.group(1);
                        }
                    }
                    columnInfo.setLength(length);
                    columnInfo.setTypeName(colType);
                    columnInfo.setDbBaseDatatype(DbBaseDatatype.parse(colType));
                    columnInfo.setTableName(tableName);
                    columnInfo.setPrecision(presion);
                    columnInfo.setPartition(0);
                    columnInfo.setScale(scale);
                    columnInfo.setNullable(true);
                    columnInfoList.add(columnInfo);
                }
            }
            List<FieldSchema> partitionKeys = table.getPartitionKeys();
            if (partitionKeys != null) {
                for (int i = 0; i < partitionKeys.size(); i++) {
                    FieldSchema colSchema = partitionKeys.get(i);
                    ColumnInfo columnInfo = new ColumnInfo();
                    columnInfo.setOrderNumber(columnInfoList.size() + 1);
                    columnInfo.setName(colSchema.getName());
                    String colType = colSchema.getType();
                    String regex="([A-Za-z0-9]+)(\\([0-9]+,?[0-9]*\\)).*";
                    Pattern pattern = Pattern.compile(regex);
                    Matcher matcher = pattern.matcher(colType);
                    int length = 0;
                    int presion = 0;
                    int scale = 0;
                    if(matcher.find()){
                        colType = matcher.group(1);
                        String[] lengths = matcher.group(2).replaceAll("\\(|\\)","").split(",");
                        if(lengths.length==1){
                            length=lengths[0].matches("[0-9]+")?Integer.parseInt(lengths[0]):0;
                            presion=0;
                            scale=0;
                        } else {
                            presion=lengths[0].matches("[0-9]+")?Integer.parseInt(lengths[0]):0;
                            scale=lengths[1].matches("[0-9]+")?Integer.parseInt(lengths[1]):0;
                        }

                    } else {
                        regex="([a-zA-Z0-9]+)<.*>.*";
                        pattern = Pattern.compile(regex);
                        matcher = pattern.matcher(colType);
                        if(matcher.find()){
                            colType=matcher.group(1);
                        }
                    }
                    columnInfo.setLength(length);
                    columnInfo.setTypeName(colType);
                    columnInfo.setDbBaseDatatype(DbBaseDatatype.parse(colType));
                    columnInfo.setTableName(tableName);
                    columnInfo.setPrecision(presion);
                    columnInfo.setPartition(1);
                    columnInfo.setScale(scale);
                    columnInfo.setNullable(true);
                    columnInfoList.add(columnInfo);
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return columnInfoList;
    }

    public List<ColumnInfo> getPartitionsColumn(String tableName, String DatabaseName) {
        String sql = "show partitions " + tableName;
        List<ColumnInfo> columnInfoList = new ArrayList<>();
        ResultSet rs = null;
        try {
            Statement state = this.connection.createStatement();
            rs = state.executeQuery(sql);
            while (rs.next()) {
                String columnName = rs.getString(1);
                String colType = rs.getString(2);
                if (StringUtils.isNull(columnName) || columnName.indexOf("#") == 0) {
                    continue;
                }
                ColumnInfo column = new ColumnInfo();
                column.setTableName(tableName);
                column.setName(columnName);
                String typeName = colType;
                column.setDbBaseDatatype(DbBaseDatatype.parse(typeName));
                column.setTypeName(typeName);
                columnInfoList.add(column);
            }
        } catch (SQLException e) {
            logger.error("获取hive partition列信息失败");
            e.printStackTrace();
        } catch (Exception ex) {
            if (ex instanceof ParseException) {

            }
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return columnInfoList;
    }

    @Override
    public void close() {
        if (client != null) {
            client.close();
        }
    }
}
