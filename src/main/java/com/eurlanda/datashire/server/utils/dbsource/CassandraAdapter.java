package com.eurlanda.datashire.server.utils.dbsource;

import com.datastax.driver.core.*;
import com.eurlanda.datashire.entity.CassandraConnectionSquid;
import com.eurlanda.datashire.entity.DBSourceTable;
import com.eurlanda.datashire.enumeration.DataBaseType;
import com.eurlanda.datashire.enumeration.datatype.DbBaseDatatype;
import com.eurlanda.datashire.server.utils.entity.DBConnectionInfo;
import com.eurlanda.datashire.server.utils.entity.operation.BasicTableInfo;
import com.eurlanda.datashire.server.utils.entity.operation.ColumnInfo;
import com.eurlanda.datashire.utility.FilterUtil;
import com.eurlanda.datashire.utility.StringUtils;

import java.util.*;


/**
 * Created by Eurlanda on 2017/4/10.
 */
public class CassandraAdapter extends AbsDBAdapter implements INewDBAdapter {
    private Cluster cluster;
    private Session session;
    private DBConnectionInfo info;

    public Cluster getCluster() {
        return cluster;
    }

    public void setCluster(Cluster cluster) {
        this.cluster = cluster;
    }

    public Session getSession() {
        return session;
    }

    public void setSession(Session session) {
        this.session = session;
    }

    public CassandraAdapter(DBConnectionInfo info) {
        this.info = info;
        initClusterAndSession(info.getHost().split(","), info.getPort(), info.getVerification(), info.getUserName(), info.getPassword(), info.getKeyspace(), info.getCluster());
    }

    public static DBConnectionInfo getCassandraConnection(CassandraConnectionSquid connectSquid) {
        DBConnectionInfo dbInfo = new DBConnectionInfo();
        dbInfo.setDbType(DataBaseType.CASSANDRA);
        dbInfo.setHost(connectSquid.getHost());
        dbInfo.setPort(StringUtils.isNull(connectSquid.getPort()) ? -1 : Integer.parseInt(connectSquid.getPort()));
        dbInfo.setUserName(connectSquid.getUsername());
        dbInfo.setPassword(connectSquid.getPassword());
        dbInfo.setKeyspace(connectSquid.getKeyspace());
        dbInfo.setCluster(connectSquid.getCluster());
        dbInfo.setVerification(connectSquid.getVerificationMode());
        return dbInfo;
    }

    public void initClusterAndSession(String host, int port, int verification, String username, String password, String keyspace, String clusterName) {
        try {
            if (port >= 0) {
                if (verification == 1) {
                    cluster = Cluster.builder().addContactPoints(host).withPort(port).withClusterName(clusterName).withCredentials(username, password).build();
                } else {
                    cluster = Cluster.builder().addContactPoints(host).withPort(port).withClusterName(clusterName).build();
                }

            } else {
                if (verification == 1) {
                    cluster = Cluster.builder().addContactPoints(host).withClusterName(clusterName).withCredentials(username, password).build();
                } else {
                    cluster = Cluster.builder().addContactPoints(host).withClusterName(clusterName).build();
                }
            }
            if (!cluster.getMetadata().getClusterName().equals(clusterName)) {
                throw new com.datastax.driver.core.exceptions.InvalidQueryException("cluster不存在");
            }
            session = cluster.connect(keyspace);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    public void initClusterAndSession(String[] host, int port, int verification, String username, String password, String keyspace, String clusterName) {
        try {
            PoolingOptions poolingOptions = new PoolingOptions();
            poolingOptions.setConnectionsPerHost(HostDistance.REMOTE, 2, 10)
                    .setConnectionsPerHost(HostDistance.LOCAL, 2, 10);
            if (port >= 0) {
                if (verification == 1) {
                    cluster = Cluster.builder().addContactPoints(host).withPort(port).withClusterName(clusterName).withPoolingOptions(poolingOptions).withCredentials(username, password).build();
                } else {
                    cluster = Cluster.builder().addContactPoints(host).withPort(port).withClusterName(clusterName).withPoolingOptions(poolingOptions).build();
                }

            } else {
                if (verification == 1) {
                    cluster = Cluster.builder().addContactPoints(host).withClusterName(clusterName).withCredentials(username, password).withPoolingOptions(poolingOptions).build();
                } else {
                    cluster = Cluster.builder().addContactPoints(host).withClusterName(clusterName).withPoolingOptions(poolingOptions).build();
                }
            }
            if (!cluster.getMetadata().getClusterName().equals(clusterName)) {
                throw new com.datastax.driver.core.exceptions.InvalidQueryException("cluster不存在");
            }
            session = cluster.connect(keyspace);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    public Session getSession(Cluster cluster, String keyspace) {
        if (cluster != null) {
            session = cluster.connect(keyspace);
        }
        return session;
    }

    public Session getSession(Cluster cluster) {
        if (cluster != null) {
            session = cluster.connect();
        }
        return session;
    }

    @Override
    public List<BasicTableInfo> getAllTables(String filter) {
        List<BasicTableInfo> tables = new ArrayList<>();
        if (cluster != null && session != null) {
            //虽然cluster.connect(keyspace),但是session获得的cluster依然还是获取的所有的keyspace
            Cluster cluster = session.getCluster();
            List<KeyspaceMetadata> metas = cluster.getMetadata().getKeyspaces();
            FilterUtil filterUtil = new FilterUtil(filter);
            for (KeyspaceMetadata meta : metas) {
                if (!meta.getName().equals(info.getKeyspace())) {
                    continue;
                }
                Collection<TableMetadata> tableMetas = meta.getTables();
                for (TableMetadata tableMetadata : tableMetas) {
                    if (!filterUtil.check(tableMetadata.getName())) {
                        continue;
                    }
                    List<ColumnMetadata> primaryKeys = tableMetadata.getPrimaryKey();
                    BasicTableInfo info = new BasicTableInfo();
                    info.setTableName(tableMetadata.getName());
                    info.setKeyspace(meta.getName());
                    info.setCluster(cluster.getClusterName());
                    info.setDbType(DataBaseType.CASSANDRA);
                    List<ColumnInfo> columnInfoList = new ArrayList<>();
                    List<ColumnMetadata> columnMetadatas = tableMetadata.getColumns();
                    for (ColumnMetadata metadata : columnMetadatas) {
                        ColumnInfo column = new ColumnInfo();
                        column.setName(metadata.getName());
                        DbBaseDatatype dbType = DbBaseDatatype.parse(metadata.getType().getName().name().toUpperCase());
                        column.setDbBaseDatatype(dbType);
                        column.setTypeName(metadata.getType().getName().name().toUpperCase());
                        column.setTableName(tableMetadata.getName());
                        for (ColumnMetadata primaryKey : primaryKeys) {
                            if (primaryKey.getName().equals(metadata.getName())
                                    /*&& dbType != DbBaseDatatype.TEXT
                                    && dbType != DbBaseDatatype.ASCII
                                    && dbType != DbBaseDatatype.BLOB
                                    && dbType != DbBaseDatatype.VARCHAR*/) {
                                column.setPrimary(true);
                            }
                        }
                        columnInfoList.add(column);
                    }

                    info.setColumnList(columnInfoList);
                    tables.add(info);
                }

            }
        }
        return tables;
    }

    @Override
    public List<ColumnInfo> getTableColumns(String tableName, String DatabaseName) {
        String sql = "describe table " + tableName;
        ResultSet rs = session.execute(sql);
        return null;
    }

    public static List<ColumnInfo> getTableColumns(List<BasicTableInfo> tableInfoList, DBSourceTable table) {
        List<ColumnInfo> columnInfoList = new ArrayList<>();
        for (BasicTableInfo tableInfo : tableInfoList) {
            if (tableInfo.getTableName().equals(table.getTable_name())) {
                List<ColumnInfo> columnInfos = tableInfo.getColumnList();
                if(columnInfos!=null){
                    for(ColumnInfo columnInfo : columnInfos){
                        DbBaseDatatype dbType = columnInfo.getDbBaseDatatype();
                        if(dbType == DbBaseDatatype.TEXT
                                || dbType == DbBaseDatatype.ASCII
                                || dbType == DbBaseDatatype.BLOB
                                || dbType == DbBaseDatatype.VARCHAR){
                            columnInfo.setPrimary(false);
                        }
                    }
                }
                columnInfoList.addAll(tableInfo.getColumnList());
            }
        }

        return columnInfoList;
    }

    @Override
    public void close() {
        if (cluster != null) {
            cluster.close();
        }
        if (session != null) {
            session.close();
        }
    }

    @Override
    public List<Map<String, Object>> queryForList(String sql) {
        List<Map<String, Object>> resultMap = new ArrayList<>();
        ResultSet rs = session.execute(sql);
        if (rs != null) {
            for (Row row : rs) {
                Map<String, Object> map = new HashMap<>();
                ColumnDefinitions columns = row.getColumnDefinitions();
                List<ColumnDefinitions.Definition> definitions = columns.asList();
                for (ColumnDefinitions.Definition definition : definitions) {
                    Object value = row.getObject(definition.getName());
                    if(value instanceof LocalDate){
                        value = value+"";
                    }
                    if(StringUtils.isNull(value)){
                        value = value+"";
                    }
                    map.put(definition.getName().toUpperCase(), value);
                }
                resultMap.add(map);
            }
        }
        return resultMap;
    }
}
