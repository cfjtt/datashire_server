package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;
import com.eurlanda.datashire.enumeration.DSObjectType;

import java.sql.Types;
import java.util.List;

/**
 * Cassandra 连接squid
 * Created by Eurlanda on 2017/4/10.
 */
@MultitableMapping(pk = "ID", name = {"DS_SQUID"}, desc = "")
public class CassandraConnectionSquid extends Squid {
    {
        this.setType(DSObjectType.CASSANDRA.value());
    }

    @ColumnMpping(name = "host", desc = "", nullable = true, precision = 100, type = Types.VARCHAR, valueReg = "")
    private String host;
    @ColumnMpping(name = "port", desc = "", nullable = true, precision = 10, type = Types.VARCHAR, valueReg = "")
    private String port;
    @ColumnMpping(name = "keyspace", desc = "cassandra", nullable = true, precision = 500, type = Types.VARCHAR, valueReg = "")
    private String keyspace;
    @ColumnMpping(name = "cluster", desc = "cassandra", nullable = true, precision = 500, type = Types.VARCHAR, valueReg = "")
    private String cluster;
    @ColumnMpping(name = "verification_mode", desc = "验证方式(cassandra)", nullable = true, precision = 1, type = Types.INTEGER, valueReg = "0")
    private int verificationMode;
    @ColumnMpping(name = "username", desc = "", nullable = true, precision = 100, type = Types.VARCHAR, valueReg = "")
    private String username;
    @ColumnMpping(name = "password", desc = "", nullable = true, precision = 100, type = Types.VARCHAR, valueReg = "")
    private String password;
    @ColumnMpping(name = "db_type_id", desc = "", nullable = true, precision = 0, type = Types.INTEGER, valueReg = "")
    private int db_type;

    private List<SourceTable> sourceTableList;

    public List<SourceTable> getSourceTableList() {
        return sourceTableList;
    }

    public void setSourceTableList(List<SourceTable> sourceTableList) {
        this.sourceTableList = sourceTableList;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getDb_type() {
        return db_type;
    }

    public void setDb_type(int db_type) {
        this.db_type = db_type;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    public int getVerificationMode() {
        return verificationMode;
    }

    public void setVerificationMode(int verificationMode) {
        this.verificationMode = verificationMode;
    }
}
