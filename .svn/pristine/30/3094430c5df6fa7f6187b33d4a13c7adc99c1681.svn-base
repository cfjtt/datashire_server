package com.eurlanda.datashire.dao.impl;

import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.dao.ISourceColumnDao;
import com.eurlanda.datashire.entity.CassandraExtractSquid;
import com.eurlanda.datashire.entity.DataSquid;
import com.eurlanda.datashire.entity.DocExtractSquid;
import com.eurlanda.datashire.entity.HBaseExtractSquid;
import com.eurlanda.datashire.entity.KafkaExtractSquid;
import com.eurlanda.datashire.entity.SourceColumn;
import com.eurlanda.datashire.entity.SystemHiveExtractSquid;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.utility.DatabaseException;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SourceColumnDaoImpl extends BaseDaoImpl implements ISourceColumnDao {

    public SourceColumnDaoImpl() {
    }

    public SourceColumnDaoImpl(IRelationalDataManager adapter) {
        this.adapter = adapter;
    }

    @Override
    public List<SourceColumn> getSourceColumnByTableId(int source_table_id) {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("source_table_id", source_table_id);
        return adapter.query2List2(true, map, SourceColumn.class);
    }

    @Override
    public int deleteSourceColumnByTableId(int source_table_id) throws SQLException {
        Map<String, String> map = new HashMap<String, String>();
        map.put("source_table_id", source_table_id + "");
        return adapter.delete(map, SourceColumn.class);
    }

    @Override
    public List<SourceColumn> getSourceColumnBySquidId(int source_squid_id) {
        String sql = "select a.source_table_id, a.data_type, a.nullable, a.length, a.`precision`, a.scale, a.relative_order, a.isunique, a.ispk, a.id, a.name from ds_source_column a inner join ds_source_table b on a.source_table_id = b.id where source_squid_id = "
                + source_squid_id + " order by source_table_id asc,b.id asc";
        return adapter.query2List(true, sql, null, SourceColumn.class);
    }

    @Override
    public String getTableNameBySquidId(int squidId, int squidType) throws SQLException {
        Map<String, Object> tempParams = null;
        String tableName = "";
        Class c = SquidTypeEnum.classOfValue(squidType);
        if (c == DataSquid.class
                || c == SystemHiveExtractSquid.class
                || c == CassandraExtractSquid.class) {
            String sql = "select table_name from ds_source_table where id in (" +
                    "select source_table_id from ds_squid where id=" + squidId + ")";
            tempParams = adapter.query2Object(true, sql, null);
        } else if (c == DocExtractSquid.class) {
            String sql = "select table_name from ds_source_table where id in (" +
                    "select source_table_id from ds_squid where id=" + squidId + ")";
            tempParams = adapter.query2Object(true, sql, null);
        } else if (c == KafkaExtractSquid.class) {
            String sql = "select table_name from ds_source_table where id in (" +
                    "select source_table_id from ds_squid where id=" + squidId + ")";
            tempParams = adapter.query2Object(true, sql, null);
        } else if (c == HBaseExtractSquid.class) {
            String sql = "select table_name from ds_source_table where id in (" +
                    "select source_table_id from ds_squid where id=" + squidId + ")";
            tempParams = adapter.query2Object(true, sql, null);
        }
        if (tempParams != null && tempParams.containsKey("TABLE_NAME")) {
            tableName = tempParams.get("TABLE_NAME") + "";
        }
        return tableName;
    }

    @Override
    public List<Map<String, Object>> getCopyedSourceParms(int fromSquidId,
                                                          int newFromSquidId, String tableName) throws SQLException {
        String sql = "select t2.id,t1.sourceId from (" +
                "select c.id as sourceId,c.name from DS_SOURCE_TABLE t, DS_SOURCE_COLUMN c " +
                "where t.id=c.source_table_id and t.source_squid_id=" + fromSquidId + " and t.table_name='" + tableName + "' " +
                ")t1 inner join (" +
                "select c.id,c.name from DS_SOURCE_TABLE t, DS_SOURCE_COLUMN c " +
                "where t.id=c.source_table_id and t.source_squid_id=" + newFromSquidId + " and t.table_name='" + tableName + "'" +
                ")t2 on t1.name=t2.name";
        return adapter.query2List(true, sql, null);
    }

    /**
     * 复制SourceTable数据
     *
     * @param squidId
     * @param newSquidId
     * @throws DatabaseException
     */
    @Override
    public void copyDbSourceForData(int squidId, int newSquidId) throws DatabaseException {
        //根据源数据进行复制SourceTable数据，使用sql脚本一次性拷贝
        String sql = "INSERT INTO DS_SOURCE_TABLE(TABLE_NAME,SOURCE_SQUID_ID,IS_EXTRACTED) " +
                "SELECT TABLE_NAME," + newSquidId + " AS SOURCE_SQUID_ID,IS_EXTRACTED FROM DS_SOURCE_TABLE WHERE SOURCE_SQUID_ID=" + squidId + ";";
        int cnt = adapter.execute(sql);
        if (cnt > 0) {
            sql = "	REPLACE  INTO DS_SOURCE_COLUMN(SOURCE_TABLE_ID,NAME,DATA_TYPE,NULLABLE,LENGTH,`PRECISION`,ISPK) " +
                    "SELECT T2.SOURCE_TABLE_ID,T1.NAME,T1.DATA_TYPE,T1.NULLABLE,T1.LENGTH,T1.PRECISION,T1.ISPK FROM " +
                    "((SELECT DSC.*,DST.TABLE_NAME FROM DS_SOURCE_COLUMN DSC INNER JOIN DS_SOURCE_TABLE DST ON DSC.SOURCE_TABLE_ID=DST.ID " +
                    "WHERE DST.SOURCE_SQUID_ID=" + squidId +
                    ")T1 INNER JOIN " +
                    "(SELECT DST.ID AS SOURCE_TABLE_ID,DST.TABLE_NAME FROM DS_SOURCE_TABLE DST " +
                    "WHERE DST.SOURCE_SQUID_ID=" + newSquidId +
                    ")T2 ON T1.TABLE_NAME=T2.TABLE_NAME)";
            cnt = adapter.execute(sql);
        }
    }
}
