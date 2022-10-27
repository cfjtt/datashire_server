package com.eurlanda.datashire.dao.impl;

import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.dao.IDBSquidDao;

import java.sql.SQLException;
import java.util.Map;

/**
 * Created by zhudebin on 15-7-29.
 */
public class DBSquidDaoImpl extends BaseDaoImpl implements IDBSquidDao {

    public DBSquidDaoImpl(IRelationalDataManager adapter) {
        this.adapter = adapter;
    }

    @Override
    public int getDBTypeBySourceTableId(int sourceTableId) throws SQLException {

        String sql = "select sc.db_type_id from ds_source_table st join  DS_SQUID sc on st.source_squid_id = sc.id  where st.id = " + sourceTableId;
        Map<String, Object> result = adapter.query2Object(true, sql, null);
        if(result != null && result.containsKey("DB_TYPE_ID")) {
            return (Integer)result.get("DB_TYPE_ID");
        }
        return 0;
    }

    @Override
    public int getDBTypeBySquidId(int squidId) throws SQLException {
        String sql="select db_type_id from DS_SQUID where id="+squidId;
        Map<String, Object> result = adapter.query2Object(true, sql, null);
        if(result != null && result.containsKey("DB_TYPE_ID")) {
            return result.get("DB_TYPE_ID") == null ? 0 : (Integer) result.get("DB_TYPE_ID");
        }
        return -1;
    }
}
