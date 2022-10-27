package com.eurlanda.datashire.dao.dest.impl;

import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.dao.dest.ImpalaColumnDao;
import com.eurlanda.datashire.dao.impl.BaseDaoImpl;
import com.eurlanda.datashire.entity.Column;
import com.eurlanda.datashire.entity.dest.DestImpalaColumn;
import com.eurlanda.datashire.utility.BeanMapUtil;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by Eurlanda-dev on 2016/5/13.
 */
    public class ImpalaColumnDaoImpl extends BaseDaoImpl implements ImpalaColumnDao {

        public ImpalaColumnDaoImpl(IRelationalDataManager adapter){
            this.adapter = adapter;
    }

    /**
     * 通过squid id,获取 impalaColumn集合
     * @param squidId    inSession:false
     * @return
     */
    @Override
    public List<DestImpalaColumn> getImpalaColumnBySquidId(boolean inSession, int squidId) {
        try {
            List<Object> params = new ArrayList<>();
            params.add(squidId);
            java.util.List<Map<String, Object>> list = adapter.query2List(inSession, "SELECT EC.ID EC_ID, "
                    + "EC.SQUID_ID EC_SQUID_ID, EC.COLUMN_ID, EC.FIELD_NAME, "
                    + "EC.IS_DEST_COLUMN,EC.COLUMN_ORDER, C.* FROM DS_DEST_IMPALA_COLUMN EC "
                    + "LEFT JOIN DS_COLUMN C ON EC.COLUMN_ID = C.ID "
                    + "WHERE EC.SQUID_ID=?", params);
            List<DestImpalaColumn> impalaColumns = new ArrayList<>();
            for(Map<String, Object> map : list) {
                DestImpalaColumn impalaColumn = new DestImpalaColumn();
                impalaColumn.setId((Integer) map.get("EC_ID"));
                impalaColumn.setColumn_id((Integer) map.get("COLUMN_ID"));
                impalaColumn.setField_name((String) map.get("FIELD_NAME"));
                impalaColumn.setColumn_order((Integer) map.get("COLUMN_ORDER"));
                impalaColumn.setIs_dest_column((Integer) map.get("IS_DEST_COLUMN"));
                impalaColumn.setSquid_id((Integer) map.get("EC_SQUID_ID"));
                Column column = (Column) BeanMapUtil.convertSQLMap(Column.class, map);
                impalaColumn.setColumn(column);
                impalaColumns.add(impalaColumn);
            }
            return impalaColumns;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 通过squidId,删除
     * @param squidId
     * @return
     */
    @Override
    public void deleteImpalaColumnBySquidId(int squidId){
        List<Object> params = new ArrayList<>();
        params.add(squidId);
        try {
            adapter.execute("delete from DS_DEST_IMPALA_COLUMN where SQUID_ID=?", params);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 通过squidId和columnId删除column
     * @param squidId, columnId
     * @return
     */
    @Override
    public void deleteImpalaColumn(int squidId, int columnId) {
        List<Object> params = new ArrayList<>();
        params.add(squidId);
        params.add(columnId);
        try {
            adapter.execute("delete from DS_DEST_IMPALA_COLUMN where SQUID_ID=? AND COLUMN_ID=?", params);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


}
