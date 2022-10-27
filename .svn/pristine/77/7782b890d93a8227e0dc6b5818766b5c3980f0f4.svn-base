package com.eurlanda.datashire.dao.dest.impl;

import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.dao.dest.IHdfsColumnDao;
import com.eurlanda.datashire.dao.impl.BaseDaoImpl;
import com.eurlanda.datashire.entity.Column;
import com.eurlanda.datashire.entity.dest.DestHDFSColumn;
import com.eurlanda.datashire.utility.BeanMapUtil;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by dzp on 2016/4/27.
 */
public class HdfsColumnDaoImpl extends BaseDaoImpl implements IHdfsColumnDao {

    public HdfsColumnDaoImpl(IRelationalDataManager adapter){
        this.adapter = adapter;
    }

    /**
     * 通过squid id,获取 DestHdfsColumn集合
     * @param squidId    inSession:false
     * @return
     */
    @Override
    public List<DestHDFSColumn> getHdfsColumnBySquidId(boolean inSession, int squidId) {
        try {
          List<Object> params = new ArrayList<>();
            params.add(squidId);
            java.util.List<Map<String, Object>> list = adapter.query2List(inSession, "SELECT EC.ID EC_ID, "
                    + "EC.SQUID_ID EC_SQUID_ID, EC.COLUMN_ID, EC.FIELD_NAME, EC.IS_PARTITION_COLUMN EC_IS_PARTITION_COLUMN, "
                    + "EC.IS_DEST_COLUMN,EC.COLUMN_ORDER, C.* FROM DS_DEST_HDFS_COLUMN EC "
                    + "LEFT JOIN DS_COLUMN C ON EC.COLUMN_ID = C.ID "
                    + "WHERE EC.SQUID_ID=?", params);
            List<DestHDFSColumn> HdfsColumns = new ArrayList<>();
            for(Map<String, Object> map : list) {
                DestHDFSColumn HdfsColumn = new DestHDFSColumn();
                HdfsColumn.setId((Integer) map.get("EC_ID"));
                HdfsColumn.setColumn_id((Integer) map.get("COLUMN_ID"));
                HdfsColumn.setField_name((String) map.get("FIELD_NAME"));
                HdfsColumn.setColumn_order((Integer) map.get("COLUMN_ORDER"));
                HdfsColumn.setIs_dest_column((Integer) map.get("IS_DEST_COLUMN"));
                HdfsColumn.setSquid_id((Integer) map.get("EC_SQUID_ID"));
                Column column = (Column) BeanMapUtil.convertSQLMap(Column.class, map);
                HdfsColumn.setColumn(column);
                HdfsColumn.setIs_partition_column((Integer)map.get("EC_IS_PARTITION_COLUMN"));
                HdfsColumns.add(HdfsColumn);
            }
            return HdfsColumns;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 通过squidId,删除
     * @param squidId    inSession:false
     * @return
     */
    @Override
    public void deleteHdfsColumnBySquidId(int squidId) {
        List<Object> params = new ArrayList<>();
        params.add(squidId);
        try {
            adapter.execute("delete from DS_DEST_HDFS_COLUMN where SQUID_ID=?", params);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deleteHdfsColumn(int squidId, int columnId) {
        List<Object> params = new ArrayList<>();
        params.add(squidId);
        params.add(columnId);
        try {
            adapter.execute("delete from DS_DEST_HDFS_COLUMN where SQUID_ID=? AND COLUMN_ID=?", params);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
