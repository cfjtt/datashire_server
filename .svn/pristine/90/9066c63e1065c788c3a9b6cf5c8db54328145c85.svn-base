package com.eurlanda.datashire.dao.dest.impl;

import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.dao.dest.IEsColumnDao;
import com.eurlanda.datashire.dao.impl.BaseDaoImpl;
import com.eurlanda.datashire.entity.Column;
import com.eurlanda.datashire.entity.dest.EsColumn;
import com.eurlanda.datashire.utility.BeanMapUtil;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by zhudebin on 15-9-17.
 */
public class EsColumnDaoImpl extends BaseDaoImpl implements IEsColumnDao {

    public EsColumnDaoImpl() {
    }

    public EsColumnDaoImpl(IRelationalDataManager adapter){
        this.adapter = adapter;
    }

    /**
     * 通过squid id,获取 esColumn集合
     * @param squidId
     * @return
     */
    public List<EsColumn> getEsColumnsBySquidId(int squidId) {
        return this.getEsColumnsBySquidId(true, squidId);
    }

    public void deleteEsColumnBySquidId(int squidId) {
        List<Object> params = new ArrayList<>();
        params.add(squidId);

        try {
            adapter.execute("delete from DS_ES_COLUMN where SQUID_ID=?", params);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void deleteEsColumn(int squidId, int columnId) {
        List<Object> params = new ArrayList<>();
        params.add(squidId);
        params.add(columnId);

        try {
            adapter.execute("delete from DS_ES_COLUMN where SQUID_ID=? AND COLUMN_ID=?", params);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    // 重写IEsColumnDao中的接口，加入insession判断是否允许调用DS_ES_COLUMN表单
    @Override
    public List<EsColumn> getEsColumnsBySquidId(boolean inSession, int squidId) {
        try {
            List<Object> params = new ArrayList<>();
            params.add(squidId);
            List<Map<String, Object>> list = adapter.query2List(inSession, "SELECT EC.ID EC_ID, "
                    + "EC.SQUID_ID EC_SQUID_ID, EC.COLUMN_ID, EC.IS_MAPPING_ID, "
                    + "EC.FIELD_NAME,EC.IS_PERSIST, C.* FROM DS_ES_COLUMN EC "
                    + "LEFT JOIN DS_COLUMN C ON EC.COLUMN_ID = C.ID "
                    + "WHERE EC.SQUID_ID=?", params);
            List<EsColumn> esColumns = new ArrayList<>();
            for(Map<String, Object> map : list) {
                EsColumn esColumn = new EsColumn();
                esColumn.setId((Integer) map.get("EC_ID"));
                esColumn.setColumn_id((Integer) map.get("COLUMN_ID"));
                esColumn.setField_name((String) map.get("FIELD_NAME"));
                esColumn.setIs_mapping_id((Integer) map.get("IS_MAPPING_ID"));
                esColumn.setIs_persist((Integer) map.get("IS_PERSIST"));
                esColumn.setSquid_id((Integer) map.get("EC_SQUID_ID"));
                Column column = (Column)BeanMapUtil.convertSQLMap(Column.class, map);
                esColumn.setColumn(column);
                esColumns.add(esColumn);
            }
            return esColumns;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
