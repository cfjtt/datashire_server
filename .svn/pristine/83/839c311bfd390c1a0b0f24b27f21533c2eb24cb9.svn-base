package com.eurlanda.datashire.adapter.db;

import com.eurlanda.datashire.entity.DBConnectionInfo;
import com.eurlanda.datashire.entity.operation.BasicTableInfo;
import com.eurlanda.datashire.utility.FilterUtil;
import com.eurlanda.datashire.utility.StringUtils;

import java.util.Iterator;
import java.util.List;

/**
 * Created by My PC on 4/7/2017.
 */
public class TeradataAdapter extends AbsDBAdapter {

    public TeradataAdapter(DBConnectionInfo info){
        super(info);
    }

    @Override
    public List<BasicTableInfo> getAllTables(String filter) {

//        String sql = "SELECT TABLENAME FROM DBC.TABLES WHERE TABLEKIND = 'T'";
//        List<Map<String,Object>> pkList = this.jdbcTemplate.queryForList(sql, null);
        List<BasicTableInfo> tableInfos = super.getAllTables(filter);
        if(StringUtils.isNotNull(filter)) {
            FilterUtil filterUtil = new FilterUtil(filter);
            if(tableInfos!=null) {
                Iterator<BasicTableInfo> iterable = tableInfos.iterator();
                while(iterable.hasNext()){
                    BasicTableInfo info = iterable.next();
                    if(!filterUtil.check(info.getTableName())){
                        iterable.remove();
                    }
                }
            }
        }
        return tableInfos;
    }
}
