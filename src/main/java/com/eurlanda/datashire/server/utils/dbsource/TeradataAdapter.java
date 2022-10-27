package com.eurlanda.datashire.server.utils.dbsource;

import com.eurlanda.datashire.server.utils.entity.DBConnectionInfo;
import com.eurlanda.datashire.server.utils.entity.operation.BasicTableInfo;
import com.eurlanda.datashire.server.utils.entity.operation.ColumnInfo;
import com.eurlanda.datashire.utility.FilterUtil;
import com.eurlanda.datashire.utility.StringUtils;
import org.apache.thrift.TException;

import java.util.Iterator;
import java.util.List;

/**
 * Created by My PC on 4/7/2017.
 */
public class TeradataAdapter extends AbsDBAdapter {

    public TeradataAdapter(DBConnectionInfo info){
        super(info);
    }

    /**
     * 获取表信息
     * @param filter
     * @return
     */
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

    /**
     * 获取表的字段
     * @param tableName
     * @param DatabaseName
     * @return
     * @throws TException
     */
    @Override
    public List<ColumnInfo> getTableColumns(final String tableName, String DatabaseName) throws TException {
        String fullTableName = tableName;
        /**
         * 因为中芯的TD在表前面没有Schema的时候获取Column会出错，所以在这里统一对表名进行判断，
         * 如果没有Schema，就把数据库名称作为Schema拼在表名前面
         */
        if (tableName.indexOf(DatabaseName + ".") != 0)
            fullTableName = DatabaseName + "." + tableName;
        return super.getTableColumns(fullTableName, DatabaseName);
    }
}
