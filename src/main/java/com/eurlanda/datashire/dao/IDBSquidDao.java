package com.eurlanda.datashire.dao;

import java.sql.SQLException;

/**
 * Created by zhudebin on 15-7-29.
 */
public interface IDBSquidDao extends IBaseDao {

    /**
     * 通过source table id 获取数据库类型
     * @param sourceTableId
     * @return
     */
    public int getDBTypeBySourceTableId(int sourceTableId) throws SQLException;
    /**
     * 通过squidid获取到dbtype
     */
    public int getDBTypeBySquidId(int squidId) throws SQLException;
}
