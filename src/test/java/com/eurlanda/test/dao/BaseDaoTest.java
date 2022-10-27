package com.eurlanda.test.dao;

import com.eurlanda.datashire.adapter.HyperSQLManager;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.entity.DbSquid;
import com.eurlanda.datashire.entity.Squid;
import com.eurlanda.datashire.enumeration.DataBaseType;
import org.junit.Before;

/**
 * Created by zhudebin on 15-9-17.
 */
public class BaseDaoTest {

    protected static IRelationalDataManager adapter;

    @Before
    public void before() {
//        adapter = DataAdapterFactory.getDefaultDataManager();
        adapter = getDefaultTestDataManager();
    }

    /**
     * 系统默认数据库管理类（HyperSQLDB）
     * 	 权限表：DS_SYS_*
     * @return
     */
    public static IRelationalDataManager getDefaultTestDataManager(){
        DbSquid dataSource = new DbSquid();
        dataSource.setDb_type(DataBaseType.HSQLDB.value());
        dataSource.setDb_name("prod");
        dataSource.setHost("jdbc:hsqldb:hsql://127.0.0.1:9092/prod");

        dataSource.setUser_name("SA");
        dataSource.setPassword("");
        return new HyperSQLManager(dataSource);
    }

    protected static void prepareSquid(Squid squid, int squidflowId, String name) {
        squid.setSquidflow_id(squidflowId);
        squid.setName(name);
    }
}
