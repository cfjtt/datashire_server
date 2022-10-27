package com.eurlanda.datashire.server.hive;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.junit.Test;

import java.util.List;

/**
 * Created by zhudebin on 2017/5/4.
 */
public class HiveTest {

    @Test
    public void test() throws Exception {
        HiveConf hiveConf;
        HiveMetaStoreClient client;

        hiveConf = new HiveConf();
        client = new HiveMetaStoreClient(hiveConf);
        List<String> tables = client.getAllTables("default");
        System.out.println(tables);
    }
}
