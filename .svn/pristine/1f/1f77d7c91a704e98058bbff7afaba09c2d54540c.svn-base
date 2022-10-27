package com.eurlanda.datashire.server.service;

import com.alibaba.druid.pool.DruidDataSource;
import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.cloudService.CloudRegisterService;
import com.eurlanda.datashire.server.BaseTest;
import com.eurlanda.datashire.server.model.CloudOperateRecord;
import com.eurlanda.datashire.utility.CommonConsts;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Date;

/**
 * Created by zhudebin on 2018/9/30.
 */
public class CloudRegisterServiceTest extends BaseTest {



    @Autowired
    private CloudRegisterService cloudRegisterService;

    @Before
    public void before() {
//        ApplicationContext context = new ClassPathXmlApplicationContext(new String[] {"config/applicationContext.xml"});
//        CommonConsts.application_Context=context;
    }

    @Test
    public void testLogin() throws Exception {
        DruidDataSource ds = (DruidDataSource) CommonConsts.application_Context.getBean("dataSource");

        int b1 = ds.getActiveCount();

        for(int y=0; y<20; y++) {
            new Thread() {
                private Logger logger = LoggerFactory.getLogger(this.getClass());
                public void run() {
                    for (int i = 0; i < 100; i++) {
                        int a1 = ds.getActiveCount();
                        String returnStr =
                                cloudRegisterService.cloudLogin(
                                        "{\"SpaceId\":\"E2005001\",\"Token\":\"2000356^^^20180529004^^^"
                                                + new Date().getTime()
                                                + "\",\"RepositoryId\":2004999,\"ProjectIds\":[2005200,2005201,2005202,2005203,2005204,2005205,2005206,2005207,2005208]}");

                        int a2 = ds.getActiveCount();

                        logger.info(a1 + "----  :  ----" + a2);
                    }
                }
            }.start();

        }

        Thread.sleep(100000);

        int b2 = ds.getActiveCount();

        System.out.println("---end---" + b1 + "," + b2);
    }

    @Test
    public void testInsert() throws Exception {
        IRelationalDataManager adapter = null;
        adapter = DataAdapterFactory.getDefaultDataManager();
        adapter.openSession();
        CloudOperateRecord record = new CloudOperateRecord();
        record.setUser_id(1111);
        record.setOperate_time(new Date());
        record.setSpace_id(222);
        record.setOperate_type(0);
        record.setContent("用户: 操作类型:登录,数猎场:");
        adapter.insert2(record);
        adapter.closeSession();
    }
}
