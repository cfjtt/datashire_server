package com.eurlanda.datashire.server.dao;

import com.eurlanda.datashire.server.BaseTest;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.Map;

/**
 * Created by zhudebin on 2017/6/8.
 */
public class ServerParameterDaoTest extends BaseTest {

    @Autowired
    private ServerParameterDao serverParameterDao;

    @Test
    public void testGetLicenseKey() {
        String licenseKey = serverParameterDao.getLicenseKey();
        System.out.println("-------------------" + licenseKey);
        assert licenseKey != null;
    }

    @Test
    public void testFindList() {
        List<Map<String, Object>> list = serverParameterDao.findList();
        System.out.println(list);
    }
}
