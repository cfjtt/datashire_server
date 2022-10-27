package com.eurlanda.test.dao;

import com.eurlanda.datashire.dao.dest.IDestESSquidDao;
import com.eurlanda.datashire.dao.dest.impl.DestESSquidDaoImpl;
import com.eurlanda.datashire.entity.ExtractSquid;
import com.eurlanda.datashire.entity.Squid;
import com.eurlanda.datashire.entity.dest.DestESSquid;
import org.junit.Test;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Created by zhudebin on 15-9-17.
 */
public class DestESSquidDaoTest extends BaseDaoTest {

    @Test
    public void testSave() {
        IDestESSquidDao destESSquidDao = new DestESSquidDaoImpl(adapter);

        DestESSquid destESSquid = new DestESSquid();
        destESSquid.setHost("100.100.100.100");
        destESSquid.setEsindex("hello");
        destESSquid.setEstype("world");
        destESSquid.setKey(UUID.randomUUID().toString());

        prepareSquid(destESSquid, 1, "test");

        adapter.openSession();
        try {
            destESSquidDao.insert2(destESSquid);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            adapter.closeSession();
        }
    }

    @Test
    public void testUpdate() {
        IDestESSquidDao destESSquidDao = new DestESSquidDaoImpl(adapter);

        DestESSquid destESSquid = new DestESSquid();
        destESSquid.setId(6);
        destESSquid.setHost("100.100.100.100");
        destESSquid.setEsindex("hello222");
        destESSquid.setEstype("world22");
        destESSquid.setKey(UUID.randomUUID().toString());

        prepareSquid(destESSquid, 1, "test");

        adapter.openSession();
        try {
            destESSquidDao.update(destESSquid);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            adapter.closeSession();
        }
    }

    @Test
    public void testDelete() {
        IDestESSquidDao destESSquidDao = new DestESSquidDaoImpl(adapter);

        adapter.openSession();
        try {
            destESSquidDao.delete(6, Squid.class);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            adapter.closeSession();
        }
    }

    @Test
    public void testDelete2() {
        adapter.openSession();

        Map<String, String> params = new HashMap<>();
        params.put("id", "2");
        try {
            adapter.delete(params, ExtractSquid.class);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        adapter.closeSession();
    }

    @Test
    public void testQuery() {
        IDestESSquidDao destESSquidDao = new DestESSquidDaoImpl(adapter);

        adapter.openSession();
        try {
            List<DestESSquid> list = destESSquidDao.getObjectAll(DestESSquid.class);
            System.out.println(list);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            adapter.closeSession();
        }
    }
}
