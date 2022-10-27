package com.eurlanda.datashire.server.dao;

import com.eurlanda.datashire.server.BaseTest;
import com.eurlanda.datashire.server.model.User;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.Map;

/**
 * Created by zhudebin on 2017/6/8.
 */
public class UserDaoTest extends BaseTest {

    @Autowired
    private UserDao userDao;

    @Test
    public void testFindUser() {
        User user = new User();
        user.setUser_name("superuser");
        User u = userDao.findUser(user);
        System.out.println(u);
    }
}
