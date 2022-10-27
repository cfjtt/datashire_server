package com.eurlanda.datashire.server.service;

import com.eurlanda.datashire.server.BaseTest;
import com.eurlanda.datashire.server.model.User;
import com.eurlanda.datashire.utility.ReturnValue;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.Rollback;

import java.util.Map;

/**
 * Created by zhudebin on 2017/6/8.
 */
public class UserServiceTest extends BaseTest {

    @Autowired
    private UserService userService;

    @Test
    public void testLogin() {
        User user = new User();
        user.setUser_name("superuser");
        user.setPassword("7066a4f427769cc43347aa96b72931a");
        ReturnValue returnValue = new ReturnValue();
        Map<String, Object> map = userService.validateLoginInfo(returnValue);
        System.out.println(map);
    }

    @Test
    public void testSave() {
        User user = new User();
        user.setUser_name("helllo_111112");
        user.setPassword("11111");
        int cnt = userService.save(user);
        System.out.println(cnt);
        System.out.println(user.getId());
    }

    @Test
    public void testDelete() {
        int cnt =userService.deleteById(155);
        System.out.println(cnt);
    }

    @Test
    @Rollback(false)
    public void testUpdate() {
        User user = new User();
        user.setId(155);
        user.setEmail_address("xxx@qq.com");
        int cnt = userService.update(user);
        System.out.println(cnt);
    }
}
