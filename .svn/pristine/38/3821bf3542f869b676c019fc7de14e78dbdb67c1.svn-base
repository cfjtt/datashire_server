package com.eurlanda.datashire.server.dao;

import com.eurlanda.datashire.server.model.User;
import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Created by zhudebin on 2017/6/8.
 */
@Repository
public interface UserDao {

    User findUser(@Param("user")User user);

    User findByUsernameAndPassword(@Param("username") String username,
            @Param("password") String password);

    List<User> getAllUser();

    int save(@Param("user")User user);

    int deleteById(@Param("id") int id);

    int update(@Param("user")User user);

}
