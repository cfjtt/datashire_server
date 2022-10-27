package com.eurlanda.datashire.server.model;

import com.eurlanda.datashire.server.model.Base.BaseObject;

import java.io.Serializable;

/**
 * User实体类
 * <p>
 * Title :
 * </p>
 * <p>
 * Description:
 * </p>
 * <p>
 * Author : zdb 2017-6-8
 * </p>
 * <p>
 * Department :  JAVA后端研发部
 * </p>
 * Copyright : ©2017-2018 悦岚（上海）数据服务有限公司
 * </p>
 */
public class User extends BaseObject implements Serializable {

    // 用户名
    private String user_name;

    //用户全名
    private String full_name;

    //密码
    private String password;

    //邮箱地址
    private String email_address;

    // 最后登录时间
    private String last_logon_date;

    //角色ID
    private int role_id;

    //状态ID
    private int status_id;

    // boolean->char(1) [true:'Y', false:'N']
    // 是否激活
    private boolean is_active;

    // 创建时间
    private String creation_date;

    // Types.NULL 新增/更新时忽略，查询结果集映射时自动对应列的别名
    // 角色名称，只给前台显示使用
    private String role_name;

    // 数猎场ID，限制同一个数猎场只能登陆一个用户
    private String spaceId;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        super.id = id;
    }

    public String getFull_name() {
        return full_name;
    }

    public void setFull_name(String full_name) {
        this.full_name = full_name;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getUser_name() {
        return user_name;
    }

    public void setUser_name(String user_name) {
        this.user_name = user_name;
    }

    public String getEmail_address() {
        return email_address;
    }

    public void setEmail_address(String emailAddress) {
        email_address = emailAddress;
    }

    public String getLast_logon_date() {
        return last_logon_date;
    }

    public void setLast_logon_date(String lastLogonDate) {
        last_logon_date = lastLogonDate;
    }

    public int getRole_id() {
        return role_id;
    }

    public void setRole_id(int roleId) {
        role_id = roleId;
    }

    public int getStatus_id() {
        return status_id;
    }

    public void setStatus_id(int status_id) {
        this.status_id = status_id;
    }

    public String getRole_name() {
        return role_name;
    }

    public void setRole_name(String role_name) {
        this.role_name = role_name;
    }

    public boolean isIs_active() {
        return is_active;
    }

    public void setIs_active(boolean is_active) {
        this.is_active = is_active;
    }

    public String getSpaceId() {
        return spaceId;
    }

    public void setSpaceId(String spaceId) {
        this.spaceId = spaceId;
    }

    public String getCreation_date() {
        return creation_date;
    }

    public void setCreation_date(String creation_date) {
        this.creation_date = creation_date;
    }

    @Override
    public String toString() {
        return "User [id=" + id + ", user_name=" + user_name
                + ", full_name=" + full_name + ", password=" + password
                + ", email_address=" + email_address + ", last_logon_date="
                + last_logon_date + ", role_id=" + role_id + ", status_id="
                + status_id + ", is_active=" + is_active
                + ", role_name=" + role_name
                + "]";
    }

}