package com.eurlanda.datashire.server.dao;

import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by zhudebin on 16/4/25.
 */
public class MysqlConnectionTest {

    @Test
    public void test1() {
        Connection con = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            con = DriverManager.getConnection("jdbc:mysql://192.168.137.16:3306/datashire_database", "root", "111111");
            ResultSet rs = con.createStatement().executeQuery("show processlist");
            while(rs.next()) {
                String host =rs.getString("host");
                String command = rs.getString("command");
                System.out.println(" show processlist: \t" + host + ",\t" + command);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(con != null) {
                try {
                    con.close();
                } catch (SQLException e) {
                    System.out.println("关闭connection 异常.....");
                }
            }
        }
    }

}
