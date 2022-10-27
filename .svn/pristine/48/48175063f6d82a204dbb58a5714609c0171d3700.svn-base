package com.eurlanda.datashire.server.utils;

import com.eurlanda.datashire.adapter.FtpAdapter;
import com.eurlanda.datashire.entity.FileInfo;
import org.apache.commons.net.ftp.FTPClient;
import org.junit.Test;

import java.util.List;

/**
 * Created by zhudebin on 2017/2/22.
 */
public class FtpAdapterTest {

    public void testConnect() {
        FTPClient ftp = new FTPClient();

    }

    @Test
    public void test1() throws Exception {
        FtpAdapter adapter = new FtpAdapter();
        adapter.setStrencoding("UTF-8");
        adapter.connectServer("192.168.137.128", 21, "root", "111111", "/home");
        List<FileInfo> list = adapter.getFileList("/home", 1);
        System.out.println(list.size());
        for(FileInfo fi : list) {
            System.out.println(fi);
        }
    }
}
