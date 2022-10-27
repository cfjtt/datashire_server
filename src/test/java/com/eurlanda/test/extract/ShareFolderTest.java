package com.eurlanda.test.extract;

import com.eurlanda.datashire.adapter.RemoteFileAdapter;
import com.eurlanda.datashire.entity.FileFolderSquid;
import com.eurlanda.datashire.entity.FileInfo;
import jcifs.smb.SmbFile;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Random;

/**
 * Created by zhudebin on 15/11/24.
 */
public class ShareFolderTest {

    public static void main(String[] args) throws Exception {

        RemoteFileAdapter rfa = new RemoteFileAdapter();

        rfa.setRemoteHostIp("192.168.137.1");
        rfa.setPassword("squiding@eurlanda");
        rfa.setUsername("administrator");
        rfa.setShareDocName("/Trans");

        FileFolderSquid ffs = new FileFolderSquid();

        SmbFile smbFile = rfa.getSmbFile("192.168.137.1", "administrator", "squiding@eurlanda", "/Trans", null);

        System.out.println(smbFile);



    }

    @Test
    public void test1() throws Exception {
        RemoteFileAdapter rfa = new RemoteFileAdapter();

        FileFolderSquid ffs = new FileFolderSquid();

        ffs.setHost("192.168.137.1");
        ffs.setFile_path("/trans/业务测试数据");
        ffs.setIncluding_subfolders_flag(1);
        ffs.setMax_travel_depth(0);
        ffs.setPassword("eurlanda1");
        ffs.setUser_name("administrator");

        List<FileInfo> fileInfos = rfa.getFiles(null, ffs);

        System.out.println(fileInfos.size());
        for(FileInfo fi : fileInfos) {
            System.out.println(fi);
        }
    }

    @Test
    public void test2() throws Exception {

        for(int i=0; i<1000; i++) {
            final int finalI = i;
            new Thread() {

                private int a = finalI;

                @Override public void run() {
                    try {
                        int st = new Random().nextInt(100);
                        System.out.println("sleep time : " + st);
                        Thread.sleep(1000 + st);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    RemoteFileAdapter rfa = new RemoteFileAdapter();

                    FileFolderSquid ffs = new FileFolderSquid();

                    ffs.setHost("192.168.137.1");
                    ffs.setFile_path("/trans/业务测试数据");
                    ffs.setIncluding_subfolders_flag(1);
                    ffs.setMax_travel_depth(0);
                    ffs.setPassword("eurlanda1");
                    ffs.setUser_name("administrator");

                    List<FileInfo> fileInfos = null;
                    try {
                        fileInfos = rfa.getFiles(null, ffs);
                    } catch (Exception e) {
                        System.err.println("-------------- 异常 --------- " + a);
                    }

                    System.out.println(a);
                    Assert.assertEquals(33, fileInfos.size());
                }
            }.run();

        }
    }

}
