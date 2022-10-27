package com.eurlanda.test.extract;

import com.eurlanda.datashire.adapter.FtpAdapter;
import com.eurlanda.datashire.utility.XmlUtil;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPClientConfig;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.poi.ss.usermodel.Workbook;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;

/**
 * Created by zhudebin on 15/11/17.
 */
public class ExcelTest {

    @Test
    public void test1() throws Exception {
        FtpAdapter ftpAdapter = new FtpAdapter("","192.168.137.1","datashire","eurlanda1","/TestData",21, "xls");

        ftpAdapter.setStrencoding("UTF-8");
        ftpAdapter.reSet();

        InputStream is = ftpAdapter.fileInputStream("/TestData/business/Excel/xlsx_imdb.xlsx");

        Workbook wb = XmlUtil.create(is);

    }

    @Test
    public void test2() throws Exception {
        String file = "/TestData/测试数据/抽取之后/标题与起始数据行号 - 副本.xlsx";
        FtpAdapter ftpAdapter = new FtpAdapter("标题与起始数据行号 - 副本.xlsx","192.168.137.1","datashire","eurlanda1","/TestData/测试数据/抽取之后",21, "xls");

        ftpAdapter.setStrencoding("GBK");
        ftpAdapter.reSet();
        boolean flag = ftpAdapter.deleteFile(file);
        System.out.println(flag);



    }

    @Test
    public void test3() throws Exception {
        String file = "/TestData/测试数据/抽取之后/标题与起始数据行号 - 副本.xlsx";

        file = new String(file.getBytes("UTF-8"), "ISO-8859-1");

        System.out.println(file);

        FTPClient ftpClient = new FTPClient();
        try {
            //设置连接的超时时间,架包需要时2.0以上的
            //ftpClient.setConnectTimeout(10000);
            // 连接
            ftpClient.connect("192.168.137.1", 21);
            //设置编码格式
            ftpClient.setControlEncoding("ISO-8859-1");
            ftpClient.setAutodetectUTF8(false);
            //设置文件类型，二进制
//            ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
            //设置缓存区大小
            ftpClient.setBufferSize(3072);
            // 返回状态码
            int reply = ftpClient.getReplyCode();
            if (!FTPReply.isPositiveCompletion(reply)) {
                //不合法时断开连接

                throw new Exception("未连接到FTP，请检查连接信息");
            } else {
                // 登录
                ftpClient.login("datashire", "eurlanda1");
                int reply2 = ftpClient.getReplyCode();
                if (230 != reply2) {
                    throw new Exception("账号或者密码错误");
                }
                System.out.println("==登陆成功====");

            }

            ftpClient.setFileType(FTP.BINARY_FILE_TYPE);

            boolean flag = ftpClient.deleteFile(file);
            System.out.println(flag);
        } catch (SocketException e) {
            e.printStackTrace();
            throw new Exception("FTP的IP地址可能错误，请正确配置");
        } catch (IOException e) {
            e.printStackTrace();
            throw new Exception("FTP的端口错误,请正确配置");
        }
    }

    @Test
    public void test4() throws Exception {
        String ip = "192.168.137.1";
        int port = 21;
        String username = "datashire";
        String password = "eurlanda1";
        String file = "/TestData/测试数据/抽取之后/标题与起始数据行号 - 副本.xlsx";
        String encoding = "GBK";

        FTPClient ftpClient = new FTPClient();
        ftpClient.setControlEncoding(encoding);//设置编码集为GBK支持中文
        FTPClientConfig conf = new FTPClientConfig(FTPClientConfig.SYST_UNIX);
        conf.setServerLanguageCode("en");
        ftpClient.configure(conf);
        ftpClient.connect(ip, port);
        ftpClient.login(username, password);
        ftpClient.enterLocalPassiveMode();//设为被动模式
        ftpClient.setFileType(FTP.BINARY_FILE_TYPE); //不设为二进制传送模式，会收不到0x0D
        String tmpRemotePath= new String("/TestData/测试数据".getBytes("UTF-8"),encoding);
        System.out.println(ftpClient.changeWorkingDirectory("/TestData/测试数据/"));
        System.out.println(ftpClient.printWorkingDirectory());
        FTPFile[] files1 = ftpClient.listFiles();
        System.out.println(files1.length);
        int k = 0;
        for (k = 0; k < files1.length; k++) {
            String fileName = new String(files1[k].getName().getBytes("ISO-8859-1"), "UTF-8");
            System.out.println(fileName);
        }


    }


    private static String LOCAL_CHARSET = "GBK";// FTP协议里面，规定文件名编码为iso-8859-1
    private static String SERVER_CHARSET = "ISO-8859-1";
    private static FTPClient ftpClient;
    private void connectFtpServer() {
    if (ftpClient == null) {
        ftpClient = new FTPClient();
        }
    if (ftpClient.isConnected()) {
        return;
        }
        /**
    String host = getConfigValue(ADDRESS);
    int port = Integer.valueOf(getConfigValue(PORT));String user = getConfigValue(USER);
    String password = getConfigValue(PASSWORD);try {
        ftpClient.connect(host, port);
        if (FTPReply.isPositiveCompletion(ftpClient.getReplyCode())) {if (ftpClient.login(user, password)) {
            if (FTPReply.isPositiveCompletion(ftpClient.sendCommand("OPTS UTF8", "ON"))) {// 开启服务器对UTF-8的支持，如果服务器支持就用UTF-8编码，否则就使用本地编码（GBK）.
                LOCAL_CHARSET = "UTF-8";
                }
            ftpClient.setControlEncoding(LOCAL_CHARSET);ftpClient.enterLocalPassiveMode();// 设置被动模式ftpClient.setFileType(getTransforModule());// 设置传输的模式return;
            } else {
            throw new FileStorageException(
                    "Connet ftpServer error! Please check user or password");}
            }
        } catch (IOException e) {
        disConnectServer();
        throw new FileStorageException(
                "Connet ftpServer error! Please check the Configuration");}
    }
         */
    }
}
