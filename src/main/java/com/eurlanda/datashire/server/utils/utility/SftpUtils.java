package com.eurlanda.datashire.server.utils.utility;

import com.eurlanda.datashire.entity.FtpSquid;
import com.eurlanda.datashire.server.utils.adapter.RemoteFileAdapter;
import com.eurlanda.datashire.server.utils.fileSource.FileFolderAdapterImpl;
import com.eurlanda.datashire.utility.*;
import com.jcraft.jsch.*;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.Properties;

/**
 * sftp连接处理类
 *
 * @author lei.bin
 */
public class SftpUtils extends ExtractUtilityBase {
    public static final Logger logger = Logger.getLogger(SftpUtils.class);
    private String filter;
    Session session = null;
    Channel channel = null;

    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    /**
     * 连接sftp服务器
     *
     * @param ftpConnection FtpSquid
     * @return
     * @throws Exception
     */
    public ChannelSftp connect(FtpSquid ftpConnection) throws Exception {
        ChannelSftp sftp = null;
        try {
            int port = 21;
            String host = ftpConnection.getHost();
            String[] hostAndIP = ftpConnection.getHost().split(":");
            if (hostAndIP.length == 2) {
                port = Integer.valueOf(hostAndIP[hostAndIP.length - 1]);
                host = hostAndIP[0];
            } else if (hostAndIP.length != 1) {
                throw new Exception("IP与端口格式不正确");
            }
            String username = ftpConnection.getUser_name();
            String password = ftpConnection.getPassword();

            JSch jsch = new JSch();
            jsch.getSession(username, host, port);
            if (ftpConnection.getAllowanonymous_flag() == 1) {
                username = "anonymous";
            }
            session = jsch.getSession(username, host, port);// 根据用户名，主机ip，端口获取一个Session对象
            logger.debug("Session created.");
            if (ftpConnection.getAllowanonymous_flag() != 1) {
                session.setPassword(password);// 设置密码
            }
            Properties sshConfig = new Properties();
            sshConfig.put("StrictHostKeyChecking", "no");
            session.setConfig(sshConfig);// 为Session对象设置properties
            session.setTimeout(10000);// 设置timeout时间
            session.connect();// 通过Session建立链接
            logger.debug("Session connected.");
            logger.debug("Opening Channel.");
            channel = session.openChannel("sftp");// 打开SFTP通道
            channel.connect();// 建立SFTP通道的连接
            sftp = (ChannelSftp) channel;
            logger.debug("Connected to " + host + ".");
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("sftp连接异常");
        }
        return sftp;
    }

    /**
     * 关闭连接
     *
     * @throws Exception
     */
    public void closeChannel() throws Exception {
        if (channel != null) {
            channel.disconnect();
        }
        if (session != null) {
            session.disconnect();
        }
    }


    /**
     * 读取txt文件内容
     *
     * @param ftpSquid
     * @param fileName
     * @param length
     * @return
     * @throws Exception
     */
    public String readTxtSFTPServer(FtpSquid ftpSquid, String fileName, int length) throws Exception {
        StringBuilder builder = new StringBuilder();
        InputStream inputStream = null;
        BOMInputStream bomInputStream=null;
        InputStreamReader inputStreamReader=null;
        BufferedReader bufferedReader=null;
        ChannelSftp sftp = null;
        try {
            //连接sftp服务器
            sftp = this.connect(ftpSquid);
            sftp.cd(ftpSquid.getFile_path());
            //设置编码
            sftp.setFilenameEncoding(EncodingUtils.getEncoding(ftpSquid.getEncoding()));
            inputStream = sftp.get(fileName);
            if (inputStream == null)
                throw new Exception("下载sftp文件失败");
            //开始读数据
            bomInputStream=new BOMInputStream(inputStream);
            //设置编码
            String charset = EncodingUtils.getEncoding(ftpSquid.getEncoding());
            //不知道什么原因，用字节流读不出数据。只能用字符流。
             inputStreamReader=new InputStreamReader(bomInputStream,charset);
             bufferedReader=new BufferedReader(inputStreamReader,inputStream.available());

            int len = -1;
            boolean flag = true;
            char[] chs = new char[length];
            while ((len = bufferedReader.read(chs)) != -1 && flag) {
                String str = new String(chs, 0, len);
                builder.append(str);
                flag = false;
            }

          /*
            byte[] bytes = null;
            int len = bufferedStream.available();
            boolean flag = true;
            if (len > length) {
                bytes = new byte[length];
            } else {
                bytes = new byte[len];
            }
            while (bufferedStream.read(bytes) != -1 && flag) {
                String str = new String(bytes, charset);
                builder.append(str);
                flag=false;
            }*/
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception(e);
        } finally {
            if (bufferedReader != null) {
                bufferedReader.close();
            }
            if(inputStreamReader!=null){
                inputStreamReader.close();
            }
            if(bomInputStream!=null){
                bomInputStream.close();
            }
            if (inputStream != null) {
                inputStream.close();
            }
            sftp.disconnect();
        }
        return builder.toString();

    }





    /**
     * 下载文件
     * @param directory 下载目录
     * @param downloadFile 下载的文件
     * @param saveFile 存在本地的路径
     * @param sftp
     * @throws Exception
     */
    public void download(String directory, String downloadFile,String saveFile, ChannelSftp sftp, String enCoding) throws Exception {
        FileOutputStream fileOutputStream=null;
        try {
            sftp.cd(directory);
            File file = new File(saveFile);
            sftp.setFilenameEncoding(enCoding);
            fileOutputStream=new FileOutputStream(file);
            sftp.get(downloadFile,fileOutputStream);
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("sftp下载文件失败");
        } finally {
            this.closeChannel();
            if(fileOutputStream!=null){
                fileOutputStream.close();
            }
        }
    }
    /**
     * 下载整个文件并读取内容
     * @param ftpSquid
     * @param fileName
     * @param downLoad_Path
     * @return
     * @throws Exception
     */
    private String downloadAndReadFromSFTPServer(FtpSquid ftpSquid,String fileName, String downLoad_Path,int length) throws Exception {
        String outFileContent="";
        try {
            //连接服务器
            ChannelSftp sftp=this.connect(ftpSquid);
            //下载文件
            this.download(ftpSquid.getFile_path(), fileName, downLoad_Path, sftp, EncodingUtils.getEncoding(ftpSquid.getEncoding()));
            //调用统一读取文件内容
            FileFolderAdapterImpl fileAdapter=new FileFolderAdapterImpl();
            //调用统一读取office方法  0:因为这边用到的流没有关闭，在读取office时删除临时文件时删除不掉的。只有等所有的流都关闭了才可以删除文件。
            outFileContent = fileAdapter.readOffice(downLoad_Path,fileName,0,length);
        }catch (Exception e){
            e.printStackTrace();
            throw new Exception(e);
        }finally {
            //删除下载的临时文件
            RemoteFileAdapter.deleteFile(downLoad_Path);
        }
        return outFileContent;
    }


    /**
     * 读取office文件内容
     *
     * @param ftpSquid
     * @param fileName
     * @return
     * @throws Exception
     */
    public String readOfficeSFTPServer(FtpSquid ftpSquid, String fileName,int length) throws Exception {
        String returnStr = null;
        String downLoad_Path=RemoteFileAdapter.ftpDownload_Path + "/"+ fileName;
        RemoteFileAdapter.createDir(RemoteFileAdapter.ftpDownload_Path);
        try {
            returnStr=this.downloadAndReadFromSFTPServer(ftpSquid,fileName,downLoad_Path,length);
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception(e);
        }
        return returnStr;
    }
}
