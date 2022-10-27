package com.eurlanda.datashire.server.utils.utility;

import cn.com.jsoft.jframe.utils.FileUtils;
import com.eurlanda.datashire.entity.FtpSquid;
import com.eurlanda.datashire.server.exception.ErrorMessageException;
import com.eurlanda.datashire.server.utils.adapter.FtpAdapter;
import com.eurlanda.datashire.socket.ServerEndPoint;
import com.eurlanda.datashire.utility.CreateFileUtil;
import com.eurlanda.datashire.utility.MessageCode;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;

/**
 * ftp获取文件内容工具类
 * Created by Eurlanda - Java、 on 2017/7/31.
 */
public class FtpUtils {
    private static Logger logger = Logger.getLogger(FtpUtils.class);// 记录日志
    FtpAdapter ftpAdapter = new FtpAdapter();


    /**
     * 连接Ftp,并读取Txt文件
     * @param ftpSquid
     * @return
     * @throws Exception
     */
    public String getReadTxtFtpConnection(FtpSquid ftpSquid,String fileName,int length) throws Exception{
        String returnStr=null;
        try {
            if(ftpSquid.getAllowanonymous_flag()==1){
                ftpAdapter.setUserName("anonymous");
            }else{
                ftpAdapter.setUserName(ftpSquid.getUser_name());
                ftpAdapter.setUserPwd(ftpSquid.getPassword());
            }
            ftpAdapter.setPath(ftpSquid.getFile_path());
            String[] hostAndIP = ftpSquid.getHost().split(":");
            int port = 21;
            String ip = ftpSquid.getHost();
            if (hostAndIP.length == 2){
                ip = hostAndIP[0];
                port = Integer.valueOf(hostAndIP[1]);
            } else if (hostAndIP.length != 1){
                throw new Exception("IP与端口格式不正确");
            }
            ftpAdapter.setIp(ip);//ip
            ftpAdapter.setPort(port);
            ftpAdapter.reSet();
            returnStr=ftpAdapter.readTxtFile(ftpSquid.getFile_path() + "/" + fileName,length);

        }catch (ErrorMessageException e){
            e.printStackTrace();
            throw new ErrorMessageException(MessageCode.ERR_FTPFILE.value());
        } catch (Exception e){
            e.printStackTrace();
            throw e;
        }
        return returnStr;
    }

    /**
     * 连接Ftp,并读取Office文件
     * @param ftpSquid
     * @return
     * @throws Exception
     */
    public String getReadOfficeFtpConnection(FtpSquid ftpSquid,String fileName,int length) throws Exception{
        String returnStr=null;
        try {
            if(ftpSquid.getAllowanonymous_flag()==1){
                ftpAdapter.setUserName("anonymous");
            }else{
                ftpAdapter.setUserName(ftpSquid.getUser_name());
                ftpAdapter.setUserPwd(ftpSquid.getPassword());
            }
            ftpAdapter.setPath(ftpSquid.getFile_path());
            String[] hostAndIP = ftpSquid.getHost().split(":");
            int port = 21;
            String ip = ftpSquid.getHost();
            if (hostAndIP.length == 2){
                ip = hostAndIP[0];
                port = Integer.valueOf(hostAndIP[1]);
            } else if (hostAndIP.length != 1){
                throw new Exception("IP与端口格式不正确");
            }
            ftpAdapter.setIp(ip);//ip
            ftpAdapter.setPort(port);
            ftpAdapter.reSet();
            returnStr=ftpAdapter.downloadFtpFileAndRead(ftpSquid.getFile_path(), fileName,length);

        }catch (IOException e){
            e.printStackTrace();
            throw new Exception(e);
        } catch (Exception e){
            e.printStackTrace();
            throw new Exception(e);
        }
        return returnStr;
    }

    /**
     * 连接服务器。获取流
     * @param ftpSquid
     * @param fileName
     * @return
     * @throws Exception
     */
    public InputStream readXmlFtpConnection(FtpSquid ftpSquid,String fileName)throws Exception{
        InputStream inputStream=null;
        try {
            if(ftpSquid.getAllowanonymous_flag()==1){
                ftpAdapter.setUserName("anonymous");
            }else{
                ftpAdapter.setUserName(ftpSquid.getUser_name());
                ftpAdapter.setUserPwd(ftpSquid.getPassword());
            }
            ftpAdapter.setPath(ftpSquid.getFile_path());
            String[] hostAndIP = ftpSquid.getHost().split(":");
            int port = 21;
            String ip = ftpSquid.getHost();
            if (hostAndIP.length == 2){
                ip = hostAndIP[0];
                port = Integer.valueOf(hostAndIP[1]);
            } else if (hostAndIP.length != 1){
                throw new Exception("IP与端口格式不正确");
            }
            ftpAdapter.setIp(ip);//ip
            ftpAdapter.setPort(port);
            ftpAdapter.reSet();
            // 从服务器上读取指定的文件
            inputStream = ftpAdapter.getReadXml(ftpSquid.getFile_path() + "/" + fileName);
        }catch (Exception e){
            e.printStackTrace();
            throw new Exception("连接服务器失败!");
        }
        return inputStream;
    }




}
