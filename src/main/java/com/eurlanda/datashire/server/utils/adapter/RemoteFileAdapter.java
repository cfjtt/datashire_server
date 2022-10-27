package com.eurlanda.datashire.server.utils.adapter;

import com.eurlanda.datashire.entity.FileFolderSquid;
import com.eurlanda.datashire.exception.SystemErrorException;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.SysConf;
import jcifs.smb.NtlmPasswordAuthentication;
import jcifs.smb.SmbFile;
import jcifs.smb.SmbFileInputStream;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.*;

/**远程连接文件
 * Created by Eurlanda - Java、 on 2017/7/27.
 */
public class RemoteFileAdapter {
    private static Logger logger = Logger.getLogger(RemoteFileAdapter.class);// 记录日志
    private String remoteHostIp;  //远程主机IP
    private String username;       //登陆账户
    private String password;      //登陆密码
    private String shareDocName;  //共享文件夹名称
    private String filter;//过滤条件
    private String finalPath;//用户输入的路径
    private int finalDepth;//遍历层数

    //临时下载文件路径
    public static String ftpDownload_Path = System.getProperty("java.io.tmpdir");

    /**
     * 连接远程服务器
     * @param remoteHostIp
     * @param username
     * @param password
     * @param shareDocName
     * @param fileName
     * @return
     * @throws IOException
     */
    public static SmbFile getSmbFile(String remoteHostIp, String username, String password,
                                     String shareDocName, String fileName) throws IOException {
        String conStr = null;
        StringBuffer sb = new StringBuffer();
        sb.append("smb://").append(remoteHostIp);
        if(!StringUtils.isEmpty(shareDocName)) {
            sb.append("/").append(shareDocName);
        }
        if(!StringUtils.isEmpty(fileName)) {
            if(fileName.startsWith("smb://")) {
                sb = new StringBuffer(fileName);
            } else {
                if(!sb.toString().endsWith("/") && !fileName.startsWith("/")) {
                    sb.append("/");
                }
                sb.append(fileName);
            }
        } else {
            sb.append("/");
        }
        conStr = sb.toString();
        conStr=conStr.substring(conStr.indexOf("smb://"+remoteHostIp+"/"));
        SmbFile smbFile = null;
        NtlmPasswordAuthentication auth = null;
        if(StringUtils.isBlank(username)||StringUtils.isBlank(password)){
            smbFile = new SmbFile(conStr);
        }else{
            auth = new NtlmPasswordAuthentication(remoteHostIp, username, password);
            smbFile = new SmbFile(conStr, auth);
        }
        SysConf sysConf=new SysConf();
        jcifs.Config.setProperty("jcifs.smb.client.disablePlainTextPasswords",sysConf.getValue("jcifs.smb.client.disablePlainTextPasswords"));
        jcifs.Config.setProperty("jcifs.smb.client.responseTimeout",sysConf.getValue("jcifs.smb.client.responseTimeout"));   //客户端等待服务器响应时间
        jcifs.Config.setProperty("jcifs.smb.client.soTimeout",sysConf.getValue("jcifs.smb.client.soTimeout"));     //读取数据超时时间
        jcifs.Config.setProperty("jcifs.smb.client.dfs.disabled",sysConf.getValue("jcifs.smb.client.dfs.disabled"));
        smbFile.connect();
        return smbFile;
    }

    public RemoteFileAdapter() {
    }

    public RemoteFileAdapter(String remoteHostIp, String username, String password) {
        this.remoteHostIp = remoteHostIp;
        this.username = username;
        this.password = password;
    }

    public String getRemoteHostIp() {
        return remoteHostIp;
    }

    public void setRemoteHostIp(String remoteHostIp) {
        this.remoteHostIp = remoteHostIp;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getShareDocName() {
        return shareDocName;
    }

    public void setShareDocName(String shareDocName) {
        this.shareDocName = shareDocName;
    }

    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    public String getFinalPath() {
        return finalPath;
    }

    public void setFinalPath(String finalPath) {
        this.finalPath = finalPath;
    }

    public int getFinalDepth() {
        return finalDepth;
    }

    public void setFinalDepth(int finalDepth) {
        this.finalDepth = finalDepth;
    }

    public static boolean createDir(String destDirName) {
        File dir = new File(destDirName);
        if (dir.exists()) {
            logger.info("=======================文件夹已经存在==============================");
            return false;
        }
        if (!destDirName.endsWith(File.separator)) {
            destDirName = destDirName + File.separator;
        }
        logger.info("=======================创建文件夹==============================" + destDirName);
        //创建目录
        if (dir.mkdirs()) {
            return true;
        } else {
            return false;
        }
    }



    /**
     * 下载office文件到临时目录下
     * @param fileFolderSquid
     * @param fileName
     */
    public static void downLoadFileByFileFolderConnectionSquid(FileFolderSquid fileFolderSquid, String fileName) throws Exception{
        InputStream in = null;
        OutputStream out = null;
        SmbFileInputStream smbFileInputStream=null;
        FileOutputStream fileOutputStream=null;
        String username=fileFolderSquid.getUser_name();
        String password=fileFolderSquid.getPassword();
        //ip地址
        String remoteHostIp=fileFolderSquid.getHost();
        //文件目录
        String shareDocName=fileFolderSquid.getFile_path();
        try {
            createDir(ftpDownload_Path); /* 检查临时目录是否存在，如不存在就创建 */
            //连接远程服务器
            SmbFile remoteFile = getSmbFile(remoteHostIp, username, password, shareDocName, fileName);
            //fileName= fileName.substring(fileName.lastIndexOf("/")+1,fileName.length());
            File localFile = new File(ftpDownload_Path+"/"+fileName);  //File.separator
            if(localFile.getParent()!=null && !new File(localFile.getParent()).exists()){
                new File(localFile.getParent()).mkdirs();
            }
            smbFileInputStream=new SmbFileInputStream(remoteFile);
            in = new BufferedInputStream(smbFileInputStream);
            fileOutputStream=new FileOutputStream(localFile);
            out = new BufferedOutputStream(fileOutputStream);
            byte[] buffer = new byte[1024];
            int count = 0;
            while((count = in.read(buffer))>0){
                out.write(buffer, 0, count);
                out.flush();
                buffer = new byte[1024];
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e);
            throw new SystemErrorException(MessageCode.ERR_FILE,"共享文件下载出错");
        } finally {
            if(out!=null){
                out.close();
            }
            if(fileOutputStream!=null){
                fileOutputStream.close();
            }
            if(in!=null){
                in.close();
            }
            if(smbFileInputStream!=null){
                smbFileInputStream.close();
            }
        }
    }

    /**
     * 删除本地文件
     * @param filePathName 文件的全路径
     * @return
     */
    public static boolean deleteFile(String filePathName)
    {
        boolean flag=false;
        File file=new File(filePathName);
        // 路径为文件且不为空则进行删除
        if (file.isFile() && file.exists()) {
            flag=file.delete();
        }
        return flag;
    }

}
