package com.eurlanda.datashire.adapter.db;

import com.eurlanda.datashire.entity.FileInfo;
import com.eurlanda.datashire.entity.HdfsSquid;
import com.eurlanda.datashire.utility.FilterUtil;
import com.eurlanda.datashire.utility.HdfsUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.AccessControlException;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Eurlanda-dev on 2016/4/8.
 */
public class HdfsAdapter {
    static Logger logger = Logger.getLogger(HdfsAdapter.class);
    private FTPClient ftpClient;
    private String fileName;
    private String ip = ""; // 服务器IP地址
    private String userName = ""; // 用户名
    private String userPwd = ""; // 密码
    private int port = 0; // 端口号
    private String path = ""; // 读取文件的存放目录
    private String fileType = "";//读取文件类型
    private String strencoding;//编码
    private String finalPath;//路径

    public String getStrencoding() {
        return strencoding;
    }

    public void setStrencoding(String strencoding) {
        this.strencoding = strencoding;
    }

    public String getFileType() {
        return fileType;
    }

    public void setFileType(String fileType) {
        this.fileType = fileType;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getUserPwd() {
        return userPwd;
    }

    public void setUserPwd(String userPwd) {
        this.userPwd = userPwd;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }



    public List<FileInfo> getFileList(String path,int depth,HdfsSquid hdfsConnection) throws Exception {
        List<FileInfo> fileLists = new ArrayList<FileInfo>();
        finalPath=path;
        showDir(fileLists,path,depth,-1,hdfsConnection);
        return fileLists;
    }

    /**
     *
     * @param fileLists
     * @param depth  用户定义的最大深度,0代表无穷
     * @param currentDepth 当前遍历的深度
     * @throws Exception
     */

    public void showDir(List<FileInfo> fileLists,String path,int depth,int currentDepth,HdfsSquid hdfsConnection) throws Exception {
        currentDepth++;
        if (depth!=0&&depth==currentDepth) {
            return;
        }
        // 获得指定目录下所有文件名
        FTPFile[] ftpFiles = null;
        FileSystem fs = HdfsUtils.getFileSystem(hdfsConnection);
        FileStatus[] fileStatusList = null ;
        //如果遍历到的文件夹有访问权限，则跳出对改文件夹的遍历
        try {
            fileStatusList = fs.listStatus(new Path(path));
        } catch (AccessControlException e){
            e.printStackTrace();
            return;
        }
            FilterUtil filter = new FilterUtil(hdfsConnection.getFilter());
            if (fileStatusList.length < 1) {
                logger.error("该路径下没有文件");
            }
            for (int i = 0; i < fileStatusList.length; ++i) {
                FileInfo fileInfo = new FileInfo();
                if (fileStatusList[i].isFile()) {
                    if (filter.check(fileStatusList[i].getPath().getName().toString())) {
                        fileInfo.setFileName(replacePath(finalPath, path + "/" + fileStatusList[i].getPath().getName()));
                        fileInfo.setIs_directory(false);
                        fileLists.add(fileInfo);
                    }
                } else if (fileStatusList[i].isDirectory()) {
                    showDir(fileLists, path + "/" + fileStatusList[i].getPath().getName(), depth, currentDepth, hdfsConnection);
                }
            }

    }

    /**
     * 替换路径
     *
     * @param path_one
     *            用户输入的路径
     * @param path_two
     *            文件所在的标准路径
     * @return
     */
    public static String replacePath(String path_one, String path_two) {
        return path_two.replaceAll("\\\\", "/").substring(path_one.replaceAll("\\\\", "/").length()+1, path_two.length());
    }
}
