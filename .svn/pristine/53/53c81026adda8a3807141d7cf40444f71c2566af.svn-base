package com.eurlanda.datashire.server.utils.adapter;

import com.eurlanda.datashire.entity.DocExtractSquid;
import com.eurlanda.datashire.entity.FileInfo;

import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.text.ParseException;
import java.util.List;

/**
 * ftp处理adapter接口
 * 
 * @author lei.bin
 * 
 */
public interface IFtpAdapter {
	/**
	 * function:设置编码格式，并且获取连接
	 * @throws Exception
	 */
	public void reSet() throws Exception;

	/**
	 * function:连接到服务器
	 * @param ip 
	 * @param port
	 * @param userName
	 * @param userPwd
	 * @param path
	 * @throws SocketException
	 * @throws IOException
	 * @throws Exception
	 */
	public void connectServer(String ip, int port, String userName,
                              String userPwd, String path) throws SocketException, IOException,Exception;

	/**
	 * function:检查被切换的文件夹路径是否正确
	 * 
	 * @return
	 */
	public boolean isDirExist();

	/**
	 * function:关闭连接
	 * @throws IOException            
	 */
	public void closeServer() throws Exception;

	/**
	 * @return function:读取指定目录下的指定后缀的文件名
	 * @throws IOException
	 */
	public List<FileInfo> getFileList(String path, int depth) throws Exception;

	/**
	 * 
	 * @return function:从服务器上读取指定的文件(除office之外的文件)
	 * @throws ParseException
	 * @throws IOException
	 */
	public String readTxtFile(String fileName,int length) throws Exception, IOException;
	/**
	 * 
	 * @param docExtractSquid
	 * @return
	 * @throws ParseException
	 * @throws IOException
	 */
	public List<String> readFile(DocExtractSquid docExtractSquid) throws Exception, IOException;
    /**
     * 从服务器上读取office文件,并且读取内容
     * @param filePath 文件全路径
     * @param fileName 文件名称
     * @return
     * @throws Exception
     */
	public String downloadFileAndRead(String filePath, String fileName, int doc_format) throws Exception;
	/**
	 * @param fileName 文件名
	 *function:删除文件
	 */
	public boolean deleteFile(String fileName) throws Exception;

    public InputStream fileInputStream(String filePath) throws IOException;

    public void downloadFile(String downDir, String fileName, String filePath)throws Exception;

}
