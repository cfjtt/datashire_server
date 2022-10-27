package com.eurlanda.datashire.server.utils.adapter;

import cn.com.jsoft.jframe.utils.CharsetDetector;
import cn.com.jsoft.jframe.utils.FileUtils;
import com.eurlanda.datashire.adapter.FileAdapter;
import com.eurlanda.datashire.entity.DocExtractSquid;
import com.eurlanda.datashire.entity.FileInfo;
import com.eurlanda.datashire.entity.FtpSquid;
import com.eurlanda.datashire.server.exception.ErrorMessageException;
import com.eurlanda.datashire.server.utils.fileSource.FileFolderAdapterImpl;
import com.eurlanda.datashire.socket.ServerEndPoint;
import com.eurlanda.datashire.utility.CreateFileUtil;
import com.eurlanda.datashire.utility.FilterUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.sun.jersey.api.MessageException;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

import java.io.*;
import java.net.SocketException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Service
public class FtpAdapter implements IFtpAdapter {
	static Logger logger = Logger.getLogger(FtpAdapter.class);
	private FTPClient ftpClient;
	private String fileName ;
	private String ip = ""; // 服务器IP地址
	private String userName = ""; // 用户名
	private String userPwd = ""; // 密码
	private int port = 0; // 端口号
	private String path = ""; // 读取文件的存放目录
	private String fileType="";//读取文件类型
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
	/**
	 * init ftp servere
	 */
	public FtpAdapter()  {

    }

    public FtpAdapter(String fileName, String ip, String userName, String userPwd, String path, int port, String fileType)
	{
		this.fileName=fileName;
		//new String(fileName.getBytes("GBK"),"iso-8859-1");
		this.ip=ip;
		this.userName=userName;
		this.userPwd=userPwd;
		this.path=path;
	    //new String(path.getBytes("GBK"),"iso-8859-1");
		this.port=port;
		this.fileType=fileType;
		
	}
	@Override
	public void reSet() throws Exception {
		try {
			if(StringUtils.isBlank(strencoding))
			{
				strencoding="GBK";//默认编码
			}
					//System.getProperty("file.encoding");
			if(StringUtils.isBlank(path))
			{
				//不输入路径的时候默认为根目录
				path="/";
			}
			this.connectServer(ip, port, userName, userPwd, path);
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception("连接异常");
		}
	}

	@Override
	public void connectServer(String ip, int port, String userName,
			String userPwd, String path) throws SocketException, IOException,Exception {
		// TODO Auto-generated method stub
		ftpClient = new FTPClient();
		try {
			//设置连接的超时时间,架包需要时2.0以上的
			//ftpClient.setConnectTimeout(10000);
			// 连接
			ftpClient.connect(ip, port);
			//设置编码格式
			ftpClient.setControlEncoding(strencoding);
			//设置文件类型，二进制
			ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
			//设置缓存区大小
			ftpClient.setBufferSize(3072);
			// 返回状态码
			int reply = ftpClient.getReplyCode();
			if (!FTPReply.isPositiveCompletion(reply)) {
				//不合法时断开连接
				this.closeServer();
				throw new Exception("未连接到FTP，请检查连接信息");
			} else {
				// 登录
				ftpClient.login(userName, userPwd);
				int reply2 = ftpClient.getReplyCode();
				if (230 != reply2) {
					throw new Exception("账号或者密码错误");
				}
				logger.debug("==登陆成功====");
				//设置连接模式
				//被动模式ftpClient.enterLocalPassiveMode();
				//主动模式ftpClient.enterLocalActiveMode();
				// 跳转到指定目录
                /**
                 * 此处将判断去掉了,因为有文件没有.后缀 的,会报异常
                 * 不明白此处为什么会加这个判断
                 */
                //				if (!path.contains(".")) {
//					if (!this.isDirectoryExist(path)) {
//						throw new Exception("指定目录不存在");
//					}
//				}else {
//
//				}
			}
		} catch (SocketException e) {
			this.closeServer();
			e.printStackTrace();
			throw new Exception("FTP的IP地址可能错误，请正确配置");
		} catch (IOException e) {
			this.closeServer();
			e.printStackTrace();
			throw new Exception("FTP的端口错误,请正确配置");
		}
	}

	@Override
	public boolean isDirExist() {
		// TODO Auto-generated method stub
		  try {   
		        ftpClient.changeWorkingDirectory(path);   
		     } catch (Exception e) {  
		         //e.printStackTrace();  
		        return false;   
		     }   
		     return true; 
	}




	//修正FTP文件夹不报错新加方法  用来判断文件夹是否存在
	public boolean isDirectoryExist(String path) {
		List dirlist=new ArrayList();
		int idx=path.lastIndexOf("/");
		String path1=path.substring(0,idx+1);
		try {
			ftpClient.changeWorkingDirectory(path1);
			ftpClient.setControlEncoding(strencoding);
			FTPFile[] ftpFiles = ftpClient.listFiles(new String(path1.getBytes(strencoding), "ISO-8859-1"));
			for(int i =0;i<ftpFiles.length;i++){
				if(ftpFiles[i].isDirectory()) {
					dirlist.add(path1+ftpFiles[i].getName());
				}
			}
			if(dirlist.contains(path)){
				return true;
			}else {
				return false;
			}
		} catch (IOException e) {
			return false;
		}
	}


	@Override
	public void closeServer() throws Exception {
		// TODO Auto-generated method stub
		if (ftpClient.isConnected()) {
			try {
				ftpClient.logout();
				ftpClient.disconnect();
			} catch (IOException e) {
				e.printStackTrace();
				throw new Exception("关闭ftp连接异常");
			}
		}
	}

	@Override
	public List<FileInfo> getFileList(String path,int depth) throws Exception {
		List<FileInfo> fileLists = new ArrayList<FileInfo>();
		finalPath=path;
		showDir(fileLists,path,depth,-1);
		return fileLists;
	}
    /**
     *
     * @param fileLists
     * @param depth  用户定义的最大深度,0代表无穷
     * @param currentDepth 当前遍历的深度
     * @throws Exception
     */
	public void showDir(List<FileInfo> fileLists,String path,int depth,int currentDepth) throws Exception {
		currentDepth++;
		if (depth!=0&&depth==currentDepth) {
			return;
		}
		//切换到指定目录
		ftpClient.changeWorkingDirectory(path);
		ftpClient.setControlEncoding(strencoding);
		// 获得指定目录下所有文件名
		FTPFile[] ftpFiles = null;
		try {
			ftpFiles = ftpClient.listFiles(path);
			FilterUtil filter=new FilterUtil(fileType);
			for (int i = 0; ftpFiles != null && i < ftpFiles.length; i++) {
				FTPFile file = ftpFiles[i];
				FileInfo fileInfo=new FileInfo();
				if(file.isDirectory())
				{
					showDir(fileLists, path+"/"+file.getName(), depth, currentDepth);
				/*	fileInfo.setFileName(file.getName());//名称
					fileInfo.setFilePath(ftpClient.printWorkingDirectory());//路径
					fileInfo.setFileType(String.valueOf(file.getType()));//类型
					fileInfo.setModifyDate(new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(file.getTimestamp().getTime()));//修改时间
					fileInfo.setIs_directory(true);
					fileLists.add(fileInfo);*/
				}else if (file.isFile()) {
					if(filter.check(file.getName())) {
						fileInfo.setFileName(replacePath(finalPath, path+"/"+file.getName()));//名称
						fileInfo.setFilePath(ftpClient.printWorkingDirectory());//路径
						fileInfo.setFileType(String.valueOf(file.getType()));//类型
						fileInfo.setModifyDate(new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(file.getTimestamp().getTime()));//修改时间
						fileInfo.setFileSize(new Long(file.getSize()/1024).toString());//文件大小(kb)
						fileInfo.setIs_directory(false);
						fileLists.add(fileInfo);
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
			ftpClient.disconnect();
			throw new Exception("获取文件列表异常");
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

	@Override
	public boolean deleteFile(String fileName) throws Exception {
		// TODO Auto-generated method stub
		try {
			return ftpClient.deleteFile(fileName);
		} catch (IOException e) {
			e.printStackTrace();
			throw new Exception("删除失败");
		}
	}

	@Override
	public String downloadFileAndRead(String filePath, String fileName,int doc_format) throws Exception {
		FileOutputStream fos = null;
		String outFileContent = "";

		String exString = FileUtils.getFileEx(fileName);
		//新生成的文件名
		String newFileName = com.eurlanda.datashire.utility.StringUtils.isNotBlank(exString) ?
				UUID.randomUUID().toString() + "." + exString :
				UUID.randomUUID().toString();
		String downLoad_Path = ServerEndPoint.ftpDownload_Path + "/" + newFileName;// 下载文件的位置
		try {
			CreateFileUtil.createDir(ServerEndPoint.ftpDownload_Path);
			fos = new FileOutputStream(downLoad_Path);
			//使用老文件名从指定服务器下载文件
			String newFilePath = filePath + "/" + fileName;
			boolean success = ftpClient.retrieveFile(new String(newFilePath.getBytes(strencoding), "iso-8859-1"), fos);

			// 读取下载的文件
			FileAdapter fileAdapter = new FileAdapter();
			outFileContent = fileAdapter.getFileContent(newFileName, downLoad_Path, doc_format);
		} catch (IOException e) {
			e.printStackTrace();
			// TODO: handle exception
			throw e;
		} finally {
			this.closeServer();
			if (!CreateFileUtil.deleteFile(downLoad_Path)) {
				logger.error("从FTP下载到本地的文件没有删除成功,请手动清理");
			}
		}
		return outFileContent;
	}
	
	/**
	 * 把Ftp上的文件下载到本地
	 * @param filePath
	 * @param fileName
	 * @return
	 * @author bo.dang
	 * @date 2014年5月14日
	 */
	public String downloadFile(String filePath, String fileName) throws Exception{
	        FileOutputStream fos = null;
	        String downLoadPath = ServerEndPoint.ftpDownload_Path + "/"+ fileName;// 下载文件的位置
	        try {
	            
	            if(!new File(ServerEndPoint.ftpDownload_Path).exists()){
	                new File(ServerEndPoint.ftpDownload_Path).mkdirs();
	            }
	            File newFile = new File(downLoadPath);
	            // 如果文件不存在，就创建新的文件
	            if(!newFile.exists()){
	                newFile.createNewFile();
	            }
	            fos = new FileOutputStream(downLoadPath);
	            ftpClient.retrieveFile(new String(filePath.getBytes(strencoding),"iso-8859-1"), fos);
	        } catch (IOException e) {
	            e.printStackTrace();
	            // TODO: handle exception
	            throw new Exception("FTP客户端下载出错");
	        } finally {
	            try {
	                this.closeServer();
	            } catch (Exception e) {
	                // TODO Auto-generated catch block
	                logger.debug("关闭异常");
	            }
	        }
	     return downLoadPath;
	}
	/**
	 * ftp下载文件
	 * @param filePath
	 * @param fileName
	 * @return
	 * @throws Exception
	 */
	public boolean downloadFtpFile(String filePath, String fileName)
			throws Exception {
		FileOutputStream fos = null;
		String downLoadPath = ServerEndPoint.ftpDownload_Path + File.separator
				+ fileName;// 下载文件的位置
		String mkdirpath=downLoadPath.substring(0,downLoadPath.lastIndexOf("/")>0?downLoadPath.lastIndexOf("/"):downLoadPath.length());
		String newFilePath = filePath + File.separator  + fileName;
		logger.info("文件下载的位置 :"+downLoadPath);
		logger.info("newFilePath :"+newFilePath);
		logger.info("创建文件夹的位置:"+mkdirpath);
		boolean success = true;
		try {
			logger.info("开始创建文件夹");
			File mkdirFile = new File(mkdirpath);
			if (!mkdirFile.exists()) {
				mkdirFile.mkdirs();
				mkdirFile.setReadable(true);
				mkdirFile.setWritable(true);
				mkdirFile.setExecutable(true);
			}
			logger.info("文件夹创建成功");
			logger.info("开始创建文件");
			//设置权限
			//Runtime.getRuntime().exec("chmod 777 "+downLoadPath);
			File newFile = new File(downLoadPath);

			// 如果文件不存在，就创建新的文件
			if (!newFile.exists()) {

				logger.info("文件不存在,开始创建文件");
				newFile.createNewFile();
				newFile.setReadable(true);
				newFile.setWritable(true);
				newFile.setExecutable(true);

			}
			logger.info("文件创建成功");
			fos = new FileOutputStream(downLoadPath);
			logger.info("开始写入文件");
			success = ftpClient.retrieveFile(new String(newFilePath.getBytes(strencoding), "iso-8859-1"),fos);
			if (!success) {
				throw new Exception("FTP客户端下载出错");
			}
		} catch (IOException e) {
			logger.error(e);
			e.printStackTrace();

			// TODO: handle exception
			throw new Exception("FTP客户端下载出错");
		} finally {
			try {
				this.closeServer();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				logger.debug("关闭异常");
			}
		}
		return success;
	}
    @Override
    public InputStream fileInputStream(String filePath) throws IOException {
        return ftpClient.retrieveFileStream(new String(filePath.getBytes(strencoding),"iso-8859-1"));
    }

    @Override
    public void downloadFile(String downDir, String fileName, String filePath)throws Exception {
        FileOutputStream fos = null;
        String downLoad_Path = downDir + "/"+ fileName;// 下载文件的位置
        try {
            System.out.printf("downDir:%s,fileName:%s,filePath:%s%n", downDir, fileName, filePath);
            fos = new FileOutputStream(downLoad_Path);
            ftpClient.retrieveFile(new String(filePath.getBytes(strencoding),"iso-8859-1"), fos);
        } catch (IOException e) {
            throw new Exception("FTP客户端下载出错");
        } finally {
            try {
                this.closeServer();
            } catch (Exception e) {
                logger.debug("关闭异常");
            }
        }
    }
    
    
    public static void main(String[] args) {
        String fileName = "book.dtd";
        String downLoadPath = ServerEndPoint.ftpDownload_Path + "/"+ fileName;// 下载文件的位置

            
            if(!new File(ServerEndPoint.ftpDownload_Path).exists()){
                new File(ServerEndPoint.ftpDownload_Path).mkdirs();
            }
            File newFile = new File(downLoadPath);
            // 如果文件不存在，就创建新的文件
            if(!newFile.exists()){
                try {
                    newFile.createNewFile();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
            System.out.println("successfully!");
    }

	@Override
	public List<String> readFile(DocExtractSquid docExtractSquid)
			throws ParseException, IOException {
		// TODO Auto-generated method stub
		return null;
	}


	/**
	 * @param fileName
	 * @return function:从服务器上读取指定的文件内容
	 * @throws ParseException
	 * @throws IOException
	 */
	@Override
	public String readTxtFile(String fileName, int length) throws IOException,Exception
	{
		InputStream ins = null;
		StringBuilder builder =new StringBuilder();
		BufferedInputStream bufferedStream=null;
		BOMInputStream bomInputStream=null;
		String charset=null;
		try {
			if (StringUtils.isBlank(fileName)) {
				throw new ErrorMessageException(MessageCode.ERR_FILENAME.value());
			}
			// 从服务器上读取指定的文件
			ins = ftpClient.retrieveFileStream(new String(fileName.getBytes(strencoding),"iso-8859-1"));
			if (null == ins) {
				throw new ErrorMessageException(MessageCode.ERR_FTPFILE.value());
			}
			//文件编码探测，解决各种编码格式引起的乱码问题
			CharsetDetector charsetDetector = new CharsetDetector();
			bomInputStream=new BOMInputStream(ins);
			bufferedStream = new BufferedInputStream(bomInputStream,ins.available());
			//获取文件编码格式
			charset = charsetDetector.detect(bufferedStream, bufferedStream.available() > 1024 * 1024 ? 1024 * 1024 : bufferedStream.available());
			byte[] bytes=null;
			int len =bufferedStream.available();
			//如果文件的长度大于规定的长度，则只读规定长度。
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
			}
			// 主动调用一次getReply()把接下来的226消费掉. 这样做是可以解决这个返回null问题
			//ftpClient.getReply();
		} catch (IOException e) {
			e.printStackTrace();
			throw new Exception("读取文件异常!",e);
		}finally {
			if(bufferedStream!=null){
				bufferedStream.close();
			}
			if(bomInputStream!=null){
				bomInputStream.close();
			}
			if (ins != null) {
				ins.close();
			}
		}
		return builder.toString();
	}

	/**
	 * 读取Office文件内容
	 * @param fileName
	 * @return
	 * @throws IOException
	 * @throws Exception
	 */
	public String readOfficeFile(String fileName) throws IOException,Exception{
		InputStream ins = null;
		String returnStr =null;
		try {
			if (StringUtils.isBlank(fileName)) {
				throw new Exception("读取的文件名为空");
			}
			// 从服务器上读取指定的文件
			ins = ftpClient.retrieveFileStream(new String(fileName.getBytes(strencoding),"iso-8859-1"));
			if (null == ins) {
				throw new Exception("该路径下无该文件,或者文件类型不支持");
			}
			//调用统一读取office文件方法
			FileFolderAdapterImpl folderAdapter=new FileFolderAdapterImpl();
			//returnStr=folderAdapter.readOffice(ins,fileName);
		}catch (Exception e){
			e.printStackTrace();
			throw new Exception("文件读取失败!");
		}finally {
			if(ins!=null){
				ins.close();
			}
		}
		return returnStr;

	}

	/**
	 * 下载文件
	 * @param filePath
	 * @param fileName
	 * @return
	 * @throws Exception
	 */
    public String downloadFtpFileAndRead(String filePath, String fileName,int length) throws Exception {
        FileOutputStream fos = null;
        String outFileContent = "";
        String exString = FileUtils.getFileEx(fileName);
        //新生成的文件名
        String newFileName = com.eurlanda.datashire.utility.StringUtils.isNotBlank(exString) ?
                UUID.randomUUID().toString() + "." + exString :
                UUID.randomUUID().toString();
        String downLoad_Path = RemoteFileAdapter.ftpDownload_Path + "/" + newFileName;// 下载文件的位置
        try {
        	//创建文件夹
            RemoteFileAdapter.createDir(RemoteFileAdapter.ftpDownload_Path);
            fos = new FileOutputStream(downLoad_Path);
            //使用老文件名从指定服务器下载文件
            String newFilePath = filePath + "/" + fileName;
            boolean success = ftpClient.retrieveFile(new String(newFilePath.getBytes(strencoding), "iso-8859-1"), fos);
            // 读取下载的文件
            FileFolderAdapterImpl  fileAdapter = new FileFolderAdapterImpl();
            //调用统一读取office方法  0:因为这边用到的流没有关闭，在读取office时删除临时文件时删除不掉的。只有等所有的流都关闭了才可以删除文件。
            outFileContent = fileAdapter.readOffice(downLoad_Path,newFileName, 0,length);
        } catch (Exception e){
			e.printStackTrace();
			throw new Exception(e);
		}finally {
            this.closeServer();
            if(fos!=null){
                fos.close();
            }
            //删除下载的临时文件
			RemoteFileAdapter.deleteFile(downLoad_Path);
        }
        return outFileContent;
    }


    /**
     * 读取xml文件。返回文件流
     * @param fileName
     * @return
     * @throws Exception
     */
	public InputStream getReadXml(String fileName)throws Exception{
		InputStream inputStream=null;
		try {
			// 从服务器上读取指定的文件
			inputStream = ftpClient.retrieveFileStream(new String(fileName.getBytes(strencoding),"iso-8859-1"));
		}catch (Exception e){
			e.printStackTrace();
			throw new Exception("读取文件失败!",e);
		}
		return inputStream;
		}


}
