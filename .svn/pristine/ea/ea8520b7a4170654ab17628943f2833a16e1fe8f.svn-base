package com.eurlanda.datashire.utility;

import cn.com.jsoft.jframe.utils.FileUtils;
import com.eurlanda.datashire.adapter.FileAdapter;
import com.eurlanda.datashire.entity.DocExtractSquid;
import com.eurlanda.datashire.entity.FileInfo;
import com.eurlanda.datashire.entity.FtpSquid;
import com.eurlanda.datashire.enumeration.ExtractFileType;
import com.eurlanda.datashire.socket.ServerEndPoint;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.log4j.Logger;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.openxml4j.opc.PackageAccess;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

/**
 * sftp连接处理类
 * @author lei.bin
 *
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
			if (hostAndIP.length == 2){
				port = Integer.valueOf(hostAndIP[hostAndIP.length -1]);
				host = hostAndIP[0];
			} else if (hostAndIP.length != 1){
				throw new Exception("IP与端口格式不正确");
			}
			String username = ftpConnection.getUser_name();
			String password = ftpConnection.getPassword();

			//com.jcraft.jsch.Logger sftpLogger = new SettleLogger();//日志
			//JSch.setLogger(sftpLogger);
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

	public List<String> getExcelFileContentFromSFTP(FtpSquid ftpSquid, String fileName, DocExtractSquid docExtractSquid) throws Exception {
		CreateFileUtil.createDir(ServerEndPoint.ftpDownload_Path);
		List<String> list = new ArrayList<String>();
		String downLoad_Path = ServerEndPoint.ftpDownload_Path + "/" + fileName;
		ChannelSftp sftp = this.connect(ftpSquid);
		this.download(ftpSquid.getFile_path(), fileName, downLoad_Path, sftp, EncodingUtils.getEncoding(ftpSquid.getEncoding()));
		String path = ServerEndPoint.ftpDownload_Path + "/" + fileName;
		String csvPath = "";
		if (docExtractSquid.getDoc_format() == ExtractFileType.XLS.value()) {
			/*InputStream in = new FileInputStream(ServerEndPoint.ftpDownload_Path + "/" + fileName);
			BOMInputStream bomInputStream = new BOMInputStream(in);
			HSSFWorkbook workbook = new HSSFWorkbook(bomInputStream);
			ExcelExtractor extractor = new ExcelExtractor(workbook);
			extractor.setFormulasNotResults(true);
			extractor.setIncludeSheetNames(false);
			in.close();
			list = this.getXlsValues(list, docExtractSquid, extractor);*/
			XLS2CSV xls2csv = new XLS2CSV(path,csvPath);
			xls2csv.process();
			FileInputStream stream = new FileInputStream(csvPath);
			BufferedReader readerBuffer = new BufferedReader(new InputStreamReader(stream,"utf-8"));
			int count = 0;
			int lineCount = 0;
			int headerRowNo = docExtractSquid.getHeader_row_no()-1;// 列信息所在行
			int firstDataRowNo = docExtractSquid.getFirst_data_row_no()-1;// 值属性所在行
			int endDataRowNo = firstDataRowNo + 50;
			String line="";
			while ((line = readerBuffer.readLine()) != null) {
				if (lineCount >= endDataRowNo)
					break;
				if (lineCount == headerRowNo) {
					list.add(line);
				} else if (lineCount != headerRowNo && lineCount <= endDataRowNo) {
					if(lineCount>=firstDataRowNo) {
						list.add(line);
					}
				}
				lineCount++;
			}
			//return list;
		} else if (docExtractSquid.getDoc_format() == ExtractFileType.XLSX.value()) {
			/*XSSFExcelExtractor xssfExcelExtractor = new XSSFExcelExtractor(ServerEndPoint.ftpDownload_Path + "/" + fileName);
			xssfExcelExtractor.setFormulasNotResults(true);
			xssfExcelExtractor.setIncludeSheetNames(false);
			xssfExcelExtractor.close();
			list = this.getXlsxValues(list, docExtractSquid, xssfExcelExtractor);*/
			csvPath=path.substring(0,path.indexOf(".xlsx"))+".csv";
			OPCPackage p = OPCPackage.open(path, PackageAccess.READ);
			File file = new File(csvPath);
			if(!file.exists()){
				file.createNewFile();
			}
			FileOutputStream outStream = new FileOutputStream(file);
			PrintStream out = new PrintStream(outStream,false,"utf-8");
			XLSX2CSV xlsx2csv = new XLSX2CSV(p, out, 10);
			xlsx2csv.process();
			p.close();
			out.flush();
			out.close();
			//读取csv文件8
			FileInputStream input = new FileInputStream(file);
			BufferedReader readerBuffer = new BufferedReader(new InputStreamReader(input));
			int count = 0;
			int lineCount = 0;
			int headerRowNo = docExtractSquid.getHeader_row_no()-1;// 列信息所在行
			int firstDataRowNo = docExtractSquid.getFirst_data_row_no()-1;// 值属性所在行
			int endDataRowNo = firstDataRowNo + 50;
			String line="";
			while ((line = readerBuffer.readLine()) != null) {
				if (lineCount >= endDataRowNo)
					break;
				if (lineCount == headerRowNo) {
					list.add(line);
				} else if (lineCount != headerRowNo && lineCount <= endDataRowNo) {
					if(lineCount>=firstDataRowNo) {
						list.add(line);
					}
				}
				lineCount++;
			}
			//return list;
		}
		CreateFileUtil.deleteFile(path);
		CreateFileUtil.deleteFile(csvPath);
		return list;
	}

	/**
	 * 读取文件的全部内容
	 * @param ftpSquid
	 * @param fileName
	 * @param doc_format
	 * @return
	 * @throws Exception
	 */
	public String getSftpFileContent(FtpSquid ftpSquid,String fileName,int doc_format) throws Exception
	{
		String outputFileContent = "";
		String downLoad_Path=ServerEndPoint.ftpDownload_Path + "/"+ fileName;
		CreateFileUtil.createDir(ServerEndPoint.ftpDownload_Path);

		String exString = FileUtils.getFileEx(fileName);
		//按照不同的文件类型做不同的处理
		if (".doc|.docx|.xls|.xlsx|.pdf".contains(exString) && com.eurlanda.datashire.utility.StringUtils.isNotEmpty(exString))
			outputFileContent = downloadAndReadFromSFTPServer(ftpSquid, fileName, downLoad_Path, doc_format);
		else
			outputFileContent = downloadAPartAndReadFromSFTPServer(ftpSquid, fileName, downLoad_Path, doc_format);
		return outputFileContent;
	}

	/**
	 * 按照文本的方式读取签50行数据
	 * @param ftpSquid
	 * @param fileName
	 * @param downLoad_Path
	 * @param doc_format
	 * @return
     * @throws Exception
     */
	private String downloadAPartAndReadFromSFTPServer(FtpSquid ftpSquid,String fileName, String downLoad_Path, int doc_format) throws Exception {
		//下载文件
		ChannelSftp sftp=this.connect(ftpSquid);

		sftp.cd(ftpSquid.getFile_path());
		//非常奇怪，设置utf-8可以获取到文件，设置gbk却会报异常,有时设置gbk正常，其他编码的不正常
		sftp.setFilenameEncoding(EncodingUtils.getEncoding(ftpSquid.getEncoding()));
		InputStream fileIs = sftp.get(fileName);
		if (fileIs == null)
			throw new Exception("下载sftp文件失败");
		//开始按行读数据
		BufferedReader reader = new BufferedReader(new InputStreamReader(new BOMInputStream(fileIs), EncodingUtils.getEncoding(ftpSquid.getEncoding())));
		String line = "";
		StringBuilder builder = new StringBuilder(150);
		int lineNumber = 0;
		while ((line = reader.readLine()) != null) {
			builder.append(line);
			builder.append("\r\n");
			lineNumber ++;
			if (lineNumber > 50)
				break;
		}
		reader.close();
		if(fileIs != null)
			fileIs.close();
		sftp.disconnect();

		//读取文件内容
		FileAdapter fileAdapter=new FileAdapter();

		return builder.toString();
	}

	/**
	 * 现在整个文件并读取内容
	 * @param ftpSquid
	 * @param fileName
	 * @param downLoad_Path
	 * @param doc_format
	 * @return
     * @throws Exception
     */
	private String downloadAndReadFromSFTPServer(FtpSquid ftpSquid,String fileName, String downLoad_Path, int doc_format) throws Exception {
		String outFileContent="";
		boolean flag=false;
		//下载文件
		ChannelSftp sftp=this.connect(ftpSquid);
		this.download(ftpSquid.getFile_path(), fileName, downLoad_Path, sftp, EncodingUtils.getEncoding(ftpSquid.getEncoding()));
		//读取文件内容
		FileAdapter fileAdapter=new FileAdapter();
		outFileContent = fileAdapter.getFileContent(fileName, downLoad_Path,doc_format);
		flag=CreateFileUtil.deleteFile(ServerEndPoint.ftpDownload_Path + "/" + fileName);
		if (!flag)
			logger.error("删除sftp下载文件失败");
		return outFileContent;
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
		try {
			sftp.cd(directory);
			File file = new File(saveFile);
			sftp.setFilenameEncoding(enCoding);
			sftp.get(downloadFile, new FileOutputStream(file));
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception("sftp下载文件失败");
		} finally {
			this.closeChannel();
		}
	}

	/**
	* 列出目录下的文件
	* @param directory 要列出的目录
	* @param sftp
	* @param filter 过滤条件
	* @return
	* @throws SftpException
	*/
    public List<FileInfo> listFiles(FtpSquid ftpConnection, int Max_travel_depth, int currentPath, List<FileInfo> fileInfos, String directory, ChannelSftp sftp, String filter) throws Exception {
        if(Max_travel_depth!=0 && Max_travel_depth==currentPath){
			return fileInfos;
		}
        //保存根目录
        String finalPath = ftpConnection.getFile_path();
        System.out.println(directory);
		sftp.setFilenameEncoding("UTF-8");
		Vector<LsEntry> lsEntries = null;
		try {
			//sftp.cd(directory);
			lsEntries=sftp.ls(directory);
		} catch(SftpException e){
			if(sftp.SSH_FX_NO_SUCH_FILE == e.id){
				lsEntries=new Vector<>();
			}
		}
		currentPath++;
		FilterUtil util=new FilterUtil(filter);
		for(int i=0;i<lsEntries.size();i++){
			LsEntry lsEntry = lsEntries.get(i);
			FileInfo fileInfo = new FileInfo();
			//String longName = new String(lsEntry.getLongname().getBytes("UTF-8"),"UTF-8");
			String longName = lsEntry.getLongname();
            if (ftpConnection.getIncluding_subfolders_flag() == 1 && longName.substring(0, 1).equals("d")) {
                //递归子目录
				if("/".equals(directory)){
					directory="";
				}
				String fileName = lsEntry.getFilename();
				if(fileName.indexOf(".")==0 || fileName.indexOf("..")==0){
					continue;
				}
                listFiles(ftpConnection, Max_travel_depth, currentPath, fileInfos, directory + "/" + fileName, sftp, filter);
            } else if(longName.substring(0,1).equals("-")){
				String fileName = lsEntry.getFilename();
				if(util.check(fileName)) {
//					fileInfo.setFilePath(directory + "/" + fileName);
//					fileInfo.setFileName(directory + "/" + fileName);
                    fileInfo.setFilePath(directory + "/" + fileName);
                    fileInfo.setFileName(replacePath(finalPath, directory + "/" + fileName));
                    fileInfo.setIs_directory(false);
					fileInfos.add(fileInfo);
				}
			}
		}
		return fileInfos;
	}

    /**
     * 替换路径
     *
     * @param path_one 用户输入的路径
     * @param path_two 文件所在的标准路径
     * @return
     */
    public static String replacePath(String path_one, String path_two) {
        return path_two.replaceAll("\\\\", "/").substring(path_one.replaceAll("\\\\", "/").length() + 1, path_two.length());
    }

    /**
     * 上传文件 
     * @param directory 上传的目录 
     * @param uploadFile 要上传的文件 
     * @param sftp 
	 * @throws Exception 
     */  
    public void upload(String directory, String uploadFile, ChannelSftp sftp) throws Exception {  
        try {  
            sftp.cd(directory);  
            sftp.setFilenameEncoding("UTF-8");
            File file=new File(uploadFile);
            System.out.println(file.getName());
            sftp.put(new FileInputStream(file), file.getName());  
        } catch (Exception e) {  
            e.printStackTrace(); 
            throw new Exception("上传文件失败");
        }  
    }
}
