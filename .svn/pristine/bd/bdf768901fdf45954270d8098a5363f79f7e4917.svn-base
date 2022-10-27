package com.eurlanda.datashire.utility;

import cn.com.jsoft.jframe.utils.FileUtils;
import com.eurlanda.datashire.adapter.FtpAdapter;
import com.eurlanda.datashire.entity.DocExtractSquid;
import com.eurlanda.datashire.entity.FileInfo;
import com.eurlanda.datashire.entity.FtpSquid;
import com.eurlanda.datashire.entity.SourceTable;
import com.eurlanda.datashire.enumeration.EncodingType;
import com.eurlanda.datashire.enumeration.ExtractFileType;
import com.eurlanda.datashire.socket.ServerEndPoint;
import org.apache.log4j.Logger;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.openxml4j.opc.PackageAccess;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class FtpUtils extends ExtractUtilityBase {
	private static Logger logger = Logger.getLogger(FtpUtils.class);// 记录日志
	FtpAdapter ftpAdapter = new FtpAdapter();

	/**
	 * 通过FTP浏览文件夹下面的所有文件。 返回该节点定义的目录下面的所有文件和目录
	 * @throws Exception 
	 */
	public List<FileInfo> browseftpFile(FtpSquid ftpConnection) throws Exception {
		List<FileInfo> fileInfos = new ArrayList<FileInfo>();
		try {

			if(ftpConnection.getAllowanonymous_flag()==1){
				ftpAdapter.setUserName("anonymous");
			}else{
				ftpAdapter.setUserName(ftpConnection.getUser_name());
				ftpAdapter.setUserPwd(ftpConnection.getPassword());
			}
			ftpAdapter.setPath(ftpConnection.getFile_path());
			String[] hostAndIP = ftpConnection.getHost().split(":");
			int port = 21;
			String ip = ftpConnection.getHost();
			if (hostAndIP.length == 2){
				ip = hostAndIP[0];
				port = Integer.valueOf(hostAndIP[1]);
			} else if (hostAndIP.length != 1){
				throw new Exception("IP与端口格式不正确");
			}
			ftpAdapter.setIp(ip);
			ftpAdapter.setPort(port);
			ftpAdapter.setFileType(ftpConnection.getFilter());
			ftpAdapter.setStrencoding(EncodingType.toFtpEncoding((ftpConnection.getEncoding()==3 || ftpConnection.getEncoding()==1) ? 2 : ftpConnection.getEncoding()));
			ftpAdapter.reSet();
			int depth= ftpConnection.getIncluding_subfolders_flag() == 1 ? ftpConnection.getMax_travel_depth() : 1;
			String path=ftpConnection.getFile_path();
			fileInfos = ftpAdapter.getFileList(path,depth);
			return fileInfos;
		} catch (Exception e) {
			// TODO: handle exception
			throw new Exception("游览FTP节点信息异常");
		}
	}
	/**
	 * 读取文件中的内容
	 * @return
	 */
	public String getFtpFileContent(FtpSquid ftpSquid,String  fileName,int doc_format) throws Exception{
		// TODO Auto-generated method stub
		String fileContent = "";
		CreateFileUtil.createDir(ServerEndPoint.ftpDownload_Path);
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
		//文档类型是将整个文件下载后并读取，文本是下载一部分
		String exString = FileUtils.getFileEx(fileName);
		if (".doc|.docx|.xls|.xlsx|.pdf".contains(exString) && com.eurlanda.datashire.utility.StringUtils.isNotEmpty(exString))
			fileContent = ftpAdapter.downloadFileAndRead(ftpSquid.getFile_path(), fileName,doc_format);
		else
			fileContent = ftpAdapter.readFile(ftpSquid.getFile_path() + "/" + fileName);

		return fileContent;
	}
	
	/**
	 * 获得FtpFile在本地文件path
	 * @param ftpSquid
	 * @param tableName
	 * @return
	 * @author bo.dang
	 * @throws Exception 
	 * @date 2014年5月14日
	 */
	public String getFtpFilePath(FtpSquid ftpSquid, String tableName) throws Exception{
	       String filePath = ftpSquid.getFile_path() + "/" + tableName;
	        try {
	            ftpAdapter.setUserName(ftpSquid.getUser_name());
	            ftpAdapter.setUserPwd(ftpSquid.getPassword());
//	          System.out.println(node.getAttributeMap().get(
//	                  "FileName").length()+1);
	            ftpAdapter.setPath(filePath);
				String[] hostAndIP = ftpSquid.getHost().split(":");
				String ip= ftpSquid.getHost();
				int port = 21;
				if (hostAndIP.length == 2){
					ip = hostAndIP[0];
					port = Integer.valueOf(hostAndIP[1]);
				} else if (hostAndIP.length != 1){
					throw new Exception("IP与端口格式不正确");
				}
				ftpAdapter.setIp(ip);//ip
				ftpAdapter.setPort(port);
	            ftpAdapter.reSet();
	            filePath = ftpAdapter.downloadFile(filePath, tableName);
	        } catch (Exception e) {
	            // TODO: handle exception
	            throw new Exception("读取FTP文件信息异常");
	        }
	        return filePath;
	}
	/**
	 *读取execle内容
	 * @return
	 * @throws Exception 
	 */
	public List<String> getExcelFileFromFTP(FtpSquid ftpSquid, String  fileName, DocExtractSquid docExtractSquid) throws Exception
	{
		List<String> list=new ArrayList<String>();
		CreateFileUtil.createDir(ServerEndPoint.ftpDownload_Path);
		this.downloadFtpFile(ftpSquid, fileName);
		String path = ServerEndPoint.ftpDownload_Path + "/" + fileName;
		String csvPath = null;
		if(docExtractSquid.getDoc_format() == ExtractFileType.XLS.value()) {
			/*InputStream in = new FileInputStream(ServerEndPoint.ftpDownload_Path + "/" + fileName);
			BOMInputStream bomInputStream = new BOMInputStream(in);
			HSSFWorkbook workbook = new HSSFWorkbook(bomInputStream);
			ExcelExtractor extractor = new ExcelExtractor(workbook); 
			extractor.setFormulasNotResults(true);
			extractor.setIncludeSheetNames(false);
			in.close();
			list = super.getXlsValues(list, docExtractSquid, extractor);*/
			csvPath = path.substring(0,path.indexOf(".xls"))+".csv";
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
			/*XSSFExcelExtractor xssfExcelExtractor=new XSSFExcelExtractor(ServerEndPoint.ftpDownload_Path + "/" + fileName);
			xssfExcelExtractor.setFormulasNotResults(true);
			xssfExcelExtractor.setIncludeSheetNames(false);
			list = this.getXlsxValues(list, docExtractSquid, xssfExcelExtractor);
			xssfExcelExtractor.close();*/
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
		boolean flag = CreateFileUtil.deleteFile(path);
		CreateFileUtil.deleteFile(csvPath);
		return list;
	}

	/**
	 * 下载文件
	 * @param ftpSquid
	 * @param fileName
	 * @return
	 * @throws Exception 
	 */
	public boolean downloadFtpFile(FtpSquid ftpSquid,String fileName) throws Exception
	{
		boolean success=true;
		String host = ftpSquid.getHost();
		int port = 21;
		String[] hostAndIp = host.split(":");
		if (hostAndIp.length == 2){
			host = hostAndIp[0];
			port = Integer.valueOf(hostAndIp[1]);
		} else if (hostAndIp.length != 1){
			throw new Exception("IP与端口格式不正确");
		}
		ftpAdapter.setIp(host);
		ftpAdapter.setPort(port);
		ftpAdapter.setUserName(ftpSquid.getUser_name());
		ftpAdapter.setUserPwd(ftpSquid.getPassword());
		ftpAdapter.setPath(ftpSquid.getFile_path());
		ftpAdapter.reSet();
		success=ftpAdapter.downloadFtpFile(ftpSquid.getFile_path(), fileName);
		return success;
	}

	/**
	 * file到sourceTable的转换
	 * 
	 * @param fileInfos
	 * @return
	 */
	public List<SourceTable> converFile2Source(List<FileInfo> fileInfos)throws Exception {
		List<SourceTable> list = new ArrayList<SourceTable>();
		for (int i = 0; i < fileInfos.size(); i++) {
			SourceTable sourceTable = new SourceTable();
			sourceTable.setTableName(fileInfos.get(i).getFileName());
			if(fileInfos.get(i).isIs_directory())
			{
				sourceTable.setIs_directory(true);
			}else
			{
				sourceTable.setIs_directory(false);
			}
			list.add(sourceTable);
		}
		return list;

	}

	/**
	 * 对路径进行处理
	 * @param filePath
	 * @return
	 */
	public static String replacePath(String filePath)
	{
		if("\\".equals(filePath.substring(filePath.length()-1)))
		{
			filePath=filePath.substring(0,filePath.length()-1);
		}else if("/".equals(filePath.substring(filePath.length()-1)))
		{
			filePath=filePath.substring(0,filePath.length()-1);
		}
		return filePath;
	} 
	public static void main(String[] args) throws Exception {
		FtpUtils ftpUtils=new FtpUtils();
		FtpSquid ftpSquid=new FtpSquid();
		ftpSquid.setHost("192.168.137.1");
		ftpSquid.setPassword("datashire");
		ftpSquid.setUser_name("eurlanda1");
		ftpSquid.setFile_path("/TestData");
		DocExtractSquid docExtractSquid=new DocExtractSquid();
		docExtractSquid.setTable_name("花菜.txt");
		List<FileInfo> fileInfos=ftpUtils.browseftpFile(ftpSquid);
		for(FileInfo fileInfo:fileInfos)
		{
			System.out.println(fileInfo.getFileName());
		}
		//读取文件内容
		//System.out.println(ftpUtils.getFtpFileContent(ftpSquid, docExtractSquid));
	}
}
