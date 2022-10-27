package com.eurlanda.datashire.utility;

import cn.com.jsoft.jframe.utils.CharsetDetector;
import cn.com.jsoft.jframe.utils.FileUtils;
import cn.com.jsoft.jframe.utils.fileParsers.OfficeFileParser;
import cn.com.jsoft.jframe.utils.fileParsers.TxtFileParser;
import com.eurlanda.datashire.adapter.FileAdapter;
import com.eurlanda.datashire.entity.DocExtractSquid;
import com.eurlanda.datashire.entity.FileFolderSquid;
import com.eurlanda.datashire.entity.FileInfo;
import com.eurlanda.datashire.enumeration.ExtractFileType;
import com.eurlanda.datashire.exception.SystemErrorException;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.log4j.Logger;
import org.apache.pdfbox.pdfparser.PDFParser;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.util.PDFTextStripper;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.openxml4j.opc.PackageAccess;

import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * file文件工具处理类
 *
 * @author eurlanda01
 *
 */
public class FileFolderUtils extends ExtractUtilityBase {
	FileAdapter fileAdapter = new FileAdapter();
	public static final Logger logger = Logger.getLogger(FileFolderUtils.class);
	String finalPath = "";
	/**
	 * 读取指定路径下的文件列表
	 *
	 * @param fileFolderConnection
	 * @return
	 */
	public List<FileInfo> browseFile(FileFolderSquid fileFolderConnection) {
		// TODO Auto-generated method stub
		String path = "";
		String filters = "";
		List<FileInfo> fileInfos = new ArrayList<FileInfo>();
		try {
			// 根据路径和过滤文件类型查询文件列表
			if (StringUtils.isNotBlank(fileFolderConnection.getFile_path())) {
				path = fileFolderConnection.getFile_path();// 路径
			}
			if (StringUtils.isNotBlank(fileFolderConnection.getFilter())) {
				filters = fileFolderConnection.getFilter();// 过滤条件
			}
			finalPath = path;
			showDir(fileInfos, path, filters,
					fileFolderConnection.getIncluding_subfolders_flag() ==1? fileFolderConnection.getMax_travel_depth() : 1, -1);

		} catch (Exception e) {
			// TODO: handle exception
			throw new SystemErrorException(MessageCode.ERR_FILE, "游览文件信息异常");
		}
		return fileInfos;
	}
	/**
	 * 遍历文件夹
	 * @param fileInfos
	 * @param path
	 * @param filters
	 * @param depth 用户设置的最大深度,0代表无穷
	 * @param currentDepth 当前遍历的深度
	 * @throws Exception
	 */
	public void showDir(List<FileInfo> fileInfos, String path, String filters, int depth, int currentDepth) throws Exception {
		currentDepth++;
		if (depth!=0&&depth==currentDepth) {
			return;
		}
		String tempName = null;
		// 判断目录是否存在
		File baseDir = new File(path);
		if (!baseDir.exists() || !baseDir.isDirectory()) {
			throw new SystemErrorException(MessageCode.ERR_PATH, "输入的路径有误");
		} else {
			String[] filelist = baseDir.list();
			for (int i = 0; i < filelist.length; i++) {
				FileInfo fileInfo = new FileInfo();
				File readfile = new File(path + "\\" + filelist[i]);
				// System.out.println(readfile.getName());
				if (!readfile.isDirectory()) {
					tempName = readfile.getName();
					if (StringUtils.isNotBlank(filters)) {
						FilterUtil filter = new FilterUtil(filters);
						if (filter.check(tempName)) {
							fileInfo.setFileName(replacePath(finalPath,
									readfile.getAbsolutePath()));
							fileInfo.setIs_directory(false);
							fileInfos.add(fileInfo);
						}
					} else {
						fileInfo.setFileName(replacePath(finalPath,
								readfile.getAbsolutePath()));
						fileInfo.setIs_directory(false);
						fileInfos.add(fileInfo);
					}
				} else if (readfile.isDirectory()) {
					showDir(fileInfos, path + "\\" + filelist[i], filters,
							depth, currentDepth);
				}
			}
		}
		/*
		 * depth--; fileAdapter.refreshList(); while (fileAdapter.nextFile()) {
		 * FileInfo fileInfo = new FileInfo() ; if (!fileAdapter.isDirectory())
		 * { if (StringUtils.isNotBlank(fileAdapter.getFileType())) { FilterUtil
		 * filter = new FilterUtil(fileAdapter.getFileType()); if
		 * (filter.check(fileAdapter.getFileName())) {
		 * fileInfo.setFileName(fileAdapter.getFileName());
		 * fileInfo.setIs_directory(false); fileInfos.add(fileInfo); } }else {
		 * fileInfo.setFileName(fileAdapter.getFileName());
		 * fileInfo.setIs_directory(false); fileInfos.add(fileInfo); } }else
		 * {//文件夹 if(depth<=0) { continue; }
		 * System.out.println("路径为"+fileAdapter.getfilePath());
		 * System.out.println("文件夹名字为"+fileAdapter.getFileName());
		 * System.out.println("到该级目录的长度"+fileInfos.size());
		 * fileAdapter.setPath(fileAdapter.getfilePath());
		 * this.showDir(fileInfos, depth);
		 * fileInfo.setFileName(fileAdapter.getFileName());
		 * fileInfo.setIs_directory(true); fileInfos.add(fileInfo); } }
		 */
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
		return path_two.replaceAll("\\\\", "/").replaceAll(
				path_one.replaceAll("\\\\", "/"), "");
	}

	/**
	 * 读取指定路径的文件内容(区分不同的文件类型)
	 *
	 * @return
	 * @throws Exception
	 */
	public String getFileContent(String filePath, String fileName, int doc_format) throws Exception {
		if (doc_format == -1) {
			return this.getContentByName(filePath, fileName);
		} else {
			return this.getContentByType(filePath, fileName, doc_format);
		}
	}

	/**
	 * 根据文件类型读取内容
	 *
	 * @param filePath
	 * @param fileName
	 * @param doc_format
	 * @return
	 * @throws Exception
	 */
	private String getContentByType(String filePath, String fileName,
									int doc_format) throws Exception {
		fileName= fileName.substring(fileName.lastIndexOf("/")+1,fileName.length());
		// office类型
		if (doc_format == ExtractFileType.DOC.value()
				|| doc_format == ExtractFileType.DOCX.value()) {
			try {
				String text = null;
				File file = new File(FileFolderUtils.replacePath(filePath) + "/"
						+ fileName);
				InputStream in = new FileInputStream(file);
				OfficeFileParser officeFileParser = new OfficeFileParser();
				text = officeFileParser.getStringContent(in);
				return text;
			} catch (Exception e){
				e.printStackTrace();
				logger.error(e);
				throw e;
			}
		} else if (doc_format == ExtractFileType.XLS.value()) {
			/*InputStream in = new FileInputStream(
					FileFolderUtils.replacePath(filePath) + "/" + fileName);
			HSSFWorkbook workbook = new HSSFWorkbook(in);
			ExcelExtractor extractor = new ExcelExtractor(workbook);
			extractor.setFormulasNotResults(true);
			extractor.setIncludeSheetNames(false);
			return extractor.getText(100);*/
			String path = FileFolderUtils.replacePath(filePath) + "/" + fileName;
			String csvPath = path.substring(0,path.indexOf(".xls"))+".csv";
			XLS2CSV xls2csv = new XLS2CSV(path,csvPath);
			xls2csv.process();
			FileInputStream stream = new FileInputStream(csvPath);
			//文件格式探测器
			CharsetDetector detector = new CharsetDetector();
			BufferedInputStream bufferStream = new BufferedInputStream(new BOMInputStream(stream));
			if (null == stream) {
				throw new Exception("该路径下无该文件,或者文件类型不支持");}
			BufferedReader readerBuffer = new BufferedReader(new InputStreamReader(
					bufferStream, detector.detect(bufferStream,bufferStream.available()>1024*1024 ? 1024*1024 : bufferStream.available())));
			//BufferedReader readerBuffer = new BufferedReader(new InputStreamReader(stream,"utf-8"));
			int lineCount = 0;
			String line="";
			StringBuffer buffer = new StringBuffer("");
			while ((line = readerBuffer.readLine()) != null) {
				if(lineCount>100){
					break;
				}
				buffer.append(line);
				buffer.append(System.lineSeparator());
				lineCount++;
			}
			//删除csv文件
			boolean flag = CreateFileUtil.deleteFile(csvPath);
			return buffer.toString();
		} else if (doc_format == ExtractFileType.XLSX.value()) {
			/*XSSFExcelExtractor xssfExcelExtractor = new XSSFExcelExtractor(
					FileFolderUtils.replacePath(filePath) + "/" + fileName);
			xssfExcelExtractor.setFormulasNotResults(true);
			xssfExcelExtractor.setIncludeSheetNames(false);
			xssfExcelExtractor.close();
			return xssfExcelExtractor.getText(100);*/
			String path = FileFolderUtils.replacePath(filePath) + "/" + fileName;
			String csvPath = path.substring(0,fileName.indexOf(".xlsx"))+".csv";
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
			CharsetDetector detector = new CharsetDetector();
			BufferedInputStream bufferStream = new BufferedInputStream(new BOMInputStream(input));
			if (null == input) {
				throw new Exception("该路径下无该文件,或者文件类型不支持");}
			BufferedReader readerBuffer = new BufferedReader(new InputStreamReader(
					bufferStream, detector.detect(bufferStream,bufferStream.available()>1024*1024 ? 1024*1024 : bufferStream.available())));
			//BufferedReader readerBuffer = new BufferedReader(new InputStreamReader(input));
			int count = 0;
			int lineCount = 0;
			String line="";
			StringBuffer buffer = new StringBuffer("");
			while ((line = readerBuffer.readLine()) != null) {
				if(lineCount>100){
					break;
				}
				buffer.append(line);
				buffer.append(System.lineSeparator());
				lineCount++;
			}

			//删除csv文件
			boolean flag = CreateFileUtil.deleteFile(csvPath);
			return buffer.toString();
		} else if (doc_format == ExtractFileType.TXT.value()
				|| doc_format == ExtractFileType.LOG.value()
				|| doc_format == ExtractFileType.LCK.value()
				|| doc_format == ExtractFileType.CSV.value()
				|| doc_format == ExtractFileType.XML.value()
				|| doc_format == ExtractFileType.XSD.value()
				|| doc_format == ExtractFileType.DTD.value()) {// 文本类型
			TxtFileParser txtFileParser = new TxtFileParser();
			File file = new File(FileFolderUtils.replacePath(filePath) + "/"
					+ fileName);
			return txtFileParser.getStringContent(file);
		} else if (doc_format == ExtractFileType.PDF.value()) {
			return this.getTextFromPdf(FileFolderUtils.replacePath(filePath)
					+ "/" + fileName);
		}
		return null;
	}

	/**
	 * 根据文件名后最读取文件内容
	 *
	 * @param filePath
	 * @param fileName
	 * @return
	 * @throws Exception
	 */
	private String getContentByName(String filePath, String fileName)
			throws Exception {
		fileName= fileName.substring(fileName.lastIndexOf("/")+1,fileName.length());
		// office类型
		if (StringUtils.isNotBlank(FileUtils.getFileEx(fileName))
				&& ".doc|.docx".contains(FileUtils.getFileEx(fileName))) {
			OfficeFileParser officeFileParser = new OfficeFileParser();
			File file = new File(FileFolderUtils.replacePath(filePath) + "/"
					+ fileName);
			logger.info("文件是否存在Name:"+file.exists());
			return officeFileParser.getStringContent(file);
		} else if (StringUtils.isNotBlank(FileUtils.getFileEx(fileName))
				&& ".xls".contains(FileUtils.getFileEx(fileName))) {
			/*InputStream in = new FileInputStream(
					FileFolderUtils.replacePath(filePath) + "/" + fileName);
			HSSFWorkbook workbook = new HSSFWorkbook(in);
			ExcelExtractor extractor = new ExcelExtractor(workbook);
			extractor.setFormulasNotResults(true);
			extractor.setIncludeSheetNames(false);
			return extractor.getText(100);*/
			String path = FileFolderUtils.replacePath(filePath) + "/" + fileName;
			String csvPath = path.substring(0,path.indexOf(".xls"))+".csv";
			XLS2CSV xls2csv = new XLS2CSV(path,csvPath);
			xls2csv.process();
			FileInputStream stream = new FileInputStream(csvPath);
			//文件格式探测器
			CharsetDetector detector = new CharsetDetector();
			BufferedInputStream bufferStream = new BufferedInputStream(new BOMInputStream(stream));
			if (null == stream) {
				throw new Exception("该路径下无该文件,或者文件类型不支持");}
			BufferedReader readerBuffer = new BufferedReader(new InputStreamReader(
					bufferStream, detector.detect(bufferStream,bufferStream.available()>1024*1024 ? 1024*1024 : bufferStream.available())));
			//BufferedReader readerBuffer = new BufferedReader(new InputStreamReader(stream,"utf-8"));
			int lineCount = 0;
			String line="";
			StringBuffer buffer = new StringBuffer("");
			while ((line = readerBuffer.readLine()) != null) {
				if(lineCount>100){
					break;
				}
				buffer.append(line);
				buffer.append(System.lineSeparator());
				lineCount++;
			}

			//删除csv文件
			boolean flag = CreateFileUtil.deleteFile(csvPath);
			return buffer.toString();
		} else if (StringUtils.isNotBlank(FileUtils.getFileEx(fileName))
				&& ".xlsx".contains(FileUtils.getFileEx(fileName))) {
			/*XSSFExcelExtractor xssfExcelExtractor = new XSSFExcelExtractor(
					FileFolderUtils.replacePath(filePath) + "/" + fileName);
			xssfExcelExtractor.setFormulasNotResults(true);
			xssfExcelExtractor.setIncludeSheetNames(false);
			xssfExcelExtractor.close();
			return xssfExcelExtractor.getText(100);*/
			String path = FileFolderUtils.replacePath(filePath) + "/" + fileName;
			String csvPath = path.substring(0,path.indexOf(".xlsx"))+".csv";
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
			CharsetDetector detector = new CharsetDetector();
			BufferedInputStream bufferStream = new BufferedInputStream(new BOMInputStream(input));
			if (null == input) {
				throw new Exception("该路径下无该文件,或者文件类型不支持");}
			BufferedReader readerBuffer = new BufferedReader(new InputStreamReader(
					bufferStream, detector.detect(bufferStream,bufferStream.available()>1024*1024 ? 1024*1024 : bufferStream.available())));
			//BufferedReader readerBuffer = new BufferedReader(new InputStreamReader(input));
			int count = 0;
			int lineCount = 0;
			String line="";
			StringBuffer buffer = new StringBuffer("");
			while ((line = readerBuffer.readLine()) != null) {
				if(lineCount>100){
					break;
				}
				buffer.append(line);
				buffer.append(System.lineSeparator());
				lineCount++;
			}

			//删除csv文件
			boolean flag = CreateFileUtil.deleteFile(csvPath);
			return buffer.toString();
		} else if (StringUtils.isNotBlank(FileUtils.getFileEx(fileName))
				&& ".txt|.lck|.csv|.xml|.xsd|.dtd".contains(FileUtils
				.getFileEx(fileName))) {// 文本类型
			TxtFileParser txtFileParser = new TxtFileParser();
			File file = new File(FileFolderUtils.replacePath(filePath) + "/"
					+ fileName);
			return txtFileParser.getStringContent(file);
		} else if (StringUtils.isNotBlank(FileUtils.getFileEx(fileName))
				&& ".pdf".contains(FileUtils.getFileEx(fileName))) {
			return this.getTextFromPdf(FileFolderUtils.replacePath(filePath)
					+ "/" + fileName);
		} else {
			/*
			 * TxtFileParser txtFileParser=new TxtFileParser(); File file=new
			 * File(FileFolderUtils.replacePath(filePath)+"/"+fileName); return
			 * txtFileParser.getStringContent(file);
			 */
			return null;
		}
	}

	/**
	 * 读取指定路径的文件内容(区分不同的文件类型)
	 *
	 * @return
	 * @throws Exception
	 */
	public String getFileContent(File file, String fileName, int doc_format)
			throws Exception {
		if (doc_format == -1) {
			// office类型
			if (".doc|.docx".contains(FileUtils.getFileEx(fileName))) {
				OfficeFileParser officeFileParser = new OfficeFileParser();
				return officeFileParser.getStringContent(file);
			} else if (".xls".contains(FileUtils.getFileEx(fileName))) {
				InputStream in = new FileInputStream(file.getPath());
				HSSFWorkbook workbook = new HSSFWorkbook(in);
				ExcelExtractor extractor = new ExcelExtractor(workbook);
				extractor.setFormulasNotResults(true);
				extractor.setIncludeSheetNames(false);
				return extractor.getText();
			} else if (".xlsx".contains(FileUtils.getFileEx(fileName))) {
				XSSFExcelExtractor xssfExcelExtractor = new XSSFExcelExtractor(
						file.getPath());
				xssfExcelExtractor.setFormulasNotResults(true);
				xssfExcelExtractor.setIncludeSheetNames(false);
				xssfExcelExtractor.close();
				return xssfExcelExtractor.getText();
			} else if (".txt|.log|.lck|.csv|.xml|.xsd".contains(FileUtils
					.getFileEx(fileName))) {// 文本类型
				TxtFileParser txtFileParser = new TxtFileParser();
				return txtFileParser.getStringContent(file);
			} else if (".pdf".contains(FileUtils.getFileEx(fileName))) {
				return this.getTextFromPdf(file.getPath());
			} else// 按照文本类型读取
			{
				// 文本类型
				TxtFileParser txtFileParser = new TxtFileParser();
				return txtFileParser.getStringContent(file);
			}
		} else {
			// office类型
			if (doc_format == ExtractFileType.DOC.value()
					|| doc_format == ExtractFileType.DOCX.value()) {
				OfficeFileParser officeFileParser = new OfficeFileParser();
				return officeFileParser.getStringContent(file);
			} else if (doc_format == ExtractFileType.XLS.value()) {
				InputStream in = new FileInputStream(file.getPath());
				HSSFWorkbook workbook = new HSSFWorkbook(in);
				ExcelExtractor extractor = new ExcelExtractor(workbook);
				extractor.setFormulasNotResults(true);
				extractor.setIncludeSheetNames(false);
				return extractor.getText();
			} else if (doc_format == ExtractFileType.XLSX.value()) {
				XSSFExcelExtractor xssfExcelExtractor = new XSSFExcelExtractor(
						file.getPath());
				xssfExcelExtractor.setFormulasNotResults(true);
				xssfExcelExtractor.setIncludeSheetNames(false);
				xssfExcelExtractor.close();
				return xssfExcelExtractor.getText();
			} else if (doc_format == ExtractFileType.TXT.value()
					|| doc_format == ExtractFileType.LOG.value()
					|| doc_format == ExtractFileType.LCK.value()
					|| doc_format == ExtractFileType.CSV.value()
					|| doc_format == ExtractFileType.XML.value()
					|| doc_format == ExtractFileType.XSD.value()
					|| doc_format == ExtractFileType.DTD.value()) {// 文本类型
				TxtFileParser txtFileParser = new TxtFileParser();
				return txtFileParser.getStringContent(file);
			} else if (doc_format == ExtractFileType.PDF.value()) {
				return this.getTextFromPdf(file.getPath());
			}
		}
		return null;
	}

	/**
	 * 根据指定的分隔符读取
	 *
	 * @param ct
	 * @param docExtractSquid
	 * @return
	 */
	public List<String> getDocContent(String ct,DocExtractSquid docExtractSquid, String encoding) {
		String content = ct;
		// 封装ByteArrayInputStream-->InputStreamReader-->BufferedReader
		BufferedReader br = new BufferedReader(new InputStreamReader(
				new ByteArrayInputStream(content.getBytes(Charset.forName(encoding))),
				Charset.forName(encoding)));
		List<String> list=getDocContent(br,docExtractSquid.getRow_delimiter(),docExtractSquid.getRow_delimiter_position(),docExtractSquid.getHeader_row_no() - 1,docExtractSquid.getFirst_data_row_no());
		return list;
	}


	/**
	 * @AUTHOR akachi.tao
	 * @param br 文件流
	 * @param row_delimiter 分隔符
	 * @param row_delimiter_position 分隔符位置
	 * @param header_row_no 标题行号
	 * @param first_data_row_no 数据开始行号
	 * @return
	 */
	public List<String> getDocContent(BufferedReader br,String row_delimiter,int row_delimiter_position,int header_row_no,int first_data_row_no) {
		List<String> list = new ArrayList<String>();
		try {
			String line="";
			int endDataCiybt =  50;//只要获取50条数据。
			int lineCount = 0;
			boolean isFalst=true;
			char[] charArray=new char[1024];
			int j=br.read(charArray);
			while (j!=-1&&lineCount<endDataCiybt) {
				line += new String(new String(charArray, 0, j));
				String[] s = line.split(row_delimiter);
				if (s != null && s.length >= 1) {
					line = "";
					for (int i = 0; i < s.length; i++) {
						if (!(isFalst && row_delimiter_position == 0)) {
							if (i == s.length - 1) {
								line = new String(s[i]);
								if (s[i].length() > 1048576) {
									throw new IOException("row length > 1048576");
								}
							} else {
								if (lineCount == header_row_no) {//如果是标题行号插入到第一行
									list.add(0, new String(s[i]));
								} else if (lineCount >= first_data_row_no - 1) {//开始数据行号之后再开始
									list.add(new String(s[i]));
								}
								//list.add(new String(s[i]));
								lineCount++;
							}

						} else {
							isFalst = false;//如果是分隔符在前并且是第一行则不记录行
						}
					}
				}
				j = br.read(charArray);
			}

			if(line!=null&&line.length()!=0){
				if(lineCount==header_row_no){//如果是标题行号插入到第一行
					list.add(0,line);
				}else if(lineCount>=first_data_row_no-1){//开始数据行号之后再开始
					list.add(line);
				}
				//list.add(line);
			}
			if (list.size()==0){
				list.add("defaultName");
			}
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		return list;
	}

	/**
	 * 根据指定的分隔符读取
	 *
	 * @param ct
	 * @param docExtractSquid
	 * @return
	 */
	public List<String> getPDFContent(String ct,
									  DocExtractSquid docExtractSquid, String encoding) throws Exception {
		List<String> list = new ArrayList<String>();
		List<String> resultList = null;
		encoding = "utf8";
		String content = ct;
		try {
			// 封装ByteArrayInputStream-->InputStreamReader-->BufferedReader
			BufferedReader br = new BufferedReader(new InputStreamReader(
					new ByteArrayInputStream(content.getBytes(Charset
							.forName(encoding))), Charset.forName(encoding)));
			String line;
			int lineCount = 0;
			// System.out.println(ct);
			/*
			 * int headerRowNo = docExtractSquid.getHeader_row_no();// 列信息所在行
			 * int firstDataRowNo = docExtractSquid.getFirst_data_row_no();//
			 * 值属性所在行
			 */int headerRowNo = 0;// 列信息所在行
			int firstDataRowNo = 1;// 值属性所在行
			int endDataRowNo = firstDataRowNo + 50;
			int row_delimiter_position = 0;
			String row_delimiter = "University of Sydney Research Contracts 2010";
			// 结果集
			resultList = new ArrayList<String>();
			// 行记录
			String rowData = null;
			// 转换之后的行记录
			String resultRowData = "";
			// 第一行记录
			String firstRowData = null;

			int count = 0;
			while ((line = br.readLine()) != null) {
				// 如果换行符的位置是在Begin：0
				if (0 == row_delimiter_position) {
					count++;
					// 第一行记录
					if (1 == count) {
						firstRowData = line.trim();
						if (firstRowData.equals(row_delimiter)) {
							continue;
						}

					}
					// 如果第一行记录就是换行符
					/*
					 * if(firstRowData.equals(row_delimiter)){
					 *
					 * }
					 */
					rowData = line.trim();
					if (rowData.equals(row_delimiter)) {
						System.out.println(resultRowData);
						resultList.add(resultRowData);
						resultRowData = "";
					} else {
						resultRowData = resultRowData + " " + rowData;
					}
				}
				// 如果换行符位置在end:1
				else if (1 == row_delimiter_position) {
					rowData = line.trim();
					if (rowData.equals(row_delimiter)) {
						resultList.add(resultRowData);
						resultRowData = "";
					}
					resultRowData = resultRowData + " " + rowData;
				}
			}

			for (int i = 0; i < resultList.size(); i++) {

			}
			/*
			 * while ( (line = br.readLine()) != null ) { if (-1 == headerRowNo)
			 * { if (lineCount >= firstDataRowNo && lineCount < endDataRowNo) {
			 * list.add(line); } } else { if (headerRowNo == lineCount) {
			 * list.add(line); } else { if (lineCount >= firstDataRowNo &&
			 * lineCount < endDataRowNo) { list.add(line); } } } lineCount++; }
			 */
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
			throw new Exception("读取文件异常");
		}
		return resultList;
	}

	/**
	 * 读取前50行
	 *
	 * @param ct
	 * @param encoding
	 * @return
	 */
	public String getPartContent(String ct, String encoding) throws Exception {
		StringBuilder builder = new StringBuilder(200);
		String content = ct;
		try {
			// 封装ByteArrayInputStream-->InputStreamReader-->BufferedReader
			//文件编码探测器
			CharsetDetector detector = new CharsetDetector();
			//BufferedInputStream bufferStream = new BufferedInputStream(new BOMInputStream(is));
			/*if (null == is) {
				throw new Exception("该路径下无该文件,或者文件类型不支持");}*/
			/*br = new BufferedReader(new InputStreamReader(
					bufferStream, detector.detect(bufferStream,bufferStream.available())));*/
			BufferedInputStream bufferStream = new BufferedInputStream(new ByteArrayInputStream(content.getBytes()));
			BufferedReader br = new BufferedReader(new InputStreamReader(bufferStream,detector.detect(bufferStream,bufferStream.available()>1024*1024 ? 1024*1024 : bufferStream.available())));
			String line;
			int lineCount = 0;
			while ((line = br.readLine()) != null) {
				builder.append(line);
				builder.append("\r\n");
				lineCount++;
				if (lineCount > 49) {// 只读取前50行的内容(只针对文本)
					break;
				}
			}
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
			throw new Exception("读取文件异常");
		}
		return builder.toString();
	}

	/**
	 * pdf单独读取
	 *
	 * @param filePath
	 * @return
	 * @throws Exception
	 */
	public String getTextFromPdf(String filePath) throws Exception {
		String result = "";
		FileInputStream is = null;
		PDDocument document = null;
		try {
			is = new FileInputStream(filePath);
			PDFParser parser = new PDFParser(is);
			parser.parse();
			document = parser.getPDDocument();

			PDFTextStripper stripper = new PDFTextStripper();
			stripper.setSortByPosition(true);
			result = stripper.getText(document);
			//改用IText来读取PDF文件
			/*PdfReader reader = new PdfReader(is);
			reader.setAppendable(true);
			PdfReaderContentParser parser = new PdfReaderContentParser(reader);
			StringBuffer buffer = new StringBuffer(result);
			//设置字体
			AsianFontMapper mapper = new AsianFontMapper("ChineseSimplifiedFont","ChineseSimplifiedEncoding_H");
			int num = reader.getNumberOfPages();

			for(int i=1;i<=num;i++){
				TextExtractionStrategy strategy = parser.processContent(i,new LocationTextExtractionStrategy());
				buffer.append(strategy.getResultantText());
			}
			result=buffer.toString();*/
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception("读取pdf异常");
		} finally {
			if (is != null) {
				try {
					is.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (document != null) {
				try {
					document.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return result;
	}

	/**
	 * 对路径进行处理
	 *
	 * @param filePath
	 * @return
	 */
	public static String replacePath(String filePath) {
		if ("\\".equals(filePath.substring(filePath.length() - 1))) {
			filePath = filePath.substring(0, filePath.length() - 1);
		} else if ("/".equals(filePath.substring(filePath.length() - 1))) {
			filePath = filePath.substring(0, filePath.length() - 1);
		}
		return filePath;
	}

	public static void main1(String[] args) throws Exception {
		FileFolderUtils fileFolderUtils = new FileFolderUtils();
		String ct = "";
		try {
			ct = fileFolderUtils.getContentByName("D:\\新建文件夹\\spark\\xls",
					"test2.xls");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		DocExtractSquid docExtractSquid = new DocExtractSquid();
		docExtractSquid.setHeader_row_no(1);
		docExtractSquid.setFirst_data_row_no(2);
		List<String> strings = fileFolderUtils.getDocContent(ct,
				docExtractSquid, "GBK");
		for (String string : strings) {
			System.out.println(string);
		}
	}

	/**
	 * 快速获取文本的前N行数据
	 *
	 * @param top
	 * @param path
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static String getSomeContent(int top, String path, String encoding)
			throws FileNotFoundException, IOException {
		StringBuilder builder = new StringBuilder(500);
		// 快速读取文件的前N行
		RandomAccessFile br = new RandomAccessFile(path, "r");// 只读r
		String str = null;
		int i = 0;
		while ((str = br.readLine()) != null) {
			builder.append(new String(str.getBytes("ISO-8859-1"), encoding));
			builder.append("/r/n");
			i++;
			if (i > top) {// 读取用输入的top值
				break;
			}
		}
		br.close();
		return builder.toString();
	}

	/**
	 * 快速读取前N行
	 *
	 * @param top
	 * @param path
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static List<String> getContentByNumber(int top, String path,
												  String encoding) throws FileNotFoundException, IOException {
		// 快速读取文件的前top行
		List<String> lines = new ArrayList<String>();
		RandomAccessFile br = new RandomAccessFile(path, "r");// 只读r
		String str = null;
		int i = 0;
		while ((str = br.readLine()) != null) {
			lines.add(new String(str.getBytes("ISO-8859-1"), encoding));
			i++;
			if (i > top - 1) {// 读取用输入的top值
				break;
			}
		}
		br.close();
		return lines;
	}

}