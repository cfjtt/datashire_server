package com.eurlanda.datashire.utility;

import cn.com.jsoft.jframe.utils.FileUtils;
import com.eurlanda.datashire.entity.DocExtractSquid;
import com.eurlanda.datashire.entity.FileFolderSquid;
import com.eurlanda.datashire.entity.HdfsSquid;
import com.eurlanda.datashire.enumeration.HDFSCompressionType;
import jcifs.smb.SmbFile;
import jcifs.smb.SmbFileInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ReadCSVFileUtil {

	/**
	 * 根据文件内容来取CSV的数据源
	 * readCSVFileByContent(这里用一句话描述这个方法的作用)
	 *
	 * @Title: readCSVFileByContent
	 * @Description: TODO
	 * @param content
	 * @param docExtractSquid
	 * @param encoding
	 * @return
	 * @throws IOException 设定文件
	 * @return List<List<String>> 返回类型
	 * @throws
	 * @author bo.dang
	 */
	public static List<List<String>> readCSVFileByContent(String content, DocExtractSquid docExtractSquid, String encoding) throws IOException {
		//封装ByteArrayInputStream-->InputStreamReader-->BufferedReader
		BufferedReader inStream = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(content.getBytes(Charset.forName(encoding))), Charset.forName(encoding)));
		return readCSVFile(inStream, docExtractSquid.getHeader_row_no(), docExtractSquid.getFirst_data_row_no(), docExtractSquid.getDelimiter(), docExtractSquid.getRow_delimiter(),docExtractSquid.getRow_delimiter_position());
	}

	/**
	 *
	 * 对远程共享文件进行读取所指定的行(除开office文档)
	 *  若远程文件的路径为：shareDoc\test.txt,则参数为test.txt(其中shareDoc为共享目录名称);
	 *  若远程文件的路径为：shareDoc\doc\text.txt,则参数为doc\text.txt;
	 *
	 * @return  文件指定行
	 * @param docExtractSquid
	 * @param fileFolderSquid
	 * @param fileName
	 * @return
	 * @throws IOException
	 */
	public static List<List<String>> readCSVFileByPath(DocExtractSquid docExtractSquid, FileFolderSquid fileFolderSquid, String fileName) throws IOException {
		SmbFile smbFile = null;
		BufferedReader inStream = null;
		//构建连接字符串,并取得文件连接
		String conStr = null;
		String username=fileFolderSquid.getUser_name();
		String password=fileFolderSquid.getPassword();
		String remoteHostIp=fileFolderSquid.getHost();
		String shareDocName=fileFolderSquid.getFile_path();
		if(StringUtils.isBlank(username)&&StringUtils.isBlank(password))
		{
			conStr = "smb://"+remoteHostIp+"/"+shareDocName+"/"+fileName;
		}else
		{
			conStr = "smb://"+username+":"+password+"@"+remoteHostIp+"/"+shareDocName+"/"+fileName;
		}
		smbFile = new SmbFile(conStr);
		// 创建reader
		inStream = new BufferedReader(new InputStreamReader(new SmbFileInputStream(smbFile), "utf-8"));
		//封装ByteArrayInputStream-->InputStreamReader-->BufferedReader
		return readCSVFile(inStream, docExtractSquid.getHeader_row_no(), docExtractSquid.getFirst_data_row_no(), docExtractSquid.getDelimiter(), docExtractSquid.getRow_delimiter(),docExtractSquid.getRow_delimiter_position());
	}

	/**
	 * 从HDFS中读取CSV文件
	 * getCSVFileFromHDFS(这里用一句话描述这个方法的作用)
	 * TODO(这里描述这个方法适用条件 – 可选)
	 *
	 * @Title: getCSVFileFromHDFS
	 * @Description: TODO
	 * @param hdfsConnection
	 * @param docExtractSquid
	 * @param fileName
	 * @param encode
	 * @return
	 * @throws Exception 设定文件
	 * @return List<List<String>> 返回类型
	 * @throws
	 * @author bo.dang
	 */
	public static List<List<String>> getCSVFileFromHDFS(HdfsSquid hdfsConnection, DocExtractSquid docExtractSquid, String fileName, String encode) throws Exception{
		BufferedReader inStream = null;
		if(".doc|.docx|.pdf".contains(FileUtils.getFileEx(fileName)))
		{
			HdfsUtils hdfsUtils = new HdfsUtils();
			//读取文件的一部分
			String content = hdfsUtils.getPartContent(hdfsConnection, fileName, EncodingUtils.getEncoding(docExtractSquid.getEncoding()), docExtractSquid.getDoc_format(), docExtractSquid.getCompressiconCodec() != HDFSCompressionType.NOCOMPRESS.value());
			inStream = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(content.getBytes(Charset.forName(encode))), Charset.forName(encode)));
			//下载文件到本地
			return readCSVFile(inStream, docExtractSquid.getHeader_row_no(), docExtractSquid.getFirst_data_row_no(), docExtractSquid.getDelimiter(), docExtractSquid.getRow_delimiter(),docExtractSquid.getRow_delimiter_position());
		}
		else {
			FileSystem fs = HdfsUtils.getFileSystem(hdfsConnection);
			String dst=HdfsUtils.replacePath(hdfsConnection.getFile_path())+"/"+fileName;
			Path path = new Path(dst);
			// 路径是否存在
			if (fs.exists(path)) {
				FSDataInputStream is = fs.open(path);
				InputStreamReader isr;
				HdfsUtils hdfsUtils = new HdfsUtils();
				//解压缩处理
				if (docExtractSquid.getCompressiconCodec() == HDFSCompressionType.NOCOMPRESS.value())
					isr = new InputStreamReader(is);
				else
					isr = new InputStreamReader(hdfsUtils.unCompress(is, HDFSCompressionType.valueOf(docExtractSquid.getCompressiconCodec())));
				inStream = new BufferedReader(isr);
				return readCSVFile(inStream, docExtractSquid.getHeader_row_no(), docExtractSquid.getFirst_data_row_no(), docExtractSquid.getDelimiter(), docExtractSquid.getRow_delimiter(),docExtractSquid.getRow_delimiter_position());
			}
			else {
				return null;
			}
		}

	}


	public static List<List<String>> readCSVFile(BufferedReader inStream, int headerRowNo, int firstDataRowNo, String delimited, String row_delimited,int row_delimiter_position) throws IOException {
		// 循环对文件进行读取
		List<List<String>> docList = new ArrayList<List<String>>();
		int endDataRowNo = firstDataRowNo + 50;
		try {
			FileFolderUtils ffu=new FileFolderUtils();
			List<String> list = ffu.getDocContent(inStream,row_delimited,row_delimiter_position,headerRowNo,firstDataRowNo);
			for (String data : list) {
				if (StringUtils.isNotNull(data)){
					docList.add(splitCSV(data));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			// 发生异常时关闭流
			if (inStream != null) {
				inStream.close();
			}
		}

		return docList;
	}

	/**
	 * 调用此方法时应该确认该类已经被正常初始化<br>
	 * 该方法用于读取csv文件的下一个逻辑行<br>
	 * 读取到的内容放入向量中<br>
	 * 如果该方法返回了false，则可能是流未被成功初始化<br>
	 * 或者已经读到了文件末尾<br>
	 * 如果发生异常，则不应该再进行读取
	 *
	 * @return 返回值用于标识是否读到文件末尾
	 * @throws Exception
	 */
	public static List<String> readCSVFileLineCount(String newLineStr)
			throws IOException, Exception {

		List<String> rowDataList = null;
		// 声明逻辑行
		String logicLineStr = "";
		// 用于存放读到的行
		StringBuilder strb = new StringBuilder();
		// 声明是否为逻辑行的标志，初始化为false
		boolean isLogicLine = false;
		//String newLineStr = null;

		while (!isLogicLine) {
			// String newLineStr = inStream.readLine();
			if (newLineStr == null) {
				strb = null;
				// vContent = null;
				isLogicLine = true;
				break;
			}
			if (newLineStr.startsWith("#")) {
				isLogicLine = true;
				// 去掉注释
				continue;
			}
			if (!strb.toString().equals("")) {
				strb.append("/r/n");
			}
			// strb.
					/*
					 * strb.append(newLineStr); String oldLineStr =
					 * strb.toString();
					 */
			String oldLineStr = newLineStr.toString();
			if (oldLineStr.indexOf(",") == -1) {
				// 如果该行未包含逗号
				if (containsNumber(oldLineStr, "\"") % 2 == 0) {
					// 如果包含偶数个引号
					isLogicLine = true;
					break;
				} else {
					if (oldLineStr.startsWith("\"")) {
						if (oldLineStr.equals("\"")) {
							continue;
						} else {
							String tempOldStr = oldLineStr.substring(1);
							if (isQuoteAdjacent(tempOldStr)) {
								// 如果剩下的引号两两相邻，则不是一行
								continue;
							} else {
								// 否则就是一行
								isLogicLine = true;
								break;
							}
						}
					} else {
						// 否则就是一行
						isLogicLine = true;
						break;
					}
				}
			} else {
				// quotes表示复数的quote
				String tempOldLineStr = oldLineStr.replace("\"\"", "");
				int lastQuoteIndex = tempOldLineStr.lastIndexOf("\"");
				if (lastQuoteIndex == 0) {
					continue;
				} else if (lastQuoteIndex == -1) {
					isLogicLine = true;
					break;
				} else {
					tempOldLineStr = tempOldLineStr
							.replace("\",\"", "");
					lastQuoteIndex = tempOldLineStr.lastIndexOf("\"");
					if (lastQuoteIndex == 0) {
						continue;
					}
					if (tempOldLineStr.charAt(lastQuoteIndex - 1) == ',') {
						continue;
					} else {
						isLogicLine = true;
						break;
					}
				}
			}
		}

		// 提取逻辑行
		// logicLineStr = strb.toString();
		logicLineStr = newLineStr.toString();
		if (logicLineStr != null) {
			rowDataList = new ArrayList<String>();
			// 拆分逻辑行，把分离出来的原子字符串放入向量中
			while (logicLineStr != null && !logicLineStr.equals("")) {
				String[] ret = readAtomString(logicLineStr);
				String atomString = ret[0];
				logicLineStr = ret[1];
				if(StringUtils.isNotNull(atomString)){
					if(atomString.startsWith(".")){
						try {
							Integer.valueOf(atomString.substring(1));
							atomString = "0"+ atomString;
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}
				rowDataList.add(atomString);
				// vContent.add(atomString);
			}
			//docList.add(rowDataList);
			//System.out.println(rowDataList.toString());
			isLogicLine = false;
		}
		return rowDataList;
	}

	/**
	 * 用逗号进行分割字符串，不忽略任何一个分隔符
	 * @param newLineStr
	 * @return
	 * @throws IOException
	 * @throws Exception
	 */
	public static List<String> readCSVFileLineCount2(String newLineStr, String delimited)
			throws IOException, Exception {
		List<String> list = new ArrayList<String>();
		String[] strs = newLineStr.split(delimited, -1);
		if (strs!=null&&strs.length>0){
			for (String string : strs) {
				list.add(string);
			}
		}
		return list;
	}

	public static List<String> splitCSV(String txt) {

		String reg = "\\G(?:^|,)(?:\"([^\"]*+(?:\"\"[^\"]*+)*+)\"|([^\",]*+))";
		// 即 \G(?:^|,)(?:"([^"]*+(?:""[^"]*+)*+)"|([^",]*+))

		Matcher matcherMain = Pattern.compile(reg).matcher("");
		Matcher matcherQuoto = Pattern.compile("\"\"").matcher("");

		matcherMain.reset(txt);
		List strList = new ArrayList();
		while (matcherMain.find()) {
			String field;
			if (matcherMain.start(2) >= 0) {
				field = matcherMain.group(2);
			} else {
				field = matcherQuoto.reset(matcherMain.group(1)).replaceAll("\"");
			}
			strList.add(field);
		}
		return strList;
	}

	/**
	 * 读取一个逻辑行中的第一个字符串，并返回剩下的字符串<br>
	 * 剩下的字符串中不包含第一个字符串后面的逗号<br>
	 *
	 * @param lineStr
	 *            一个逻辑行
	 * @return 第一个字符串和剩下的逻辑行内容
	 */
	public static String[] readAtomString(String lineStr) {
		String atomString = "";// 要读取的原子字符串
		String orgString = "";// 保存第一次读取下一个逗号时的未经任何处理的字符串
		String[] ret = new String[2];// 要返回到外面的数组
		boolean isAtom = false;// 是否是原子字符串的标志
		String[] commaStr = lineStr.split(",", -1);
		if (StringUtils.isNull(commaStr) || commaStr.length == 0) {
			return ret;
		}
		while (!isAtom) {
			for (String str : commaStr) {
				if (!atomString.equals("")) {
					atomString = atomString + ",";
				}
				atomString = atomString + str;
				orgString = atomString;
				if (!isQuoteContained(atomString)) {
					// 如果字符串中不包含引号，则为正常，返回
					isAtom = true;
					break;
				} else {
					if (!atomString.startsWith("\"")) {
						// 如果字符串不是以引号开始，则表示不转义，返回
						isAtom = true;
						break;
					} else if (atomString.startsWith("\"")) {
						// 如果字符串以引号开始，则表示转义
						if (containsNumber(atomString, "\"") % 2 == 0) {
							// 如果含有偶数个引号
							String temp = atomString;
							if (temp.endsWith("\"")) {
								temp = temp.replace("\"\"", "");
								if (temp.equals("")) {
									// 如果temp为空
									atomString = "";
									isAtom = true;
									break;
								} else {
									// 如果temp不为空，则去掉前后引号
									temp = temp.substring(1,
											temp.lastIndexOf("\""));
									if (temp.indexOf("\"") > -1) {
										// 去掉前后引号和相邻引号之后，若temp还包含有引号
										// 说明这些引号是单个单个出现的
										temp = atomString;
										temp = temp.substring(1);
										temp = temp.substring(0,
												temp.indexOf("\""))
												+ temp.substring(temp
												.indexOf("\"") + 1);
										atomString = temp;
										isAtom = true;
										break;
									} else {
										// 正常的csv文件
										temp = atomString;
										temp = temp.substring(1,
												temp.lastIndexOf("\""));
										temp = temp.replace("\"\"", "\"");
										atomString = temp;
										isAtom = true;
										break;
									}
								}
							} else {
								// 如果不是以引号结束，则去掉前两个引号
								temp = temp.substring(1, temp.indexOf('\"', 1))
										+ temp.substring(temp.indexOf('\"', 1) + 1);
								atomString = temp;
								isAtom = true;
								break;
							}
						} else {
							// 如果含有奇数个引号
							// TODO 处理奇数个引号的情况
							if (!atomString.equals("\"")) {
								String tempAtomStr = atomString.substring(1);
								if (!isQuoteAdjacent(tempAtomStr)) {
									// 这里做的原因是，如果判断前面的字符串不是原子字符串的时候就读取第一个取到的字符串
									// 后面取到的字符串不计入该原子字符串
									tempAtomStr = atomString.substring(1);
									int tempQutoIndex = tempAtomStr
											.indexOf("\"");
									// 这里既然有奇数个quto，所以第二个quto肯定不是最后一个
									tempAtomStr = tempAtomStr.substring(0,
											tempQutoIndex)
											+ tempAtomStr
											.substring(tempQutoIndex + 1);
									atomString = tempAtomStr;
									isAtom = true;
									break;
								}
							}
						}
					}
				}
			}
		}
		// 先去掉之前读取的原字符串的母字符串
		if (lineStr.length() > orgString.length()) {
			lineStr = lineStr.substring(orgString.length());
		} else {
			lineStr = "";
		}
		// 去掉之后，判断是否以逗号开始，如果以逗号开始则去掉逗号
		if (lineStr.startsWith(",")) {
			if (lineStr.length() > 1) {
				lineStr = lineStr.substring(1);
			} else {
				lineStr = "";
			}
		}
		ret[0] = atomString;
		ret[1] = lineStr;
		return ret;
	}

	/**
	 * 该方法取得父字符串中包含指定字符串的数量<br>
	 * 如果父字符串和字字符串任意一个为空值，则返回零
	 *
	 * @param parentStr
	 * @param parameter
	 * @return
	 */
	public static int containsNumber(String parentStr, String parameter) {
		int containNumber = 0;
		if (parentStr == null || parentStr.equals("")) {
			return 0;
		}
		if (parameter == null || parameter.equals("")) {
			return 0;
		}
		for (int i = 0; i < parentStr.length(); i++) {
			i = parentStr.indexOf(parameter, i);
			if (i > -1) {
				i = i + parameter.length();
				i--;
				containNumber = containNumber + 1;
			} else {
				break;
			}
		}
		return containNumber;
	}

	/**
	 * 该方法用于判断给定的字符串中的引号是否相邻<br>
	 * 如果相邻返回真，否则返回假<br>
	 *
	 * @param p_String
	 * @return
	 */
	public static boolean isQuoteAdjacent(String p_String) {
		boolean ret = false;
		String temp = p_String;
		temp = temp.replace("\"\"", "");
		if (temp.indexOf("\"") == -1) {
			ret = true;
		}
		// TODO 引号相邻
		return ret;
	}

	/**
	 * 该方法用于判断给定的字符串中是否包含引号<br>
	 * 如果字符串为空或者不包含返回假，包含返回真<br>
	 *
	 * @param p_String
	 * @return
	 */
	public static boolean isQuoteContained(String p_String) {
		boolean ret = false;
		if (p_String == null || p_String.equals("")) {
			return false;
		}
		if (p_String.indexOf("\"") > -1) {
			ret = true;
		}
		return ret;
	}

	/*	    *//**
	 * 读取文件标题
	 *
	 * @return 正确读取文件标题时返回 true,否则返回 false
	 * @throws Exception
	 * @throws IOException
	 */
	/*
	 * public boolean readCSVFileTitle() throws IOException, Exception { String
	 * strValue = ""; boolean isLineEmpty = true; do { if (!readCSVNextRecord())
	 * { return false; } if (vContent.size() > 0) { strValue = (String)
	 * vContent.get(0); } for (String str : vContent) { if (str != null &&
	 * !str.equals("")) { isLineEmpty = false; break; } } // csv 文件中前面几行以 #
	 * 开头为注释行 } while (strValue.trim().startsWith("#") || isLineEmpty); return
	 * true; }
	 */

	public static void main(String[] args) throws Exception {
		//String file = "D:\\work\\Trans\\业务测试数据\\csv_imdb.csv";
		String file = "D:\\work\\ Trans\\业 务测试数据\\price.csv";
		//BufferedReader inStream = new BufferedReader(new FileReader(new File(file)));
		// readCSVFile(file);
		//readCSVFile(inStream, 0, 1, ",");
		for (String string : file.split(" ")) {
			System.out.println(string);
		}
	}
}