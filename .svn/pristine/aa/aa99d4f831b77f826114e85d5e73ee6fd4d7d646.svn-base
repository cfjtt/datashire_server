package com.eurlanda.datashire.server.utils.fileSource;

import cn.com.jsoft.jframe.utils.CharsetDetector;
import cn.com.jsoft.jframe.utils.FileUtils;
import cn.com.jsoft.jframe.utils.fileParsers.OfficeFileParser;
import com.eurlanda.datashire.entity.DocExtractSquid;
import com.eurlanda.datashire.entity.FileFolderSquid;
import com.eurlanda.datashire.entity.SourceColumn;
import com.eurlanda.datashire.entity.XmlExtractSquid;
import com.eurlanda.datashire.enumeration.datatype.DbBaseDatatype;
import com.eurlanda.datashire.exception.SystemErrorException;
import com.eurlanda.datashire.server.utils.adapter.RemoteFileAdapter;
import com.eurlanda.datashire.server.utils.utility.ReadDTDUtils;
import com.eurlanda.datashire.server.utils.utility.ReadXSDUtils;
import com.eurlanda.datashire.utility.*;
import jcifs.smb.SmbFile;
import jcifs.smb.SmbFileInputStream;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.pdfbox.pdfparser.PDFParser;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.util.PDFTextStripper;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.openxml4j.opc.PackageAccess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Eurlanda - Java、 on 2017/7/25.
 */
public class FileFolderAdapterImpl implements FileSourceAdapter<FileFolderSquid> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileFolderAdapterImpl.class);

    @Override
    public String getTXTFileContent(FileFolderSquid fileSquid, String fileName, int length) throws Exception {
        String returnStr = null;
        String filePath = fileSquid.getFile_path();
        SmbFile smbFile = null;
        FileInputStream fileInputStream = null;
        SmbFileInputStream smbInputStream = null;
        BufferedInputStream bufferedStream = null;
        try {
            //读取本地TXT文件
            if (StringUtils.isBlank(fileSquid.getHost()) || fileSquid.getHost().equals(OSUtils.getLocalIP())) {
                File file = new File(FileFolderUtils.replacePath(filePath) + "/" + fileName);
                fileInputStream = new FileInputStream(file);
                bufferedStream = new BufferedInputStream(fileInputStream,fileInputStream.available());
            } else {
                //读取共享Txt文件
                String userName = fileSquid.getUser_name();
                String passWord = fileSquid.getPassword();
                String host = fileSquid.getHost();
                //连接共享文件服务器
                smbFile = RemoteFileAdapter.getSmbFile(host, userName, passWord, filePath, fileName);
                if (smbFile == null) {
                    throw new Exception("连接服务器失败!");
                }
                smbInputStream = new SmbFileInputStream(smbFile);

                bufferedStream = new BufferedInputStream(new BOMInputStream(smbInputStream),smbFile.getContentLength()>length ? length : smbFile.getContentLength());
            }
            //调用统一读取txt 文件的方法
            returnStr = readBufStreamTxt(bufferedStream);
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error("[getTXTFileContent===================================exception]", e);
            throw new Exception("文件读取异常", e);
        } finally {
            if (bufferedStream != null) {
                bufferedStream.close();
            }
            if (smbInputStream != null) {
                smbInputStream.close();
            }
            if (fileInputStream != null) {
                fileInputStream.close();
            }
        }
        return returnStr;
    }

    @Override
    public String getOfficeFileContent(FileFolderSquid fileSquid, String fileName, int length) throws Exception {
        String returnStr = null;
        String filePath = fileSquid.getFile_path();
        try {
            //判断本地文件还是共享文件  如果服务器名称和本机相同，也按照本地文件读取
            if (StringUtils.isBlank(fileSquid.getHost()) || fileSquid.getHost().equals(OSUtils.getLocalIP())) {
                //调用读取office文件方法
                String path = FileFolderUtils.replacePath(filePath) + "/" + fileName;
                returnStr = readOffice(path, fileName, 1, length);
            } else {
                //调用下载读取文件方法
                RemoteFileAdapter.downLoadFileByFileFolderConnectionSquid(fileSquid, fileName);
                //下载文件的临时路径
                String tempPath = RemoteFileAdapter.ftpDownload_Path;
                String path = FileFolderUtils.replacePath(tempPath) + "/" + fileName;
                //调用读取office文件方法
                returnStr = readOffice(path, fileName, 2, length);
            }

        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error("[getTXTFileContent===================================exception]", e);
            throw new SystemErrorException(MessageCode.ERR_FILE, "读取文件异常");
        }

        return returnStr;
    }

    @Override
    public List<SourceColumn> getXMLFileContent(FileFolderSquid fileSquid, XmlExtractSquid xmlExtractSquid, String fileName) throws Exception {
        //连接squid 的路径
        String fileSquidPath = fileSquid.getFile_path();
        //获取XmlExtractSquid的定义文件名称
        String xsd_dtd_file = xmlExtractSquid.getXsd_dtd_file();
        //获取XmlExtractSquid的元数据路径
        String xsd_dtd_path = xmlExtractSquid.getXsd_dtd_path();
        SmbFile smbFile = null;
        InputStream fileInputStream = null;
        SmbFileInputStream smbFileInputStream = null;
        List<SourceColumn> tempList = null;
        SourceColumn sourceColumn = null;
        try {
            //判断本地文件还是共享文件  如果服务器名称和本机相同，也按照本地文件读取
            if (StringUtils.isBlank(fileSquid.getHost()) || fileSquid.getHost().equals(OSUtils.getLocalIP())) {
                String filePath = "";
                // 获取文件路径
                if (fileSquidPath.endsWith("\\")) {
                    filePath = fileSquidPath + xsd_dtd_file;
                } else {
                    filePath = fileSquidPath + "/" + xsd_dtd_file;
                }
                File file = new File(filePath);
                fileInputStream = new FileInputStream(file);
            } else {
                //远程共享文件
                String userName = fileSquid.getUser_name();
                String passWord = fileSquid.getPassword();
                String host = fileSquid.getHost();
                //连接共享文件服务器
                smbFile = RemoteFileAdapter.getSmbFile(host, userName, passWord, fileSquidPath, fileName);
                if (smbFile == null) {
                    throw new Exception("连接服务器失败!");
                }
                smbFileInputStream = new SmbFileInputStream(smbFile);
                fileInputStream = new BufferedInputStream(smbFileInputStream,smbFile.getContentLength());
            }
            List<XMLNodeUtils> xmlNodeList = null;
            // 如果是xsd文件
            if (xsd_dtd_file.endsWith(".xsd")) {
                xmlNodeList = ReadXSDUtils.paserXSD(fileInputStream, xsd_dtd_path, null);
            }
            // 如果是DTD文件
            else if (xsd_dtd_file.endsWith(".dtd")) {
                xmlNodeList = ReadDTDUtils.parseDTD(fileInputStream, xsd_dtd_path, null);
            }
            XMLNodeUtils xsdNode = null;
            tempList = new ArrayList<SourceColumn>();
            for (int j = 0; j < xmlNodeList.size(); j++) {
                xsdNode = xmlNodeList.get(j);
                sourceColumn = new SourceColumn();
                sourceColumn.setName(xsdNode.getName());
                sourceColumn.setData_type(xsdNode.getType());
                if (xsdNode.getType() == DbBaseDatatype.DECIMAL.value()) {
                    sourceColumn.setPrecision(xsdNode.getLength());
                } else {
                    sourceColumn.setLength(xsdNode.getLength());
                }
                sourceColumn.setSource_table_id(xmlExtractSquid.getSource_table_id());
                sourceColumn.setRelative_order(j + 1);
                sourceColumn.setNullable(true);
                tempList.add(sourceColumn);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception(e);
        } finally {
            if (fileInputStream != null) {
                fileInputStream.close();
            }

            if (smbFileInputStream != null) {
                smbFileInputStream.close();
            }
        }

        return tempList;
    }

    /**
     * 统一读取txt文件内容的方法
     *
     * @param bufferedStream
     * @return
     * @throws Exception
     */
    public String readBufStreamTxt(BufferedInputStream bufferedStream) throws Exception {
        StringBuffer returnStr = new StringBuffer();
        try {
            //文件编码探测器
            CharsetDetector charsetDetector = new CharsetDetector();
            String charSet = charsetDetector.detect(bufferedStream,200);
            byte[] bytes = null;
            boolean flag = true;
            int len = bufferedStream.available();
           /* if (len > length) {
                bytes = new byte[length];
            } else {
                bytes = new byte[len];
            }*/
            bytes=new byte[len];
            while (bufferedStream.read(bytes) != -1 && flag) {
                String str = new String(bytes, charSet);
                returnStr.append(str);
                flag = false;
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("读取文件异常!");
        }
        return returnStr.toString();
    }


    /**
     * 读取Office文件
     *
     * @param path 文件路径
     * @param fileName 文件名称
     * @param del      1：代表读取的是本地文件。读取完之后不用删除。
     *                 2：代表读取的是远程文件。由于远程文件是先下载再读取。读取完之后需要删除临时文件。
     * @return
     * @throws Exception
     */
    public String readOffice(String path, String fileName, int del, int length) throws Exception {
        String returnStr = null;
        FileInputStream stream = null;
        BufferedInputStream bufferStream = null;
        FileOutputStream outStream = null;
        fileName = fileName.substring(fileName.lastIndexOf("/") + 1, fileName.length());
        String csvPath = "";
        try {
            if (StringUtils.isNotBlank(FileUtils.getFileEx(fileName)) && ".doc|.docx".contains(FileUtils.getFileEx(fileName))) {
                OfficeFileParser officeFileParser = new OfficeFileParser();
                File file = new File(path);
                returnStr = officeFileParser.getStringContent(file);
            } else if (StringUtils.isNotBlank(FileUtils.getFileEx(fileName))
                    && ".xls".contains(FileUtils.getFileEx(fileName))) {
                csvPath = path.substring(0, path.indexOf(".xls")) + ".csv";
                XLS2CSV xls2csv = new XLS2CSV(path, csvPath);
                xls2csv.process();
                stream = new FileInputStream(csvPath);
                if (null == stream) {
                    throw new Exception("该路径下无该文件,或者文件类型不支持");
                }
                bufferStream = new BufferedInputStream(stream,stream.available()>length ? length :stream.available());
                returnStr = readBufStreamTxt(bufferStream);
            } else if (StringUtils.isNotBlank(FileUtils.getFileEx(fileName))
                    && ".xlsx".contains(FileUtils.getFileEx(fileName))) {
                csvPath = path.substring(0, path.indexOf(".xlsx")) + ".csv";
                OPCPackage p = OPCPackage.open(path, PackageAccess.READ);
                File file = new File(csvPath);
                if (!file.exists()) {
                    file.createNewFile();
                }
                outStream = new FileOutputStream(file);
                PrintStream out = new PrintStream(outStream, false, "utf-8");
                XLSX2CSV xlsx2csv = new XLSX2CSV(p, out, 10);
                xlsx2csv.process();
                p.close();
                out.flush();
                out.close();
                //读取csv文件8
                stream = new FileInputStream(file);
                if (null == stream) {
                    throw new Exception("该路径下无该文件,或者文件类型不支持");
                }
                bufferStream = new BufferedInputStream(stream,stream.available()>length ? length :stream.available());
                returnStr = readBufStreamTxt(bufferStream);
            } else if (StringUtils.isNotBlank(FileUtils.getFileEx(fileName))
                    && ".pdf".contains(FileUtils.getFileEx(fileName))) {
                stream = new FileInputStream(path);
                PDFParser parser = new PDFParser(stream);
                parser.parse();
                PDDocument document = parser.getPDDocument();
                PDFTextStripper stripper = new PDFTextStripper();
                stripper.setSortByPosition(true);
                returnStr = stripper.getText(document);
            }
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error("[readSharedTxt===================================exception]", e);
            throw new SystemErrorException(MessageCode.ERR_FILE, "读取文件异常");
        } finally {
            if (bufferStream != null) {
                bufferStream.close();
            }
            if (stream != null) {
                stream.close();
            }
            if (outStream != null) {
                outStream.close();
            }
            //删除csv文件
            if (csvPath != "") {
                RemoteFileAdapter.deleteFile(csvPath);
            }
            //删除下载的临时文件
            if (del == 2) {
                boolean delFlag = RemoteFileAdapter.deleteFile(path);
                if (!delFlag) {
                    LOGGER.error("下载到本地的远程文件没有删除成功,请手动清理");
                }
            }
        }

        return returnStr;
    }


    public static void main(String[] args) {
        FileFolderSquid localFolderSquid = new FileFolderSquid();
        localFolderSquid.setHost("");
        localFolderSquid.setPassword("eurlanda1");
        localFolderSquid.setUser_name("administrator");
        localFolderSquid.setFile_path("D:\\");
        FileFolderSquid fileFolderSquid = new FileFolderSquid();
        fileFolderSquid.setHost("192.168.137.1");
        fileFolderSquid.setPassword("eurlanda1");
        fileFolderSquid.setUser_name("administrator");
        fileFolderSquid.setFile_path("/Trans/TestData");
        XmlExtractSquid xmlExtractSquid = new XmlExtractSquid();
        xmlExtractSquid.setXsd_dtd_file("Demo.xsd");
        xmlExtractSquid.setXsd_dtd_path("/Demo/Categories");
        try {
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


}
