package com.eurlanda.datashire.server.utils.fileSource;

import com.eurlanda.datashire.entity.DocExtractSquid;
import com.eurlanda.datashire.entity.FtpSquid;
import com.eurlanda.datashire.entity.SourceColumn;
import com.eurlanda.datashire.entity.XmlExtractSquid;
import com.eurlanda.datashire.enumeration.datatype.DbBaseDatatype;
import com.eurlanda.datashire.server.exception.ErrorMessageException;
import com.eurlanda.datashire.server.utils.utility.*;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.XMLNodeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * ftp获取文件内容
 * Created by Eurlanda - Java、 on 2017/7/31.
 */
public class FTPAdapterImpl implements FileSourceAdapter<FtpSquid> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FTPAdapterImpl.class);

    /**
     * 读取txt文件内容
     * @param ftpSquid 连接的ftp
     * @param fileName 文件名称
     * @param length 读取长度
     * @return
     * @throws Exception
     */
    @Override
    public String getTXTFileContent(FtpSquid ftpSquid, String fileName, int length) throws Exception {

        String returnStr = "";
        try {
            if (ftpSquid.getProtocol() == 0) {
                FtpUtils ftpUtils = new FtpUtils();// ftp
                LOGGER.error("[getReadTxtFtpConnection===================================exception]", ftpSquid,fileName,length);
                returnStr = ftpUtils.getReadTxtFtpConnection(ftpSquid, fileName, length);
            } else if (ftpSquid.getProtocol() == 1) {//sftp
                SftpUtils sftpUtils = new SftpUtils();
                LOGGER.error("[readTxtSFTPServer===================================exception]", ftpSquid,fileName,length);
                returnStr = sftpUtils.readTxtSFTPServer(ftpSquid, fileName, length);
            }
        } catch (ErrorMessageException e){
            e.printStackTrace();
            LOGGER.error("[getTXTFileContent===================================exception]", e);
            throw new ErrorMessageException(MessageCode.ERR_FTPFILE.value());
        }catch (Exception e) {
            e.printStackTrace();
            LOGGER.error("[getTXTFileContent===================================exception]", e);
            throw new Exception(e);
        }
        return returnStr;
    }

    /**
     * 读取office文件内容
     * @param ftpSquid
     * @param fileName
     * @return
     * @throws Exception
     */
    @Override
    public String getOfficeFileContent(FtpSquid ftpSquid, String fileName, int length) throws Exception {
        String returnStr = null;
        try {
            if (ftpSquid.getProtocol() == 0) {//ftp
                FtpUtils ftpUtils = new FtpUtils();
                LOGGER.error("[getReadOfficeFtpConnection===================================exception]", ftpSquid,fileName);
                returnStr = ftpUtils.getReadOfficeFtpConnection(ftpSquid, fileName,length);
            } else if (ftpSquid.getProtocol() == 1) {//sftp
                SftpUtils sftpUtils = new SftpUtils();
                LOGGER.error("[readOfficeSFTPServer===================================exception]", ftpSquid,fileName);
                returnStr = sftpUtils.readOfficeSFTPServer(ftpSquid, fileName,length);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception( e);
        }
        return returnStr;
    }

    /**
     * 读取xml文件内容
     * @param ftpSquid
     * @param xmlExtractSquid
     * @param fileName
     * @return
     * @throws Exception
     */
    @Override
    public List<SourceColumn> getXMLFileContent(FtpSquid ftpSquid, XmlExtractSquid xmlExtractSquid, String fileName) throws Exception {
        //获取XmlExtractSquid的定义文件名称
        String xsd_dtd_file = xmlExtractSquid.getXsd_dtd_file();
        //获取XmlExtractSquid的元数据路径
        String xsd_dtd_path = xmlExtractSquid.getXsd_dtd_path();
        List<SourceColumn> tempList = null;
        SourceColumn sourceColumn = null;
        InputStream inputStream=null;
        try {
            FtpUtils ftpUtils = new FtpUtils();
            LOGGER.error("[readOfficeSFTPServer===================================exception]", ftpSquid,fileName);
            inputStream=ftpUtils.readXmlFtpConnection(ftpSquid,fileName);
            List<XMLNodeUtils> xmlNodeList = null;
            // 如果是xsd文件
            if (xsd_dtd_file.endsWith(".xsd")) {
                xmlNodeList = ReadXSDUtils.paserXSD(inputStream, xsd_dtd_path, null);
            }
            // 如果是DTD文件
            else if (xsd_dtd_file.endsWith(".dtd")) {
                xmlNodeList = ReadDTDUtils.parseDTD(inputStream, xsd_dtd_path, null);
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
        }catch (Exception e){
            e.printStackTrace();
            throw new Exception(e);
        }
        return tempList;
    }


    public static void main(String[] args) {
        FtpSquid ftpSquid = new FtpSquid();
        ftpSquid.setHost("192.168.137.1:21");
        ftpSquid.setUser_name("datashire");
        ftpSquid.setPassword("eurlanda1");
        ftpSquid.setFile_path("/TestData");
        XmlExtractSquid xmlExtractSquid=new XmlExtractSquid();
        xmlExtractSquid.setXsd_dtd_path("/Demo/Categories");
        xmlExtractSquid.setXsd_dtd_file("Demo.xsd");
        try {
            String Str=new FTPAdapterImpl().getOfficeFileContent(ftpSquid,"test002.xls",0);
            System.out.println(Str);
            DocExtractSquid squid=new DocExtractSquid();
            squid.setDoc_format(1);
            squid.setRow_delimiter("\r\n");
            squid.setRow_format(1);
            squid.setHeader_row_no(20);
            squid.setFirst_data_row_no(21);
            squid.setDelimiter("\t");
            squid.setType(1041);
            squid.setField_length(5);
            List<List<String>> strList= DocExtractSquidUtil.getDocTableList(Str,squid);
            for(List<String> list:strList){
                for(String str:list){
                    System.out.print(str+"\t\t");
                }
                System.out.println();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


}
