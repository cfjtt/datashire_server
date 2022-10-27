package com.eurlanda.datashire.server.utils.fileSource;

import cn.com.jsoft.jframe.utils.CharsetDetector;
import cn.com.jsoft.jframe.utils.FileUtils;
import com.eurlanda.datashire.entity.DocExtractSquid;
import com.eurlanda.datashire.entity.HdfsSquid;
import com.eurlanda.datashire.entity.SourceColumn;
import com.eurlanda.datashire.entity.XmlExtractSquid;
import com.eurlanda.datashire.enumeration.HDFSCompressionType;
import com.eurlanda.datashire.enumeration.datatype.DbBaseDatatype;
import com.eurlanda.datashire.server.exception.ErrorMessageException;
import com.eurlanda.datashire.server.utils.adapter.RemoteFileAdapter;
import com.eurlanda.datashire.server.utils.utility.HDFSUtils;
import com.eurlanda.datashire.server.utils.utility.ReadDTDUtils;
import com.eurlanda.datashire.server.utils.utility.ReadXSDUtils;
import com.eurlanda.datashire.socket.ServerEndPoint;
import com.eurlanda.datashire.utility.*;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.openxml4j.opc.PackageAccess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parquet.example.data.Group;
import parquet.example.data.GroupFactory;
import parquet.example.data.simple.SimpleGroupFactory;
import parquet.format.converter.ParquetMetadataConverter;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.ParquetReader;
import parquet.hadoop.example.GroupReadSupport;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.schema.MessageType;

import java.io.*;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * 具体实现HDFS获取元数据的类
 */
public class HDFSAdapterImp implements FileSourceAdapter<HdfsSquid> {

    private static final Logger logger = LoggerFactory.getLogger(HDFSAdapterImp.class);
    private static HDFSUtils hdfsUtils = new HDFSUtils();

    /**
     * 通过Connection Squid与文件名获取文件中的内容
     *
     * @param fileConncetionSquid 源Squid
     * @param fileName            文件名
     * @param length              字节长度
     * @return 文件内容
     */
    @Override
    public String getTXTFileContent(HdfsSquid fileConncetionSquid, String fileName, int length) throws Exception {
        DocExtractSquid docExtractSquid = new DocExtractSquid();
        BufferedReader reader = null;
        FSDataInputStream is = null;
        FileSystem fs = null;
        RecordReader orcReader = null;
        ParquetReader<Group> parquetReader = null;
        BufferedInputStream bufferedStream = null;
        BOMInputStream bomInputStream = null;
        InputStreamReader input = null;
        InputStream inputStream = null;
        String charsetName = null;

        StringBuffer strBuffer = new StringBuffer(200);
        int encoding = fileConncetionSquid.getEncoding();
        String encode = EncodingUtils.getEncoding(encoding);
        try {
            fs = hdfsUtils.getFileSystem(fileConncetionSquid);
            String dst = hdfsUtils.replacePath(fileConncetionSquid.getFile_path()) + "/" + fileName;
            Path path = new Path(dst);
            // 路径是否存在
            if (fs.exists(path)) {
                is = fs.open(path);
                //解压缩处理
                if (docExtractSquid.getCompressiconCodec() == HDFSCompressionType.NOCOMPRESS.value()) {
                    //input = new InputStreamReader(is,Charset.forName(encode));
                    if (fileName.indexOf(".orc") > 0) {
                        org.apache.hadoop.hive.ql.io.orc.Reader orcFile = OrcFile.createReader(fs, path);
                        orcReader = OrcInputFormat.createReaderFromFile(orcFile, fs.getConf(), 0, orcFile.getContentLength());
                        if (orcReader != null) {
                            while (orcReader.hasNext()) {
                                Object row = orcReader.next(null);
                                strBuffer.append(row + "");
                            }
                        }

                    } else if (fileName.indexOf(".parquet") > 0) {
                        ParquetMetadata readFooter = ParquetFileReader.readFooter(fs.getConf(), fs.getFileStatus(path), ParquetMetadataConverter.NO_FILTER);
                        final MessageType schema = readFooter.getFileMetaData().getSchema();
                        GroupFactory factory = new SimpleGroupFactory(schema);
                        Group group = factory.newGroup();
                        GroupReadSupport readSupport = new GroupReadSupport();
                        parquetReader = new ParquetReader<Group>(fs.getConf(), fs.getFileStatus(path).getPath(), readSupport);
                        String line = "";
                        while ((line = parquetReader.read() + "") != null) {
                            strBuffer.append(line);
                        }
                    } else {
                        //文件编码探测器
                        CharsetDetector detector = new CharsetDetector();
                        bomInputStream = new BOMInputStream(is);
                        bufferedStream = new BufferedInputStream(bomInputStream,bomInputStream.available());
                        charsetName = detector.detect(bufferedStream, bufferedStream.available() > 1024 * 1024 ? 1024 * 1024 : bufferedStream.available());
                        if (charsetName == null) {
                            charsetName = "utf-8";
                        }
                        try {
                            reader = new BufferedReader(new InputStreamReader(bufferedStream, charsetName));
                        } catch (Exception e) {
                            if (e instanceof UnsupportedEncodingException) {
                                charsetName=encode;
                                reader = new BufferedReader(new InputStreamReader(bomInputStream, encode));
                            }
                        }
                        byte[] bytes = null;
                        boolean flag = true;
                        int len = bufferedStream.available();
                        if (len > length) {
                            //传多少拿多少
                            bytes = new byte[length];
                        } else {
                            //如果少于20M就全拿
                            bytes = new byte[len];
                        }
                        while (bufferedStream.read(bytes) != -1 && flag) {
                            String str = new String(bytes, charsetName);
                            strBuffer.append(str);
                            flag = false;
                        }
                    }
                } else {
                    input = new InputStreamReader(hdfsUtils.unCompress(is, HDFSCompressionType.valueOf(docExtractSquid.getCompressiconCodec())), Charset.forName(encode));
                    reader = new BufferedReader(input);
                    int len = -1;
                    boolean flag = true;
                    char[] chs = new char[length];
                    while ((len = reader.read(chs)) != -1 && flag) {
                        String str = new String(chs, 0, len);
                        strBuffer.append(str);
                        flag = false;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("获取HDFS文本异常", e);
            throw new ErrorMessageException(MessageCode.ERR_HDFS.value());
        } finally {
            try {
                if (inputStream != null) {
                    inputStream.close();
                }
                if (orcReader != null) {
                    orcReader.close();
                }
                if (parquetReader != null) {
                    parquetReader.close();
                }
                if (bufferedStream != null) {
                    bufferedStream.close();
                }
                if (bomInputStream != null) {
                    bomInputStream.close();
                }
                if (reader != null) {
                    reader.close();
                }
                if (input != null) {
                    input.close();
                }
                if (is != null) {
                    is.close();
                }
                if (fs != null) {
                    fs.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return strBuffer.toString();
    }


    /**
     * 获取Office类型的文件
     *
     * @param fileName 文件名
     * @return 文件内容
     */
    @Override
    public String getOfficeFileContent(HdfsSquid fileConncetionSquid, String fileName, int length) throws Exception {
        String newFileName = null;
        String returnStr="";
        try {
            //下载文件到本地  newFileName是文件下载保存在本地的名称
            newFileName = hdfsUtils.downFromHdfs(fileConncetionSquid, fileName);
            String path = ServerEndPoint.ftpDownload_Path + "/" + newFileName;
            //调用统一读取office文件方法
            FileFolderAdapterImpl folderAdapter=new FileFolderAdapterImpl();
            returnStr = folderAdapter.readOffice(path, fileName, 2, length);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("获取HDFS Office文件异常", e);
            throw new ErrorMessageException(MessageCode.ERR_HDFS.value());
        }
        return returnStr;
    }

    /**
     * 获取XML类型的文件
     *
     * @param fileConnectionSquid 源Squid
     * @param fileName            文件名
     * @return 文件内容
     */
    @Override
    public List<SourceColumn> getXMLFileContent(HdfsSquid fileConnectionSquid, XmlExtractSquid xmlExtractSquid, String fileName) throws Exception {
        List<SourceColumn> tempList = null;
        ReturnValue out = new ReturnValue();
        SourceColumn sourceColumn = null;
        InputStream inputStream = null;
        try {
            // 得到XmlExtractSquid的path
            String xsd_dtd_path = xmlExtractSquid.getXsd_dtd_path();

            String dsf = "hdfs://" + fileConnectionSquid.getHost() + fileConnectionSquid.getFile_path() + "/" + fileName;
            // 获取conf配置
            Configuration conf = new Configuration();
            // 实例化一个文件系统
            FileSystem fs = FileSystem.get(URI.create(dsf.replace(" ", "%20")), conf);
            // 读出流
            inputStream = fs.open(new Path(dsf));

            List<XMLNodeUtils> xmlNodeList = null;
            // 如果是xsd文件
            if (fileName.endsWith(".xsd")) {
                //通过流的方式去读取数据
                xmlNodeList = ReadXSDUtils.paserXSD(inputStream, xsd_dtd_path, out);
            }
            // 如果是DTD文件
            else if (fileName.endsWith(".dtd")) {
                xmlNodeList = ReadDTDUtils.parseDTD(inputStream, xsd_dtd_path, out);
            }

            if (StringUtils.isNull(xmlNodeList) || xmlNodeList.isEmpty()) {
                return null;
            }
            XMLNodeUtils xsdNode = null;
            //转换成SourceColumn
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
            logger.error("获取HDFS XML文件异常", e);
            throw new ErrorMessageException(MessageCode.ERR_HDFS.value());
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return tempList;
    }
}
