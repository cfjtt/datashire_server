package com.eurlanda.datashire.server.utils.utility;

import cn.com.jsoft.jframe.utils.FileUtils;
import cn.com.jsoft.jframe.utils.fileParsers.OfficeFileParser;
import cn.com.jsoft.jframe.utils.fileParsers.TxtFileParser;
import com.eurlanda.datashire.entity.HdfsSquid;
import com.eurlanda.datashire.enumeration.ExtractFileType;
import com.eurlanda.datashire.enumeration.HDFSCompressionType;
import com.eurlanda.datashire.socket.ServerEndPoint;
import com.eurlanda.datashire.utility.CreateFileUtil;
import com.eurlanda.datashire.utility.StringUtils;
import com.eurlanda.datashire.utility.XLS2CSV;
import com.eurlanda.datashire.utility.XLSX2CSV;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.pdfbox.pdfparser.PDFParser;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.util.PDFTextStripper;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.openxml4j.opc.PackageAccess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.util.UUID;

/**
 * Created by eurlanda - new 2 on 2017/7/27.
 */
public class HDFSUtils {

    private static final Logger logger = LoggerFactory.getLogger(HDFSUtils.class);


    /**
     * 获取fileSystem
     *
     * @return
     */
    public static FileSystem getFileSystem(HdfsSquid hdfsConnection) throws Exception {
        Configuration configuration = new Configuration();
//        configuration.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
//        configuration.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());
        String host = hdfsConnection.getHost();
        String HDFS_URL = "hdfs://" + host;
        FileSystem fileSystem = FileSystem.get(URI.create(HDFS_URL), configuration);
        return fileSystem;
    }



    /**
     * 解压缩
     *
     * @param inputStream
     * @param cType
     * @return
     */
    public InputStream unCompress(FSDataInputStream inputStream, HDFSCompressionType cType) {
        String gzipCodecClassName = "org.apache.hadoop.io.compress.GzipCodec";
        String snappyCodecClassName = "org.apache.hadoop.io.compress.SnappyCodec";
        String defaultCodecClassName = "org.apache.hadoop.io.compress.DefaultCodec";
        String lz4CodecClassName = "org.apache.hadoop.io.compress.Lz4Codec";
        String bzip2CodecClassName = "org.apache.hadoop.io.compress.BZip2Codec";
        String deflateCodecClassName = "org.apache.hadoop.io.compress.DeflateCodec";

        try {
            Class<?> codecClass = null;
            switch (cType) {
                case BZIP:
                    codecClass = Class.forName(bzip2CodecClassName);
                    break;
                case GZIP:
                    codecClass = Class.forName(gzipCodecClassName);
                    break;
                case SNAPPY:
                    codecClass = Class.forName(snappyCodecClassName);
                    break;
                case DEFLATE:
                    codecClass = Class.forName(deflateCodecClassName);
                    break;
                case LZ4:
                    codecClass = Class.forName(lz4CodecClassName);
                    break;
                default:
                    codecClass = Class.forName(defaultCodecClassName);
                    break;
            }
            Configuration conf = new Configuration();
            CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
            return codec.createInputStream(inputStream);
        } catch (ClassNotFoundException e) {
            logger.error("Compress class not found", e);
        } catch (IOException eo) {
            logger.error("uncompression error", eo);
        }
        return null;
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

    /**
     * HDFS获取流
     * @param fileConnectionSquid
     * @param fileName
     * @return
     */
    public String downFromHdfs(HdfsSquid fileConnectionSquid, String fileName) throws Exception {
        String newFileName = null;
        FileSystem fs = null;
        FSDataInputStream fsDataInputStream = null;
        OutputStream outputStream = null;
        try {
            // HDFS上的文件
            String CLOUD_DESC = "hdfs://" + fileConnectionSquid.getHost() + fileConnectionSquid.getFile_path() + "/" + fileName;
            // down到本地的文件 ,下载文件的位置
            String exString = FileUtils.getFileEx(fileName); /* 获取扩展名 */
            String LOCAL_SRC = StringUtils.isNotBlank(exString) ?
                    ServerEndPoint.ftpDownload_Path + "/" + UUID.randomUUID().toString() + "." + exString :
                    ServerEndPoint.ftpDownload_Path + "/" + UUID.randomUUID().toString();
            newFileName = LOCAL_SRC.substring(LOCAL_SRC.lastIndexOf("/") + 1);
            // 获取conf配置
            Configuration conf = new Configuration();
            // 实例化一个文件系统
            fs = FileSystem.get(URI.create(CLOUD_DESC.replace(" ", "%20")), conf);
            //检查本地是否存在临时文件夹，如果没有就创建
            CreateFileUtil.createDir(ServerEndPoint.ftpDownload_Path);
            // 读出流
            fsDataInputStream = fs.open(new Path(CLOUD_DESC));
            // 写入流
            outputStream = new FileOutputStream(LOCAL_SRC);
            // 将InputStrteam 中的内容通过IOUtils的copyBytes方法复制到OutToLOCAL中
            IOUtils.copyBytes(fsDataInputStream, outputStream, 1024, true);

        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
            logger.debug("下载文件异常(downFromHdfs)");
            throw new Exception("下载文件异常(downFromHdfs)");
        } finally {
            try {
                if (outputStream != null) {
                    outputStream.close();
                }
                if (fsDataInputStream != null) {
                    fsDataInputStream.close();
                }
                if (fs != null) {
                    fs.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return newFileName;
    }

    /**
     * 读取指定路径的文件内容(区分不同的文件类型)
     *
     * @return
     * @throws Exception
     */
    public String getFileContent(HdfsSquid hdfsSquid, String fileName, int doc_format, int length) throws Exception {
        if (doc_format == -1) {
            return getContentByName(fileName, length);
        } else {
            return getContentByType(fileName, doc_format, length);
        }
    }

    /**
     * 根据文件名读取内容
     *
     * @param fileName
     * @return
     * @throws Exception
     */
    private String getContentByName(String fileName, int length) throws Exception {
        fileName = fileName.substring(fileName.lastIndexOf("/") + 1, fileName.length());
        StringBuffer buffer = new StringBuffer("");
        BOMInputStream bomInputStream = null;
        FileInputStream stream = null;
        BufferedReader readerBuffer = null;
        FileOutputStream outStream = null;
        try {
            // office类型
            if (".doc|.docx".contains(FileUtils.getFileEx(fileName))) {
                OfficeFileParser officeFileParser = new OfficeFileParser();
                File file = new File(ServerEndPoint.ftpDownload_Path + "/"
                        + fileName);
                bomInputStream = new BOMInputStream(new FileInputStream(file));
                return officeFileParser.getStringContent(bomInputStream);
            } else if (".xls".contains(FileUtils.getFileEx(fileName))) {
                //转换成csv，防止内存溢出
                String path = ServerEndPoint.ftpDownload_Path + "/" + fileName;
                String csvPath = ServerEndPoint.ftpDownload_Path + "/1" + fileName.substring(0, fileName.indexOf(".xls")) + ".csv";
                XLS2CSV xls2csv = new XLS2CSV(path, csvPath);
                xls2csv.process();
                stream = new FileInputStream(csvPath);
                readerBuffer = new BufferedReader(new InputStreamReader(stream, "utf-8"));
                int len = -1;
                boolean flag = true;
                char[] chs = new char[length];
                while ((len = readerBuffer.read(chs)) != -1 && flag) {
                    String str = new String(chs, 0, len);
                    buffer.append(str);
                    flag = false;
                }
                //删除csv文件
                flag = CreateFileUtil.deleteFile(csvPath);
                return buffer.toString();
            } else if (".xlsx".contains(FileUtils.getFileEx(fileName))) {
                //转换成csv,防止内存溢出
                String path = ServerEndPoint.ftpDownload_Path + "/" + fileName;
                String csvPath = ServerEndPoint.ftpDownload_Path + "/1" + fileName.substring(0, fileName.indexOf(".xlsx")) + ".csv";
                OPCPackage p = OPCPackage.open(path, PackageAccess.READ);
                File file = new File(csvPath);
                if (!file.exists()) {
                    file.createNewFile();
                }
                outStream = new FileOutputStream(file);
                PrintStream out = new PrintStream(outStream, true, "utf-8");
                XLSX2CSV xlsx2csv = new XLSX2CSV(p, out, 10);
                xlsx2csv.process();
                p.close();
                out.flush();
                out.close();
                //读取csv文件
                stream = new FileInputStream(file);
                readerBuffer = new BufferedReader(new InputStreamReader(stream));
                int len = -1;
                boolean flag = true;
                char[] chs = new char[length];
                while ((len = readerBuffer.read(chs)) != -1 && flag) {
                    String str = new String(chs, 0, len);
                    buffer.append(str);
                    flag = false;
                }
                //删除csv文件
                flag = CreateFileUtil.deleteFile(csvPath);
                return buffer.toString();
            } else if (".txt|.log|.lck|.csv|.xml|.xsd|.dtd".contains(FileUtils
                    .getFileEx(fileName))) {// 文本类型
                TxtFileParser txtFileParser = new TxtFileParser();
                File file = new File(ServerEndPoint.ftpDownload_Path + "/"
                        + fileName);
                bomInputStream = new BOMInputStream(new FileInputStream(file));
                //一定要使用参数为inputstream这个函数，因为这个函数里面有文件编码探测器,会解决乱码问题
                return txtFileParser.getStringContent(bomInputStream);
            } else if (".pdf".contains(FileUtils.getFileEx(fileName))) {
                return this.getTextFromPdf(ServerEndPoint.ftpDownload_Path
                        + "/" + fileName);
            } else {
                // 文本类型
                TxtFileParser txtFileParser = new TxtFileParser();
                File file = new File(ServerEndPoint.ftpDownload_Path + "/"
                        + fileName);
                bomInputStream = new BOMInputStream(new FileInputStream(file));
                return txtFileParser.getStringContent(bomInputStream);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (readerBuffer != null) {
                    readerBuffer.close();
                }
                if (stream != null) {
                    stream.close();
                }
                if (bomInputStream != null) {
                    bomInputStream.close();
                }
                if (outStream != null) {
                    outStream.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    /**
     * 根据文件类型读取内容
     *
     * @param fileName
     * @param doc_format
     * @return
     * @throws Exception
     */
    private String getContentByType(String fileName, int doc_format, int length)
            throws Exception {
        StringBuffer buffer = new StringBuffer("");
        fileName = fileName.substring(fileName.lastIndexOf("/") + 1, fileName.length());
        BOMInputStream bomInputStream = null;
        FileInputStream stream = null;
        FileOutputStream outStream = null;
        BufferedReader readerBuffer = null;
        try {
            // office类型
            if (doc_format == ExtractFileType.DOC.value()
                    || doc_format == ExtractFileType.DOCX.value()) {
                OfficeFileParser officeFileParser = new OfficeFileParser();
                File file = new File(ServerEndPoint.ftpDownload_Path + "/"
                        + fileName);
                bomInputStream = new BOMInputStream(new FileInputStream(file));
                return officeFileParser.getStringContent(bomInputStream);
            } else if (doc_format == ExtractFileType.XLS.value()) {
                String path = ServerEndPoint.ftpDownload_Path + "/"
                        + fileName;
                String csvPath = ServerEndPoint.ftpDownload_Path + "/1"
                        + fileName.substring(0, fileName.indexOf(".xls")) + ".csv";
                //转换成csv
                XLS2CSV xls2csv = new XLS2CSV(path, csvPath);
                xls2csv.process();
                stream = new FileInputStream(csvPath);
                readerBuffer = new BufferedReader(new InputStreamReader(stream, "utf-8"));
                int len = -1;
                boolean flag = true;
                char[] chs = new char[length];
                while ((len = readerBuffer.read(chs)) != -1 && flag) {
                    String str = new String(chs, 0, len);
                    buffer.append(str);
                    flag = false;
                }
                //删除csv文件
                CreateFileUtil.deleteFile(csvPath);
                return buffer.toString();
            } else if (doc_format == ExtractFileType.XLSX.value()) {
                //转换成csv，防止内存溢出
                String path = ServerEndPoint.ftpDownload_Path + "/"
                        + fileName;
                String csvPath = ServerEndPoint.ftpDownload_Path + "/1"
                        + fileName.substring(0, fileName.indexOf(".xls")) + ".csv";
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
                //读取csv文件
                stream = new FileInputStream(file);
                readerBuffer = new BufferedReader(new InputStreamReader(stream));
                int len = -1;
                boolean flag = true;
                char[] chs = new char[length];
                while ((len = readerBuffer.read(chs)) != -1 && flag) {
                    String str = new String(chs, 0, len);
                    buffer.append(str);
                    flag = false;
                }
                //删除csv文件
                CreateFileUtil.deleteFile(csvPath);
                return buffer.toString();
            } else if (doc_format == ExtractFileType.TXT.value()
                    || doc_format == ExtractFileType.LOG.value()
                    || doc_format == ExtractFileType.LCK.value()
                    || doc_format == ExtractFileType.CSV.value()
                    || doc_format == ExtractFileType.XML.value()
                    || doc_format == ExtractFileType.XSD.value()
                    || doc_format == ExtractFileType.DTD.value()) {// 文本类型
                TxtFileParser txtFileParser = new TxtFileParser();
                File file = new File(ServerEndPoint.ftpDownload_Path + "/"
                        + fileName);
                bomInputStream = new BOMInputStream(new FileInputStream(file));
                return txtFileParser.getStringContent(bomInputStream);
            } else if (doc_format == ExtractFileType.PDF.value()) {
                return this.getTextFromPdf(ServerEndPoint.ftpDownload_Path
                        + "/" + fileName);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (readerBuffer != null) {
                    readerBuffer.close();
                }
                if (stream != null) {
                    stream.close();
                }
                if (bomInputStream != null) {
                    bomInputStream.close();
                }
                if (outStream != null) {
                    outStream.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    /**
     * pdf单独读取
     *
     * @param filePath
     * @return
     */
    public String getTextFromPdf(String filePath) throws Exception {
        String result = null;
        FileInputStream is = null;
        PDDocument document = null;
        try {
            is = new FileInputStream(filePath);
            PDFParser parser = new PDFParser(is);
            parser.parse();
            document = parser.getPDDocument();
            PDFTextStripper stripper = new PDFTextStripper();
            result = stripper.getText(document);
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("解析pdf异常");
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

//    public String getDataSource() {
//        StringBuffer stringBuffer=new StringBuffer();
//        int len = -1;
//        boolean flag = true;
//        char[] chs = new char[length];
//        while ((len = readerBuffer.read(chs)) != -1 && flag) {
//            String str = new String(chs, 0, len);
//            strBuffer.append(str);
//            flag = false;
//        }
//        return stringBuffer.toString();
//    }

}
