package com.eurlanda.datashire.utility;

import cn.com.jsoft.jframe.utils.CharsetDetector;
import cn.com.jsoft.jframe.utils.FileUtils;
import cn.com.jsoft.jframe.utils.fileParsers.OfficeFileParser;
import cn.com.jsoft.jframe.utils.fileParsers.TxtFileParser;
import com.eurlanda.datashire.adapter.db.HdfsAdapter;
import com.eurlanda.datashire.entity.DocExtractSquid;
import com.eurlanda.datashire.entity.FileInfo;
import com.eurlanda.datashire.entity.HdfsSquid;
import com.eurlanda.datashire.enumeration.ExtractFileType;
import com.eurlanda.datashire.enumeration.HDFSCompressionType;
import com.eurlanda.datashire.socket.ServerEndPoint;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;
import org.apache.pdfbox.pdfparser.PDFParser;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.util.PDFTextStripper;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.openxml4j.opc.PackageAccess;
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
import java.util.UUID;


public class HdfsUtils extends ExtractUtilityBase {
    private static Logger logger = Logger.getLogger(HdfsUtils.class);// 记录日志

    /**
     * 获取fileSystem
     *
     * @return
     */
    public static FileSystem getFileSystem(HdfsSquid hdfsConnection) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("fs.hdfs.impl.disable.cache", "true");
//        configuration.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
//        configuration.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());
        String host = hdfsConnection.getHost();
        String HDFS_URL = "hdfs://" + host;
        FileSystem fileSystem = FileSystem.get(URI.create(HDFS_URL), configuration);
        return fileSystem;
    }

    /**
     * 获取HDFS指定目录下文件状态列表
     *
     * @return fileStatusList 获取的文件集合
     * @throws FileNotFoundException
     * @throws IOException
     */
    public static List<FileInfo> getFileStatus(HdfsSquid hdfsConnection)
            throws Exception {
        HdfsAdapter HdfsAdapter = new HdfsAdapter();
        String path = hdfsConnection.getFile_path();
        int depth = hdfsConnection.getIncluding_subfolders_flag() == 1 ? hdfsConnection.getMax_travel_depth() : 1;
        List<FileInfo> fileInfos = new ArrayList<FileInfo>();
        fileInfos = HdfsAdapter.getFileList(path, depth, hdfsConnection);
        return fileInfos;
    }

    /**
     * 下载指定的文件
     *
     * @param hdfsConnection
     * @throws FileNotFoundException
     * @throws IOException
     */
    public static String getHdfsFilePath(HdfsSquid hdfsConnection, String fileName) throws FileNotFoundException, IOException {
        String LOCAL_SRC = null;
        String newFileName = null;
        try {
            //将表名提取出来
            if (fileName.contains("/")) {
                int index = fileName.lastIndexOf("/");
                newFileName = fileName.substring(index);
            } else {
                newFileName = fileName;
            }
            // down到本地的文件
            LOCAL_SRC = ServerEndPoint.ftpDownload_Path + "/" + newFileName;
            // HDFS上的文件
            String CLOUD_DESC = "hdfs://" + hdfsConnection.getHost() + hdfsConnection.getFile_path() + "/" + fileName;
            // 获取conf配置
            Configuration conf = new Configuration();
            // 实例化一个文件系统
            FileSystem fs = FileSystem.get(URI.create(CLOUD_DESC.replace(" ", "%20")), conf);
            // 读出流
            FSDataInputStream HDFS_IN = fs.open(new Path(CLOUD_DESC));
            // 写入流
            OutputStream OutToLOCAL = new FileOutputStream(LOCAL_SRC);
            // 将InputStrteam 中的内容通过IOUtils的copyBytes方法复制到OutToLOCAL中
            IOUtils.copyBytes(HDFS_IN, OutToLOCAL, 1024, true);
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
            logger.debug("下载文件异常(downFromHdfs)");

        }

        return LOCAL_SRC;

    }

    /**
     * 上传本地文件
     *
     * @param src
     * @param dst
     * @param hdfsConnection
     * @throws IOException
     */
    public static void uploadFile(String src, String dst, HdfsSquid hdfsConnection) throws Exception {
        FileSystem fs = HdfsUtils.getFileSystem(hdfsConnection);
        Path srcPath = new Path(src); //原路径
        Path dstPath = new Path(dst); //目标路径
        //调用文件系统的文件复制函数,前面参数是指是否删除原文件，true为删除，默认为false
        fs.copyFromLocalFile(false, srcPath, dstPath);
        fs.close();
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

    public static void main(String[] args) {
        try {
            HdfsSquid hdfsSquid = new HdfsSquid();
            hdfsSquid.setHost("192.168.137.221");
        /*	hdfsSquid.setFile_path("/hbase");
            DocExtractSquid docExtractSquid=new DocExtractSquid();
			docExtractSquid.setTable_name("ORACLE的基本语法集锦.doc");
			String encode="";*/
            //测试文件目录
            /*List<FileInfo> fileStatus=HdfsUtils.getFileStatus(hdfsSquid);
			for(int i=0;i<fileStatus.size();i++)
			{
				System.out.println("文件名"+fileStatus.get(i).getFileName()+"  是否文件夹"+fileStatus.get(i).isIs_directory());
			}*/
            //测试上传文件
            uploadFile("D:/TestData/Research2010.txt", "/eurlanda/TestData/Research2010.txt", hdfsSquid);
            //读取文件内容
            //List<String> content=new HdfsUtils().readHDFSFile(hdfsSquid,docExtractSquid, docExtractSquid.getTable_name(), encode);
		/*for(String string:content)
		{
			System.out.println(string);
		}*/
            //System.out.println(content.toString());
            //new HdfsUtils().downFromHdfs(hdfsSquid, docExtractSquid);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    /**
     * 以行为单位读取文件，常用于读面向行的格式化文件
     */
    public static void readFileByLines(File file) {
        BufferedReader reader = null;
        try {
            System.out.println("以行为单位读取文件内容，一次读一整行：");
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            // 一次读入一行，直到读入null为文件结束
            while ((tempString = reader.readLine()) != null) {
                // 显示行号
                System.out.println("line " + line + ": " + tempString);
                line++;
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
    }

    /**
     * 读取hdfs文件的内容
     * dst 文件全路径
     *
     * @param hdfsConnection
     * @param docExtractSquid
     * @return
     */
    public List<String> readHDFSFile(HdfsSquid hdfsConnection, DocExtractSquid docExtractSquid, String fileName, String encode) throws Exception {
        BufferedReader reader = null;
        List<String> list = new ArrayList<String>();
        int index = fileName.lastIndexOf("/");
        String NewFileName = fileName.substring(index + 1);
        String newFileName = null;
        try {
            String extString = FileUtils.getFileEx(fileName);
            if (".doc|.docx|.xls|.xlsx|.pdf".contains(extString) && StringUtils.isNotEmpty(extString)) {
                //下载文件到本地  newFileName是文件下载保存在本地的名称
                newFileName = this.downFromHdfs(hdfsConnection, fileName);
                //读取内容
                FileFolderUtils fileFolderUtils = new FileFolderUtils();
                if (docExtractSquid.getDoc_format() == ExtractFileType.XLS.value()) {

                    //InputStream in = new FileInputStream(ServerEndPoint.ftpDownload_Path + "/" + newFileName);
                    //BOMInputStream bomInputStream = new BOMInputStream(in);
                    //转成csv防止读取大文件的时候，防止内存溢出

                    String path = ServerEndPoint.ftpDownload_Path+"/"+newFileName;
                    String csvPath = ServerEndPoint.ftpDownload_Path+"/1"+newFileName.substring(0,newFileName.indexOf(".xls"))+".csv";
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
                    CreateFileUtil.deleteFile(csvPath);
                    /*//return list;
                    HSSFWorkbook workbook = new HSSFWorkbook(bomInputStream);
                    ExcelExtractor extractor = new ExcelExtractor(workbook);
                    extractor.setFormulasNotResults(true);
                    extractor.setIncludeSheetNames(false);
                    list = super.getXlsValues(list,docExtractSquid,extractor);
                    extractor.close();*/

                } else if (docExtractSquid.getDoc_format() == ExtractFileType.XLSX.value()) {
                    //FileInputStream stream = new FileInputStream(ServerEndPoint.ftpDownload_Path+"/"+newFileName);
                    //xlsx转换成csv，防止读取大文件的excel的时候，内存溢出
                    OPCPackage p = OPCPackage.open(ServerEndPoint.ftpDownload_Path + "/" + newFileName, PackageAccess.READ);
                    String csvPath = ServerEndPoint.ftpDownload_Path + "/1" + newFileName.substring(0, newFileName.indexOf(".xlsx")) + ".csv";
                    File file = new File(csvPath);
                    if (!file.exists()) {
                        file.createNewFile();
                    }
                    FileOutputStream outStream = new FileOutputStream(file);
                    PrintStream out = new PrintStream(outStream, false, "utf-8");
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
                    int headerRowNo = docExtractSquid.getHeader_row_no() - 1;// 列信息所在行
                    int firstDataRowNo = docExtractSquid.getFirst_data_row_no() - 1;// 值属性所在行
                    int endDataRowNo = firstDataRowNo + 50;
                    String line = "";
                    while ((line = readerBuffer.readLine()) != null) {
                        if (lineCount >= endDataRowNo)
                            break;
                        if (lineCount == headerRowNo) {
                            list.add(line);
                        } else if (lineCount != headerRowNo && lineCount <= endDataRowNo) {
                            if (lineCount >= firstDataRowNo) {
                                list.add(line);
                            }
                        }
                        lineCount++;
                    }
                    CreateFileUtil.deleteFile(csvPath);
                    //return list;
                    /*XSSFExcelExtractor xssfExcelExtractor = new XSSFExcelExtractor(
                            ServerEndPoint.ftpDownload_Path + "/" + newFileName);
                    xssfExcelExtractor.setFormulasNotResults(true);
                    xssfExcelExtractor.setIncludeSheetNames(false);
                    list = super.getXlsxValues(list,docExtractSquid,xssfExcelExtractor);
                    xssfExcelExtractor.close();*/
                } else {
                    String content = this.getFileContent(hdfsConnection, newFileName, docExtractSquid.getDoc_format());
                    list = fileFolderUtils.getDocContent(content, docExtractSquid, encode);
                }
                //下载删除的文件
                boolean flag = CreateFileUtil.deleteFile(ServerEndPoint.ftpDownload_Path + "/" + fileName);
                if (!flag) {
                    logger.error("从HDFS下载到本地的文件没有删除成功,请手动清理");
                }
            } else {
                FileSystem fs = HdfsUtils.getFileSystem(hdfsConnection);
                StringBuilder builder = new StringBuilder(200);
                String dst = HdfsUtils.replacePath(hdfsConnection.getFile_path()) + "/" + fileName;
                Path path = new Path(dst);
                // 路径是否存在
                if (fs.exists(path)) {
                    FSDataInputStream is = fs.open(path);
                    InputStreamReader input = null;
                    //解压缩处理
                    if (docExtractSquid.getCompressiconCodec() == HDFSCompressionType.NOCOMPRESS.value()) {
                        //input = new InputStreamReader(is,Charset.forName(encode));
                        if (fileName.indexOf(".orc") > 0) {
                            org.apache.hadoop.hive.ql.io.orc.Reader orcFile = OrcFile.createReader(fs, path);
                            RecordReader orcReader = OrcInputFormat.createReaderFromFile(orcFile, fs.getConf(), 0, orcFile.getContentLength());
                            int line = 0;
                            int lineCount = 0;
                            int headerRowNo = docExtractSquid.getHeader_row_no();// 列信息所在行
                            int firstDataRowNo = docExtractSquid.getFirst_data_row_no();// 值属性所在行
                            int endDataRowNo = firstDataRowNo + 49;
                            if (orcReader != null) {
                                while (orcReader.hasNext()) {
                                    Object row = orcReader.next(null);
                                    if (StringUtils.isEmpty(row + "")) {
                                        continue;
                                    }
                                    if (lineCount >= endDataRowNo)
                                        break;
                                    if (lineCount == headerRowNo) {
                                        list.add(row + "");
                                    } else if (lineCount != headerRowNo && lineCount <= endDataRowNo) {
                                        list.add(row + "");
                                    }
                                    lineCount++;
                                }
                            }

                        } else if (fileName.indexOf(".parquet") > 0) {
                            ParquetMetadata readFooter = ParquetFileReader.readFooter(fs.getConf(), fs.getFileStatus(path), ParquetMetadataConverter.NO_FILTER);
                            final MessageType schema = readFooter.getFileMetaData().getSchema();
                            GroupFactory factory = new SimpleGroupFactory(schema);
                            Group group = factory.newGroup();
                            GroupReadSupport readSupport = new GroupReadSupport();
                            ParquetReader<Group> parquetReader = new ParquetReader<Group>(fs.getConf(), fs.getFileStatus(path).getPath(), readSupport);
                            String line = "";
                            int lineCount = 0;
                            int headerRowNo = docExtractSquid.getHeader_row_no();// 列信息所在行
                            int firstDataRowNo = docExtractSquid.getFirst_data_row_no();// 值属性所在行
                            int endDataRowNo = firstDataRowNo + 49;
                            while ((line = parquetReader.read() + "") != null) {
                                if (StringUtils.isEmpty(line)) {
                                    continue;
                                }
                                if (lineCount >= endDataRowNo)
                                    break;
                                if (lineCount == headerRowNo) {
                                    list.add(line);
                                } else if (lineCount != headerRowNo && lineCount <= endDataRowNo) {
                                    list.add(line);
                                }
                                lineCount++;
                            }
                        } else {
                            //文件编码探测器
                            CharsetDetector detector = new CharsetDetector();
                            BOMInputStream bomInputStream = new BOMInputStream(is);
                            BufferedInputStream bufferedStream = new BufferedInputStream(bomInputStream);
                            String charsetName = detector.detect(bufferedStream, bufferedStream.available() > 1024 * 1024 ? 1024 * 1024 : bufferedStream.available());
                            if (charsetName == null) {
                                charsetName = "utf-8";
                            }
                            try {
                                reader = new BufferedReader(new InputStreamReader(bufferedStream, charsetName));
                            } catch (Exception e) {
                                if (e instanceof UnsupportedEncodingException) {
                                    reader = new BufferedReader(new InputStreamReader(bomInputStream, encode));
                                }
                            }
//                            reader = new BufferedReader(input);
                            String line;
                            int lineCount = 0;
                            int headerRowNo = docExtractSquid.getHeader_row_no() - 1;// 列信息所在行
                            int firstDataRowNo = docExtractSquid.getFirst_data_row_no() - 1;// 值属性所在行
                            int endDataRowNo = firstDataRowNo + 50;
                            while ((line = reader.readLine()) != null) {
                                if (lineCount >= endDataRowNo)
                                    break;
                                if (lineCount == headerRowNo) {
                                    list.add(line);
                                } else if (lineCount != headerRowNo && lineCount <= endDataRowNo) {
                                    if (lineCount >= firstDataRowNo) {
                                        list.add(line);
                                    }
                                }
                                lineCount++;
                            }
                        }
                    } else {
                        input = new InputStreamReader(this.unCompress(is, HDFSCompressionType.valueOf(docExtractSquid.getCompressiconCodec())), Charset.forName(encode));
                        reader = new BufferedReader(input);
                        builder = new StringBuilder(200);
                        String line = "";
                        int lineCount = 0;
                        int headerRowNo = docExtractSquid.getHeader_row_no();// 列信息所在行
                        int firstDataRowNo = docExtractSquid.getFirst_data_row_no();// 值属性所在行
                        int endDataRowNo = firstDataRowNo + 49;
                        while ((line = reader.readLine()) != null) {
                            if (lineCount >= endDataRowNo)
                                break;
                            if (lineCount == headerRowNo) {
                                list.add(line);
                            } else if (lineCount != headerRowNo && lineCount <= endDataRowNo) {
                                list.add(line);
                            }
                            lineCount++;
                        }
                    }
                    is.close();
                    fs.close();
                }
            }
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
            throw new Exception("读取文件内容异常(readHDFSFile)", e);
        }
        return list;
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
     * 读取hdfs文件的内容的前50条信息
     * dst 文件全路径
     *
     * @param hdfsConnection
     * @param fileName
     * @return
     */
    public String getPartContent(HdfsSquid hdfsConnection, String fileName, String encode, int doc_format, boolean unCompression) throws Exception {
        BufferedReader reader = null;
        StringBuilder builder = null;
        try {
            if (".doc|.docx|.xls|.xlsx|.pdf".contains(FileUtils.getFileEx(fileName)) && FileUtils.getFileEx(fileName) != "") {
                //下载文件到本地  newFileName是文件下载保存在本地的名称
                String newFileName = this.downFromHdfs(hdfsConnection, fileName);
                //读取内容
                FileFolderUtils fileFolderUtils = new FileFolderUtils();
                String content = this.getFileContent(hdfsConnection, newFileName, doc_format);
                //删除下载在本地的文件
                boolean flag = CreateFileUtil.deleteFile(ServerEndPoint.ftpDownload_Path + "/" + newFileName);

                if (!flag) {
                    logger.error("从HDFS下载到本地的文件没有删除成功,请手动清理");
                }
                return fileFolderUtils.getPartContent(content, encode);
            } else {
                FileSystem fs = HdfsUtils.getFileSystem(hdfsConnection);
                String dst = HdfsUtils.replacePath(hdfsConnection.getFile_path()) + "/" + fileName;
                Path path = new Path(dst);
                // 路径是否存在
                if (fs.exists(path)) {
                    FSDataInputStream is = fs.open(path);
                    String extentName = path.getName().substring(path.getName().lastIndexOf(".") + 1);
                    //对压缩文件的处理
                    HDFSCompressionType cType = HDFSCompressionType.NOCOMPRESS;
                    switch (extentName) {
                        case "lz4":
                            cType = HDFSCompressionType.LZ4;
                            break;
                        case "snappy":
                            cType = HDFSCompressionType.SNAPPY;
                            break;
                        case "bz2":
                            cType = HDFSCompressionType.BZIP;
                            break;
                        case "gz":
                            cType = HDFSCompressionType.GZIP;
                            break;
                        case "deflate":
                            cType = HDFSCompressionType.DEFLATE;
                            break;
                    }
                    InputStreamReader inputStreamReader = null;
                    if (unCompression) {
                        inputStreamReader = new InputStreamReader(this.unCompress(is, cType));
                        reader = new BufferedReader(inputStreamReader);
                        builder = new StringBuilder(200);
                        String line;
                        int lineCount = 0;
                        while ((line = reader.readLine()) != null) {
                            builder.append(line);
                            builder.append(System.getProperty("line.separator"));
                            lineCount++;
                            if (lineCount > 49) {// 只读取前50行的内容(只针对文本)
                                break;
                            }
                        }
                    } else {
                        if (fileName.indexOf(".orc") > 0) {
                            //fs.getConf().set("io.compression.codecs","org.apache.hadoop.io.compress.DefaultCodec,org.apache.hadoop.io.compress.BZip2Codec,org.apache.hadoop.io.compress.Lz4Codec,org.apache.hadoop.io.compress.SnappyCodec,com.hadoop.compression.lzo.LzoCodec,org.apache.hadoop.io.compress.GzipCodec");
                            org.apache.hadoop.hive.ql.io.orc.Reader orcFile = OrcFile.createReader(fs, path);
                            RecordReader orcReader = OrcInputFormat.createReaderFromFile(orcFile, fs.getConf(), 0, orcFile.getContentLength());
                            int line = 0;
                            if (orcReader != null) {
                                builder = new StringBuilder(200);
                                while (orcReader.hasNext()) {
                                    Object row = orcReader.next(null);
                                    if (StringUtils.isNull(row + "")) {
                                        continue;
                                    }
                                    builder.append(row);
                                    builder.append(System.getProperty("line.separator"));
                                    line++;
                                    if (line > 50) {
                                        break;
                                    }
                                }
                            }

                        } else if (fileName.indexOf(".parquet") > 0) {
                            ParquetMetadata readFooter = ParquetFileReader.readFooter(fs.getConf(), fs.getFileStatus(path), ParquetMetadataConverter.NO_FILTER);
                            final MessageType schema = readFooter.getFileMetaData().getSchema();
                            GroupFactory factory = new SimpleGroupFactory(schema);
                            Group group = factory.newGroup();
                            GroupReadSupport readSupport = new GroupReadSupport();
                            ParquetReader<Group> parquetReader = new ParquetReader<Group>(fs.getConf(), fs.getFileStatus(path).getPath(), readSupport);
                            int row = 0;
                            String line = "";
                            builder = new StringBuilder(200);
                            while ((line = parquetReader.read() + "") != null) {
                                row++;
                                if (row > 50) {
                                    break;
                                }
                                if (StringUtils.isNull(line)) {
                                    continue;
                                }
                                builder.append(line);
                                builder.append(System.getProperty("line.separator"));

                            }
                        } else {
                            //编码自动探测器
                            CharsetDetector detector = new CharsetDetector();
                            BufferedInputStream bufferStream = new BufferedInputStream(new BOMInputStream(is));
                            if (null == is) {
                                throw new Exception("该路径下无该文件,或者文件类型不支持");
                            }
                            String charsetName = null;
                            try {
                                charsetName = detector.detect(bufferStream, bufferStream.available() > 1024 * 1024 ? 1024 * 1024 : bufferStream.available());
                            } catch (Exception e) {

                            }
                            if (StringUtils.isNull(charsetName)) {
                                charsetName = encode;
                            }
                            try {
                                reader = new BufferedReader(new InputStreamReader(
                                        bufferStream, charsetName));
                            } catch (Exception e) {
                                if (e instanceof UnsupportedEncodingException) {
                                    reader = new BufferedReader(new InputStreamReader(
                                            bufferStream, encode));
                                }
                            }
                            builder = new StringBuilder(200);
                            String line;
                            int lineCount = 0;
                            while ((line = reader.readLine()) != null) {
                                builder.append(line);
                                builder.append(System.getProperty("line.separator"));
                                lineCount++;
                                if (lineCount > 49) {// 只读取前50行的内容(只针对文本)
                                    break;
                                }
                            }
                        }
                    }

                    is.close();
                    fs.close();
                }
            }
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
            logger.error("读取hdfs异常:" + e);
            throw new Exception("读取文件内容异常(readHDFSFile)");
        }
        return builder.toString();
    }

    /**
     * 下载指定的文件
     *
     * @param hdfsConnection
     * @throws Exception
     */
    public String downFromHdfs(HdfsSquid hdfsConnection, String fileName) throws Exception {
        String newFileName = null;
        try {
            // HDFS上的文件
            String CLOUD_DESC = "hdfs://" + hdfsConnection.getHost() + hdfsConnection.getFile_path() + "/" + fileName;
            // down到本地的文件 ,下载文件的位置
            String exString = FileUtils.getFileEx(fileName); /* 获取扩展名 */
            String LOCAL_SRC = StringUtils.isNotBlank(exString) ?
                    ServerEndPoint.ftpDownload_Path + "/" + UUID.randomUUID().toString() + "." + exString :
                    ServerEndPoint.ftpDownload_Path + "/" + UUID.randomUUID().toString();
            newFileName = LOCAL_SRC.substring(LOCAL_SRC.lastIndexOf("/") + 1);
            // 获取conf配置
            Configuration conf = new Configuration();
            // 实例化一个文件系统
            FileSystem fs = FileSystem.get(URI.create(CLOUD_DESC.replace(" ", "%20")), conf);
            //检查本地是否存在临时文件夹，如果没有就创建
            CreateFileUtil.createDir(ServerEndPoint.ftpDownload_Path);
            // 读出流
            FSDataInputStream HDFS_IN = fs.open(new Path(CLOUD_DESC));
            // 写入流
            OutputStream OutToLOCAL = new FileOutputStream(LOCAL_SRC);

            // 将InputStrteam 中的内容通过IOUtils的copyBytes方法复制到OutToLOCAL中
            IOUtils.copyBytes(HDFS_IN, OutToLOCAL, 1024, true);

        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
            logger.debug("下载文件异常(downFromHdfs)");
            throw new Exception("下载文件异常(downFromHdfs)");
        }
        return newFileName;
    }

    /**
     * 读取指定路径的文件内容(区分不同的文件类型)
     *
     * @return
     * @throws Exception
     */
    public String getFileContent(HdfsSquid hdfsSquid, String fileName, int doc_format) throws Exception {
        if (doc_format == -1) {
            return getContentByName(fileName);
        } else {
            return getContentByType(fileName, doc_format);
        }
    }

    /**
     * 根据文件名读取内容
     *
     * @param fileName
     * @return
     * @throws Exception
     */
    private String getContentByName(String fileName) throws Exception {
        fileName = fileName.substring(fileName.lastIndexOf("/") + 1, fileName.length());
        // office类型
        if (".doc|.docx".contains(FileUtils.getFileEx(fileName))) {
            OfficeFileParser officeFileParser = new OfficeFileParser();
            File file = new File(ServerEndPoint.ftpDownload_Path + "/"
                    + fileName);
            BOMInputStream bomInputStream = new BOMInputStream(new FileInputStream(file));
            return officeFileParser.getStringContent(bomInputStream);
        } else if (".xls".contains(FileUtils.getFileEx(fileName))) {
           /* InputStream in = new FileInputStream(ServerEndPoint.ftpDownload_Path + "/"
                    + fileName);
            BOMInputStream bomInputStream = new BOMInputStream(in);*/
            //转换成csv，防止内存溢出
            String path = ServerEndPoint.ftpDownload_Path + "/" + fileName;
            String csvPath = ServerEndPoint.ftpDownload_Path + "/1" + fileName.substring(0, fileName.indexOf(".xls")) + ".csv";
            XLS2CSV xls2csv = new XLS2CSV(path, csvPath);
            xls2csv.process();
            FileInputStream stream = new FileInputStream(csvPath);
            BufferedReader readerBuffer = new BufferedReader(new InputStreamReader(stream, "utf-8"));
            int lineCount = 0;
            String line = "";
            StringBuffer buffer = new StringBuffer("");
            while ((line = readerBuffer.readLine()) != null) {
                if (lineCount > 100) {
                    break;
                }
                buffer.append(line);
                buffer.append(System.lineSeparator());
                lineCount++;
            }

            //删除csv文件
            boolean flag = CreateFileUtil.deleteFile(csvPath);
            return buffer.toString();
           /* HSSFWorkbook workbook = new HSSFWorkbook(bomInputStream);
            ExcelExtractor extractor = new ExcelExtractor(workbook);
            extractor.setFormulasNotResults(true);
            extractor.setIncludeSheetNames(false);
            extractor.close();*/
        } else if (".xlsx".contains(FileUtils.getFileEx(fileName))) {
            //转换成csv,防止内存溢出
            String path = ServerEndPoint.ftpDownload_Path + "/" + fileName;
            String csvPath = ServerEndPoint.ftpDownload_Path + "/1" + fileName.substring(0, fileName.indexOf(".xlsx")) + ".csv";
            OPCPackage p = OPCPackage.open(path, PackageAccess.READ);
            File file = new File(csvPath);
            if (!file.exists()) {
                file.createNewFile();
            }
            FileOutputStream outStream = new FileOutputStream(file);
            PrintStream out = new PrintStream(outStream, true, "utf-8");
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
            String line = "";
            StringBuffer buffer = new StringBuffer("");
            while ((line = readerBuffer.readLine()) != null) {
                if (lineCount > 100) {
                    break;
                }
                buffer.append(line);
                buffer.append(System.lineSeparator());
                lineCount++;
            }

            //删除csv文件
            boolean flag = CreateFileUtil.deleteFile(csvPath);
            return buffer.toString();
            /*XSSFExcelExtractor xssfExcelExtractor = new XSSFExcelExtractor(ServerEndPoint.ftpDownload_Path + "/"
                    + fileName);
            xssfExcelExtractor.setFormulasNotResults(true);
            xssfExcelExtractor.setIncludeSheetNames(false);
            xssfExcelExtractor.close();
            return xssfExcelExtractor.getText(100);*/
        } else if (".txt|.log|.lck|.csv|.xml|.xsd|.dtd".contains(FileUtils
                .getFileEx(fileName))) {// 文本类型
            TxtFileParser txtFileParser = new TxtFileParser();
            File file = new File(ServerEndPoint.ftpDownload_Path + "/"
                    + fileName);
            BOMInputStream bomInputStream = new BOMInputStream(new FileInputStream(file));
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
            BOMInputStream bomInputStream = new BOMInputStream(new FileInputStream(file));
            return txtFileParser.getStringContent(bomInputStream);
        }
    }

    /**
     * 根据文件类型读取内容
     *
     * @param fileName
     * @param doc_format
     * @return
     * @throws Exception
     */
    private String getContentByType(String fileName, int doc_format)
            throws Exception {
        fileName = fileName.substring(fileName.lastIndexOf("/") + 1, fileName.length());
        // office类型
        if (doc_format == ExtractFileType.DOC.value()
                || doc_format == ExtractFileType.DOCX.value()) {
            OfficeFileParser officeFileParser = new OfficeFileParser();
            File file = new File(ServerEndPoint.ftpDownload_Path + "/"
                    + fileName);
            BOMInputStream bomInputStream = new BOMInputStream(new FileInputStream(file));
            return officeFileParser.getStringContent(bomInputStream);
        } else if (doc_format == ExtractFileType.XLS.value()) {
            String path = ServerEndPoint.ftpDownload_Path + "/"
                    + fileName;
            String csvPath = ServerEndPoint.ftpDownload_Path + "/1"
                    + fileName.substring(0, fileName.indexOf(".xls")) + ".csv";
            //转换成csv
            XLS2CSV xls2csv = new XLS2CSV(path, csvPath);
            xls2csv.process();
            FileInputStream stream = new FileInputStream(csvPath);
            BufferedReader readerBuffer = new BufferedReader(new InputStreamReader(stream, "utf-8"));
            int lineCount = 0;
            String line = "";
            StringBuffer buffer = new StringBuffer("");
            while ((line = readerBuffer.readLine()) != null) {
                if (lineCount > 100) {
                    break;
                }
                buffer.append(line);
                lineCount++;
            }

            //删除csv文件
            CreateFileUtil.deleteFile(csvPath);
            return buffer.toString();
            /*InputStream in = new FileInputStream(ServerEndPoint.ftpDownload_Path + "/"
                    + fileName);
            BOMInputStream bomInputStream = new BOMInputStream(in);
            HSSFWorkbook workbook = new HSSFWorkbook(bomInputStream);
            ExcelExtractor extractor = new ExcelExtractor(workbook);
            extractor.setFormulasNotResults(true);
            extractor.setIncludeSheetNames(false);
            extractor.close();
            return extractor.getText(100);*/
        } else if (doc_format == ExtractFileType.XLSX.value()) {
           /* XSSFExcelExtractor xssfExcelExtractor = new XSSFExcelExtractor(ServerEndPoint.ftpDownload_Path + "/"
                    + fileName);
            xssfExcelExtractor.setFormulasNotResults(true);
            xssfExcelExtractor.setIncludeSheetNames(false);
            xssfExcelExtractor.close();
            return xssfExcelExtractor.getText(100);*/
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
            FileOutputStream outStream = new FileOutputStream(file);
            PrintStream out = new PrintStream(outStream, false, "utf-8");
            XLSX2CSV xlsx2csv = new XLSX2CSV(p, out, 10);
            xlsx2csv.process();
            p.close();
            out.flush();
            out.close();
            //读取csv文件8
            FileInputStream input = new FileInputStream(file);
            BufferedReader readerBuffer = new BufferedReader(new InputStreamReader(input));
            int lineCount = 0;
            String line = "";
            StringBuffer buffer = new StringBuffer("");
            while ((line = readerBuffer.readLine()) != null) {
                if (lineCount > 100) {
                    break;
                }
                buffer.append(line);
                lineCount++;
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
            BOMInputStream bomInputStream = new BOMInputStream(new FileInputStream(file));
            return txtFileParser.getStringContent(bomInputStream);
        } else if (doc_format == ExtractFileType.PDF.value()) {
            return this.getTextFromPdf(ServerEndPoint.ftpDownload_Path
                    + "/" + fileName);
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

}
