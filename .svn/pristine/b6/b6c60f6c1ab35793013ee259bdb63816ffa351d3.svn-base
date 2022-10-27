package com.eurlanda.datashire.adapter;

import cn.com.jsoft.jframe.utils.FileUtils;
import cn.com.jsoft.jframe.utils.fileParsers.OfficeFileParser;
import cn.com.jsoft.jframe.utils.fileParsers.TxtFileParser;
import com.eurlanda.datashire.entity.MetadataNode;
import com.eurlanda.datashire.enumeration.ExtractFileType;
import com.eurlanda.datashire.utility.CreateFileUtil;
import com.eurlanda.datashire.utility.FilterUtil;
import com.eurlanda.datashire.utility.XLS2CSV;
import com.eurlanda.datashire.utility.XLSX2CSV;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.pdfbox.pdfparser.PDFParser;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.util.PDFTextStripper;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.openxml4j.opc.PackageAccess;
import org.springframework.stereotype.Service;

import javax.swing.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Iterator;
import java.util.Vector;

@Service
public class FileAdapter implements IFileAdapter {
    static Logger logger = Logger.getLogger(FileAdapter.class);
    File myDir;
    File[] contents;
    Vector vectorList;
    Iterator currentFileView;
    File currentFile;
    String path;
    String fileType;

    public FileAdapter() {
        path = new String("");
        vectorList = new Vector();
    }

    public FileAdapter(String path) {
        this.path = path;
        vectorList = new Vector();
    }


    public String getFileType() {
        return fileType;
    }

    public void setFileType(String fileType) {
        this.fileType = fileType;
    }

    /**
     * 设置浏览的路径
     */
    public void setPath(String path) {
        this.path = path;
    }

    /***
     * 返回当前目录路径
     */
    public String getDirectory() throws Exception {
        return myDir.getPath();
    }

    /**
     * 刷新列表
     */
    public void refreshList() throws Exception {
        if (StringUtils.isBlank(this.path)) {
            path = "c:\\";
        }
        myDir = new File(path);
        vectorList.clear();
        contents = myDir.listFiles();
        // 重新装入路径下文件
        for (int i = 0; i < contents.length; i++) {
            vectorList.add(contents[i]);
        }
        currentFileView = vectorList.iterator();

    }

    /**
     * 移动当前文件集合的指针指到下一个条目
     *
     * @return 成功返回true, 否则false
     */
    public boolean nextFile() throws Exception {
        while (currentFileView.hasNext()) {
            currentFile = (File) currentFileView.next();
            return true;
        }
        return false;
    }

    /**
     * 返回当前指向的文件对象的文件名称
     */
    public String getFileName() throws Exception {
        return currentFile.getName();
    }

    /**
     * 返回当前指向的文件对象的文件尺寸
     */
    public String getFileSize() throws Exception {
        return new Long(currentFile.length() / 1024).toString();
    }

    /**
     * 返回当前指向的文件对象的最后修改日期
     */
    public String getFileTimeStamp() throws Exception {
        Calendar cal = Calendar.getInstance();
        long time = currentFile.lastModified();
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        cal.setTimeInMillis(time);
        return formatter.format(cal.getTime());
    }

    /**
     * 返回当前指向的文件对象是否是一个文件目录
     */
    public boolean isDirectory() throws Exception {
        return currentFile.isDirectory();
    }

    /**
     * 返回当前的文件类型
     */
    public String getFileTypeDesc() throws Exception {
        return new JFileChooser().getTypeDescription(currentFile);
    }

    /**
     * 读取指定路径的文件内容(区分不同的文件类型)
     *
     * @return
     * @throws Exception
     */
    public String getFileContent(MetadataNode metadataNode) throws Exception {
        //office类型
        if (".pdf|.doc|.docx|.xls|.xlsx".contains(FileUtils.getFileEx(metadataNode.getAttributeMap().get("FileName")))) {
            OfficeFileParser officeFileParser = new OfficeFileParser();
            File file = new File(metadataNode.getAttributeMap().get("Path"));
            return officeFileParser.getStringContent(file);
        } else if (".txt|.log|.lck|.csv|.xml|.xsd".contains(FileUtils.getFileEx(metadataNode.getAttributeMap().get("FileName")))) {//文本类型
            TxtFileParser txtFileParser = new TxtFileParser();
            File file = new File(metadataNode.getAttributeMap().get("Path"));
            return txtFileParser.getStringContent(file);
        }
        return null;
    }

    /**
     * 过滤指定文件夹下的指定文件
     *
     * @param file
     */
    public void listPath(File file) throws Exception {
        // 接收筛选过后的文件对象数组
        //用文件对象调用listFiles(FilenameFilter filter)方法,  
        //返回抽象路径名数组，这些路径名表示此抽象路径名表示的目录中满足指定过滤器的文件和目录  
        File files[] = file.listFiles(new FilterUtil(fileType));
        //遍历出指定文件路径下的所有符合筛选条件的文件  
        for (File f : files) {
            if (f.isDirectory()) {
                listPath(f.getAbsoluteFile());
            } else {
                System.out.println("所在文件夹=" + f.getParent());
                System.out.println("文件名=" + f.getName());
            }
        }
    }

    /**
     * 获取文件绝对路径
     *
     * @return
     */
    public String getfilePath() {
        if (currentFile.isDirectory()) {
            return currentFile.getAbsolutePath();
        } else {
            return currentFile.getParent();
        }
    }

    @Override
    public String getFileContent(String fileName, String filePath, int doc_format)
            throws Exception {
        if (doc_format == -1) {
            return getContenByName(fileName, filePath);
        } else {
            return getContentByType(filePath, doc_format);
        }
    }

    /**
     * 根据文件名获取内容
     *
     * @param fileName
     * @param filePath
     * @return
     * @throws Exception
     */
    private String getContenByName(String fileName, String filePath)
            throws Exception {
        //没有后缀名的
        if (FileUtils.getFileEx(fileName) == "") {
            // 文本类型
            TxtFileParser txtFileParser = new TxtFileParser();
            File file = new File(filePath);
            return txtFileParser.getStringContent(file);
        } else if (".doc|.docx".contains(FileUtils.getFileEx(fileName))) {
            OfficeFileParser officeFileParser = new OfficeFileParser();
            File file = new File(filePath);
            BOMInputStream bomInputStream = new BOMInputStream(new FileInputStream(file));
            return officeFileParser.getStringContent(bomInputStream);
        } else if (".xls".contains(FileUtils.getFileEx(fileName))) {
            String csvPath = filePath.substring(0, filePath.indexOf(".xls")) + ".csv";
            XLS2CSV xls2csv = new XLS2CSV(filePath, csvPath);
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
            /*InputStream in = new FileInputStream(filePath);
			BOMInputStream bomInputStream = new BOMInputStream(in);
			HSSFWorkbook workbook = new HSSFWorkbook(bomInputStream);
			ExcelExtractor extractor = new ExcelExtractor(workbook);
			extractor.setFormulasNotResults(true);
			extractor.setIncludeSheetNames(false);
			in.close();
			return extractor.getText();*/
            //转换成csv

        } else if (".xlsx".contains(FileUtils.getFileEx(fileName))) {
            String csvPath = filePath.substring(0, filePath.indexOf(".xlsx")) + ".csv";
            OPCPackage p = null;
            try {
                p = OPCPackage.open(filePath, PackageAccess.READ);
            } catch (Exception e) {
                e.printStackTrace();
            }

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
			/*InputStream in = new FileInputStream(filePath);
			BOMInputStream bomInputStream = new BOMInputStream(in);
			XSSFWorkbook workbook = new XSSFWorkbook(bomInputStream);

			XSSFExcelExtractor xssfExcelExtractor=new XSSFExcelExtractor(workbook);
			xssfExcelExtractor.setFormulasNotResults(true);
			xssfExcelExtractor.setIncludeSheetNames(false);
			xssfExcelExtractor.close();
			return xssfExcelExtractor.getText();*/
        } else if (".txt|.log|.lck|.csv|.xml|.xsd|.dtd".contains(FileUtils
                .getFileEx(fileName))) {// 文本类型
            TxtFileParser txtFileParser = new TxtFileParser();
            File file = new File(filePath);
            BOMInputStream bomInputStream = new BOMInputStream(new FileInputStream(file));
            return txtFileParser.getStringContent(bomInputStream);
        } else if (".pdf".contains(FileUtils.getFileEx(fileName))) {
            return this.getTextFromPdf(filePath);
        } else// 所有其他类型,以及没有后缀名的
        {
            // 文本类型
            TxtFileParser txtFileParser = new TxtFileParser();
            File file = new File(filePath);
            BOMInputStream bomInputStream = new BOMInputStream(new FileInputStream(file));
            return txtFileParser.getStringContent(bomInputStream);
        }
    }

    /**
     * 根据文件类型获取内容
     *
     * @param filePath
     * @param doc_format
     * @return
     * @throws Exception
     */
    private String getContentByType(String filePath, int doc_format)
            throws Exception {
        // office类型
        if (doc_format == ExtractFileType.DOC.value()
                || doc_format == ExtractFileType.DOCX.value()) {
            OfficeFileParser officeFileParser = new OfficeFileParser();
            File file = new File(filePath);
            return officeFileParser.getStringContent(file);
        } else if (doc_format == ExtractFileType.XLS.value()) {
			/*InputStream in = new FileInputStream(filePath);
			BOMInputStream bomInputStream = new BOMInputStream(in);
			HSSFWorkbook workbook = new HSSFWorkbook(bomInputStream);
			ExcelExtractor extractor = new ExcelExtractor(workbook);
			extractor.setFormulasNotResults(true);
			extractor.setIncludeSheetNames(false);
			in.close();
			return extractor.getText();*/
            //转化成csv
            String csvPath = filePath.substring(0, filePath.indexOf(".xls")) + ".csv";
            XLS2CSV xls2csv = new XLS2CSV(filePath, csvPath);
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
        } else if (doc_format == ExtractFileType.XLSX.value()) {
			/*InputStream in = new FileInputStream(filePath);
			BOMInputStream bomInputStream = new BOMInputStream(in);
			XSSFWorkbook workbook = new XSSFWorkbook(bomInputStream);
			XSSFExcelExtractor xssfExcelExtractor = new XSSFExcelExtractor(workbook);
			xssfExcelExtractor.setFormulasNotResults(true);
			xssfExcelExtractor.setIncludeSheetNames(false);
			xssfExcelExtractor.close();
			return xssfExcelExtractor.getText();*/
            String csvPath = filePath.substring(0, filePath.indexOf(".xlsx")) + ".csv";
            OPCPackage p = OPCPackage.open(filePath, PackageAccess.READ);
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

        } else if (doc_format == ExtractFileType.TXT.value()
                || doc_format == ExtractFileType.LOG.value()
                || doc_format == ExtractFileType.LCK.value()
                || doc_format == ExtractFileType.CSV.value()
                || doc_format == ExtractFileType.XML.value()
                || doc_format == ExtractFileType.XSD.value()
                || doc_format == ExtractFileType.DTD.value()) {
            TxtFileParser txtFileParser = new TxtFileParser();
            File file = new File(filePath);
            BOMInputStream bomInputStream = new BOMInputStream(new FileInputStream(file));
            return txtFileParser.getStringContent(bomInputStream);
        } else if (doc_format == ExtractFileType.PDF.value()) {
            return this.getTextFromPdf(filePath);
        } else {
            // 文本类型
            TxtFileParser txtFileParser = new TxtFileParser();
            File file = new File(filePath);
            BOMInputStream bomInputStream = new BOMInputStream(new FileInputStream(file));
            return txtFileParser.getStringContent(bomInputStream);
        }
    }

    /**
     * pdf单独读取
     *
     * @param filePath
     * @return
     */
    public String getTextFromPdf(String filePath) {
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
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
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
     * pdf单独读取
     *
     * @param file
     * @return
     */
    public String getTextFromPdf(File file) {
        String result = null;
        FileInputStream is = null;
        PDDocument document = null;
        try {
            is = new FileInputStream(file);
            PDFParser parser = new PDFParser(is);
            parser.parse();
            document = parser.getPDDocument();
            PDFTextStripper stripper = new PDFTextStripper();
            result = stripper.getText(document);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
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

    public static void main(String[] args) {
        FileAdapter fileAdapter = new FileAdapter();
        try {
            System.out.println(fileAdapter.getContentByType("C:\\javac\\eurlanda\\ftpdownload\\test1.xlsx", 4));
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
