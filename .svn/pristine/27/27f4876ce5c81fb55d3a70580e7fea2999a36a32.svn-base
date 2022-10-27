package com.eurlanda.datashire.server.utils.utility;

import com.eurlanda.datashire.entity.DocExtractSquid;
import com.eurlanda.datashire.enumeration.ExtractFileType;
import com.eurlanda.datashire.enumeration.ExtractRowFormatEnum;
import com.eurlanda.datashire.server.exception.ErrorMessageException;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.OSUtils;
import com.eurlanda.datashire.utility.ReadCSVFileUtil;
import com.eurlanda.datashire.utility.StringUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class DocExtractSquidUtil {
    protected static final Logger logger = LogManager.getLogger(DocExtractSquidUtil.class);

    private static final String ROWDELIMITER="\n";
    private static final String COLUMNDELIMITER="\t";
    /**
     * 将字符串转换为table格式
     * @param docString  字符串
     * @param docExtractSquid  抽取的squid
     * @return
     */
    public static List<List<String>> getDocTableList(String docString, DocExtractSquid docExtractSquid) throws Exception {
        if (StringUtils.isNull(docString) || docString.isEmpty()) {
            return null;
        }
        //最终返回的集合
        List<List<String>> returnList = new ArrayList<List<String>>();
        try{
        //换行符
        String row_delimiter = docExtractSquid.getRow_delimiter();
        // 分隔符
        String delimiter = docExtractSquid.getDelimiter();
        // 文件格式
        int doc_Format = docExtractSquid.getDoc_format();

        //在linux上将\r\n,替换成\n
        if (!OSUtils.isWindowsOS()) {
            String lineSeparator = System.getProperty("line.separator");
            if(doc_Format==ExtractFileType.CSV.value()){
                row_delimiter=row_delimiter;
            }else {
                if (docExtractSquid.getRow_delimiter() != null && docExtractSquid.getRow_delimiter().contains("\r\n")) {
                    row_delimiter = docExtractSquid.getRow_delimiter().replaceAll("\r\n", lineSeparator);
                }
            }
            if (docExtractSquid.getDelimiter() != null && docExtractSquid.getDelimiter().contains("\r\n")) {
                delimiter = docExtractSquid.getDelimiter().replaceAll("\r\n", lineSeparator);
            }
        }

        //如果文件是xlsx格式，将换行符\r\n转换成\f  我们自己规定的换行符
        if(doc_Format==ExtractFileType.XLSX.value()){
            row_delimiter = ROWDELIMITER;
        }
        // 文件中数据（记录）的格式(0:Delimited,1:FixedLength, 2:Text)
        ExtractRowFormatEnum row_format = ExtractRowFormatEnum.parse(docExtractSquid.getRow_format());
        // 分割的字段长度
        int field_length = docExtractSquid.getField_length();
        // 标题行
        int header_row_no = docExtractSquid.getHeader_row_no() - 1;
        // 数据起始行
        int first_data_row_no = docExtractSquid.getFirst_data_row_no() - 1;
        String[] rowStr = null;
        //存放所有的行数据。只取起始行后的100行数据。
        List<String> tableList = new ArrayList<String>();

        //标题集合
        String titlecolumn = "";
        //起始数据集合
        List<String> rowDataList = new ArrayList<>();

        if (row_delimiter != null && !row_delimiter.equals("")) {
            //根据换行符分割为行
            rowStr = docString.split(row_delimiter);
            //判断标题行和起始行数是否大于总行数，大于则将第一行为标题行，后面的是数据起始行
            if (header_row_no >= rowStr.length) {
                header_row_no = 0;
                first_data_row_no = 1;
            }else if(first_data_row_no>=rowStr.length){//如果数据起始行大于数据总行数，则默认数据起始行为标题行=1
                first_data_row_no=header_row_no+1;
            }
            for (int i = 0; i < rowStr.length; i++) {
                if (i == header_row_no) {
                    //标题行号数据
                    titlecolumn = rowStr[i];
                } else if (i >= first_data_row_no && i <= first_data_row_no + 99) {
                    //起始数据
                    rowDataList.add(rowStr[i]);
                }
                continue;
            }
            //第一个为标题行
            tableList.add(titlecolumn);
            //后面的行都是数据
            tableList.addAll(rowDataList);
        }

        //如果标题行号小于0，则向返回的集合第一条数据添加一个空的集合。
        int x = 0;
        if (header_row_no < 0) {
            List<String> nullColumn = new ArrayList<>();
            returnList.add(nullColumn);
            x = 1;
        }

        //数据格式为TEXT处理方式
        if (row_format == ExtractRowFormatEnum.TEXT) {
              //每一行的数据就为一列
            for (int i = x; i < tableList.size(); i++) {
                String table = tableList.get(i);
                List<String> columnList = new ArrayList<>();
                columnList.add(table);
                returnList.add(columnList);
            }
            //数据格式为Delimiter处理方式
        }else if (row_format == ExtractRowFormatEnum.DELIMITER) {
            if (delimiter!=null && !delimiter.equals("")) {
                // 如果是Excel文件，需要根据\t分割列
                if(doc_Format == ExtractFileType.XLS.value() || doc_Format ==  ExtractFileType.XLSX.value()){
                    for(int i = x; i < tableList.size(); i++){
                        List<String> columnList=new ArrayList<>();
                        String table = tableList.get(i);
                        Pattern p = Pattern.compile(COLUMNDELIMITER);
                        Matcher m = p.matcher(table);
                        if(m.find()){
                            columnList = Arrays.asList(table.split(COLUMNDELIMITER));
                        }
                        else {
                            //没有\t ，则直接分成一列
                            columnList.add(table);
                        }
                        returnList.add(columnList);
                    }
                } else if(doc_Format == ExtractFileType.CSV.value()){
                      for(int i=x;i<tableList.size();i++){
                          List<String> columnList  = new ArrayList<>();
                          String table = tableList.get(i);
                          columnList = ReadCSVFileUtil.splitCSV(table);
                          returnList.add(columnList);
                      }
                } else {
                    //如果分隔符为”|“，转换一下。
                    /*if(delimiter.equals("|")){
                        for (int i = x; i < tableList.size(); i++) {
                            String table = tableList.get(i);
                            List<String>  columnList = Arrays.asList(table.split("\\|"));
                            returnList.add(columnList);
                        }
                        //在linux上将\r\n,替换成\n
                    }else*/
                    if(delimiter.contains("\r\n")){
                        if(!OSUtils.isWindowsOS()){
                            String lineSeparator = System.getProperty("line.separator");
                            delimiter = delimiter.replaceAll("\r\n",lineSeparator);
                        }
                        for (int i = x; i < tableList.size(); i++) {
                            String table = tableList.get(i);
                            List<String> columnList = Arrays.asList(table.split(delimiter));
                            returnList.add(columnList);
                        }

                    }else{
                        //去除特殊的。delimiter 是什么就根据什么分割。
                        for (int i = x; i < tableList.size(); i++) {
                            String table = tableList.get(i);
                            List<String> columnList = Arrays.asList(table.split(delimiter,-1));
                            returnList.add(columnList);
                        }
                    }
                }
            }
               //数据格式为fixedlength处理方式
        }else if (row_format == ExtractRowFormatEnum.FIXEDLENGTH) {
            if (field_length != 0) {
                for (int i = x; i < tableList.size(); i++) {
                    //分割的长度
                    int split=field_length;
                    //每一行的数据
                    String table = tableList.get(i);
                    Double taLength = Double.valueOf(table.length());
                    // 去掉小数之后，自动加1
                    int rowTotal = (int) Math.ceil(taLength / field_length);
                    //每一行列的个数
                    String[] columnArray = new String[rowTotal];
                    int size = field_length;
                    int start = 0;
                    if (rowTotal > 0) {
                        for (int j = 0; j < columnArray.length; j++) {
                            if (j == 0) {
                                //字符的长度小于截取的长度。则取全部字符
                                if (table.length() < field_length) {
                                    columnArray[0] = table.substring(start, table.length());
                                } else {
                                    columnArray[0] = table.substring(start, field_length);
                                }
                                start = field_length;
                            } else if (j > 0 && j < columnArray.length - 1) {
                                split = split + size;
                                columnArray[j] = table.substring(start, split);
                                start = split;
                            } else {
                                //最后一列，直接截取剩下的全部
                                columnArray[j] = table.substring(start, table.length());
                            }
                        }
                        returnList.add(Arrays.asList(columnArray));
                    }
                }
            }
        }
        }catch (PatternSyntaxException e){
            e.printStackTrace();
            //元数据为空，或者解析失败。
            logger.debug(String.format("特殊分隔符需要转义!"));
            throw new ErrorMessageException(MessageCode.ERR_Special_characters.value());
        }catch (Exception e){
            e.printStackTrace();
        }
        return returnList;
    }


}

