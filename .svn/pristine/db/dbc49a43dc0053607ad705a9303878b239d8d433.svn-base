package com.eurlanda.datashire.server.utils.utility;

import com.eurlanda.datashire.enumeration.datatype.SystemDatatype;
import com.eurlanda.datashire.utility.DateTimeUtils;
import com.eurlanda.datashire.utility.StringUtils;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 对数据检测的工具类
 * 用来获取字段类型
 * 检查定义字段名称
 */
public class DataTypeDetecation {

    /*用来计数，最后算出该使用哪个类型作为字段类型*/
    private static int intType = 0;
    private static int decimalType = 0;
    private static int doubleType = 0;
    private static int varcharType = 0;
    private static int dateType = 0;
    private static int timestampType = 0;

    /**
     * 检查单一字段下的数据集合中数据最终可以定义为哪种类型，只允许传入100个数据，超过100个就会抛出异常
     * @param columnData
     * @return
     */
    public static SystemDatatype checkColumnType(List<String> columnData) {
        if (columnData.size() > 100)
            throw new RuntimeException("checkColumnType函数传入的集合数量超过100个，请检查代码。");
        initNumbers();
        for (String data :columnData){
            parseType(data);
        }

        SystemDatatype type = SystemDatatype.UNKNOWN;

        if(varcharType!=0||dateType!=0||timestampType!=0) {
            type = SystemDatatype.NVARCHAR;
        }else  if(varcharType==0&&dateType==0&&timestampType==0&&doubleType!=0){
            type=SystemDatatype.DOUBLE;
        }else if(varcharType==0&&dateType==0&&timestampType==0&&doubleType==0&&decimalType!=0){
            type=SystemDatatype.DECIMAL;
        }else if(varcharType==0&&dateType==0&&timestampType==0&&doubleType==0&&decimalType==0 && intType!=0){
            type=SystemDatatype.INT;
        }else{
            type=SystemDatatype.NVARCHAR;
        }

        return type;
    }

    /**
     * 通过单元格数据获取该数据的类型
     * @param cellData
     * @return
     */
    private static SystemDatatype parseType(String cellData) {
        if(StringUtils.isNull(cellData)){
            return SystemDatatype.NVARCHAR;
        }
        Object ob;
        // Types.TIMESTAMP
        try {
            DateTimeUtils.DateTimeFormater.setLenient(false);
            ob = new Timestamp(DateTimeUtils.DateTimeFormater.parse(cellData).getTime());
            timestampType ++;
            return SystemDatatype.DATETIME;
        } catch (Exception e) {
        }

        // Types.DATE
        try {
            ob = DateTimeUtils.DateFormater.parse(cellData);
            dateType ++;
            return SystemDatatype.DATETIME;

        } catch (Exception e) {
        }
        // Types.INTEGER
        try {
            ob = Integer.valueOf(cellData);
            intType ++;
            return SystemDatatype.INT;

        } catch (Exception e) {
        }
        // Types.DOUBLE
        try {
            ob = Double.valueOf(cellData);
            if(Double.isInfinite((Double) ob) || ob.toString().indexOf("E")>0){
                varcharType++;
            } else {
                doubleType++;
            }
            return SystemDatatype.DOUBLE;
        } catch (Exception e) {
        }
        // Types.VARCHAR
        try {
            ob = String.valueOf(cellData);
            varcharType ++;
            return SystemDatatype.NVARCHAR;
        } catch (Exception e) {
        }
        return SystemDatatype.NVARCHAR;
    }

    /**
     * 传入标题行所有字段的集合与字段的总数，根据数猎的字段命名规范将其标准化。
     * 会对重名，中文，非法名称，字段与标题行不符等问题进行处理。
     * @param fieldNames 标题行名称集合
     * @param fieldNumber 字段的数量
     * @return 根据字段数量生成的标准Column名称
     */
    public static List<String> getColumnNames(List<String> fieldNames, int fieldNumber){
        List<String> columnNames = new ArrayList<>();

        //预防空指针无法处理下游逻辑
        List<String> names;
        if (fieldNames == null)
            names = new ArrayList<>();
        else
            names = fieldNames;

        for (String fieldName : names) {
            //一定要先检查数据库字段的合法性
            String checkedDBName = checkDBFieldNames(fieldName);
            //然后再检查重名
            String checkDuplicateName = checkDuplicateNames(checkedDBName, columnNames);
            columnNames.add(checkDuplicateName);
        }

        //通过循环将不够的字段补齐
        for (int i = 0; i < fieldNumber - names.size(); i++){
            String checkDuplicateName = checkDuplicateNames("col", columnNames);
            columnNames.add(checkDuplicateName);
        }

        return columnNames;
    }

    /**
     * 将计数器初始化
     */
    private static void initNumbers(){
        intType = 0;
        decimalType = 0;
        doubleType = 0;
        varcharType = 0;
        dateType = 0;
        timestampType = 0;
    }

    /**
     * 检查名称是否符合数据库字段合法性
     * @param name 需要检查的名称
     * @return 检查后修改正确的名称
     */
    private static String checkDBFieldNames(String name){
        //字段长度太长。只取前200个
        if(name.length()>200){
            name=name.substring(0,200);
        }
       String regEx = "(^_([a-zA-Z0-9\\s]_?)*$)|(^[a-zA-Z](_?[a-zA-Z0-9\\s])*_?$)"; //校验数据库字段合法性的正则表达式
       // String regEx = "(^_([a-zA-Z0-9]_?)*$)|(^[a-zA-Z](_?[a-zA-Z0-9])*_?$)"; //校验数据库字段合法性的正则表达式
        Pattern pattern = Pattern.compile(regEx);
        Matcher matcher = pattern.matcher(name);
        if (matcher.matches())
            return name.trim().replace("\r","").replace("\n","");
        else
            return "col";
    }

    /**
     * 对重名进行检测与处理，最后返回全新的名称
     * @param name 当前需要检测的名称
     * @param columnNames 已经存在的名称
     * @return 重命名的名称
     */
    private static String checkDuplicateNames(String name, List<String> columnNames){
        if (columnNames.contains(name)){
            String newName = name + "_1";
            while (columnNames.contains(newName)){
                //这里绝对不会出现数组越界
                String numberString = newName.split("_")[1];
                int newNumber = Integer.parseInt(numberString) + 1;
                newName = name + "_" + newNumber;
            }
            return newName;
        }else {
            return name;
        }
    }
}
