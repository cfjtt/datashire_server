package com.eurlanda.datashire.utility;

import com.eurlanda.datashire.entity.DocExtractSquid;

import java.util.List;

/**
 * Created by My PC on 7/4/2016.
 * 几种文档类型抽取的工具父类
 */
public class ExtractUtilityBase {

    public List<String> getXlsxValues(List<String> list, DocExtractSquid docExtractSquid, XSSFExcelExtractor xssfExcelExtractor) {
        if (0 > docExtractSquid.getHeader_row_no() - 1) {
            int firstRow = docExtractSquid.getFirst_data_row_no()-1;
            int lastRow =  xssfExcelExtractor.getRowNo() >= docExtractSquid.getFirst_data_row_no() + 49 ? docExtractSquid.getFirst_data_row_no() + 49 : xssfExcelExtractor.getRowNo();
            list = xssfExcelExtractor.getList(firstRow,lastRow);//只取数据起始行后50条数据
        } else {
            list.add(xssfExcelExtractor.getList(docExtractSquid.getHeader_row_no() - 1).get(0));//标题行数据
            if(xssfExcelExtractor.getRowNo()>(docExtractSquid.getFirst_data_row_no()-1)){
                int firstRow = docExtractSquid.getFirst_data_row_no()-1;
                int lastRow = xssfExcelExtractor.getRowNo() >= docExtractSquid.getFirst_data_row_no() + 49 ? docExtractSquid.getFirst_data_row_no() + 49 : xssfExcelExtractor.getRowNo();
                list.addAll(xssfExcelExtractor.getList(firstRow,lastRow));//数据起始行后50条数据
            }
        }
        return list;
    }

    public List<String> getXlsValues(List<String> list, DocExtractSquid docExtractSquid, ExcelExtractor extractor) {
        if (0 > docExtractSquid.getHeader_row_no() - 1) {
            int firstRow = docExtractSquid.getFirst_data_row_no()-1;
            int lastRow = extractor.getLastRow() >= docExtractSquid.getFirst_data_row_no() + 49 ? docExtractSquid.getFirst_data_row_no() + 49 : extractor.getLastRow();
            //list = extractor.getList().subList(docExtractSquid.getFirst_data_row_no()-1, extractor.getList().size() >= docExtractSquid.getFirst_data_row_no() + 50 ? docExtractSquid.getFirst_data_row_no() + 50 : extractor.getList().size());//只取数据起始行后50条数据
            list = extractor.getList(firstRow,lastRow);
        } else {
            list.add(extractor.getList(docExtractSquid.getHeader_row_no()-1).get(0));//标题行数据
            //文件总行数大于文件起始行
            if(extractor.getLastRow()>docExtractSquid.getFirst_data_row_no()-1){
                int firstRow = docExtractSquid.getFirst_data_row_no()-1;
                int lastRow = extractor.getLastRow() >= docExtractSquid.getFirst_data_row_no() + 49 ? docExtractSquid.getFirst_data_row_no() + 49 : extractor.getLastRow();
                list.addAll(extractor.getList(firstRow,lastRow));//数据起始行后50条数据
            }
        }
        return list;
    }
}
