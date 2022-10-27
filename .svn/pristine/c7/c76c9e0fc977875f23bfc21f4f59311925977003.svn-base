package com.eurlanda.datashire.sprint7.service.squidflow.subservice;

import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.entity.Column;
import com.eurlanda.datashire.entity.DocExtractSquid;
import com.eurlanda.datashire.entity.ReferenceColumn;
import com.eurlanda.datashire.entity.SourceColumn;
import com.eurlanda.datashire.entity.TableExtractSquid;
import com.eurlanda.datashire.entity.Transformation;
import com.eurlanda.datashire.entity.TransformationLink;
import com.eurlanda.datashire.entity.operation.Grow;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.enumeration.ExtractRowFormatEnum;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.enumeration.TransformationTypeEnum;
import com.eurlanda.datashire.enumeration.datatype.DbBaseDatatype;
import com.eurlanda.datashire.utility.DateTimeUtils;
import com.eurlanda.datashire.utility.DebugUtil;
import com.eurlanda.datashire.utility.OSUtils;
import com.eurlanda.datashire.utility.StringUtils;
import org.apache.commons.lang.math.NumberUtils;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DocExtractSquidServiceSub extends ExtractServicesub {
    public DocExtractSquidServiceSub(String token) {
        super(token);
    }

    public DocExtractSquidServiceSub(IRelationalDataManager adapter) {
        super(adapter);
    }

    public DocExtractSquidServiceSub(String token,
            IRelationalDataManager adapter) {
        super(token, adapter);
    }

    public static int intType = 0;
    public static int decimalType = 0;
    public static int floatType = 0;
    public static int varcharType = 0;
    public static int dateType = 0;
    public static int timestampType = 0;
    
    // 获取ExtractSquid详细信息（如果不在同一个事务里，adapter连接打开、关闭需要调用函数自己处理）
    @SuppressWarnings("deprecation")
    public void setExtractSquidData(String token, final TableExtractSquid squid, boolean inSession,
            IRelationalDataManager adapter) {
        squid.setType(DSObjectType.EXTRACT.value());
        if (squid != null) {
            logger.debug("(extract) squid detail, id=" + squid.getId()
                    + ", name=" + squid.getName() + ", table_name="
                    + squid.getTable_name() + ", type="
                    + SquidTypeEnum.parse(squid.getSquid_type()));
        }
        Map<String, String> paramMap = new HashMap<String, String>(1);
        Map<String, Object> paramMap2;
        List<ReferenceColumn> sourceColumnlist = null;
        List<Integer> sourceColumnId = new ArrayList<Integer>();
        List<Integer> transId = new ArrayList<Integer>();
        // boolean host_column_deleted = false;
        int source_squid_id = 0;
        try {
            if (!inSession) {
                adapter.openSession();
            }
            // 所有目标列
            if (squid.getColumns() == null || squid.getColumns().isEmpty()) {
                paramMap.clear();
                paramMap.put("squid_id", Integer.toString(squid.getId(), 10));
                squid.setColumns(adapter.query2List(true, paramMap, Column.class));
            }
            // 是否增量抽取及表达式
            paramMap2 = adapter.query2Object(true,
                    "SELECT IS_INCREMENTAL, INCREMENTAL_EXPRESSION FROM ds_squid WHERE ID="
                            + squid.getId(), null);
            if (paramMap2 != null && !paramMap2.isEmpty()) {
                squid.setIs_incremental("Y".equals(StringUtils.valueOf2(
                        paramMap2, "is_incremental")));
                squid.setIncremental_expression(StringUtils.valueOf2(paramMap2,
                        "incremental_expression"));
            } else {
                paramMap2 = new HashMap<String, Object>(5);
            }
            // 所有引用列
            sourceColumnlist = adapter
                    .query2List(
                            true,
                            "select c.id,r.host_squid_id squid_id,r.*, nvl2(c.id, 'N','Y') host_column_deleted from"
                                    + " DS_SOURCE_COLUMN c right join DS_REFERENCE_COLUMN r on r.column_id=c.id"
                                    + " where r.reference_squid_id="
                                    + squid.getId(), null,
                            ReferenceColumn.class);
            squid.setSourceColumns(sourceColumnlist);

            // 所有ColumnId
            if (sourceColumnlist != null && !sourceColumnlist.isEmpty()) {
                for (int i = 0; i < sourceColumnlist.size(); i++) {
                    if (!sourceColumnId.contains(sourceColumnlist.get(i)
                            .getColumn_id())) {
                        sourceColumnId.add(sourceColumnlist.get(i)
                                .getColumn_id());
                    }
                    // if(sourceColumnlist.get(i).isHost_column_deleted()){
                    // logger.warn("host_column_deleted: "+sourceColumnlist.get(i));
                    // host_column_deleted = true; // 只要有一个上游列被删除就表示有问题
                    // }
                    if (sourceColumnlist.get(i).getSquid_id() > 0) {
                        source_squid_id = sourceColumnlist.get(i).getSquid_id();
                    }
                }
            }

            // 所有transformation（包括虚拟、真实变换）
            // logger.debug("get all transformation by squid_id = "+squid.getId());
            paramMap.clear();
            paramMap.put("squid_id", Integer.toString(squid.getId(), 10));
            List<Transformation> transformations = adapter.query2List(true,
                    paramMap, Transformation.class);
            if (transformations != null && !transformations.isEmpty()) {
                for (int i = 0; i < transformations.size(); i++) {
                    transId.add(transformations.get(i).getId());
                }
            }

            // 所有上游引用列的虚拟变换
            if (sourceColumnId != null && !sourceColumnId.isEmpty()) {
                paramMap2.clear();
                if (source_squid_id > 0) {
                    paramMap2.put("squid_id", source_squid_id);
                }
                paramMap2.put("column_id", sourceColumnId);
                paramMap2.put("transformation_type_id",
                        TransformationTypeEnum.VIRTUAL.value());
                StringUtils.addAll(transformations, adapter.query2List2(true,
                        paramMap2, Transformation.class));
            }
            squid.setTransformations(transformations);

            // 所有transformation link
            List<TransformationLink> transformationLinks = null;
            if (transId != null && !transId.isEmpty()) {
                paramMap2.clear();
                paramMap2.put("TO_TRANSFORMATION_ID", transId);
                transformationLinks = adapter.query2List2(true, paramMap2,
                        TransformationLink.class);
                squid.setTransformationLinks(transformationLinks);
            }
            // 上游的column被删除
            // PushMessagePacket.push(
            // new MessageBubble(squid.getKey(), squid.getKey(),
            // MessageBubbleCode.ERROR_REFERENCE_COLUMN_DELETED.value(),
            // !host_column_deleted),
            // token);
            if (DebugUtil.isDebugenabled())
                logger.debug(DebugUtil.squidDetail(squid));
        } catch (Exception e) {
            logger.error("getExtractSquidData-datas", e);
        } finally {
            if (!inSession) {
                adapter.closeSession();
            }
        }
    }

    /**
     * 
     * getCSVList(这里用一句话描述这个方法的作用)
     * TODO(这里描述这个方法适用条件 – 可选) 
     * 
     * @Title: getCSVList
     * @Description: TODO
     * @param csvList
     * @param docExtractSquid
     * @return 设定文件 
     * @return List<SourceColumn> 返回类型
     * @throws 
     * @author bo.dang
     */
    public static List<SourceColumn> getCSVList(List<List<String>> csvList, DocExtractSquid docExtractSquid) {

        if (StringUtils.isNull(csvList) || csvList.isEmpty()) {
            return null;
        }
        // 每一行数据记录的列的集合
        String[] columnArray = null;
        // 每一行数据记录的集合
        String[] rowDataArray = null;
        // 文件格式
        int doc_Format = docExtractSquid.getDoc_format();
        // 文件中数据（记录）的格式(0:Delimited,1:FixedLength, 2:Text)
        int row_format = docExtractSquid.getRow_format();
        // 分隔符
        String delimiter = docExtractSquid.getDelimiter();
        // 字段长度
        int field_length = docExtractSquid.getField_length();
        // header的字段number
        int header_row_no = docExtractSquid.getHeader_row_no();
        // 第一条数据记录
        int first_data_row_no = docExtractSquid.getFirst_data_row_no();
        // 数据集
        List<SourceColumn> sourceColumnList = new ArrayList<SourceColumn>();
        SourceColumn sourceColumn = null;
        // 处理docExtractSquid的数据
        // for(int i = header_row_no; i<docList.size(); i++){
        List<String> headerRowList = null;
        String headerRow = null;
        if(header_row_no <= -1 && row_format==2){
        	columnArray = new String[0];
        }
        else {
	        // 取得标题行
        	headerRowList = csvList.get(0);
        	if(StringUtils.isNull(headerRowList) || headerRowList.isEmpty()){
        		columnArray = new String[1];
        		columnArray[0] = null;
        	}
        	else {
        		columnArray = new String[headerRowList.size()];
        		for (int i = 0; i < headerRowList.size(); i++) {
    				columnArray[i] = headerRowList.get(i);
				}
        	}
        }
        	
/*        if(first_data_row_no > docList.size()){
            return null;
        }*/
        //String firstDataRow = docList.get(first_data_row_no);
        // 数据中的第一条记录
        // if(i >= first_data_row_no){
        List<String[]> rowDataList = new ArrayList<String[]>();
        int maxRowDataSize = 0;
      
        int[] rowCountArray = null;
        if(header_row_no <= -1){
        	rowCountArray = new int[csvList.size()];
        }
        else {
        	rowCountArray = new int[csvList.size()-1];
        }
        int x = 0;
        if(header_row_no <= -1){
        	x = 0;
        }
        else {
        	x = 1;
        }
        List<String> cellDataList = null;
        for (int i = x; i < csvList.size(); i++) {
    	   cellDataList = csvList.get(i);
    	   //获取单元数据最多的列
    	   if (cellDataList!=null&&cellDataList.size()>maxRowDataSize){
    		   maxRowDataSize = cellDataList.size();
    	   }
    	   if(StringUtils.isNull(cellDataList) || cellDataList.isEmpty()){
    		   rowDataList.add(null);
    		   if(0 == x){
    			   rowCountArray[i] = 0;
    		   } else {
    			   rowCountArray[i-1] = 0;
    		   }
    	   }
    	   else {
    		   if(0 == x){
    			   rowCountArray[i] = cellDataList.size();
    		   }else {
    			   rowCountArray[i-1] = cellDataList.size();
    		   }
    		   rowDataArray = new String[cellDataList.size()];
    		   for (int j = 0; j < cellDataList.size(); j++) {
    			   rowDataArray[j] = cellDataList.get(j);
			   }
    		   rowDataList.add(rowDataArray);
    	   }
		}
        //获取抽样列中长度最长的行作为标题
        if (header_row_no==-1 && row_format!=2){
        	columnArray = new String[maxRowDataSize];
        	for(int z=0;z<maxRowDataSize;z++){
        		columnArray[z] = "";
        	}
		}
        Map<String,Grow> docGrowList = convertDocGrowList(columnArray, rowCountArray, rowDataList);
        if (StringUtils.isNull(docGrowList)) {
            return null;
        }
        
        Grow grow = null;
        int n=0;
        for (Entry<String, Grow> docMap : docGrowList.entrySet()) {
        	grow = docMap.getValue();
            sourceColumn = new SourceColumn();
            sourceColumn.setName(grow.getName());
            sourceColumn.setData_type(grow.getType());
            sourceColumn.setLength(grow.getLength());
            //去掉防止order长度过大
            sourceColumn.setRelative_order(++n);
            sourceColumn.setSource_table_id(docExtractSquid.getSource_table_id());
            sourceColumn.setNullable(true);
            sourceColumnList.add(sourceColumn);
        }
        return sourceColumnList;
    }
    
    /**
     * 
     * getList(这里用一句话描述这个方法的作用)
     * TODO(这里描述这个方法适用条件 – 可选) 
     * 
     * @Title: getList
     * @Description: TODO
     * @param docList
     * @param docExtractSquid
     * @return 设定文件 
     * @return List<SourceColumn> 返回类型
     * @throws 
     * @author bo.dang
     */
    public static List<SourceColumn> getDocList(List<String> docList, DocExtractSquid docExtractSquid) {
        if (StringUtils.isNull(docList) || docList.isEmpty()) {
            return null;
        }
        // 每一行数据记录的列的集合
        String[] columnArray = null;
        // 每一行数据记录的集合
        String[] rowDataArray = null;
        //在linux上将\r\n,替换成\n
        logger.info("替换后的换行符为:"+docExtractSquid.getDelimiter());
        // 文件格式
        int doc_Format = docExtractSquid.getDoc_format();
        // 文件中数据（记录）的格式(0:Delimited,1:FixedLength, 2:Text)
        ExtractRowFormatEnum row_format = ExtractRowFormatEnum.parse(docExtractSquid.getRow_format());
        // 分隔符
        String delimiter = docExtractSquid.getDelimiter();
        // 字段长度
        int field_length = docExtractSquid.getField_length();
        // header的字段number
        int header_row_no = docExtractSquid.getHeader_row_no()-1;
        // 第一条数据记录
        int first_data_row_no = docExtractSquid.getFirst_data_row_no()-1;
        // 数据集
        List<SourceColumn> sourceColumnList = new ArrayList<SourceColumn>();
        SourceColumn sourceColumn = null;
        // 处理docExtractSquid的数据
        String  headerRow = null;
        // 起始数据值
        int x = 0;
        int[] rowCountArray = null;
        if(header_row_no <= -1 && row_format == ExtractRowFormatEnum.TEXT){
        	//columnArray = new String[0];
        	x = 0;
            rowCountArray = new int[docList.size()];
        }
        else {
            //设置起始数据行
        	x = 1;
            rowCountArray=new int[docList.size()];
            /*if(header_row_no<0){
                headerRow = docList.get(0);
            } else {
                headerRow = docList.get(header_row_no);
            }*/
            headerRow=docList.get(0);
	        // 如果RowFormat=0，那么说明是用分隔符Delimited来分割
	        if (row_format == ExtractRowFormatEnum.DELIMITER) {
	            if(delimiter==null||delimiter==""){
	                return null;
	            }
	            // 如果是Excel，需要\t分割
	            if(doc_Format == 5 || doc_Format == 4){
	                Pattern p = Pattern.compile("\t");
	                Matcher m = p.matcher(headerRow);
	                if(m.find()){
	                    columnArray = headerRow.split("\t");
	                }
	                else {
	                	columnArray = new String[1];
	                	columnArray[0] = headerRow;
	                }
	            }
	            else {
                    if(delimiter.equals("|")){
                        columnArray=headerRow.split("\\|");
                    }else {
                        //split(delimiter,-1)保证不会受空字符串的影响
                        //如果分隔符是/r/n,避免使用spilt(delimiter,-1)
                        logger.info("分隔符为:"+delimiter);
                        if(delimiter.contains("\r\n")){
                            if(!OSUtils.isWindowsOS()){
                                String lineSeparator = System.getProperty("line.separator");
                                delimiter = delimiter.replaceAll("\r\n",lineSeparator);
                            }
                            columnArray = headerRow.split(delimiter);
                        } else {
                            columnArray = headerRow.split(delimiter,-1);
                        }
                    }
	            }
	        }
	        // 如果RowFormat=1，那么说明是用固定长度FixedLength来分割
	        else if (row_format == ExtractRowFormatEnum.FIXEDLENGTH) {
	            if(StringUtils.isNull(field_length) ){
	                
	            }
	            // 去掉小数之后，自动加1
	            int rowTotal = (int) Math.ceil(headerRow.length()*1.0 / field_length);
	            columnArray = new String[rowTotal];
	            for (int j = 0; j < columnArray.length; j++) {
                    int length = field_length>headerRow.length()?headerRow.length():field_length;
                    StringBuffer buffer = new StringBuffer(headerRow.substring(j*length, (j*length + length)>headerRow.length()?headerRow.length():(j*length+length)));
                    while(buffer.length()<3){
                        buffer.append(",");
                    }
                    columnArray[j] = buffer.toString();
                    //columnArray[j] = headerRow.substring(j*length, (j*length + length)>headerRow.length()?headerRow.length():(j*length+length));
	               // columnArray[j] = headerRow.substring(j,j+length);
                }
	        }
	        // 如果RowFormat=2，那么说明源文件是txt,整个txt中的内容就代表一个column定义
	        else if (row_format == ExtractRowFormatEnum.TEXT) {
	            columnArray = new String[1];
	            columnArray[0] = headerRow;
	        }
	        //如果没有标题，启用默认的column名
	        if (header_row_no <0 && columnArray != null && columnArray.length>0){
	        	for (int i=0;i<columnArray.length;i++){
	        		columnArray[i]="";
	        	}
	        }
        
    }
/*        if(first_data_row_no > docList.size()){
            return null;
        }*/
        //String firstDataRow = docList.get(first_data_row_no);
        // 数据中的第一条记录
        // if(i >= first_data_row_no){
        List<String[]> rowDataList = new ArrayList<String[]>();
        String rowData = null;
        //设置起始数据行,如果头号为<=-1，从第一行开始去数据，否则从第二行开始去数据。
        if(header_row_no<=-1){
            x=0;
        } else {
            x=1;
        }
        // 1.如果RowFormat=0，那么说明是用分隔符Delimited来分割
        if (row_format == ExtractRowFormatEnum.DELIMITER) {
            //rowDataArray = firstDataRow.split(delimiter);
            // 如果是Excel，需要\t分割
        	for (int i = x; i < docList.size(); i++) {
        		rowData = docList.get(i);
        		if(StringUtils.isNull(rowData)) {
        			rowDataList.add(null);
          			if(x == 0){
        				rowCountArray[i] = 0;
        			}
        			else {
        				rowCountArray[i-1] = 0;
        			}
        			continue;
        		}
                if(doc_Format == 5 || doc_Format == 4){
                    Pattern p = Pattern.compile("\t");
                    Matcher m = p.matcher(rowData);
                    if(m.find()){
                        rowDataArray = rowData.split("\t");
                    }
                    else {
                        rowDataArray = new String[1];
                        rowDataArray[0] = rowData;
                    }
                }
                else {
                    rowDataArray = rowData.split(delimiter);
                }
                if(x == 0){
                    rowCountArray[i] = rowDataArray.length;
                }
                else {
                    rowCountArray[i-1] = rowDataArray.length;
                }
               rowDataList.add(rowDataArray);
        	}
/*            if(StringUtils.isNull(columnArray) || StringUtils.isNull(rowDataArray)){
            	return null;
            }*/
            //docResultList = convertType(columnArray, rowDataArray);
        }
        // 2.如果RowFormat=1，那么说明是用固定长度FixedLength来分割
        else if (row_format == ExtractRowFormatEnum.FIXEDLENGTH) {
        	for (int i = x; i < docList.size(); i++) {
        		rowData = docList.get(i);
        		if(StringUtils.isNull(rowData)) {
        			rowDataList.add(null);
        			if(x == 0){
        				rowCountArray[i] = 0;
        			}
        			else {
        				rowCountArray[i-1] = 0;
        			}
        			continue;
        		}
            // 去掉小数之后，自动加1
            int rowTotal = (int) Math.ceil(rowData.length()*1.0 / field_length);
            rowDataArray = new String[rowTotal];
            for (int j = 0; j < rowTotal; j++) {
                rowDataArray[j] = rowData.substring(j, j + field_length>rowData.length()?rowData.length():j+field_length);
            }
			if(x == 0){
				rowCountArray[i] = rowDataArray.length;
			}
			else {
				rowCountArray[i-1] = rowDataArray.length;
			}
            rowDataList.add(rowDataArray);
        	}
            //docResultList = convertType(columnArray, rowDataArray);
        }
        // 3.如果RowFormat=2，那么说明源文件是txt,整个txt中的内容就代表一个column定义
        else if(row_format == ExtractRowFormatEnum.TEXT) {
        	for (int i = x; i < docList.size(); i++) {
        		rowData = docList.get(i);
        		if(StringUtils.isNull(rowData)) {
        			rowDataList.add(null);
        			if(x == 0){
        				rowCountArray[i] = 0;
        			}
        			else {
        				rowCountArray[i-1] = 0;
        			}
        			continue;
        		}
            rowDataArray = new String[1];
            rowDataArray[0] = rowData;
			if(x == 0){
				rowCountArray[i] = rowDataArray.length;;
			}
			else {
				rowCountArray[i-1] = rowDataArray.length;
			}
            rowDataList.add(rowDataArray);
            
        	}
            //docResultList = convertType(columnArray, rowDataArray);
        }

        Map<String,Grow> docGrowList = convertDocGrowList(columnArray, rowCountArray, rowDataList);
        if (StringUtils.isNull(docGrowList)) {
            return null;
        }
        Grow grow = null;
        int m=0;
        for (Entry<String,Grow> docMap : docGrowList.entrySet()) {
            String sourceColumnName=null;
        	grow = docMap.getValue();
            sourceColumn = new SourceColumn();
            if(grow.getName().length()>50){
                sourceColumnName=grow.getName().substring(0,10);
                sourceColumn.setName(sourceColumnName);
            }else {
                sourceColumn.setName(grow.getName());
            }
            sourceColumn.setData_type(grow.getType());
            sourceColumn.setLength(grow.getLength());
            sourceColumn.setRelative_order(++m);
            sourceColumn.setSource_table_id(docExtractSquid.getSource_table_id());
            sourceColumn.setNullable(true);
            sourceColumnList.add(sourceColumn);
        }
        return sourceColumnList;
    	
    }
    
    
    /**
     * 得到数据源sourceColumn
     * @param docList
     * @param docExtractSquid
     * @return
     * @author bo.dang
     * @date 2014年5月7日
     */
    public static List<SourceColumn> getDocSourceList(List<String> docList,
            DocExtractSquid docExtractSquid) {
        if (StringUtils.isNull(docList)) {
            return null;
        }
        // 每一行数据记录的列的集合
        String[] columnArray = null;
        // 每一行数据记录的集合
        String[] rowDataArray = null;
        // 文件格式
        int doc_Format = docExtractSquid.getDoc_format();
        // 文件中数据（记录）的格式(0:Delimited,1:FixedLength, 2:Text)
        ExtractRowFormatEnum row_format = ExtractRowFormatEnum.parse(docExtractSquid.getRow_format());
        // 分隔符
        String delimiter = docExtractSquid.getDelimiter();
        // 字段长度
        int field_length = docExtractSquid.getField_length();
        // header的字段number
        int header_row_no = docExtractSquid.getHeader_row_no();
        // 第一条数据记录
        int first_data_row_no = docExtractSquid.getFirst_data_row_no();

        // 数据集
        Map<String, Integer> columnListMap;
        List<SourceColumn> sourceColumnList = new ArrayList<SourceColumn>();
        SourceColumn sourceColumn = null;
        // 处理docExtractSquid的数据
        // for(int i = header_row_no; i<docList.size(); i++){
        String headerRow = null;
        if(header_row_no > docList.size()){
            return null;
        }
        // Text:无结构文本
        if(row_format == ExtractRowFormatEnum.TEXT){
            if(header_row_no <= -1){
                sourceColumn = new SourceColumn();
                sourceColumn.setName("defaultName");
                sourceColumn.setNullable(true);
                sourceColumn.setData_type(DbBaseDatatype.NVARCHAR.value());
                sourceColumn.setLength(256);
                sourceColumn.setRelative_order(1);
                sourceColumn.setSource_table_id(docExtractSquid.getSource_table_id());
                sourceColumnList.add(sourceColumn);
                return sourceColumnList;
            }
            else {
                headerRow = docList.get(header_row_no);
            }
        }
        else {
            headerRow = docList.get(header_row_no);
            
        }
        if (StringUtils.isNull(headerRow)) {
            return null;
        }
        // 列定义的number
        // if(i == header_row_no){
        // 如果RowFormat=0，那么说明是用分隔符Delimited来分割
        if (row_format == ExtractRowFormatEnum.DELIMITER) {
            if(StringUtils.isNull(delimiter)){
                return null;
            }
            // 如果是Excel，需要\t分割
            if(doc_Format == 5 || doc_Format == 4){
                Pattern p = Pattern.compile("\t");
                Matcher m = p.matcher(headerRow);
                if(m.find()){
                    columnArray = headerRow.split("\t");
                }
                else {
                	columnArray = new String[1];
                	columnArray[0] = headerRow;
                }
            }
            else {
                columnArray = headerRow.split(delimiter);
            }
        }
        // 如果RowFormat=1，那么说明是用固定长度FixedLength来分割
        else if (row_format == ExtractRowFormatEnum.FIXEDLENGTH) {
            if(StringUtils.isNull(field_length) ){
                
            }
            // 去掉小数之后，自动加1
            int rowTotal = (int) Math.ceil(headerRow.length() / field_length);
            columnArray = new String[rowTotal];
            for (int j = 0; j < columnArray.length; j++) {
                columnArray[j] = headerRow.substring(j, j + field_length);
            }
        }
        // 如果RowFormat=2，那么说明源文件是txt,整个txt中的内容就代表一个column定义
        else if (row_format == ExtractRowFormatEnum.TEXT) {
            columnArray = new String[1];
            columnArray[0] = headerRow;
        }
        List<Map<String, Integer>> docResultList = null;
        if(first_data_row_no > docList.size()){
            return null;
        }
        String firstDataRow = docList.get(first_data_row_no);
        // 数据中的第一条记录
        // if(i >= first_data_row_no){
        // 如果RowFormat=0，那么说明是用分隔符Delimited来分割
        if (row_format == ExtractRowFormatEnum.DELIMITER) {
            //rowDataArray = firstDataRow.split(delimiter);
            // 如果是Excel，需要\t分割
            if(doc_Format == 5 || doc_Format == 4){
                Pattern p = Pattern.compile("\t");
                Matcher m = p.matcher(firstDataRow);
                if(m.find()){
                    rowDataArray = firstDataRow.split("\t");
                }
            }
            else {
                rowDataArray = firstDataRow.split(delimiter);
            }
/*            if(StringUtils.isNull(columnArray) || StringUtils.isNull(rowDataArray)){
            	return null;
            }*/
            docResultList = convertType(columnArray, rowDataArray);
        }
        // 如果RowFormat=1，那么说明是用固定长度FixedLength来分割
        else if (row_format == ExtractRowFormatEnum.FIXEDLENGTH) {
            // 去掉小数之后，自动加1
            int rowTotal = (int) Math.ceil(firstDataRow.length() / field_length);
            rowDataArray = new String[rowTotal];
            for (int j = 0; j < columnArray.length; j++) {
                rowDataArray[j] = firstDataRow.substring(j, j + field_length);
            }

            docResultList = convertType(columnArray, rowDataArray);
        }
        // 如果RowFormat=2，那么说明源文件是txt,整个txt中的内容就代表一个column定义
        else if(row_format == ExtractRowFormatEnum.TEXT) {
            rowDataArray = new String[1];
            rowDataArray[0] = firstDataRow;
            docResultList = convertType(columnArray, rowDataArray);
        }

        if (StringUtils.isNull(docResultList)) {
            return null;
        }
        
        String header = null;
        int type = 0;
        for (int y = 0; y < docResultList.size(); y++) {
            columnListMap = docResultList.get(y);
            sourceColumn = new SourceColumn();
            Iterator it = columnListMap.entrySet().iterator();
            while (it.hasNext()){
            	Entry entry = (Entry) it.next();
            	header = String.valueOf(entry.getKey());
            	type = (Integer) entry.getValue();
            }
            if(StringUtils.isNotNull(header) && header.trim().length() >=50){
            	sourceColumn.setName(header.trim().substring(0, 20));
            } else {
            	sourceColumn.setName(header);
            }
            sourceColumn.setData_type(type);
            sourceColumn.setRelative_order(y+1);
            sourceColumn.setSource_table_id(docExtractSquid.getSource_table_id());
            sourceColumn.setNullable(true);
            sourceColumnList.add(sourceColumn);
        }
        return sourceColumnList;

    }
    
    /**
     * 获取每行数据记录的列集合信息
     * @param row 每行数据记录
     * @param row_format 数据分隔格式
     * @param delimiter 分隔符
     * @param field_length 固定长度分隔符
     * @return String[] 每行数据记录的列集合信息
     * @author bo.dang
     * @date 2014年5月14日
     */
    public String[] getRowArrayList(
            String row,
            ExtractRowFormatEnum row_format,
            String delimiter,
            int field_length){
        
        // 每一行数据记录的集合
        String[] rowDataArray = null;
        // 数据中的第一条记录
        // if(i >= first_data_row_no){
        // 如果RowFormat=0，那么说明是用分隔符Delimited来分割
        if (row_format == ExtractRowFormatEnum.DELIMITER) {
            rowDataArray = row.split(delimiter);
        }
        // 如果RowFormat=1，那么说明是用固定长度FixedLength来分割
        else if (row_format == ExtractRowFormatEnum.FIXEDLENGTH) {
            // 去掉小数之后，自动加1
            int rowTotal = (int) Math.ceil(row.length() / field_length);
            rowDataArray = new String[rowTotal];
            for (int j = 0; j < rowTotal; j++) {
                rowDataArray[j] = row.substring(j, j + field_length);
            }

        }
        // 如果RowFormat=2，那么说明源文件是txt,整个txt中的内容就代表一个column定义
        else if(row_format == ExtractRowFormatEnum.TEXT) {
            rowDataArray = new String[1];
            rowDataArray[0] = row;
        }
        
        return rowDataArray;
    }
    
    /**
     * 转换column对应的类型
     * @param columnArray
     * @param rowCountArray
     * @param rowDataList
     * @return
     * @author bo.dang
     * @date 2014年5月7日
     */
    public static Map<String,Grow> convertDocGrowList(String[] columnArray, int[] rowCountArray,
        List<String[]> rowDataList) {
    	// 对数据进行排序
        Arrays.sort(rowCountArray);
        
        String[] rowData = null;
        // 数据类型数组
        int[] typeArray = null;
        // 数据length数组
        int[] lengthArray = null;
        // 数据结果集
        List<Grow> docGrowList = new ArrayList<Grow>();
        Map<String,Grow> docGrowMap = new LinkedHashMap<>();
        Grow grow = null;
        // 列名的数目最大值
        int maxColumnCount = columnArray==null?0:columnArray.length;
        // 数据列的数目最大值
        int maxRowCount = rowCountArray==null||rowCountArray.length==0?0:rowCountArray[rowCountArray.length-1];
        // 数据类型
        int type = 0;
        // 列名
        String columnName = null;
        // 默认列名的数目
        int defaultNameCount = 0;
        // 列头大于数据列
        if(maxColumnCount >= maxRowCount){
	        for (int i = 0; i < maxColumnCount; i++) {
	    		intType = 0;
	    		decimalType = 0;
	    		floatType = 0;
	    		varcharType = 0;
	    		dateType = 0;
	    		timestampType = 0;
                columnName = columnArray[i];
	    		grow = new Grow();

	    		if (i>maxRowCount){
	    			// 设为默认的NVARCHAR
	        		grow.setType(DbBaseDatatype.NVARCHAR.value());
	        		// 设为默认的长度:256
	        		grow.setLength(256);
	    		}else{
		    		typeArray = new int[rowDataList.size()];
		    		lengthArray = new int[rowDataList.size()];
		    		if (lengthArray==null||lengthArray.length==0){
		    			// 设为默认的NVARCHAR
		        		grow.setType(DbBaseDatatype.NVARCHAR.value());
		        		// 设为默认的长度:256
		        		grow.setLength(256);
		    		}else{
			    		for (int j = 0; j < rowDataList.size(); j++) {
			    			  rowData = rowDataList.get(j);
			    			  if(rowData==null || i > rowData.length-1){
			    				  typeArray[j] = parseType(null);
			    				  lengthArray[j] = 256;
			    			  }
			    			  else {
			    				  //logger.info(i);
			    				  //logger.info(rowData[i]);
			    				  typeArray[j] = parseType(rowData[i]);
			    				  if(StringUtils.isNull(rowData[i])){
			    					  //默认varchar 256
			    					  lengthArray[j] = 256;
			    				  }
			    				  else {
			    					  lengthArray[j] = rowData[i].length(); 
			    					  //System.out.println(columnArray[i]+":"+rowData[i].length());
			    				  }
			    			  }
						}
			            // 数据类型数组
			         int allTypeArray[] = {intType, decimalType, floatType, varcharType, dateType, timestampType};

			    		
			            Arrays.sort(allTypeArray);
			            Arrays.sort(lengthArray);
			            // 获取数据类型
                        if(varcharType!=0||dateType!=0||timestampType!=0){
                            type = DbBaseDatatype.NVARCHAR.value();
                        }else  if(varcharType==0&&dateType==0&&timestampType==0&&floatType!=0){
                            type=DbBaseDatatype.FLOAT.value();
                        }else if(varcharType==0&&dateType==0&&timestampType==0&&floatType==0&&decimalType!=0){
                            type=DbBaseDatatype.DECIMAL.value();
                        }else {
                            type=DbBaseDatatype.INT.value();
                        }


			         //   type = validateType(allTypeArray[allTypeArray.length - 1]);
			            
			            grow.setType(type);
			            grow.setLength(lengthArray[lengthArray.length - 1]);
		    		}
	    		}
	    		columnName = columnArray[i];
                //此处的目的是防止column名字相同，这里是解决性能问题的关键（这里可以考虑使用hashmap）
	    		grow.setName(getDocExtractColumnName(columnName, docGrowMap));
                docGrowMap.put(grow.getName(),grow);
               // docList.add(grow);

			}
        }else{
        	if (maxColumnCount>0){
	        	for (int i = 0; i < maxColumnCount; i++) {
	        		intType = 0;
	        		decimalType = 0;
	        		floatType = 0;
	        		varcharType = 0;
	        		dateType = 0;
	        		timestampType = 0;
                    columnName = columnArray[i];
	        		grow = new Grow();
	        		if(i >= 0 && i <= maxRowCount - 1) {
                        grow.setName(getDocExtractColumnName(columnName, docGrowMap));
	        		}
	        		else {
	        			defaultNameCount ++;
	        			if(1 == defaultNameCount){
	        				grow.setName("defaultName");
	        			}
	        			else {
	        				grow.setName("defaultName_" + (defaultNameCount - 1));
	        			}
	        		}
	        		for (int j = 0; j < rowDataList.size(); j++) {
	        			  rowData = rowDataList.get(j);
	        			  typeArray = new int[rowDataList.size()];
	        			  lengthArray = new int[rowDataList.size()];
	        			  if(StringUtils.isNull(rowData)){
	        				  typeArray[j] = parseType(null);
	        				  lengthArray[j] = 256;
	        				  continue;
	        			  }
	        			  if(i > rowData.length-1){
	        				  typeArray[j] = parseType(null);
	        				  lengthArray[j] = 256;
	        			  }
	        			  else {
	        				  //logger.info(i);
	        				  //logger.info(rowData[i]);
	        				  typeArray[j] = parseType(rowData[i]);
	        				  if(StringUtils.isNull(rowData[i])){
	        					  lengthArray[j] = 256;
	        				  }
	        				  else {
	        					  lengthArray[j] = rowData[i].length(); 
	        					  //System.out.println(columnArray[i]+":"+rowData[i].length());
	        				  }
	        			  }
	        			
					}
	                // 数据类型数组
	                int allTypeArray[] = {intType, decimalType, floatType, varcharType, dateType, timestampType};
	        		
	                Arrays.sort(allTypeArray);
	                Arrays.sort(lengthArray);
	                // 获取数据类型
                    // 获取数据类型
                    if(varcharType!=0||dateType!=0||timestampType!=0){
                        type = DbBaseDatatype.NVARCHAR.value();
                    }else  if(varcharType==0&&dateType==0&&timestampType==0&&floatType!=0){
                        type=DbBaseDatatype.FLOAT.value();
                    }else if(varcharType==0&&dateType==0&&timestampType==0&&floatType==0&&decimalType!=0){
                        type=DbBaseDatatype.DECIMAL.value();
                    }else {
                        type=DbBaseDatatype.INT.value();
                    }
	        		grow.setType(type);
	        		grow.setLength(lengthArray[lengthArray.length - 1]);
	                //docGrowList.add(grow);
                    docGrowMap.put(grow.getName(),grow);
				}
        	}else{
        		grow = new Grow();
        		grow.setName("defaultName");
        		grow.setType(DbBaseDatatype.NVARCHAR.value());
        		grow.setLength(256);
        		//docGrowList.add(grow);
                docGrowMap.put(grow.getName(),grow);
        	}
        }
        
        return docGrowMap;
    }

    /**
     * 通过元数据集获取Column的名称
     * @param origName
     * @param growList
     * @return
     */
    private static String getDocExtractColumnName(String origName, Map<String,Grow> growList){
        String columnName = "defaultName";
        if(StringUtils.isNotNull(origName)){
            if(origName.trim().length() > 50) {
                if(origName.trim().substring(0, 20).contains(" ")){
                    columnName = origName.trim().substring(0, 20).replaceAll(" ", "_");
                }
                else {
                    columnName = origName.trim();
                }
            }
            else {
                columnName = origName.trim();
            }
            //name为""""为两个双引号时转换成默认名称
            if(origName.replace("\"","").equals("")){
                columnName = "defaultName";
            }
        }
            if (growList.containsKey(columnName)){  /*不能区分大小写*/
                String[] nameItems = columnName.split("_");
                String lastNameItem = nameItems[nameItems.length - 1];
                int lastNameNumber = NumberUtils.toInt(lastNameItem, -1);
                if (lastNameNumber == -1)
                    columnName = getDocExtractColumnName(columnName + "_1", growList);
                else
                    columnName = getDocExtractColumnName(columnName.replace("_"+lastNameNumber, "") + "_" + (lastNameNumber + 1), growList);
            }

        return columnName;
    }

    /**
     * 转换column对应的类型
     * @param columnArray
     * @param rowDataArray
     * @return
     * @author bo.dang
     * @date 2014年5月7日
     */
    public static List<Map<String, Integer>> convertType(String[] columnArray,
            String[] rowDataArray) {
        Map<String, Integer> columnListMap = null;
        List<Map<String, Integer>> docResultList = new ArrayList<Map<String, Integer>>();
        int length = 0;
        int columnLength = 0;
        int dataLength = 0;
        if(StringUtils.isNull(columnArray)){
        	columnLength = 0;
        }
        else {
        	columnLength = columnArray.length;
        }
        if(StringUtils.isNull(rowDataArray)){
        	dataLength = 0;
        }
        else {
        	dataLength = rowDataArray.length;
        }
        if(dataLength < columnLength){
            length = columnLength;
        }
        else {
            length = dataLength;
        }
        // 如果lenght为0，说明columnArray和rowDataArray都为NULL
        if(0 == length){
        	columnListMap = new HashMap<String, Integer>();
        	columnListMap.put("defaultName", DbBaseDatatype.NVARCHAR.value());
        	docResultList.add(columnListMap);
        	return docResultList;
        }
        if(0 != columnLength && 0 == dataLength){
        	for (int i = 0; i < columnLength; i++) {
        		columnListMap = new HashMap<String, Integer>();
        		columnListMap.put(columnArray[i], DbBaseDatatype.NVARCHAR.value());
        		//columnListMap.put("defaultName" + (i++), DbBaseDatatype.UNKNOWN.value());
        		docResultList.add(columnListMap);
			}
        }
        else if(0 == columnLength && 0 != dataLength){
        	for (int j = 0; j < columnLength; j++) {
        		columnListMap = new HashMap<String, Integer>();
        		columnListMap.put("defaultName" + (j++), parseType(rowDataArray[j]));
        		docResultList.add(columnListMap);
			}
        }
        // 如果列表名的数目大于数据列
        else if(columnLength >= dataLength && dataLength > 0){
        	for (int m = 0; m < columnLength; m++) {
        		columnListMap = new HashMap<String, Integer>();
        		if(m <= dataLength - 1){
        			columnListMap.put(columnArray[m], parseType(rowDataArray[m]));
        		}
        		else {
        			columnListMap.put(columnArray[m], DbBaseDatatype.UNKNOWN.value());
        			
        		}
        		docResultList.add(columnListMap);
			}
        }
        // 如果数据列大于列表名的数目
        else if(dataLength >= columnLength && columnLength > 0){
        	for (int n = 0; n < dataLength; n++) {
        		columnListMap = new HashMap<String, Integer>();
        		if(n <= columnLength - 1){
        			columnListMap.put(columnArray[n], parseType(rowDataArray[n]));
        		}
        		else {
        			columnListMap.put("defaultName" + (n + 1 - columnLength), parseType(rowDataArray[n]));
        			//columnListMap.put(columnArray[m], DbBaseDatatype.UNKNOWN.value());
        			
        		}
        		//columnListMap.put("defaultName" + (m++), parseType(rowDataArray[m]));
        		docResultList.add(columnListMap);
			}
        }
        
/*        for (int k = 0; k < length; k++) {
            rowCell = rowDataArray[k];
            columnListMap = new HashMap<String, Integer>();
            columnListMap.put(columnArray[k], parseType(rowCell));
            docResultList.add(columnListMap);

        }*/
        return docResultList;
    }

    /**
     * 判断数据类型
     * parseType(这里用一句话描述这个方法的作用)
     * TODO(这里描述这个方法适用条件 – 可选) 
     * 
     * @Title: parseType
     * @Description: TODO
     * @param rowCell
     * @return 设定文件 
     * @return Integer 返回类型 
     * @throws 
     * @author bo.dang
     */
    public static Integer parseType(String rowCell){
    	if(StringUtils.isNull(rowCell)){
    		varcharType ++;
    		return DbBaseDatatype.NVARCHAR.value();
    	}
			 Object ob;
		    // Types.TIMESTAMP
		    try {
		        DateTimeUtils.DateTimeFormater.setLenient(false);
		        ob = new Timestamp(DateTimeUtils.DateTimeFormater.parse(rowCell).getTime());
		        timestampType ++;
		        return DbBaseDatatype.DATETIME.value();
		 } catch (Exception e) {
		 }
		    
		 // Types.DATE
		 try {
		     ob = DateTimeUtils.DateFormater.parse(rowCell);
		     dateType ++;
		     return DbBaseDatatype.DATETIME.value();
		     
		 } catch (Exception e) {
		 }
		 // Types.INTEGER
		     try {
		         ob = Integer.valueOf(rowCell);
		         intType ++;
		         return DbBaseDatatype.INT.value();
		 
		     } catch (Exception e) {
		     }
		     // Types.FLOAT
		 try {
		     ob = Float.valueOf(rowCell);
		     floatType ++;
		     return DbBaseDatatype.FLOAT.value();
		 } catch (Exception e) {
		     // TODO: handle exception
		 }
		 /*
		  * try { d = Double.valueOf(s);
		  * System.out.println("convert successfully>>>>>>>>" + d); continue;
		  * } catch (Exception e) { // TODO: handle exception
		  * System.out.println("go on>>>>>>>>>Double>>4>>>>>>" + d); }
		  */
		 // Types.VARCHAR
		 try {
		     ob = String.valueOf(rowCell);
		     varcharType ++;
		     return DbBaseDatatype.NVARCHAR.value();
		     // docResultList.get(k).get(columnArray[k]).put("string",
		     // docResultList.get(k).get(columnArray[k]).get("string") +
		     // count);
		 } catch (Exception e) {
		 }
		 varcharType ++;
    	return DbBaseDatatype.NVARCHAR.value();
    }
    
    /**
     * 
     * validateType:验证数据类型
     * TODO(这里描述这个方法适用条件 – 可选) 
     * 
     * @Title: validateType
     * @Description: TODO
     * @param rowType
     * @return 设定文件 
     * @return Integer 返回类型 
     * @throws 
     * @author bo.dang
     */
    public static Integer validateType(int rowType){
    	int type = 0;
		if(rowType == intType) {
			type = DbBaseDatatype.INT.value();
		}
		else if (rowType == decimalType) {
			type = DbBaseDatatype.DECIMAL.value();
		}
		else if(rowType == floatType) {
			type = DbBaseDatatype.FLOAT.value();
		}
		else if (rowType == varcharType) {
			type = DbBaseDatatype.NVARCHAR.value();
		}
		else if (rowType == dateType) {
			type = DbBaseDatatype.NVARCHAR.value();
		}
		else if (rowType == timestampType) {
			type = DbBaseDatatype.NVARCHAR.value();
		}
		
		return type;
    }
    
    /**
     * 根据换行符来解析TXT、DOC和PDF文件
     * getDocListByRowDelimiter(这里用一句话描述这个方法的作用)
     * TODO(这里描述这个方法适用条件 – 可选) 
     * 
     * @Title: getDocListByRowDelimiter
     * @Description: TODO
     * @param docList
     * @param docExtractSquid
     * @return 设定文件 
     * @return List<SourceColumn> 返回类型
     * @throws 
     * @author bo.dang
     */
    public static List<String> getDocListByRowDelimiter(List<String> docList, DocExtractSquid docExtractSquid) {
       if (StringUtils.isNull(docList) || docList.isEmpty()) {
           return null;
       }
       // 每一行数据记录的列的集合
       String[] columnArray = null;
       // 每一行数据记录的集合
       String[] rowDataArray = null;
       // 文件格式
       int doc_Format = docExtractSquid.getDoc_format();
       // 文件中数据（记录）的格式(0:Delimited,1:FixedLength, 2:Text)
       int row_format = docExtractSquid.getRow_format();
       // 分隔符
       String delimiter = docExtractSquid.getDelimiter();
       // 字段长度
       int field_length = docExtractSquid.getField_length();
       // header的字段number
       int header_row_no = docExtractSquid.getHeader_row_no();
       // 第一条数据记录
       int first_data_row_no = docExtractSquid.getFirst_data_row_no();
       // 换行符
       String row_delimiter = docExtractSquid.getRow_delimiter();
       // 记录区分符在记录中的位置
       int row_delimiter_position = docExtractSquid.getRow_delimiter_position();
       // 结果集
       List<String> resultList = new ArrayList<String>();
       // 行记录
       String rowData = null;
       // 转换之后的行记录
       String resultRowData = "";
       // 第一行记录
       String firstRowData = null;
       // 如果换行符的位置是在Begin：0
       if(0 == row_delimiter_position) {
    	   // 第一行记录
    	   firstRowData = docList.get(0);
    	   // 如果第一行记录就是换行符
    	   if(firstRowData.equals(row_delimiter)){
    		   
    	   }
    	   for (int i = 0; i < docList.size(); i++) {
    		   if(firstRowData.equals(row_delimiter)){
    			   continue;
    		   }
    		   rowData = docList.get(i);
    		   if(StringUtils.isNotNull(rowData) && rowData.equals(row_delimiter)){
    			   resultList.add(resultRowData);
    			   resultRowData = "";
    		   }
    		   resultRowData += rowData;
		}
       }
       // 如果换行符位置在end:1
       else if(1 == row_delimiter_position) {
    	   for (int i = 0; i < docList.size(); i++) {
    		   rowData = docList.get(i);
    		   if(StringUtils.isNotNull(rowData) && rowData.equals(row_delimiter)){
    			   resultList.add(resultRowData);
    			   resultRowData = "";
    		   }
    		   resultRowData += rowData;
		}
       }
       

       return resultList;
    }
    
    public static void main(String[] args) {
		String temp = "Suite 99320 255 - 510th Avenue S.W.";
		System.out.println(temp.length());
	}
}
