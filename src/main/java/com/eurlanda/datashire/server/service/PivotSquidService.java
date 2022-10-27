package com.eurlanda.datashire.server.service;

import com.alibaba.fastjson.JSONArray;
import com.eurlanda.datashire.enumeration.TransformationTypeEnum;
import com.eurlanda.datashire.server.dao.*;
import com.eurlanda.datashire.server.model.Base.SquidModelBase;
import com.eurlanda.datashire.server.model.*;
import com.eurlanda.datashire.utility.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by test on 2017/11/24.
 */
@Service
public class PivotSquidService {

    @Autowired
    private PivotSquidDao pivotSquidDao;
    @Autowired
    private ColumnDao columnDao;
    @Autowired
    private GetMeatDataService getMeatDataService;
    @Autowired
    private SquidLinkDao squidLinkDao;
    @Autowired
    private TransformationDao transformationDao;
    @Autowired
    private TransformationLinkDao transformationLinkDao;
    @Autowired
    private TransformationInputsDao transformationInputsDao;
    @Autowired
    private SquidDao squidDao;
    @Autowired
    private ReferenceColumnDao referenceColumnDao;
    @Autowired
    private ColumnService columnService;

    private static final Logger logger = LoggerFactory.getLogger(PivotSquidService.class);

    public int insertPivotSquid(PivotSquid pivotSquid){
        pivotSquidDao.insertPivotSquid(pivotSquid);
        return pivotSquid.getId();
    }
    public int updatePivotSquid(PivotSquid pivotSquid){
        return  pivotSquidDao.updatePivotSquid(pivotSquid);
    }
    public PivotSquid selectByPrimaryKey(Integer id) {
        return pivotSquidDao.selectByPrimaryKey(id);
    }


    public Map<String,Object> generatePivotColumns(PivotSquid pivotSquid) throws Exception{
        Map<String,Object> map = new HashMap<>();


        //分组列，value,pivot列不能重复,pivot列值要与pivotColumn类型相兼容
        Map<String,Object> result = validatePivotValueIsCompatible(pivotSquid);
        if(!(boolean)result.get("flag")){//验证失败
            return result;
        }
        Map<String,Object> valueResultMap =  validateAggregationTypeIsCompatibleValueType(pivotSquid);
        if(!(boolean)valueResultMap.get("flag")){//验证失败
            return valueResultMap;
        }
        //将之前的trans,transLink,transInput,column删除
        validatePivotDataIsIdentical(pivotSquid);
        //更新pivotSquid
        updatePivotSquid(pivotSquid);
        //1.生成转换后的column,注意类型是value列的类型
        String pivotValueJson = pivotSquid.getPivotColumnValue();
        List<String> pivotValue = JSONArray.parseArray(pivotValueJson,String.class);
        //去重处理
        HashSet<String> noEqualsValue = new HashSet<>(pivotValue);
        //用来批量添加的column集合
        List<Column> columns=new ArrayList<>();
        //查询右侧的transformation
        List<Transformation> transformationRightList = new ArrayList<>();
        //用来批量添加的左边的transformation集合
        List<Transformation> transformationLeftList = new ArrayList<>();
        //用来需要的添加的transformation集合
        List<Transformation> transformations=new ArrayList<>();
        //用来批量添加的transformationLink集合
        List<TransformationLink> transformationLinkList=new ArrayList<>();
        //用来批量添加的referenceColumn集合
        List<ReferenceColumn> referenceColumns=new ArrayList<>();
        //获取value列的字段信息
        Column valueColum = columnDao.selectByPrimaryKey(pivotSquid.getValueColumnId());
        //查询出上游的SquidLink
        SquidLink squidLink = squidLinkDao.getSquidLinkListByToSquidId(pivotSquid.getId());
        int noGroupColumnOrder = 0;
        int s = 1;
        for(String pivotColumn : noEqualsValue){

            Column column = new Column();
            if(valueResultMap.get("dataType") != null){
                column.setData_type((Integer) valueResultMap.get("dataType"));
            }else {
                column.setData_type(valueColum.getData_type());
            }
            column.setAggregation_type(valueColum.getAggregation_type());
            column.setLength(valueColum.getLength());
            column.setPrecision(valueColum.getPrecision());
            column.setNullable(valueColum.isNullable());
            column.setRelative_order(++noGroupColumnOrder);
            //如果包含中文就转换为自定义名字
            if(pivotColumn == null){//允许有NULL值行
               column.setName("col_" + s++);
            }else if(StringUtils.isHavaChinese(pivotColumn)){//判断是否有汉字
               column.setName("col_" + s++);
            }else if(pivotColumn.equals("")){ //空字符串
               column.setName("col_" + s++);
            }else if(StringUtils.isStartWithNumber(pivotColumn)){//是否以数字开头
                column.setName("col_" + s++);
            }else if(StringUtils.nameIsCol_number(pivotColumn)) {
               column.setName("col_" + s++);
            } else {
               if (!pivotColumn.equals(StringUtils.replaceSource(pivotColumn))) { //有违法字符
                   column.setName("col_" + s++);
               }else { //没有违法字符,要保留原有名字
                   for (Column column1 :columns){//防止与之前的column重名!
                       if(column1.getName().equals(pivotColumn)){
                           column1.setName("col_" + s++);
                       }
                   }

                   column.setName(pivotColumn);
               }
            }
            column.setDescription(pivotColumn);
            column.setSquid_id(pivotSquid.getId());

            columns.add(column);
        }


        //添加分组列
        //分组列，value,pivot列不能重复
        String groupColumnIds = pivotSquid.getGroupByColumnIds();
        //分组列id
        String[] groupByIds = null;

        if(groupColumnIds.equals("")){
            groupByIds = new String[0];//分组列为空
        }else{
            groupByIds = groupColumnIds.split(",");
        }


        List<Column> groupByColumnList = new ArrayList<>();

        int groupColumnOrder = noEqualsValue.size();
        for (String columnId : groupByIds){

            if(columnId.equals("") || columnId == null){
                continue;
            }
            Column newColumn = columnDao.selectByPrimaryKey(Integer.parseInt(columnId));
            newColumn.setSquid_id(pivotSquid.getId());
            newColumn.setId(null);
            //确保分组列与非分组列不会重名
            for(Column noGruopColumn : columns){
                String exitsName = noGruopColumn.getName();
                if(newColumn.getName().equals(exitsName)){
                    noGruopColumn.setName("col_" + s++);
                }
            }
            newColumn.setRelative_order(++groupColumnOrder);
            newColumn.setIs_groupby(1);//分组列
            groupByColumnList.add(newColumn);
        }

        //非分组列入库
        columnDao.insert(columns);
        List<Integer> noGroupByColumnIdList = new ArrayList<>();
        for(Column noGruopByColumn : columns){
            noGroupByColumnIdList.add(noGruopByColumn.getId());//保存非分组列columnId
        }




        //分组列column入库
        if(groupByColumnList.size() > 0){
            columnDao.insert(groupByColumnList);
        }


        List<Integer> groupByColumnIdList = new ArrayList<>();
        for(Column gruopByColumn : groupByColumnList){
            groupByColumnIdList.add(gruopByColumn.getId());//保存分组列columnId
        }


        //整合所有的columns传给前台
        columns.addAll(groupByColumnList);
        map.put("ColumnList",columns);

        //右侧虚拟transformation
        List<Integer> linkIds = new ArrayList<>();
        linkIds.add(squidLink.getId());
        //pivotSquid只能有一个上游,一个link和一个pivotSquid对应一个上游，对应一组referenceColumn
        referenceColumns = referenceColumnDao.selectReColumnBySquLinkid(linkIds);
        for(ReferenceColumn referenceColumn:referenceColumns){
            transformationRightList.add(getMeatDataService.initTransformation(squidLink.getTo_squid_id(),referenceColumn.getColumn_id(),TransformationTypeEnum.VIRTUAL.value(),referenceColumn.getData_type(),1));
        }


        //添加左侧虚拟transformation
        for(Column column:columns){
            transformationLeftList.add(getMeatDataService.initTransformation(squidLink.getTo_squid_id(),column.getId(),TransformationTypeEnum.VIRTUAL.value(),column.getData_type(),1));
        }
        transformations.addAll(transformationLeftList);
        transformations.addAll(transformationRightList);
        transformationDao.insert(transformations);
        List<com.eurlanda.datashire.server.model.TransformationInputs> transformationInputsLists=new ArrayList<>();
        for(Transformation transformation : transformations){
            List<Map<String,Object>> mapList=SquidLinkService.getInputscache().get(transformation.getTranstype()+"");
            if(mapList != null && mapList.size() > 0){
                List<com.eurlanda.datashire.server.model.TransformationInputs> transformationInputsList=new ArrayList<>();
                for(Map<String,Object> inputsMap:mapList){
                    com.eurlanda.datashire.server.model.TransformationInputs transformationInputs=new com.eurlanda.datashire.server.model.TransformationInputs();
                    String description=inputsMap.get("description").toString();
                    String relative_order=inputsMap.get("inputOrder").toString();
                    transformationInputs.setRelative_order(Integer.parseInt(relative_order));
                    transformationInputs.setInput_Data_Type(transformation.getOutput_data_type());
                    transformationInputs.setDescription(description);
                    transformationInputs.setSource_transform_id(0);
                    transformationInputs.setTransformationId(transformation.getId());
                    transformationInputsList.add(transformationInputs);
                }
                transformation.setInputs(transformationInputsList);
                transformationInputsLists.addAll(transformationInputsList);
            }
        }
        //批量创建transformationInputs
        transformationInputsDao.insert(transformationInputsLists);
        map.put("TransformationList",transformations);
        //初始化transformationLink
        Integer rightTransformationId = null;
        for(int i = 0; i < transformationRightList.size(); i++){
            Integer rigthTransformationColumnId = transformationRightList.get(i).getColumn_id();
            if((int)valueColum.getId() == (int)rigthTransformationColumnId){
                rightTransformationId = transformationRightList.get(i).getId();
            }
        }


        boolean flag2 = false;
        //pivot列的column与原value列相连
        for(int i = 0 ;i < transformationLeftList.size();i++){
            TransformationLink transformationLink = null;
            boolean flag = false;
            for(int j = 0; j < noGroupByColumnIdList.size();j++){
                if((int)transformationLeftList.get(i).getColumn_id() == (int)noGroupByColumnIdList.get(j)){//非分组列与原value列相连
                    transformationLink = getMeatDataService.mergeTransformationLink(rightTransformationId,transformationLeftList.get(i).getId(),i+1);
                    transformationLinkList.add(transformationLink);
                    flag = true;
                }
            }
            if (flag){
                continue;
            }

            if(flag2){
                break;
            }
            int num = i;//该循环只需要执行一次
            for(int j = 0; j < transformationRightList.size();j++){
                for(int k = 0; k < groupByIds.length; k++){
                    if(groupByIds[k].equals(transformationRightList.get(j).getColumn_id()+"")){//分组列与原分组列列相连
                        Transformation refTrans = transformationRightList.get(j);
                        int refColumnId = refTrans.getColumn_id();
                        for(ReferenceColumn rc :  referenceColumns) {
                            if(rc.getColumn_id() == refColumnId) {
                                for(Column c : columns) {
                                    if(c.getName().equals(rc.getName())) {
                                        for(Transformation tran : transformationLeftList) {
                                            if (tran.getColumn_id().intValue() == c.getId().intValue()) {
                                                transformationLink = getMeatDataService.mergeTransformationLink(refTrans.getId(),tran.getId(),i+1);
                                                transformationLinkList.add(transformationLink);
                                                flag2 = true;
                                            }
                                        }
                                    }
                                }
                            }
                        }



                    }
                }

            }

        }
        //批量添加transformationLink
        transformationLinkDao.insert(transformationLinkList);
        map.put("TransformationLinkList",transformationLinkList);

        //根据下游查询下下游  同步下游
        List<SquidLink> squidLinks = squidLinkDao.getSquidLinkListByFromSquid(pivotSquid.getId());//查询出所有从当前Squid发出的link
        if(squidLinks != null && squidLinks.size() > 0){
            //遍历每一条link
            for(SquidLink squidLink1 : squidLinks){
                //查出第i条link的第一个下游
                SquidModelBase squidModelBase = squidDao.selectByPrimaryKey(squidLink1.getTo_squid_id());
                //递归每一条link的下游，下下游，。。。。
                getMeatDataService.synchronousInsertColumn(pivotSquid.getId(),squidModelBase.getId(),squidModelBase.getSquid_type(),transformations,columns,referenceColumns);
            }
        }
        map.put("flag",true);
        return map;
    }




    /**
     * 校验聚合类型与value列类型是否兼容
     * @return
     */
    public  Map<String,Object> validateAggregationTypeIsCompatibleValueType(PivotSquid pivotSquid) throws Exception{
        Map<String,Object> resultMap = new Hashtable<>();
        resultMap.put("errorType","2");//聚合类型与value列类型不兼容
        Integer valueColumnId = pivotSquid.getValueColumnId();
        Column valueColum = columnDao.selectByPrimaryKey(valueColumnId);
        if(valueColumnId == null || valueColum == null){
            logger.error("--------valueColumnId或valueColum为空----------------");
            resultMap.put("flag",false);
            resultMap.put("errorType",3);
            return resultMap;
        }
        int valueSysDataType = valueColum.getData_type();//获取系统类型
        int valueMappingDataType = dataTypeMapping(valueSysDataType);//获取自定义映射类型
        Integer aggreType = pivotSquid.getAggregationType();
        if(aggreType == null){
            logger.error("聚合方式为空");
            resultMap.put("flag",false);
            return resultMap;
        }
        if(valueMappingDataType == -1 || valueMappingDataType == 4){
            logger.error("value列类型不合法！");
            resultMap.put("flag",false);
            return resultMap;
        }

        if(aggreType == 1){//avg
            if(valueMappingDataType != 1){
                logger.error("-----只有数值类型才能求平均值--------");
                resultMap.put("flag",false);
                return resultMap;
            }else{
                //逻辑型，二进制类型也不能去平均 csv
                if(valueSysDataType == 4 || valueSysDataType == 11 || valueSysDataType == 12 || valueSysDataType == 15){
                    logger.error("-----逻辑型，二进制类型不能取平均值--------");
                    resultMap.put("flag",false);
                    return resultMap;
                }else {//满足avg的条件，将value列的类型修改为double类型
                    resultMap.put("dataType",6);
                }
            }

        }else if(aggreType == 4){//count
            resultMap.put("dataType",1);
        }else if(aggreType == 3){//sum
            if(valueMappingDataType != 1){
                logger.error("-----sum必须为数值类型--------");
                resultMap.put("flag",false);
                return resultMap;
            }else{
                if(valueSysDataType == 15 || valueSysDataType == 4 || valueSysDataType == 11 || valueSysDataType == 12){
                    logger.error("-----二进制，逻辑型，csv不能做sum--------");
                    resultMap.put("flag",false);
                    return resultMap;
                }
            }
        }

        resultMap.put("flag",true);
        return resultMap;
    }

    /**
     * 判断pivot是否发生变换(暂时生成pivot列之前就删除一次之前的数据)
     * 待优化
     */
    public boolean validatePivotDataIsIdentical(PivotSquid vaildatePivotSquid) throws  Exception{


        //同步下游时，删除下游所有分支之前的数据
        List<Integer> delColumns = columnDao.selectColumnBySquidId(vaildatePivotSquid.getId());
        if(delColumns.size() > 0){
            columnService.deleteColumn(delColumns,0);
        }
        return true;
    }

    public Integer dataTypeMapping(int dateType){
        /**
         * BIGINT(1),//Long
         INT(2), //int
         TINYINT(3),//short
         BIT(4), //逻辑型，可表示true/false Y/N 0/1 M/F on/off
         DECIMAL(5),//BigDecimal
         DOUBLE(6),//double
         NCHAR(8),//char
         NVARCHAR(9),//String
         CSN(10),//字符串，数值组装
         BINARY(11), // 定长二进制
         VARBINARY(12), // 变长二进制
         DATETIME(13),
         SMALLINT(14),
         CSV(15),//字符串拼接的数组
         FLOAT(58),
         DATE(18),
         OBJECT(21),
         MAP(1022),
         ARRAY(86),

         UNKNOWN(0);
         */
        switch (dateType){
            case 1 :
            case 2 :
            case 3 :
            case 4 :
            case 5 :
            case 6 :
            case 11 :
            case 12 :
            case 14 :
            case 58 :
            case 10 :
                return 1;//数值类型
            case 8 :
            case 9 :
            case 15 :
                return 2;//文本
            case 13 :
            case 18 :
                return 3;//时间
            case 21 :
            case 1022 :
            case 86 :
                return 4;//引用类型
            default:
                return -1;//未知类型

        }
    }
    //校验pivot列与pivot列值是否兼容
    public Map<String,Object> validatePivotValueIsCompatible(PivotSquid pivotSquid) throws Exception{
        Map<String,Object> resultMap = new Hashtable<>();
        resultMap.put("errorType",1);//pivot列值与pivot列不兼容
        Integer pivotColumnId = pivotSquid.getPivotColumnId();
        Column pivotColumn = columnDao.selectByPrimaryKey(pivotColumnId);
        if(pivotColumn == null || pivotColumnId == null){
            logger.error("-------pivotColumn或pivotColumnId为null----------");
            resultMap.put("flag",false);
            resultMap.put("errorType",2);//上游的pivotColumn被删除
            return resultMap;
        }
        int pivotSysDateType = pivotColumn.getData_type();//系统数据类型
        int pivotMappingType = dataTypeMapping(pivotSysDateType);//自定义映射类型
        //不允许的类型
        if(pivotMappingType == 4 || pivotMappingType == -1){
            logger.error("-------pivotColumn为非法类型----------");
            resultMap.put("flag",false);
            return  resultMap;
        }
        //pivotValue列值
        String pivotValueJson = pivotSquid.getPivotColumnValue();
        List<String> pivotValue = JSONArray.parseArray(pivotValueJson,String.class);
        if(pivotValue == null || pivotValue.size() < 1){
            logger.error("----------pivotValue为空--------------");
            resultMap.put("flag",false);
            return resultMap;
        }
        //1.只能出现数字和+，-
        String regex = "^[+|-]{0,1}[0-9]+$";
        switch (pivotMappingType){
            case 1 ://数值类型,不能为null
                String regexFloat = "^[+-]?\\d+(\\.\\d{1,8})?$";
                String regex2 = "^[0-1]+$";//只能出现0和1
                String regexBigDecimal = "^[+-]?\\d+(\\.\\d{1,2147483647})?$";
                String regexDouble = "^[+-]?\\d+(\\.\\d{1,16})?$";

                for(String valueStr : pivotValue){
                    if(valueStr == null){
                        logger.error("----------pivot列为数值类型时，pivot列值不能为null--------------");
                        resultMap.put("flag",false);
                        return resultMap;
                    } else if(pivotSysDateType == 4){//bit型
                        if( !("true".equalsIgnoreCase(valueStr) || "false".equalsIgnoreCase(valueStr)) ){
                            logger.error("----------bit类型只能为true或false--------------");
                            resultMap.put("flag",false);
                            return resultMap;
                        }
                    }else if(pivotSysDateType == 11 || pivotSysDateType == 12){//二进制

                        if(!valueStr.matches(regex2)){
                            logger.error("----------二进制类型只能为0或1--------------");
                            resultMap.put("flag",false);
                            return resultMap;
                        }
                    }else if(pivotSysDateType == 1){//bigint对应long

                        if(!valueStr.matches(regex)){
                            logger.error("----------bigint类型错误--------------");
                            resultMap.put("flag",false);
                            return resultMap;
                        }else if(Long.parseLong(valueStr) < Long.MIN_VALUE || Long.parseLong(valueStr) > Long.MAX_VALUE){//验证范围
                            logger.error("----------bigint类型超出范围--------------");
                            resultMap.put("flag",false);
                            return resultMap;
                        }
                        /**
                         *  INT(2), //int
                         TINYINT(3),//short
                         */
                    }else if(pivotSysDateType == 2  || pivotSysDateType == 15){//int,csv全部对应Integer
                        if(!valueStr.matches(regex)){
                            logger.error("----------int类型错误--------------");
                            resultMap.put("flag",false);
                            return resultMap;
                        }else if(Integer.parseInt(valueStr) < Integer.MIN_VALUE || Integer.parseInt(valueStr) > Integer.MAX_VALUE){//验证范围
                            logger.error("----------int类型超出范围--------------");
                            resultMap.put("flag",false);
                            return resultMap;
                        }
                    }else if(pivotSysDateType == 3){ //TINYINT(3)
                        if(!valueStr.matches(regex)){
                            logger.error("----------tinyInt类型错误--------------");
                            resultMap.put("flag",false);
                            return resultMap;
                        }else if(Byte.parseByte(valueStr) < Byte.MIN_VALUE || Byte.parseByte(valueStr) > Byte.MAX_VALUE){//验证范围
                            logger.error("----------tinyInt类型超出范围--------------");
                            resultMap.put("flag",false);
                            return resultMap;
                        }
                    }else if(pivotSysDateType == 14){ //SMALLINT(14),
                        if(!valueStr.matches(regex)){
                            logger.error("----------smallInt类型错误--------------");
                            resultMap.put("flag",false);
                            return resultMap;
                        }else if(Short.parseShort(valueStr) < Short.MIN_VALUE || Short.parseShort(valueStr) > Short.MAX_VALUE){//验证范围
                            logger.error("----------smallInt类型超出范围--------------");
                            resultMap.put("flag",false);
                            return resultMap;
                        }
                    }else if(pivotSysDateType == 5){//DECIMAL对应bigDECIMAL

                        if(!valueStr.matches(regexBigDecimal)){
                            logger.error("----------DECIMAL类型错误！--------------");
                            resultMap.put("flag",false);
                            return resultMap;
                        }else if(Double.parseDouble(valueStr) < -Double.MAX_VALUE/2 || Double.parseDouble(valueStr) > Double.MAX_VALUE/2){
                            System.out.println(Double.parseDouble(valueStr));
                            logger.error("----------DECIMAL超过范围错误！--------------");
                            resultMap.put("flag",false);
                            return resultMap;
                        }
                    }else if(pivotSysDateType == 6){ //double

                        if(!valueStr.matches(regexDouble)){
                            logger.error("----------Double类型错误！--------------");
                            resultMap.put("flag",false);
                            return resultMap;
                        }else if(Double.parseDouble(valueStr) < -Double.MAX_VALUE/2 || Double.parseDouble(valueStr) > Double.MAX_VALUE/2){
                            logger.error("----------Double超过范围错误！--------------");
                            resultMap.put("flag",false);
                            return resultMap;
                        }
                    }else if(pivotSysDateType == 58){//float

                        if(!valueStr.matches(regexFloat)){
                            logger.error("----------Float类型错误！--------------");
                            resultMap.put("flag",false);
                            return resultMap;
                        }else if(Float.parseFloat(valueStr) < -Float.MAX_VALUE/2 || Float.parseFloat(valueStr) > Float.MAX_VALUE/2){
                            logger.error("----------Float超过范围错误！--------------");
                            resultMap.put("flag",false);
                            return resultMap;
                        }
                    }
                }
                break;
            case 2 ://文本类型
                /*for(String valueStr : pivotValue){
                    if(pivotSysDateType == 8 || pivotSysDateType == 9 || pivotSysDateType == 10){ //nchar,nvchar,csn对应String
                        //String暂无限制?
                    }
                }*/
                break;
            case 3 ://时间类型
                String regexDateTime = "^(((20[0-3][0-9]-(0[13578]|1[02])-(0[1-9]|[12][0-9]|3[01]))|(20[0-3][0-9]-(0[2469]|11)-(0[1-9]|[12][0-9]|30))) (20|21|22|23|([0-1][0-9])):([0-5][0-9]):([0-5][0-9]))$";
                String regexDate = "^((20[0-3][0-9]-(0[13578]|1[02])-(0[1-9]|[12][0-9]|3[01]))|(20[0-3][0-9]-(0[2469]|11)-(0[1-9]|[12][0-9]|30)))$";
                Pattern p = null;
                for(String valueStr : pivotValue){
                    if(pivotSysDateType == 13){
                        try{
                            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                            sdf.parse(valueStr);
                        }catch (Exception e){
                            logger.error("----------DATETIME时间类型格式错误！--------------");
                            resultMap.put("flag",false);
                            return resultMap;
                        }

                    }else if(pivotSysDateType == 18){
                        p = Pattern.compile(regexDate);
                        if(!p.matcher(valueStr).matches()){ // DATE(18),
                            logger.error("----------DATE时间类型格式错误！--------------");
                            resultMap.put("flag",false);
                            return resultMap;
                        }
                    }

                }
                break;

        }
        resultMap.put("flag",true);
        return resultMap;
    }

    /**
     * 验证聚合方式和已经生成的Column类型是否匹配
     * validatePivotValueIsCompatible
     */
    public Map<String,Object> validateColumnAndAgreeTypeIsCompatible(PivotSquid pivotSquid){
        Map<String,Object> map = new Hashtable<>();
        //查询pivot生成的Column
        Column column = columnDao.selectPivotColumnPaged(pivotSquid.getId());
        Integer columnDataType = null;
        Integer aggreType = pivotSquid.getAggregationType();
        Column valueColumn = columnDao.selectByPrimaryKey(pivotSquid.getValueColumnId());
        Integer valueColumnType = valueColumn.getData_type();
        if(column != null){
            //非分组列
            columnDataType = column.getData_type();
            if((int)aggreType == 1){
                if((int)columnDataType != 6){
                    map.put("flag",false);
                    return map;
                }
            }else if((int)aggreType == 4){
                if((int)columnDataType != 1){
                    map.put("flag",false);
                    return map;
                }
            }else if((int)aggreType == 2 || (int)aggreType == 3 || (int)aggreType == 7){
                if((int)columnDataType != (int)valueColumnType){
                    map.put("flag",false);
                    return map;
                }
            }
        }
        map.put("flag",true);
        return map;
    }
    /**
     * 验证groupId与生成列是否一致
     */
    public Map<String,Object> validateColumnAndGroupIdIsAgreement(PivotSquid pivotSquid){
        Map<String,Object> map = new Hashtable<>();
        //查询pivot生成的Column
        String groupByIdStr = pivotSquid.getGroupByColumnIds();
        String[] groupByIds = groupByIdStr.split(",");
        if(groupByIds.length == 1 && groupByIds[0].equals("")){
            map.put("flag",false);
            return map;
        }
        List<Column> groupByColumns = columnDao.selectGroupByColumns(pivotSquid.getId());
        //先验证所选分组列与生成的分组列column数量是否一致
        if(groupByIds.length != groupByColumns.size()){//数量不一致
            map.put("flag",false);
            return map;
        }
        for (String groupById : groupByIds){
            Column sourceColumn = columnDao.selectByPrimaryKey(Integer.parseInt(groupById));
            boolean flag = false;
            if(groupByColumns.size() > 0){
                for (Column column : groupByColumns){
                    String pivotColumnName = column.getName();//分组列生成的columnName与原有Name相同
                    if(pivotColumnName.equals(sourceColumn.getName())){
                        flag = true;
                    }

                }
                if(!flag){//pivot生成的分组column中没有用户选中的column
                    map.put("flag",false);
                    return map;
                }
            }
        }

        map.put("flag",true);
        return map;
    }

    /**
     * 验证所选value列是否发生改变
     */
    public Map<String,Object> validateValueColumnIsChange(PivotSquid pivotSquid){
        Map<String,Object> map = new Hashtable<>();
        try{
            Integer valueColumnId = pivotSquid.getValueColumnId();
            Integer linkCount = transformationLinkDao.selectCountByColumnId(valueColumnId,pivotSquid.getId());
            //查询非分组列的数量
            Column paraColumn = new Column();
            paraColumn.setSquid_id(pivotSquid.getId());
            paraColumn.setIs_groupby(null);
            Integer noGroupByColumnCount = columnDao.selectCountNoGroupByColumnColumn(paraColumn);
            if(linkCount.intValue() != noGroupByColumnCount.intValue()){
                map.put("flag",false);
                return map;
            }
        }catch (Exception e){
            e.printStackTrace();
            logger.error("---------验证所选value列是否发生改变发生错误!--------");
        }

        map.put("flag",true);
        return map;
    }








}
