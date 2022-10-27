package com.eurlanda.datashire.complieValidate;

import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.dao.*;
import com.eurlanda.datashire.dao.impl.*;
import com.eurlanda.datashire.entity.*;
import com.eurlanda.datashire.enumeration.SchedulerLogStatus;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.enumeration.TransformationTypeEnum;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.StringUtils;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Eurlanda-dev on 2016/8/9.
 */
public class ValidateTransProcess {
    private static String token;

    public static String getToken() {
        return token;
    }

    public static void setToken(String token) {
        ValidateTransProcess.token = token;
    }

    private static Logger logger = Logger.getLogger(ValidateTransProcess.class);// 记录日志
    public static final String SQL_SELECT = "SELECT * FROM A WHERE ";

    //获取Transformation集合
    public static List<Transformation> getTransformationList(Squid squid, IRelationalDataManager adapter) {
        ITransformationDao iTransformationDao = new TransformationDaoImpl(adapter);
        List<Transformation> transformationList = null;
        try {
            transformationList = iTransformationDao.getTransListBySquidId(squid.getId());
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            return transformationList;
        }

    }

    /**
     * 1.验证是否是孤立的Transformation
     * 2.验证Term Extract Transformation表达式为空
     * 3.验证Predict Transformation中引用的模型不存在
     * 4.Tokenization Transformation 字典不存在
     * 5. InverseQuantity RulesQuery Transformation中引用的模型不存在
     * 6.验证Transfomation 入参是否完整
     *
     * @param buildInfoList
     * @param map
     * @param squid
     * @param adapter
     * @param transformationList
     * @return
     */
    public static List<BuildInfo> validateSingleTrans(List<BuildInfo> buildInfoList, List<Column> columnList, Map<String, Object> map, Squid squid, IRelationalDataManager adapter, List<Transformation> transformationList, Map<String, Object> variableSquid) {
        ITransformationLinkDao iTransformationLinkDao = new TransformationLinkDaoImpl(adapter);
        ITransformationInputsDao transformationInputsDao = new TransformationInputsDaoImpl(adapter);
        if (transformationList != null && transformationList.size() > 0) {
            for (Transformation transformation : transformationList) {

                String subTargetName = null;
                //当Transformation和column相关联时
                if (transformation.getTranstype() == 0 && transformation.getColumn_id() != 0) {
                    //查找column的名字
                    Map<String, String> params = new HashMap<String, String>();
                    params.put("id", transformation.getColumn_id() + "");
                    Column column = adapter.query2Object(params, Column.class);
                    if (StringUtils.isNotNull(column)) {
                        subTargetName = column.getName();
                    }
                } else {
                    subTargetName = transformation.getName();
                }
                //根据transid，查询tranLink集合(判断是否是孤立的Transformation),只有当transformation不与column关联时
                if (transformation.getTranstype() != 0 && transformation.getColumn_id() == 0) {
                    //判断是否是孤立的Transformation
                    List<TransformationLink> transformationLinkList = iTransformationLinkDao.getTransLinkByTransId(transformation.getId());
                    if (transformationLinkList == null || transformationLinkList.size() == 0) {
                        logger.info("编译检查: 验证孤立的Transformation");
                        BuildInfo buildInfo = new BuildInfo(squid, map);
                        buildInfo.setMessageCode(MessageCode.ERR_SINGLE_TRANS.value());
                        buildInfo.setInfoType(SchedulerLogStatus.WARNING.getValue());
                        List<String> subTargetNames = new ArrayList<String>();
                        subTargetNames.add(subTargetName);
                        buildInfo.setSubTargetName(subTargetNames);
                        buildInfoList.add(buildInfo);
                    } else {
                        //判断Tranformation有没有出参
                        boolean flag = false;
                        for (TransformationLink trans : transformationLinkList) {
                            if (trans.getFrom_transformation_id() == transformation.getId()) {
                                flag = true;
                                break;
                            }
                        }
                        //没有出参
                        if (!flag) {
                            BuildInfo buildInfo = new BuildInfo(squid, map);
                            buildInfo.setMessageCode(MessageCode.ERR_TRANSOUTPUTS_ISNULL.value());
                            buildInfo.setInfoType(SchedulerLogStatus.WARNING.getValue());
                            List<String> subTargetNames = new ArrayList<String>();
                            subTargetNames.add(subTargetName);
                            buildInfo.setSubTargetName(subTargetNames);
                            buildInfoList.add(buildInfo);
                        }
                        //判断所有的transformation的筛选条件
                        String sqlText = transformation.getTran_condition();
                        String error = null;
                        if (sqlText == null || sqlText.length() == 0) {
                            error = null;
                        } else {
                            error = ValidateJoinProcess.validateSqlIsIllegal(SQL_SELECT + " " + sqlText, map);
                        }
                        if (error == null) {
                            List<String> subTarget = validateSql(sqlText, squid, false, true, false, columnList, (List<DSVariable>) variableSquid.get("variable"), (List<Squid>) variableSquid.get("targetSquid"), adapter);
                            if (subTarget.size() > 0 && subTarget != null) {
                                BuildInfo buildInfo = new BuildInfo(squid, map);
                                buildInfo.setMessageCode(MessageCode.ERR_EXPRESS_ISILLEGAL.value());
                                buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                                for (String str : subTarget) {
                                    str = "error:" + str;
                                }
                                for (int i = 0; i < subTarget.size(); i++) {
                                    String str = "error:" + subTarget.get(i);
                                    subTarget.set(i, str);
                                }
                                List<String> subTargetNames = new ArrayList<String>();
                                subTargetNames.add(subTargetName);
                                subTargetNames.addAll(subTarget);
                                buildInfo.setSubTargetName(subTargetNames);
                                buildInfoList.add(buildInfo);
                            }
                        } else {
                            BuildInfo buildInfo = new BuildInfo(squid, map);
                            buildInfo.setMessageCode(MessageCode.ERR_EXPRESS_ISILLEGAL.value());
                            buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                            error = "语法错误:" + error.substring(error.indexOf("Encountered") + 11, error.indexOf("line") - 3) + "附近有错";
                            List<String> subTargetNames = new ArrayList<String>();
                            subTargetNames.add(subTargetName);
                            subTargetNames.add("error:TransFilter(" + error + ")");
                            buildInfo.setSubTargetName(subTargetNames);
                            buildInfoList.add(buildInfo);

                        }

                        //判断traninput是否合法
                        ITransformationInputsDao inputsDao = new TransformationInputsDaoImpl(adapter);
                        List<TransformationInputs> inputsList = inputsDao.getTransInputListByTransId(transformation.getId());
                        if (inputsList != null) {
//          s
                            for (TransformationInputs input : inputsList) {
                                sqlText = input.getIn_condition();
                                if (sqlText == null || sqlText.length() == 0) {
                                    error = null;
                                } else {
                                    error = ValidateJoinProcess.validateSqlIsIllegal(SQL_SELECT + " " + sqlText, map);
                                }
                                if (error == null) {
                                    List<String> subTarget = validateSql(sqlText, squid, false, false, true, columnList, (List<DSVariable>) variableSquid.get("variable"), (List<Squid>) variableSquid.get("targetSquid"), adapter);
                                    if (subTarget.size() > 0 && subTarget != null) {
                                        BuildInfo buildInfo = new BuildInfo(squid, map);
                                        buildInfo.setMessageCode(MessageCode.ERR_EXPRESS_ISILLEGAL.value());
                                        buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                                        for (String str : subTarget) {
                                            str = "error:" + str;
                                        }
                                        for (int i = 0; i < subTarget.size(); i++) {
                                            String str = "error:" + subTarget.get(i);
                                            subTarget.set(i, str);
                                        }
                                        List<String> subTargetNames = new ArrayList<String>();
                                        subTargetNames.add(subTargetName);
                                        subTargetNames.addAll(subTarget);
                                        buildInfo.setSubTargetName(subTargetNames);
                                        buildInfoList.add(buildInfo);
                                    }
                                } else {
                                    BuildInfo buildInfo = new BuildInfo(squid, map);
                                    buildInfo.setMessageCode(MessageCode.ERR_EXPRESS_ISILLEGAL.value());
                                    buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                                    error = "语法错误:" + error.substring(error.indexOf("Encountered") + 11, error.indexOf("line") - 3) + "附近有错";
                                    List<String> subTargetNames = new ArrayList<String>();
                                    subTargetNames.add(subTargetName);
                                    subTargetNames.add("error:TransInput(" + error + ")");
                                    buildInfo.setSubTargetName(subTargetNames);
                                    buildInfoList.add(buildInfo);
                                }
                            }
                        }
                    }


                }
               /* //判断TeamExtract 表达式是否为空
                if (transformation.getTranstype() == TransformationTypeEnum.TERMEXTRACT.value()) {
                    logger.info("编译检查: TeamExtract 表达式是否合法");
                    //表达式是否为空已经包含在了入参不完整
                    try {
                            //判断表达式是否合法
                            String sqlText = transformation.getTran_condition();
                            //获得连接的squidjoin，作为表名的限制条件
                            List<SquidModelBase> squidList =(List<SquidModelBase>)variableSquid.get("targetSquid");
                            //查询该squid下面所有的变量
                            //IVariableDao variableDao = new VariableDaoImpl(adapter);
                            List<DSVariable> dsVariableList =(List<DSVariable>) variableSquid.get("variable");
                            String error=null;
                            if(sqlText!=null && sqlText.length()>0){
                                //error=sqlValidator.parse(SQL_SELECT+" "+sqlText);
                                error=ValidateJoinProcess.validateSqlIsIllegal(SQL_SELECT+" "+sqlText);
                            }
                            //先校验表达式语法是否是合法的
                            if (error == null) {
                                //语法没有错误，校验表名和字段名字是否错误
                                List<String> subTarget= validateSql(sqlText,squid,false,true,false,columnList,dsVariableList,squidList);
                                if (subTarget.size()>0&&subTarget!=null) {
                                    BuildInfo buildInfo = new BuildInfo(squid,map);
                                    buildInfo.setMessageCode(MessageCode.ERR_TEAMSTRACT_ISILLEGAL.value());
                                    buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                                    for(String str : subTarget){
                                        str="error:"+str;
                                    }
                                    for(int i=0;i<subTarget.size();i++){
                                        String str="error:"+subTarget.get(i);
                                        subTarget.set(i,str);
                                    }
                                    subTargetNames.addAll(subTarget);
                                    buildInfo.setSubTargetName(subTargetNames);
                                    buildInfoList.add(buildInfo);
                                }
                            } else {
                                BuildInfo buildInfo = new BuildInfo(squid,map);
                                buildInfo.setMessageCode(MessageCode.ERR_TEAMSTRACT_ISILLEGAL.value());
                                buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                                //error="语法错误:第"+error.substring(error.indexOf("line")+4,error.indexOf("line")+7)+"行,第"+error.substring(error.indexOf("column")+6,error.indexOf("column")+);
                                error = "语法错误:"+error.substring(error.indexOf("Encountered")+11,error.indexOf("line")-3)+"附近有错";
                                subTargetNames.add("error:"+error);
                                buildInfo.setSubTargetName(subTargetNames);
                                buildInfoList.add(buildInfo);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                }*/

                //判断Predict Transformation中引用的数据挖掘模型不存在
                //判断 InverseQuantity RulesQuery Transformation中引用的模型不存在
                if (transformation.getTranstype() == TransformationTypeEnum.PREDICT.value()
                        || transformation.getTranstype() == TransformationTypeEnum.INVERSEQUANTIFY.value() ||
                        transformation.getTranstype() == TransformationTypeEnum.INVERSENORMALIZER.value()) {
                    //验证是否为空
                    if (transformation.getModel_squid_id() == 0) {
                        BuildInfo buildInfo = new BuildInfo(squid, map);
                        if (transformation.getTranstype() == TransformationTypeEnum.INVERSEQUANTIFY.value() ||
                                transformation.getTranstype() == TransformationTypeEnum.PREDICT.value()||
                                transformation.getTranstype() == TransformationTypeEnum.INVERSENORMALIZER.value()
                                ) {
                            buildInfo.setMessageCode(MessageCode.ERR_INVERSWQUANTITY_NULL.value());
                        }
                        buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                        List<String> subTargetNames = new ArrayList<String>();
                        subTargetNames.add(subTargetName);
                        buildInfo.setSubTargetName(subTargetNames);
                        buildInfoList.add(buildInfo);
                    } else {
                        //验证ModelSquid是否存在
                        Map<String, String> params = new HashMap<String, String>();
                        params.put("id", transformation.getModel_squid_id() + "");
                        Squid modelSquid = adapter.query2Object(params, Squid.class);
                        if (StringUtils.isNull(modelSquid)) {
                            BuildInfo buildInfo = new BuildInfo(squid, map);
                            buildInfo.setMessageCode(MessageCode.ERR_INVERSWQUANTITY_NULL.value());
                            buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                            List<String> subTargetNames = new ArrayList<String>();
                            subTargetNames.add(subTargetName);
                            buildInfo.setSubTargetName(subTargetNames);
                            buildInfoList.add(buildInfo);
                        }
                    }
                }

                //判断Tokenization Transformation 字典不存在
                if (transformation.getTranstype() == TransformationTypeEnum.TOKENIZATION.value()) {
                    if (transformation.getIs_using_dictionary() == 1) {
                        Map<String, String> params = new HashMap<String, String>();
                        params.put("id", transformation.getDictionary_squid_id() + "");
                        Squid dictionarySquid = adapter.query2Object(params, Squid.class);
                        if (StringUtils.isNull(dictionarySquid)) {
                            BuildInfo buildInfo = new BuildInfo(squid, map);
                            buildInfo.setMessageCode(MessageCode.ERR_TOKENIZATION_NULL.value());
                            buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                            List<String> subTargetNames = new ArrayList<String>();
                            subTargetNames.add(subTargetName);
                            buildInfo.setSubTargetName(subTargetNames);
                            buildInfoList.add(buildInfo);
                        }
                    }
                }

                //判断DateInc增减值是否为空
                if (transformation.getTranstype() == TransformationTypeEnum.DATEINC.value()) {
                    if (StringUtils.isNull(transformation.getConstant_value())) {
                        BuildInfo buildInfo = new BuildInfo(squid, map);
                        buildInfo.setMessageCode(MessageCode.ERR_DATAINC_ISNULL.value());
                        buildInfo.setInfoType(SchedulerLogStatus.WARNING.getValue());
                        List<String> subTargetNames = new ArrayList<String>();
                        subTargetNames.add(subTargetName);
                        buildInfo.setSubTargetName(subTargetNames);
                        buildInfoList.add(buildInfo);
                    }
                }

                //判断入参是否完整(只有当transformation不是column的时候，才有可能入参)
                if (transformation.getTranstype() != 0 && transformation.getColumn_id() == 0) {
                    //当trans为rulequery时，不检查是否入参完整
                    if (transformation.getTranstype() == TransformationTypeEnum.RULESQUERY.value()) {
                        continue;
                    }
                    List<TransformationInputs> transformationInputs = transformationInputsDao.getTransInputListByTransId(transformation.getId());
                    if(transformation.getTranstype()==TransformationTypeEnum.CHOICE.value() && transformationInputs.size()==0){
                        BuildInfo buildInfo = new BuildInfo(squid, map);
                        buildInfo.setMessageCode(MessageCode.ERR_TRANSINPUS_NOTALL.value());
                        buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                        List<String> subTargetNames = new ArrayList<String>();
                        subTargetNames.add(subTargetName);
                        buildInfo.setSubTargetName(subTargetNames);
                        buildInfoList.add(buildInfo);
                    }

                    //当trans为NUMASSEMBLE和CSVASSEMBLE时，必须要有一个入参
                    if (transformation.getTranstype() == TransformationTypeEnum.NUMASSEMBLE.value()
                            || transformation.getTranstype() == TransformationTypeEnum.CSVASSEMBLE.value()
                            || transformation.getTranstype() == TransformationTypeEnum.NULLPERCENTAGE.value()) {
                        if (transformationInputs.size() == 0) {
                            BuildInfo buildInfo = new BuildInfo(squid, map);
                            buildInfo.setMessageCode(MessageCode.ERR_TRANSINPUS_At_Least_One.value());
                            if(transformation.getTranstype() == TransformationTypeEnum.NULLPERCENTAGE.value()){
                                buildInfo.setMessageCode(MessageCode.ERR_TRANSINPUS_NOTALL.value());
                            }
                            buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                            List<String> subTargetNames = new ArrayList<String>();
                            subTargetNames.add(subTargetName);
                            buildInfo.setSubTargetName(subTargetNames);
                            buildInfoList.add(buildInfo);
                        }
                    }
                    //当trans为quantify时，只要实现一个入参就可以了
                    for (TransformationInputs transformationInput : transformationInputs) {
                        if (transformationInput.getSource_transform_id() < 1 && transformationInput.getRelative_order() != -1) {
                            BuildInfo buildInfo = new BuildInfo(squid, map);
                            buildInfo.setMessageCode(MessageCode.ERR_TRANSINPUS_NOTALL.value());
                            buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                            List<String> subTargetNames = new ArrayList<String>();
                            subTargetNames.add(subTargetName);
                            buildInfo.setSubTargetName(subTargetNames);
                            buildInfoList.add(buildInfo);
                            break;
                        }
                    }

                }

            }
        }
        return buildInfoList;
    }

    /**
     * 校验关系型数据库的sql
     *
     * @param sqlText
     * @param squid
     * @param columnList
     * @param variableList
     * @param targetSquidList
     * @return
     */
    public static List<String> validateSql(String sqlText, Squid squid, boolean isJoinCondition, boolean isTrans, boolean isTransInput, List<Column> columnList, List<DSVariable> variableList, List<Squid> targetSquidList, IRelationalDataManager adapter) {
        logger.info("编译状态检查: 检查表达式语法是否正确");
        List<String> subTarget = new ArrayList<String>();
        //将sql语句转换成对应的表名,字段名
        if (sqlText == null || sqlText.length() == 0) {
            return subTarget;
        }
        String[] childSqlText = sqlText.split("\\s+(?i)(AND|or|like|on|where)\\s+");
        for (String text : childSqlText) {
            //text=text.replaceAll("\\s*\\(\\s*|\\s*\\)\\s*", " ");
            //针对in,not,is null,is notnull等情况
            String regex = "(\\s+)(?i)(in\\s*\\(|not\\s*\\(|any\\s*\\(|all\\s*\\(|between\\s+|is\\s+|as\\s+)(\\s*)|(\\s*)(?i)(=|>=|<=|<>|!=|>|<)(\\s*)";
            String[] childTexts = text.split(regex);
            for (String childText : childTexts) {
                //childText=childText.replaceAll("\\s*\\(\\s*|\\s*\\)\\s*", " ");
                boolean flag = false;
                boolean variableFlag = false;
                //如果嵌套子查询
                String regexSql = ".*\\s*(select)\\s+.*\\s+(from)\\s+.*((where)\\s+)?.*";
                if (childText.matches(regexSql)) {
                    continue;
                }
                regex = "[0-9]+";
                if (childText.matches(regex) || StringUtils.isNull(childText)) {
                    continue;
                }
                //校验是否是字符串  例如  like '%m%'
                if((childText.startsWith("'") && childText.endsWith("'")) || (childText.startsWith("\"") && childText.endsWith("\""))){
                    continue;
                }
                //按照+,-,*,/,数据库自带函数分割(修改bug4831)
                //String[] grandChildSqlText = childText.split("\\s*(\\+|-|\\*|%|/)\\s*|\\s+(?i)(null|not)|\\s*(?i)(max\\(|min\\(|count\\(|avg\\(|sum\\(|cast\\(|CONVERT\\(|ascii\\(|char\\(|lower\\(|upper\\(|str\\(|LTRIM\\(|RTRIM\\(|left\\(|RIGHT\\(|SUBSTRING\\(|CHARINDEX\\(|PATINDEX\\(|QUOTENAME\\(|REPLICATE\\(|REVERSE\\(|REPLACE\\(|SPACE\\(|STUFF\\(|day\\(|month\\(|year\\(|DATEADD\\(|DATEDIFF\\(|DATENAME\\(|DATEPART\\(|GETDATE\\(|len\\()");
                //String[] grandChildSqlText=childText.split("\\s*(\\+|-|\\*|%|/)\\s*|\\s+(?i)(null|not)|\\s*(?i)([a-zA-Z]+\\.?[a-zA-Z]*\\()");
                String[] grandChildSqlText = childText.split("\\s*(\\+|-|\\*|%|/)\\s*|\\s+(?i)(null|not)|\\s*(?i)([a-zA-Z]+\\.?[a-zA-Z]*\\(.*\\))");
                for (String grandChildText : grandChildSqlText) {
                    grandChildText = grandChildText.replaceAll("\\s*\\(\\s*|\\s*\\)\\s*", " ").trim();
                    flag = false;
                    if (grandChildText.matches(regex) || StringUtils.isNull(grandChildText)) {
                        continue;
                    }
                    //判断是否符合切分规则
                    String regexIsCanSplit = "[^\"'][a-zA-Z_0-9]+\\.[a-zA-Z_0-9]+[^\"']";
                    String[] greatGrandChildText = null;
                    if(grandChildText.matches(regexIsCanSplit)){
                        greatGrandChildText = grandChildText.split("\\.");
                    } else {
                        greatGrandChildText = grandChildSqlText;
                    }
                    /*if (greatGrandChildText.length == 1) {
                        continue;
                    }*/
                    if (greatGrandChildText.length == 1) {
                        //此时判断该字段是否是column字段，如果是应该提示缺少表达式
                        //判断字段名是否正确
                        String greatGrandChildValue = greatGrandChildText[0].trim();
                        if (greatGrandChildValue.indexOf("@") != 0) {
                            for (Column column : columnList) {
                                if (greatGrandChildValue.equals(column.getName())) {
                                    /*//当类型为db extract时，允许直接使用字段名
                                    if(squid.getSquid_type()==SquidTypeEnum.EXTRACT.value()){
                                        flag=true;
                                        break;
                                    }*/
                                    flag = true;
                                    if (isJoinCondition) {
                                        subTarget.add("SquidJoin:字段(" + column.getName() + ")之前缺少表名");
                                    } else if (isTrans) {
                                        subTarget.add("TransFilter:字段(" + column.getName() + ")之前缺少表名");
                                    } else if (isTransInput) {
                                        subTarget.add("TransInput:字段(" + column.getName() + ")之前缺少表名");
                                    } else {
                                        //表示检测到变量，说明缺少表名
                                        subTarget.add("SquidFilter:字段(" + column.getName() + ")之前缺少表名");
                                    }
                                    break;
                                }
                            }
                        }
                        if (!flag) {
                            //除了stage和exception squid其他的都是可以引用变量
                            if ((squid.getSquid_type() != SquidTypeEnum.STAGE.value()
                                    || (squid.getSquid_type() == SquidTypeEnum.STAGE.value() && (isJoinCondition || isTrans || isTransInput)))
                                    && squid.getSquid_type() != SquidTypeEnum.EXCEPTION.value()) {
                                //如果在column中没找到，那么在dsvariable中查找,否则返回
                                //如果参数是变量，如果查找不到，表达式不合法
                                if (greatGrandChildValue.indexOf("@") == 0) {
                                    for (DSVariable variable : variableList) {
                                        if (greatGrandChildValue.equals("@" + variable.getVariable_name())) {
                                            variableFlag = true;
                                            //如果找到了变量，就返回true
                                            break;
                                        }
                                    }
                                    //如果没有找到变量
                                    if (!variableFlag) {
                                        if (isJoinCondition) {
                                            subTarget.add("SquidJoin:变量(" + greatGrandChildValue.substring(1) + ")不存在");
                                        } else if (isTrans) {
                                            subTarget.add("TransFilter:变量(" + greatGrandChildValue.substring(1) + ")不存在");
                                        } else if (isTransInput) {
                                            subTarget.add("TransInput:变量(" + greatGrandChildValue.substring(1) + ")不存在");
                                        } else {
                                            subTarget.add("SquidFilter:变量(" + greatGrandChildValue.substring(1) + ")不存在");
                                        }

                                    }
                                } else {

                                    variableFlag = true;

                                }
                            } else {
                                //如果是stage,exception，不能存在变量
                                if (greatGrandChildValue.indexOf("@") == 0) {
                                   /* if(isJoinCondition){
                                        subTarget.add("SquidJoin:SquidModelBase("+squid.getName()+")不允许使用变量");
                                    } else {
                                        subTarget.add("SquidFilter:SquidModelBase("+squid.getName()+")不允许使用变量");
                                    }*/
                                    if (!isJoinCondition && !isTrans && !isTransInput) {
                                        subTarget.add("SquidFilter:SquidModelBase(" + squid.getName() + ")不允许使用变量");
                                    }
                                    variableFlag = false;
                                } else {
                                    variableFlag = true;
                                }

                            }
                        }
                        //如果没有找到变量，或者找到了column，说明表达式不合法
                        if (flag || (variableFlag == false)) {
                            return subTarget;
                        }
                        flag = false;
                        continue;
                    }
                    String tableName = greatGrandChildText[0].trim();
                    String colName = greatGrandChildText[1].trim();
                    if (tableName.matches(regex) || colName.matches(regex)) {
                        continue;
                    }
                    //判断表名是否是正确，这里的表名是和上游连接的squid
                    //校验Team Extract表达式,SquidJoin表达式和stage filter
                    if ((squid.getSquid_type() == SquidTypeEnum.STAGE.value()
                            || squid.getSquid_type() == SquidTypeEnum.STREAM_STAGE.value()
                            || squid.getSquid_type() == SquidTypeEnum.EXCEPTION.value())
                            && (targetSquidList != null && targetSquidList.size() > 0)) {
                        int targetSquidId = 0;
                        //判断表名是否正确
                        tableName = tableName.replaceAll("`", "");
                        for (Squid targetSquid : targetSquidList) {
                            if (targetSquid.getName().equals(tableName)) {
                                flag = true;
                                targetSquidId = targetSquid.getId();
                                break;
                            }
                        }
                        //当表名出现错误的时候，结束,无需继续执行
                        if (!flag) {
                            if (isJoinCondition) {
                                subTarget.add("SquidJoin:表名(" + tableName + ")错误，应是和上游的squid一致");
                            } else if (isTrans) {
                                subTarget.add("TransFilter:表名(" + tableName + ")错误，应是和上游的squid一致");
                            } else if (isTransInput) {
                                subTarget.add("TransInput:表名(" + tableName + ")错误，应是和上游的squid一致");
                            } else {
                                subTarget.add("SquidFilter:表名(" + tableName + ")错误，应是和上游的squid一致");
                            }

                            return subTarget;
                        }
                        flag = false;
                        //判断字段名是否正确
                        for (Column column : columnList) {
                            //判断指定字段名下面的表名是否正确
                            if ((column.getSquid_id() == targetSquidId) && colName.equals(column.getName())) {
                                flag = true;
                                break;
                            }
                        }
                        //如果在column中没找到，那么在dsvariable中查找,否则返回
                        if (!flag) {
                            for (DSVariable variable : variableList) {
                                if (colName.equals(variable.getVariable_name())) {
                                    flag = true;
                                    break;
                                }
                            }
                        }
                        if (!flag) {
                            if (isJoinCondition) {
                                subTarget.add("SquidJoin:字段(" + colName + ")不存在");
                            } else if (isTrans) {
                                subTarget.add("TransFilter:字段(" + colName + ")不存在");
                            } else if (isTransInput) {
                                subTarget.add("TransInput:字段(" + colName + ")不存在");
                            } else {
                                subTarget.add("SquidFilter:字段(" + colName + ")不存在");
                            }
                            return subTarget;
                        }

                    }
                    //判断表名是否是正确，这里的表名是和上游连接的squid（dataMiningSquid）
                    String className = SquidTypeEnum.classOfValue(squid.getSquid_type()).getSimpleName();
                    if ("DataMiningSquid".equals(className)) {
                        ISquidDao squidDao = new SquidDaoImpl(adapter);
                        String name = null;
                        List<Squid> squids = null;
                        int targetSquidId = 0;
                        try {
                            squids = squidDao.getUpSquidIdsById(squid.getId());
                            if (squids != null && squids.size() > 0) {
                                name = squids.get(0).getName();
                                targetSquidId = squids.get(0).getId();
                            }

                            //判断表名是否正确
                            tableName = tableName.replaceAll("`", "");
                            if (name.equals(tableName)) {
                                flag = true;
                            }
                            //当表名出现错误的时候，结束,无需继续执行
                            if (!flag) {
                                if (isTrans) {
                                    subTarget.add("TransFilter:表名(" + tableName + ")错误，应是和上游的squid一致");
                                } else if (isTransInput) {
                                    subTarget.add("TransInput:表名(" + tableName + ")错误，应是和上游的squid一致");
                                } else {
                                    subTarget.add("SquidFilter:表名(" + tableName + ")错误，应是和上游的squid一致");
                                }
                                return subTarget;
                            }
                            IReferenceColumnDao referenceColumnDao = new ReferenceColumnDaoImpl(adapter);
                            List<ReferenceColumn> referenceColumnList = referenceColumnDao.getRefColumnListByRefSquidId(targetSquidId);
                            flag = false;
                            //判断字段名是否正确
                            for (ReferenceColumn column : referenceColumnList) {
                                //判断指定字段名下面的表名是否正确
                                if ((column.getReference_squid_id() == targetSquidId) && colName.equals(column.getName())) {
                                    flag = true;
                                    break;
                                }
                            }
                            if (!flag) {
                                if (isTrans) {
                                    subTarget.add("TransFilter:字段(" + colName + ")不存在");
                                } else if (isTransInput) {
                                    subTarget.add("TransInput:字段(" + colName + ")不存在");
                                } else {
                                    subTarget.add("SquidFilter:字段(" + colName + ")不存在");
                                }
                                return subTarget;
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    //校验Squid filter(除了数据源的Squid的filter，落地，以及datamingsquid,以及StageSquid)
                    if ("StageSquid".equals(className)
                            || "ExceptionSquid".equals(className)
                            || "StreamStageSquid".equals(className) || "DataMiningSquid".equals(className) || "HBaseExtractSquid".equals(className)) {
                        continue;
                    } else {
                        //校验squid的filter
                        if (tableName.equals(squid.getName())) {
                            flag = true;
                        }
                        if (!flag) {
                            if (isJoinCondition) {
                                subTarget.add("SquidJoin:表名(" + tableName + ")错误,应和当前squid一致");
                            } else if (isTrans) {
                                subTarget.add("TransFilter:表名(" + tableName + ")错误,应和当前squid一致");
                            } else if (isTransInput) {
                                subTarget.add("TransInput:表名(" + tableName + ")错误,应和当前squid一致");
                            } else {
                                subTarget.add("SquidFilter:表名(" + tableName + ")错误,应和当前squid一致");
                            }
                            return subTarget;
                        }
                        flag = false;
                        //判断字段名是否存在
                        for (Column column : columnList) {
                            if (colName.equals(column.getName())) {
                                flag = true;
                                break;
                            }
                        }
                        //如果在column中没找到，那么在dsvariable中查找,否则返回
                        if (!flag) {
                            for (DSVariable variable : variableList) {
                                if (colName.equals(variable.getVariable_name())) {
                                    flag = true;
                                    break;
                                }
                            }
                        }
                        if (!flag) {
                            if (isJoinCondition) {
                                subTarget.add("SquidJoin:字段(" + (colName) + ")不存在");
                            } else if (isTrans) {
                                subTarget.add("TransFilter:字段(" + (colName) + ")不存在");
                            } else if (isTransInput) {
                                subTarget.add("TransInput:字段(" + (colName) + ")不存在");
                            } else {
                                subTarget.add("SquidFilter:字段(" + (colName) + ")不存在");
                            }
                            return subTarget;
                        }

                    }
                }

            }
        }
        return subTarget;
    }
}