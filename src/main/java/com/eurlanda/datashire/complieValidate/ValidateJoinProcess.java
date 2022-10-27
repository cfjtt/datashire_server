package com.eurlanda.datashire.complieValidate;

import com.akiban.sql.parser.SQLParser;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.dao.*;
import com.eurlanda.datashire.dao.impl.*;
import com.eurlanda.datashire.entity.*;
import com.eurlanda.datashire.enumeration.DataBaseType;
import com.eurlanda.datashire.enumeration.JoinType;
import com.eurlanda.datashire.enumeration.SchedulerLogStatus;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.StringUtils;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Eurlanda-dev on 2016/8/9.
 */
public class ValidateJoinProcess {
    private static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(ValidateJoinProcess.class);// 记录日志
    private static String token;

    public static String getToken() {
        return token;
    }

    public static void setToken(String token) {
        ValidateJoinProcess.token = token;
    }

    public static final String SQL_SELECT = "SELECT * FROM A WHERE ";
    private static final String SQL_JOIN = "SELECT * FROM A JOIN B ON ";

    public static List<SquidJoin> getSquidJoinListByJoinId(Squid squid, IRelationalDataManager adapter) {
        ISquidJoinDao squidJoinDao = new SquidJoinDaoImpl(adapter);
        ISquidDao squidDao = new SquidDaoImpl(adapter);
        List<SquidJoin> squidJoinList = new ArrayList<SquidJoin>();
        //如果是exception，则查找上游的上游的squid
        if (squid.getSquid_type() == SquidTypeEnum.EXCEPTION.value()) {
            try {
                //获得上游的squid
                List<Squid> squidList = squidDao.getUpSquidIdsById(squid.getId());
                if (squidList != null) {
                    for (Squid targetsquid : squidList) {
                        List<SquidJoin> squidJoins = new ArrayList<SquidJoin>();
                        if (targetsquid.getSquid_type() == SquidTypeEnum.STAGE.value()) {
                            squidJoins = squidJoinDao.getSquidJoinListByJoinedId(targetsquid.getId());
                        } else {
                            //如果上游不是stage
                            SquidJoin squidJoin = new SquidJoin();
                            squidJoin.setTarget_squid_id(targetsquid.getId());
                            squidJoins.add(squidJoin);
                        }
                        if (squidJoins != null) {
                            squidJoinList.addAll(squidJoins);
                        }

                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } else {
            squidJoinList = squidJoinDao.getSquidJoinListByJoinedId(squid.getId());
        }
        return squidJoinList;
    }

    public static List<Squid> getSquidListByJoinList(List<SquidJoin> squidJoinList, IRelationalDataManager adapter) {
        List<Squid> squidList = new ArrayList<Squid>();
        for (SquidJoin squidJoin : squidJoinList) {
            Map<String, String> params = new HashMap<String, String>();
            params.put("id", squidJoin.getTarget_squid_id() + "");
            Squid targetSquid = adapter.query2Object(params, Squid.class);
            if (StringUtils.isNotNull(targetSquid)) {
                squidList.add(targetSquid);
            }
        }
        return squidList;
    }

    public static List<Squid> getSquidListByJoinId(Squid squid, IRelationalDataManager adapter) {
        ISquidJoinDao squidJoinDao = new SquidJoinDaoImpl(adapter);
        List<SquidJoin> squidJoinList = squidJoinDao.getSquidJoinListByJoinedId(squid.getId());
        List<Squid> squidList = new ArrayList<Squid>();
        for (SquidJoin squidJoin : squidJoinList) {
            Map<String, String> params = new HashMap<String, String>();
            params.put("id", squidJoin.getTarget_squid_id() + "");
            Squid targetSquid = adapter.query2Object(params, Squid.class);
            if (StringUtils.isNotNull(targetSquid)) {
                squidList.add(targetSquid);
            }
        }
        return squidList;
    }

    //验证squid filter，join condition是否合法
    public static Map<String, Object> validateJoinAndFilter(Squid squid, List<BuildInfo> buildInfoList, Map<String, Object> map, List<Column> columnList, IRelationalDataManager adapter) throws Exception {
        Map<String, Object> outmap = new HashMap<String, Object>();
        //如果是数据源,不校验filter
        String className = SquidTypeEnum.classOfValue(squid.getSquid_type()).getSimpleName();
        //校验Squid filter(除了数据源的Squid的filter，落地，以及datamingsquid,CassandraExtract,Teardata Extract,HbaseExtract)
        if (SquidTypeEnum.isDBSourceBySquidType(squid.getSquid_type())
                || SquidTypeEnum.isDestSquid(squid.getSquid_type())
                || SquidTypeEnum.DBDESTINATION.value() == squid.getSquid_type()
                || SquidTypeEnum.ANNOTATION.value() == squid.getSquid_type()
                || SquidTypeEnum.CASSANDRA_EXTRACT.value() == squid.getSquid_type()
                || "DataMiningSquid".equals(className)) {
            return outmap;
        }
        String error = null;
        //针对dbsouce，判断上游的squid的dbType是否是TERADATA
        if (SquidTypeEnum.isExtractBySquidType(squid.getSquid_type())) {
            //获取上游的squid
            ISquidLinkDao squidLinkDao = new SquidLinkDaoImpl(adapter);
            List<SquidLink> squidLinks = squidLinkDao.getFormSquidLinkBySquidId(squid.getId());
            if (squidLinks != null && squidLinks.size() > 0) {
                IDBSquidDao squidDao = new DBSquidDaoImpl(adapter);
                int fromSquidId = squidLinks.get(0).getFrom_squid_id();
                int dbTypeId = squidDao.getDBTypeBySquidId(fromSquidId);
                if (dbTypeId == DataBaseType.TERADATA.value()) {
                    return outmap;
                } /*else if(dbTypeId == DataBaseType.HBASE_PHOENIX.value()){
                    int filterType = squid.getFilter_type();
                    if(filterType==0){
                        String filter = squid.getFilter();
                        if(StringUtils.isNotNull(filter)){
                            String sql = "select * from ds_reference_column where reference_squid_id="+squid.getId();
                            List<ReferenceColumn> referenceColumns = adapter.query2List(true,sql,null,ReferenceColumn.class);
                            String[] childFilters = filter.split("\\s+(?i)(AND|or|like|on|where)\\s+");
                            for(String childFilter : childFilters){
                                String regex = "(\\s*)(?i)(in\\s*\\(|not\\s*\\(|any\\s*\\(|all\\s*\\(|between\\s+|is\\s+|as\\s+)(\\s*)|(\\s*)(?i)(=|>=|<=|<>|!=|>|<)(\\s*)";
                                String[] grandChildFilters = childFilter.split(regex);
                                for(String grandChildFilter : grandChildFilters){
                                    if(grandChildFilter.matches("[0-9]+")){
                                        continue;
                                    } else if(grandChildFilter.matches("[A-Za-z0-9]+.{1}[A-Za-z0-9]+")){
                                        //判断是否一致
                                        boolean flag = false;
                                        if(referenceColumns!=null && referenceColumns.size()>0){
                                            for(ReferenceColumn refColumn : referenceColumns){
                                                if(grandChildFilter.trim().equals(refColumn.getName().trim())){
                                                    flag = true;
                                                    break;
                                                }
                                            }
                                        }
                                        if(!flag){
                                            error = "语法错误:" + grandChildFilter + "附近有错";
                                            BuildInfo buildInfo = new BuildInfo(squid, map);
                                            buildInfo.setMessageCode(MessageCode.ERR_EXPRESS_ISILLEGAL.value());
                                            List<String> subTargetNames = new ArrayList<>();
                                            subTargetNames.add(error);
                                            buildInfo.setSubTargetName(subTargetNames);
                                            buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                                            buildInfoList.add(buildInfo);
                                        }
                                        return outmap;
                                    }
                                }
                            }
                        }
                    }
                }*/
            } else {
                return outmap;
            }

        }
        IVariableDao iVariableDao = new VariableDaoImpl(adapter);
        List<Squid> targetSquidJoin = null;
        List<SquidJoin> squidJoins = getSquidJoinListByJoinId(squid, adapter);
        try {
            List<DSVariable> projectVariable = iVariableDao.getDSVariableByScope(0, Integer.parseInt(map.get("ProjectId") + ""));
            List<DSVariable> squidflowVariable = iVariableDao.getDSVariableByScope(1, Integer.parseInt(map.get("SquidflowId") + ""));
            List<DSVariable> variables = iVariableDao.getDSVariableByScope(2, squid.getId());
            variables.addAll(projectVariable);
            variables.addAll(squidflowVariable);
            if (squid.getSquid_type() == SquidTypeEnum.STAGE.value()
                    || squid.getSquid_type() == SquidTypeEnum.STREAM_STAGE.value()
                    || squid.getSquid_type() == SquidTypeEnum.EXCEPTION.value()) {
                targetSquidJoin = getSquidListByJoinList(squidJoins, adapter);
            }
            outmap.put("variable", variables);
            outmap.put("targetSquid", targetSquidJoin);
            //校验squid的filter
            String sqlText = squid.getFilter();
            List<String> subTarget = new ArrayList<String>();
            SQLParser sqlparse = new SQLParser();
            String sql = null;
            if (sqlText == null || sqlText.length() == 0) {
                error = null;
            } else {
                //为了让使用变量时，加上@语法，检测语法不报错
                //error=sqlValidator.parse(SQL_SELECT+" "+matcherVariable(sqlText).toString());
                //pivotSquid关键字冲突处理
                String newSqlText = sqlText;
                if(sqlText.toLowerCase().contains("pivot")){
                    newSqlText = sqlText.replaceAll("(?i)pivot","plvot");//防止关键字冲突
                }
                error = validateSqlIsIllegal(SQL_SELECT + " " + newSqlText, map);
            }
            boolean flag = true;
            if (error == null) {
                subTarget = ValidateTransProcess.validateSql(sqlText, squid, false, false, false, columnList, variables, targetSquidJoin, adapter);
                if (subTarget != null && subTarget.size() > 0) {
                    flag = false;
                }
            } else {
                flag = false;
                //提示错误时还原原有名字
                if(error.indexOf("plvot") != -1){
                    error = error.replaceAll("plvot","Pivot");
                }
                error = "语法错误:" + error.substring(error.indexOf("Encountered") + 11, error.indexOf("line") - 3) + "附近有错";
                //error = "语法错误:"+sql.substring(49)+"附近有错";
                subTarget.add(error);
            }
            if (!flag) {
                BuildInfo buildInfo = new BuildInfo(squid, map);
                buildInfo.setMessageCode(MessageCode.ERR_EXPRESS_ISILLEGAL.value());
                buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                buildInfo.setSubTargetName(subTarget);
                buildInfoList.add(buildInfo);
                flag = true;
            }

            //校验squid的squidjoin
            if (targetSquidJoin != null && targetSquidJoin.size() > 0 && squid.getSquid_type() == SquidTypeEnum.STAGE.value()) {
                for (SquidJoin targetJoinSquid : squidJoins) {
                    List<String> subTargetName = new ArrayList<String>();
                    //当squid join为left join，innerjoin，right join，full join的时候，condition不能为空
                    if (targetJoinSquid.getJoinType() == JoinType.InnerJoin.value()
                            || targetJoinSquid.getJoinType() == JoinType.LeftOuterJoin.value()
                            || targetJoinSquid.getJoinType() == JoinType.RightOuterJoin.value()
                            || targetJoinSquid.getJoinType() == JoinType.FullJoin.value()) {
                        if (StringUtils.isEmpty(targetJoinSquid.getJoin_Condition())) {
                            BuildInfo buildInfo = new BuildInfo(squid, map);
                            buildInfo.setMessageCode(MessageCode.ERR_JOINCONDITION_ISNULL.value());
                            buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                            buildInfoList.add(buildInfo);
                            return outmap;
                        }
                    }
                    sqlText = targetJoinSquid.getJoin_Condition();
                    if (sqlText == null || sqlText.length() == 0) {
                        error = null;
                    } else {
                        //error= sqlValidator.parse(SQL_JOIN+" "+matcherVariable(sqlText).toString());
                        String newSqlText = sqlText;
                        if(sqlText.toLowerCase().contains("pivot")){
                            newSqlText = sqlText.replaceAll("(?i)pivot","plvot");
                        }
                        error = validateSqlIsIllegal(SQL_SELECT + " " + newSqlText, map);
                    }
                    if (error != null) {
                        flag = false;
                        //提示错误时还原原有名字
                        if(error.indexOf("plvot") != -1){
                            error = error.replaceAll("plvot","Pivot");
                        }
                        error = "语法错误:" + error.substring(error.indexOf("Encountered") + 11, error.indexOf("line") - 3) + "附近有错";
                        subTargetName.add(error);
                    } else {
                        subTargetName = ValidateTransProcess.validateSql(sqlText, squid, true, false, false, columnList, variables, targetSquidJoin, adapter);
                        if (subTargetName != null && subTargetName.size() > 0) {
                            flag = false;
                        }
                    }
                    if (!flag) {
                        BuildInfo buildInfo = new BuildInfo(squid, map);
                        buildInfo.setMessageCode(MessageCode.ERR_EXPRESS_ISILLEGAL.value());
                        buildInfo.setInfoType(SchedulerLogStatus.ERROR.getValue());
                        buildInfo.setSubTargetName(subTargetName);
                        buildInfoList.add(buildInfo);
                        break;
                    }
                }
            }


        } catch (SQLException e) {
            e.printStackTrace();
        }
        return outmap;
    }

    public static StringBuffer matcherVariable(String sqlText) {
        //为了让使用变量时，加上@语法，检测语法不报错
        String regex = "\\s+(@)[0-9A-Za-z]+\\s*(=|!=|<>|>|<|>=|<=)\\s*(@)[0-9A-Za-z]+\\s*|\\s*(=|!=|<>|>|<|>=|<=)\\s*(@)[0-9A-Za-z]+\\s*|\\s+(@)[0-9A-Za-z]+\\s*(=|!=|<>|>|<|>=|<=)";
        Pattern pattern = Pattern.compile(regex);
        StringBuffer buffer = new StringBuffer(" " + sqlText);
        Matcher matcher = pattern.matcher(buffer);
        while (matcher.find()) {
            for (int i = 0; i < matcher.groupCount(); i++) {
                int a = i + 1;
                if ("@".equals(matcher.group(a))) {
                    buffer.replace(matcher.start(a), matcher.start(a) + 1, " ");
                }
            }
        }
        return buffer;
    }

    /**
     * 校验sql语法是否正确，不正确给出出现问题的地方
     *
     * @param sql
     * @return
     */
    public static String validateSqlIsIllegal(String sql, Map<String, Object> map) {
        String error = null;
        try {
            //DataBaseType
            if (map.containsKey("DbType")) {
                if ((Integer) map.get("DbType") != -1) {
                    if (DataBaseType.SQLSERVER.value() == (Integer) map.get("DbType")) {
                        //判断where条件是否完整(where a)
                        String sqlText = sql.substring(sql.indexOf("WHERE") + 5).trim();
                        String regex = ".*\\s*(?i)(>|>=|<|<=|<>|!=|=)\\s*.*|.*\\s+(?i)(in|not|any|all|between|is|like)\\s*.*";
                        if (!sqlText.matches(regex)) {
                            error = "Encountered '" + sqlText + "' at line 1";
                            return error;
                        }
                    }
                }
            }
            //判断like是否缺少''
            //int squidId=map.get("DbType");
            String regex = ".*\\s+(?i)(like)\\s+.*";
            boolean flag = sql.matches(regex);
            if (sql.matches(regex)) {
                //error= sqlValidator.parse(sql);
                //验证like是否缺少'/"
                regex = ".*\\s+(?i)(like)\\s+('|\")+.*('|\")+.*";
                if (!sql.matches(regex)) {
                    error = "Encountered 'like' at line 1";
                    return error;
                }
            }
            //由于下面的方法会将insert()函数当成insert关键字，所以这里当出现insert的时候
            CCJSqlParserUtil.parse(sql);
            /*if(statement instanceof Select){
                Select selectStatement = (Select) statement;
                SelectBody selectBody=selectStatement.getSelectBody();
            }*/
        } catch (Exception e) {
            e.printStackTrace();
            error = e.getCause().toString();
            if (e instanceof JSQLParserException) {
                if (error.indexOf("Encountered") > -1 && error.indexOf("INSERT") > -1) {
                    error = null;
                }
                if ((Integer) map.get("DbType") == DataBaseType.SQLSERVER.value()) {
                    if (error.indexOf("Encountered") > -1 && error.indexOf("<S_CHAR_LITERAL>") > -1) {
                        error = null;
                    }
                }
            }
        } finally {
            return error;
        }

    }
}
