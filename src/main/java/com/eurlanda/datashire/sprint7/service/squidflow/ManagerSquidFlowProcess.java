package com.eurlanda.datashire.sprint7.service.squidflow;

import cn.com.jsoft.jframe.utils.ValidateUtils;
import com.alibaba.fastjson.JSON;
import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.adapter.datatype.DataTypeConvertor;
import com.eurlanda.datashire.adapter.db.AdapterDataSourceManager;
import com.eurlanda.datashire.adapter.db.INewDBAdapter;
import com.eurlanda.datashire.cloudService.HttpClientUtils;
import com.eurlanda.datashire.common.util.DataMiningUtil;
import com.eurlanda.datashire.dao.*;
import com.eurlanda.datashire.dao.dest.IEsColumnDao;
import com.eurlanda.datashire.dao.dest.IHdfsColumnDao;
import com.eurlanda.datashire.dao.dest.ImpalaColumnDao;
import com.eurlanda.datashire.dao.dest.impl.EsColumnDaoImpl;
import com.eurlanda.datashire.dao.dest.impl.HdfsColumnDaoImpl;
import com.eurlanda.datashire.dao.dest.impl.ImpalaColumnDaoImpl;
import com.eurlanda.datashire.dao.impl.*;
import com.eurlanda.datashire.entity.*;
import com.eurlanda.datashire.entity.dest.*;
import com.eurlanda.datashire.entity.operation.BasicTableInfo;
import com.eurlanda.datashire.entity.operation.BeyondSquidException;
import com.eurlanda.datashire.entity.operation.ColumnInfo;
import com.eurlanda.datashire.entity.operation.TableIndex;
import com.eurlanda.datashire.enumeration.*;
import com.eurlanda.datashire.enumeration.datatype.DbBaseDatatype;
import com.eurlanda.datashire.enumeration.datatype.SystemDatatype;
import com.eurlanda.datashire.exception.SystemErrorException;
import com.eurlanda.datashire.server.model.PivotSquid;
import com.eurlanda.datashire.server.model.SamplingSquid;
import com.eurlanda.datashire.server.model.User;
import com.eurlanda.datashire.server.utils.Constants;
import com.eurlanda.datashire.server.utils.TokenUtil;
import com.eurlanda.datashire.sprint7.packet.InfoNewMessagePacket;
import com.eurlanda.datashire.sprint7.packet.PushMessagePacket;
import com.eurlanda.datashire.sprint7.service.squidflow.subservice.MessageBubbleService;
import com.eurlanda.datashire.sprint7.service.squidflow.subservice.SquidFlowServicesub;
import com.eurlanda.datashire.sprint7.service.squidflow.subservice.SquidIndexsServiceSub;
import com.eurlanda.datashire.utility.*;
import com.eurlanda.datashire.validator.SquidValidationTask;
import com.eurlanda.report.global.Global;
import com.ibm.db2.jcc.c.SqlException;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.mongodb.DB;
import com.mongodb.MongoTimeoutException;
import com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.SQLException;
import java.util.*;
import java.util.regex.Pattern;

/**
 * 调度squidFlow的处理类
 *
 * @author yi.zhou
 */
public class ManagerSquidFlowProcess {

    Map<Integer, Integer> destSquidMap = new HashMap<Integer, Integer>();
    private static Logger logger = Logger.getLogger(ManagerSquidFlowProcess.class);
    // token Repository的链接Connection对象（登录时候进行了保存）
    private String token;
    private String key;
    private int offset_x;
    private int offset_y;

    public ManagerSquidFlowProcess(String token, String key) {
        this.token = token;
        this.key = key;
    }

    /**
     * 设置Column Name
     *
     * @param adapter3
     * @param squidIndexes
     * @author bo.dang
     */
    public static void setColumnName(IRelationalDataManager adapter3,
                                     SquidIndexes squidIndexes) {
        ColumnDaoImpl columnDaoImpl = new ColumnDaoImpl(adapter3);
        Column column = null;
        if (StringUtils.isNotNull(squidIndexes.getColumn_id1())
                && 0 != squidIndexes.getColumn_id1()) {
            column = columnDaoImpl.selectColumnByPK(true,
                    squidIndexes.getColumn_id1());
            if (StringUtils.isNotNull(column)) {
                squidIndexes.setColumn_name_1(column.getName());
            }
        }
        if (StringUtils.isNotNull(squidIndexes.getColumn_id2())
                && 0 != squidIndexes.getColumn_id2()) {
            column = columnDaoImpl.selectColumnByPK(true,
                    squidIndexes.getColumn_id2());
            if (StringUtils.isNotNull(column)) {
                squidIndexes.setColumn_name_2(column.getName());
            }
        }
        if (StringUtils.isNotNull(squidIndexes.getColumn_id3())
                && 0 != squidIndexes.getColumn_id3()) {
            column = columnDaoImpl.selectColumnByPK(true,
                    squidIndexes.getColumn_id3());
            if (StringUtils.isNotNull(column)) {
                squidIndexes.setColumn_name_3(column.getName());
            }
        }
        if (StringUtils.isNotNull(squidIndexes.getColumn_id4())
                && 0 != squidIndexes.getColumn_id4()) {
            column = columnDaoImpl.selectColumnByPK(true,
                    squidIndexes.getColumn_id4());
            if (StringUtils.isNotNull(column)) {
                squidIndexes.setColumn_name_4(column.getName());
            }
        }
        if (StringUtils.isNotNull(squidIndexes.getColumn_id5())
                && 0 != squidIndexes.getColumn_id5()) {
            column = columnDaoImpl.selectColumnByPK(true,
                    squidIndexes.getColumn_id5());
            if (StringUtils.isNotNull(column)) {
                squidIndexes.setColumn_name_5(column.getName());
            }
        }
        if (StringUtils.isNotNull(squidIndexes.getColumn_id6())
                && 0 != squidIndexes.getColumn_id6()) {
            column = columnDaoImpl.selectColumnByPK(true,
                    squidIndexes.getColumn_id6());
            if (StringUtils.isNotNull(column)) {
                squidIndexes.setColumn_name_6(column.getName());
            }
        }
        if (StringUtils.isNotNull(squidIndexes.getColumn_id7())
                && 0 != squidIndexes.getColumn_id7()) {
            column = columnDaoImpl.selectColumnByPK(true,
                    squidIndexes.getColumn_id7());
            if (StringUtils.isNotNull(column)) {
                squidIndexes.setColumn_name_7(column.getName());
            }
        }
        if (StringUtils.isNotNull(squidIndexes.getColumn_id8())
                && 0 != squidIndexes.getColumn_id8()) {
            column = columnDaoImpl.selectColumnByPK(true,
                    squidIndexes.getColumn_id8());
            if (StringUtils.isNotNull(column)) {
                squidIndexes.setColumn_name_8(column.getName());
            }
        }
        if (StringUtils.isNotNull(squidIndexes.getColumn_id9())
                && 0 != squidIndexes.getColumn_id9()) {
            column = columnDaoImpl.selectColumnByPK(true,
                    squidIndexes.getColumn_id9());
            if (StringUtils.isNotNull(column)) {
                squidIndexes.setColumn_name_9(column.getName());
            }
        }
        if (StringUtils.isNotNull(squidIndexes.getColumn_id10())
                && 0 != squidIndexes.getColumn_id10()) {
            column = columnDaoImpl.selectColumnByPK(true,
                    squidIndexes.getColumn_id10());
            if (StringUtils.isNotNull(column)) {
                squidIndexes.setColumn_name_10(column.getName());
            }
        }
    }

    /**
     * // TODO 方法内容
     *
     * @param leftList
     * @param rightList
     * @return
     * @author bo.dang
     */
    public static <T extends Comparable<T>> boolean compare2List(
            List<T> leftList, List<T> rightList) {

        if (leftList.size() != rightList.size()) {
            return false;
        }
        Collections.sort(leftList);
        Collections.sort(rightList);
        for (int i = 0; i < leftList.size(); i++) {
            if (!leftList.get(i).equals(rightList.get(i))) {
                return false;
            }
        }
        return true;
    }

    public static void main(String[] args) {
        /*
         * DataAdapterFactory adapterFactory = DataAdapterFactory.newInstance();
		 * IRelationalDataManager adapter3 =
		 * adapterFactory.getDataManagerByDbName("test");
		 * adapter3.openSession(); int squidId = 3089; try { Map<Integer,
		 * Integer> transMap = new HashMap<Integer, Integer>(); Map<Integer,
		 * Integer> columnForTransMap = new HashMap<Integer, Integer>();
		 * ManagerSquidFlowProcess process = new ManagerSquidFlowProcess("","");
		 * String sql = "select id,column_id from ds_transformation dt " +
		 * "inner join ds_column dc " +
		 * "on dt.column_id=dc.id and dc.squid_id=dt.squid_id " +
		 * "where dt.squid_id="+squidId+" order by dc.relative_order asc";
		 * List<TransformationLink> links = new ArrayList<TransformationLink>();
		 * List<Map<String, Object>> objList = adapter3.query2List(true, sql,
		 * null); if (objList!=null){ for (Map<String, Object> map : objList) {
		 * int fromTransId = Integer.parseInt(map.get("ID")+""); int columnId =
		 * Integer.parseInt(map.get("COLUMN_ID")+"");
		 * process.getTransLinks(adapter3, fromTransId, links);
		 * columnForTransMap.put(columnId, fromTransId); } } //单个 实体
		 * Transformation，上个步骤没有进行创建的 sql = "select * from ds_transformation " +
		 * "where squid_id="
		 * +squidId+" and (column_id is null or column_id = 0)";
		 * List<Transformation> transList = adapter3.query2List(true, sql, null,
		 * Transformation.class); if (transList!=null&&transList.size()>0){ for
		 * (Transformation transformation : transList) { int transId =
		 * transformation.getId(); if (!transMap.containsKey(transId)){
		 * //process.copyTransForInputByParms(adapter3, transformation,
		 * newSquidId); //transMap.put(transId, transformation.getId());
		 * process.getTransLinks(adapter3, transId, links); } } } if
		 * (links!=null&&links.size()>0){ for (TransformationLink link : links)
		 * { System.out.print(link.getId()+","); } }
		 * System.out.println(links.size()); } catch (Exception e) {
		 * e.printStackTrace(); }
		 */

		/*
         * String info =
		 * "{\"data\":[[47492,47493],[\"\1\",\"\2\"],[\"\3\",\"\5\"],[\"-\",\"K\"],[\"\",\",\"]],\"squid_id\":7613}"
		 * ; Map<String, Object> map1 = JsonUtil.toHashMap(info); String temp =
		 * JsonUtil.toJSONString(map1); System.out.println(temp); List<String>
		 * array = JsonUtil.toGsonList(map1.get("data")+"", String.class); for
		 * (String string : array) { List<String> array1 =
		 * JsonUtil.toGsonList(string, String.class); for (String string2 :
		 * array1) { System.out.println(string2); } }
		 */

        Map<String, Object> map = new HashMap<String, Object>();
        map.put("1", (char) 1);
        map.put("2", (char) 'a');
        map.put("3", "a");
        map.put("4", "\1");
        String temp = util.JsonUtil.toJSONString(map);
        System.out.println(temp);
        if (temp != null) {
            temp = temp.substring(1);
            temp = temp.substring(0, temp.length() - 1);
        }
        Map<String, Object> tempMap = (Map<String, Object>) JsonUtil
                .toJSONObject(temp);
        System.out.println(tempMap);
    }

    public int getOffset_x() {
        return offset_x;
    }

    public void setOffset_x(int offset_x) {
        this.offset_x = offset_x;
    }

    public int getOffset_y() {
        return offset_y;
    }

    public void setOffset_y(int offset_y) {
        this.offset_y = offset_y;
    }

    /**
     * 忽略目的地squid标记，应用断点，运行squidFlow的所有squid
     *
     * @param info
     * @param out
     * @return
     */
    public Map<String, Object> runALLWithDebugging(String info, ReturnValue out) {
        logger.info("runALLWithDebugging start up");
        Map<String, Object> outputMap = new HashMap<String, Object>();
        boolean status = true;
        String taskId = "";
        try {
            Map<String, Object> params = new HashMap<String, Object>();
            Map<String, Object> inParmsMap = JsonUtil.toHashMap(info);
            try {
                Map<String, Object> inParamsMap2 = new HashMap<>();
                for (String key : inParmsMap.keySet()) {
                    Object value = inParmsMap.get(key);
                    key = key.substring(0, 1).toLowerCase() + key.substring(1);
                    inParamsMap2.put(key, value);
                }
                inParamsMap2.clear();
            } catch (Exception e) {

            }
            int squidFlowId = Integer.parseInt(inParmsMap.get("SquidFlowId")
                    + "");
            int repositoryId = Integer.parseInt(inParmsMap.get("RepositoryId")
                    + "");
            List<Integer> breakPoints = JsonUtil.toGsonList(
                    inParmsMap.get("BreakPoints") + "", Integer.class);
            List<Integer> dataViewers = JsonUtil.toGsonList(
                    inParmsMap.get("DataViewers") + "", Integer.class);
            List<Integer> destinations = JsonUtil.toGsonList(
                    inParmsMap.get("Destinations") + "", Integer.class);
            if (squidFlowId > 0) {
                if (breakPoints != null && breakPoints.size() > 0) {
                    params.put("breakPoints", breakPoints);
                }
                if (dataViewers != null && dataViewers.size() > 0) {
                    params.put("dataViewers", dataViewers);
                }
                if (destinations != null && destinations.size() > 0) {
                    params.put("destinations", destinations);
                }
                if (params.size() == 0) {
                    params.put("hello", "1");
                }
                String temp = JSON.toJSONString(params);

                RPCUtils rpc = new RPCUtils();
                logger.info("调用rpc,squidFlowId:" + squidFlowId + ",repositoryId:" + repositoryId);
                Map<String, Object> map = new HashMap<String, Object>();
                map.put("token", TokenUtil.getToken());
                map.put("squidFlowId", squidFlowId);
                map.put("key", TokenUtil.getKey());
                //目的是在引擎队列推送的时候，当taskId还没有返回时，仍然能够拿到Token的信息
                Global.pushTask2Map(squidFlowId+"",map);
                taskId = rpc.postLaunchEngineJob(0, squidFlowId, repositoryId,
                        temp, out);
                logger.info("调用rpc结束");
                if (ValidateUtils.isEmpty(taskId)) {
                    //logger.info("taskId为空，运行squidflow异常");
                    return null;
                }
                // outputMap.put("TaskId", taskId);

                //logger.info("调试时:"+TokenUtil.getToken());
                Global.pushTask2Map(taskId, map);
                Global.pushTask2Map(TokenUtil.getToken() + "_" + squidFlowId, taskId);
                Global.pushTask2Map(TokenUtil.getToken(), taskId);
                return outputMap;
            } else {
                out.setMessageCode(MessageCode.NODATA);
            }
        } catch (Exception e) {
            logger.error(
                    "[获取runALLWithDebugging=========================================exception]",
                    e);
            out.setMessageCode(MessageCode.ERR);
            status = false;
        } finally {
            if (status && StringUtils.isNotBlank(taskId)) {
                logger.info(">>>>>>>>>调度日志开始<<<<<<<<<<");
                // getSquidFlowLog(taskId);
                System.out.println(taskId + ":" + key + "&" + token);
                CommonConsts.scheduleMap.put(taskId, TokenUtil.getKey() + "&" + TokenUtil.getToken());
            }
        }
        return null;
    }

    /**
     * 元数据检查
     *
     * @param info
     * @param out
     * @return
     */
    public Map<String, Object> metaDataCheck(String info, ReturnValue out) {
        logger.info("metaDataCheck start up");
        Map<String, Object> outputMap = new HashMap<String, Object>();
        try {
            Map<String, Object> inParmsMap = JsonUtil.toHashMap(info);
            int squidFlowId = Integer.parseInt(inParmsMap.get("SquidFlowId").toString());
            int repositoryId = Integer.parseInt(inParmsMap.get("RepositoryId").toString());
            if (squidFlowId > 0) {
                RPCUtils rpc = new RPCUtils();
                CharSequence checkValue = rpc.metaDataCheck(squidFlowId, out);
                System.out.println(checkValue.toString());
                outputMap.put("CheckInfoJson", checkValue.toString());
            } else {
                out.setMessageCode(MessageCode.NODATA);
            }
        } catch (Exception e) {
            logger.error(
                    "[metaDataCheck=========================================exception]",
                    e);
            out.setMessageCode(MessageCode.ERR);
        } finally {
            return outputMap;
        }
    }

    /**
     * 启动流式计算SquidFlow
     *
     * @param info 客户端参数
     * @return 返回客户端的数据
     */
    public InfoNewMessagePacket RunFlowCalculationSquidFlow(String info) {
        ReturnValue out = new ReturnValue();
        Map<String, Object> outputMap = new HashMap<>();
        try {
            //获取客户端传递的参数
            Map<String, Object> paramMap = JsonUtil.toHashMap(info);
            int squidFlowId = Integer.valueOf(paramMap.get("SquidFlowId") + "");
            int repositoryId = Integer.valueOf(paramMap.get("RepositoryId") + "");
            User user = JsonUtil.toGsonBean(paramMap.get("User").toString(), User.class);
            //封装启动工作任务参数
            String sparkParamsJson = paramMap.get("SparkParamtersJson").toString();
            RPCUtils rpc = new RPCUtils();
            String appId = rpc.postLauchEngineFlowCalculationJob(squidFlowId, repositoryId, sparkParamsJson, out, user);

            if (appId == null) {
                out.setMessageCode(MessageCode.ERR_EXEC_SQUIDFLOW);
            } else {
                outputMap.put("ApplicationId", appId); //返回客户端数据装载
                //向所有客户端发送日志，加锁
                LockSquidFlowProcess lockSquid = new LockSquidFlowProcess();
                lockSquid.sendAllClient(repositoryId, squidFlowId, 2);
            }

        } catch (Exception ex) {
            logger.error("Run flow calculation squidflow error.", ex);
        } finally {
            logger.info(">>>>>>>>>启动流式计算SquidFlow结束<<<<<<<<<<");
        }
        InfoNewMessagePacket infoPacket = new InfoNewMessagePacket(outputMap, out.getMessageCode().value());
        return infoPacket;
    }

    /**
     * 停止流式计算SquidFlow
     *
     * @param info 客户端参数
     * @return 返回客户端的数据
     */
    public InfoNewMessagePacket StopFlowCalculationSquidFlow(String info) {
        ReturnValue outputMap = new ReturnValue();
        try {
            Map<String, Object> paramMap = JsonUtil.toHashMap(info);
            String appId = paramMap.get("ApplicationId").toString();
            User currentUser = JsonUtil.toGsonBean(paramMap.get("User").toString(), User.class);
            RPCUtils rpc = new RPCUtils();
            boolean isSucess = rpc.postStopEngineFlowCalculationJob(appId, outputMap, currentUser);
            if (!isSucess)
                outputMap.setMessageCode(MessageCode.ERR_EXEC_SQUIDFLOW);
        } catch (Exception e) {
            logger.error("Run flow calculation squidflow error.", e);
        } finally {
            logger.info(">>>>>>>>>停止流式计算SquidFlow结束<<<<<<<<<<");
        }
        InfoNewMessagePacket infoPacket = new InfoNewMessagePacket(new HashMap<>(), outputMap.getMessageCode().value());
        return infoPacket;
    }

    /**
     * 获取登陆用户在当前Repository下所有的Stream squidflow的状态
     *
     * @param info
     * @return
     */
    public InfoNewMessagePacket GetApplicationStatus(String info) {
        ReturnValue outputValue = new ReturnValue();
        Map<String, Object> outputMap = new HashMap<>();
        IRelationalDataManager adapter = null;
        Map<String, Object> paramMap = JsonUtil.toHashMap(info);
        try {
            User currentUser = JsonUtil.toGsonBean(paramMap.get("User").toString(), User.class);
            int currentRepositoryId = Integer.valueOf(paramMap.get("repositoryId").toString());
            int currentPorjectId = Integer.valueOf(paramMap.get("projectId").toString());
            boolean canUserSearchDetail = Boolean.valueOf(paramMap.get("needToDetail").toString());

            DataAdapterFactory adapterFactory = DataAdapterFactory.newInstance();
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            /*查询数据库*/
            Map<String, Object> sqlParam = new HashMap<>();
            if (canUserSearchDetail)
                sqlParam.put("launch_user_id", currentUser.getId());
            if (currentPorjectId != 0)
                sqlParam.put("project_id", currentPorjectId);
            sqlParam.put("repository_id", currentRepositoryId);
            List<ApplicationStatus> applicationStatusesList = adapter.query2List2(true, sqlParam, ApplicationStatus.class);
//			for (ApplicationStatus item : applicationStatusesList) {
//				item.setConfig("");
//			}
            //处理数据
            if (canUserSearchDetail) {
                Map<String, Object> searchParam = new HashMap<>();
                /* 用于缓存数据查询的对象，减少数据库交互次数，SquidFlow级别几乎没有重复的需要，所以不需要缓存 */
                Map<Integer, Repository> repositoryCache = new HashMap<>();
                Map<Integer, Project> projectCache = new HashMap<>();

                if (applicationStatusesList == null)
                    applicationStatusesList = new ArrayList<>();
                for (ApplicationStatus item : applicationStatusesList) {
                    Repository currentRepository = null;
                    if (repositoryCache.containsKey(item.getRepository_id())) {
                        currentRepository = repositoryCache.get(item.getRepository_id());
                    } else {
                        searchParam.clear();
                        searchParam.put("id", item.getRepository_id());
                        currentRepository = adapter.query2Object(true, searchParam, Repository.class);
                    }
                    Project currentProject = null;
                    if (projectCache.containsKey(item.getProject_id())) {
                        currentProject = projectCache.get(item.getProject_id());
                    } else {
                        searchParam.clear();
                        searchParam.put("id", item.getProject_id());
                        currentProject = adapter.query2Object(true, searchParam, Project.class);
                    }
                    searchParam.clear();
                    searchParam.put("id", item.getSquidflow_id());
                    SquidFlow currentSquidFlow = adapter.query2Object(true, searchParam, SquidFlow.class);

                    String repositoryName = "--", projectName = "--", squidFlowName = "--";
                    if (currentRepository != null)
                        repositoryName = currentRepository.getName();
                    if (currentProject != null)
                        projectName = currentProject.getName();
                    if (currentSquidFlow != null)
                        squidFlowName = currentSquidFlow.getName();

                    item.setSquidflow_url(repositoryName + "\\" + projectName + "\\" + squidFlowName);
                }
            }

            outputMap.put("ApplicationStatusList", applicationStatusesList);

        } catch (Exception ex) {
            try {
                adapter.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
            outputValue.setMessageCode(MessageCode.ERR_GETALLPROJECT);
            logger.error("获取所有的application status异常", ex);
        } finally {
            adapter.closeSession();
            InfoNewMessagePacket infoPacket = new InfoNewMessagePacket(outputMap, outputValue.getMessageCode().value());
            return infoPacket;
        }
    }

    /**
     * 删除Application Status
     *
     * @param info
     * @return
     */
    public InfoNewMessagePacket deleteApplicationStatus(String info) {
        ReturnValue outputValue = new ReturnValue();
        Map<String, Object> outputMap = new HashMap<>();
        IRelationalDataManager adapter = null;
        Map<String, Object> paramMap = JsonUtil.toHashMap(info);
        try {
            String applicationstatusId = paramMap.get("ApplicationstatusId").toString();

            DataAdapterFactory adapterFactory = DataAdapterFactory.newInstance();
            adapter = DataAdapterFactory.getDefaultDataManager();

            Map<String, String> deleteParam = new HashMap<>();
            deleteParam.put("id", applicationstatusId);
            adapter.openSession();
            int deleteNumber = adapter.delete(deleteParam, ApplicationStatus.class);
            outputMap.put("deleteNumber", deleteNumber);
        } catch (Exception ex) {
            try {
                adapter.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
            outputValue.setMessageCode(MessageCode.ERR_DELETE);
            logger.error("删除Application status异常", ex);
        } finally {
            adapter.closeSession();
            return new InfoNewMessagePacket(outputMap, outputValue.getMessageCode().value());
        }
    }

    public Map<String, Object> resumeEngine(String info, ReturnValue out) {
        Map<String, Object> outputMap = new HashMap<String, Object>();
        try {
            Map<String, Object> params = JsonUtil.toHashMap(info);
            int squidFlowId = Integer.parseInt(params.get("SquidFlowId") + "");
            int squidId = Integer.parseInt(params.get("SquidId") + "");
            RPCUtils rpc = new RPCUtils();
            String taskId = Global.getTask2Map(TokenUtil.getToken() + "_" + squidFlowId) + "";
            rpc.resumeEngine(taskId, squidId, out);
            return outputMap;
        } catch (Exception e) {
            logger.error(
                    "[获取runALLWithDebugging=========================================exception]",
                    e);
            out.setMessageCode(MessageCode.ERR);
        }
        return null;
    }

    /**
     * 跳过 debugging, 继续流转
     *
     * @param info
     * @return
     */
    public Map<String, Object> stopDebug(String info, ReturnValue out) {
        Map<String, Object> outputMap = new HashMap<String, Object>();
        String taskId = "";
        Map<String, Object> params = JsonUtil.toHashMap(info);
        int squidFlowId = Integer.parseInt(params.get("SquidFlowId") + "");
        try {
            // int repositoryId =
            // Integer.parseInt(params.get("RepositoryId")+"");
            RPCUtils rpc = new RPCUtils();
            taskId = Global.getTask2Map(TokenUtil.getToken() + "_" + squidFlowId) + "";
            rpc.stopEngine(taskId, out);
            return outputMap;
        } catch (Exception e) {
            logger.error(
                    "[获取runALLWithDebugging=========================================exception]",
                    e);
            out.setMessageCode(MessageCode.ERR);
        } finally {// 调用停止日志方法
            /*
             * JobLogService jobLogService=new JobLogService();
			 * jobLogService.stopRunningLogs(token,taskId);
			 */
            CommonConsts.scheduleMap.remove(taskId);
            Global.removeTask2Map(taskId);
            Global.removeTask2Map(TokenUtil.getToken()+"_"+squidFlowId);
            Global.removeTask2Map(TokenUtil.getToken());
            Global.removeTask2Map(squidFlowId+"");
        }
        return null;
    }

    /**
     * 复制squidFlow信息
     *
     * @param info
     * @param out
     * @param type 是否推送数据
     * @return
     */
    public Map<String, Object> copySquidFlowForParams(String info,
                                                      ReturnValue out, int type) {
        Map<String, Object> outputMap = new HashMap<String, Object>();
        DataAdapterFactory dataFactory = null;
        IRelationalDataManager adapter3 = null;
        List<Integer> squidList = new ArrayList<Integer>();
        int squidFlowId = 0;
        Timer timer = new Timer();
        final Map<String, Object> returnMap = new HashMap<String, Object>();
        returnMap.put("1", 1);
        key = TokenUtil.getKey();
        token = TokenUtil.getToken();
        try {

            //定时推送，目的是复制大量的数据的时候，始终保持前后台的通信
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    logger.info("复制squid定时推送");
                    PushMessagePacket.pushMap(returnMap, DSObjectType.SQUIDLINK, "1011",
                            "0100", key, token, MessageCode.BATCH_CODE.value());
                }
            }, 25 * 1000, 25 * 1000);

            Map<String, Object> paramsMap = JsonUtil.toHashMap(info);
            List<Integer> squidIds = JsonUtil.toGsonList(
                    paramsMap.get("SquidIds") + "", Integer.class);
            squidFlowId = Integer.parseInt(paramsMap.get("SquidFlowId") + "");
            List<Integer> squidLinkIds = JsonUtil.toGsonList(
                    paramsMap.get("SquidLinkIds") + "", Integer.class);
            Set<Integer> squidIdsSet = new HashSet<>();
            adapter3 = DataAdapterFactory.getDefaultDataManager();
            adapter3.openSession();
            //判断复制的squid的数量
            if(squidLinkIds!=null){
                ISquidLinkDao squidLinkDao = new SquidLinkDaoImpl(adapter3);
                for(Integer ids : squidLinkIds){
                    //查找出squidLink对象
                    SquidLink squidLink = squidLinkDao.getObjectById(ids,SquidLink.class);
                    if(squidLink!=null) {
                        squidIdsSet.add(squidLink.getFrom_squid_id());
                        squidIdsSet.add(squidLink.getTo_squid_id());
                    }
                }
            }
            if(squidIds!=null) {
                for (Integer ids : squidIds) {
                    squidIdsSet.add(ids);
                }
            }
            int repositoryId = Integer.parseInt(paramsMap.get("RepositoryId")
                    + "");
            //校验是否超过squid数量
            validateNum(adapter3,repositoryId,squidIdsSet.size());
			/* 设置复制squid的偏移量 */
            int offset_x = paramsMap.get("Offset_X") == null ? 0 : Integer
                    .parseInt(paramsMap.get("Offset_X") + "");
            int offset_y = paramsMap.get("Offset_Y") == null ? 0 : Integer
                    .parseInt(paramsMap.get("Offset_Y") + "");
            setOffset_x(offset_x);
            setOffset_y(offset_y);

            // 判断输入参数是否为空
            if ((squidIds == null || squidIds.size() == 0)
                    && (squidLinkIds == null || squidLinkIds.size() == 0)) {
                out.setMessageCode(MessageCode.NODATA);
                return null;
            }
            if (squidLinkIds != null && squidLinkIds.size() > 0) {
                // 重新排序
                //squidLinkIds = squidLink.orderSquidLinkIds(squidLinkIds);
                //判断在数据库排序后的squidLinkIds
                if (squidLinkIds == null) {
                    out.setMessageCode(MessageCode.NODATA);
                    return null;
                }
            }
            initCopySquid(out, adapter3, squidList, squidFlowId, squidIds,
                    squidLinkIds, repositoryId, false);
            // outputMap.put("Squids", squidList);

            // 结束后还原偏移量
            setOffset_x(0);
            setOffset_y(0);
            return outputMap;
        } catch (BeyondSquidException e) {
            try {
                if (adapter3 != null)
                    adapter3.rollback();
            } catch (SQLException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            } finally {
                timer.cancel();
                timer.purge();
            }
            out.setMessageCode(MessageCode.ERR_SQUID_COUNT_MAX);
            logger.error("copySquidFlowForParams异常", e);
        } catch (Exception e) {
            logger.error(
                    "[获取copySquidFlowForParams=========================================exception]",
                    e);
            System.out.println(e.getMessage());
            if((MessageCode.ERR_SQUID_OVER_LIMIT.value()+"").equals(e.getMessage()+"")){
                out.setMessageCode(MessageCode.ERR_SQUID_OVER_LIMIT);
            } else if (e instanceof NullPointerException) {
                out.setMessageCode(MessageCode.NODATA);
            } else {
                out.setMessageCode(MessageCode.ERR);
            }
            try {
                adapter3.rollback();
            } catch (SQLException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            } finally {
                timer.cancel();
                timer.purge();
            }
        } finally {
            /*
             * if
			 * (out.getMessageCode().value()!=MessageCode.SUCCESS.value()&&adapter3
			 * !=null){ try { adapter3.rollback(); } catch (SQLException e) { //
			 * TODO Auto-generated catch block e.printStackTrace(); } }
			 */
            timer.cancel();
            timer.purge();
            if (adapter3 != null) {
                adapter3.closeSession();
            }
            if (out.getMessageCode().value() == MessageCode.SUCCESS.value()) {
                try {
                    if (type == 0) {
                        pushCopySquidData(squidList, squidFlowId);
                        pushCopySquidLinks(squidList, squidFlowId);
                    }
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            } else {
                Map<String, Object> map = new HashMap<String, Object>();
                map.put("squidCount", CommonConsts.MaxSquidCount);
                PushMessagePacket.pushMap(map, DSObjectType.SQUIDLINK, "1011",
                        "0100", TokenUtil.getKey(), TokenUtil.getToken(), out.getMessageCode().value());
            }
        }
        return null;
    }

    public static void validateNum(IRelationalDataManager adapter,int repositoryId,int alreadyNum) throws Exception {
        //校验是否超过squid数量
        String sql = "select * from ds_sys_repository where id="+repositoryId;
        Repository repository = adapter.query2Object(true,sql,null,Repository.class);
        if(repository.getPackageId() != null && repository.getPackageId()>0){
            //查找出当前squid数量，判断是否超过范围
            String squidCountSql = "select count(1) from ds_squid ds,ds_squid_flow dsf,ds_project dp,ds_sys_repository dsr where ds.squid_flow_id = dsf.id and dsf.project_id = dp.id and dp.repository_id = dsr.id and dsr.id="+repository.getId();
            Map<String,Object> countMap = adapter.query2Object(true,squidCountSql,null);
            //查找出套餐的信息
            JdbcTemplate cloudTemplate = (JdbcTemplate) Constants.context.getBean("cloudTemplate");
            Map<String,Object> map = cloudTemplate.queryForMap("select squid_num_limit from packages where id="+repository.getPackageId());
            if(map != null){
                Integer squidNumLimit = (Integer) map.get("squid_num_limit");
                if(squidNumLimit>-1){
                    //判断当前已经存在的squid的数量
                    int count =Integer.parseInt(countMap.values().iterator().next()+"");
                    if((count+alreadyNum)>squidNumLimit){
                        throw new RuntimeException(MessageCode.ERR_SQUID_OVER_LIMIT.value()+"");
                    }
                }
            }
        }
    }
    private synchronized void initCopySquid(ReturnValue out,
                                            IRelationalDataManager adapter3, List<Integer> squidList,
                                            int squidFlowId, List<Integer> squidIds,
                                            List<Integer> squidLinkIds, Integer repositoryId,
                                            boolean isValidation) throws Exception, SQLException {
        ISourceTableDao sourceTableDao = new SourceTableDaoImpl(adapter3);
        ISquidLinkDao squidLinkDao = new SquidLinkDaoImpl(adapter3);
        ISquidDao squidDao = new SquidDaoImpl(adapter3);

        Squid newSquid = null;
        // 记录squid是否添加过
        Map<Integer, Integer> squidMap = new HashMap<Integer, Integer>();
        Map<Integer, Integer> squidMap2 = new HashMap<Integer, Integer>();
        //用来存放id和TableName
        Map<String, Integer> TableMap = new HashMap<String, Integer>();
        // 记录column的转换过程, 在添加ReferenceColumn用
        Map<String, Integer> referenceColumnMap = new HashMap<String, Integer>();
        // transformation的转换过程
        Map<Integer, Integer> transMap = new HashMap<Integer, Integer>();
        // 记录trans里面的squid引用字段的转换过程
        Map<Integer, Integer> referenceTransMap = new HashMap<Integer, Integer>();
        //当下游为TaggingGroup的时候，存放上游的column映射
        Map<String, String> tagColumnMap = new HashMap<>();

        // 多个squid根据link一起进行复制，根据link信息，进行逐步处理
        if (squidLinkIds != null && squidLinkIds.size() > 0) {
            for (int linkId : squidLinkIds) {
                SquidLink squidLink = squidLinkDao.getObjectById(linkId,
                        SquidLink.class);
                //未能查询到数据
                //得到来源ID
                int fromSquidId = squidLink.getFrom_squid_id();
                int toSquidId = squidLink.getTo_squid_id();
                if (!squidMap.containsKey(fromSquidId)) {
                    long time = System.currentTimeMillis();
                    newSquid = getSquidByType(out, adapter3, squidFlowId,
                            newSquid, squidList, fromSquidId, 1, null,
                            referenceColumnMap, squidMap, transMap,
                            referenceTransMap, repositoryId, isValidation);
                    System.out.println("拷贝上游squid所用时间 :" + (System.currentTimeMillis() - time) + "毫秒");
                    // 更新SourceTabel中的is_extracted
                    //String sql = "update ds_source_table set is_extracted = 'N' where source_squid_id = " + newSquid.getId();
                    //adapter3.execute(sql);
                    List<SourceTable> SourceTables = sourceTableDao.getSourceTableBySquidId(newSquid.getId());
                    if (SourceTables != null && SourceTables.size() > 0) {
                        for (SourceTable sourceTable : SourceTables) {
                            int SourceId = sourceTable.getId();
                            String Tables_Name = sourceTable.getTableName();
                            Tables_Name = Tables_Name.replace(".", "_");
                            TableMap.put(Tables_Name, SourceId);
                        }
                    }
                    squidMap2.put(10, newSquid.getId());
                    squidMap.put(fromSquidId, newSquid.getId());

                }
                if (!squidMap.containsKey(toSquidId)) {
                    long time = System.currentTimeMillis();
                    newSquid = getSquidByType(out, adapter3, squidFlowId,
                            newSquid, squidList, toSquidId, 1, squidLink,
                            referenceColumnMap, squidMap, transMap,
                            referenceTransMap, repositoryId, isValidation);
                    System.out.println("复制下游的Squid花费的时间:squid类别" + SquidTypeEnum.parse(newSquid.getSquid_type()).name() + ",id为:" + newSquid.getId() + "时间为:" + (System.currentTimeMillis() - time) + "毫秒");
                    String table_name2 = newSquid.getTable_name();
                    int squidtype = squidDao.getSquidTypeById(fromSquidId);
                    if (squidtype == 0 || squidtype == 12 || squidtype == 13 || squidtype == 14 || squidtype == 38 || squidtype == 43) {
                        table_name2 = table_name2.replace(".", "_");
                        String sql = "update ds_source_table set is_extracted = 'Y' where id = " + TableMap.get(table_name2) + " and source_squid_id = " + squidMap.get(fromSquidId);
                        adapter3.execute(sql);
                    }
                    squidMap.put(toSquidId, newSquid.getId());
                } else {
                    // 该squid在上次循环时，已添加，当前处理，只需要在转换对象里面根据历史id进行查询新生成的id
                    int newFromSquidId = squidMap.get(fromSquidId);
                    int newToSquidId = squidMap.get(toSquidId);
                    squidLink.setId(0);
                    squidLink.setFrom_squid_id(newFromSquidId);
                    squidLink.setTo_squid_id(newToSquidId);
                    squidLink.setKey(StringUtils.generateGUID());
                    squidLink.setSquid_flow_id(squidFlowId);
                    int cnt = squidLinkDao.insert2(squidLink);
                    if (cnt > 0) {
                        copyDataSquidForLink(adapter3, toSquidId, fromSquidId,
                                newToSquidId, squidMap, referenceColumnMap,
                                transMap);
                    }
                }
            }
        }

        // 独立squid，特殊处理，并且进行类型验证
        if (squidIds != null && squidIds.size() > 0) {
            List<Integer> squidIdList = new ArrayList<>();
            List<Integer> list = new ArrayList<>();
            // 先进行单独的squid执行完成，type为0
            for (Integer squidId : squidIds) {
                if (SquidTypeEnum.isDBSourceBySquidType(squidDao.getSquidTypeById(squidId))) {
                    squidIdList.add(squidId);
                } else {
                    list.add(squidId);
                }
            }
            squidIdList.addAll(list);
            for (Integer squid : squidIdList) {
                if (squidMap.containsKey(squid)) {
                    continue;
                }
                newSquid = getSquidByType(out, adapter3, squidFlowId, newSquid,
                        squidList, squid, 1, null, referenceColumnMap,//type=0改为type=1,就可以复制单个孤立report和map,ESsquid---zhengpeng.deng
                        squidMap, transMap, referenceTransMap, repositoryId,
                        isValidation);
                if (newSquid == null) {
                    return;
                }
                squidMap.put(squid, newSquid.getId());
                //String sql = "update ds_source_table set is_extracted = 'N' where source_squid_id = " + newSquid.getId();
                //adapter3.execute(sql);
            }
        }


        IThirdPartyParamsDao paramsDao = new ThirdPartyParamsDaoImpl(adapter3);
        ITransformationDao transDao = new TransformationDaoImpl(adapter3);
        if (squidMap != null && squidMap.size() > 0) {
            for (Integer oldSquidId : squidMap.keySet()) {
                int newSquidId = squidMap.get(oldSquidId);
                squidDao.modifyDataMiningBySquidId(newSquidId, squidMap);
                squidDao.modifyDestinationBySquidId(newSquidId, squidMap);
                squidDao.modifyTemplateBySquidId(newSquidId, oldSquidId,
                        referenceColumnMap);
                paramsDao.modifyThirdPartyParamsForSquid(newSquidId, squidMap,
                        referenceColumnMap);
            }
        }
        // 最后修改trans中引用的squidId
        if (referenceTransMap != null && referenceTransMap.size() > 0) {
            for (Integer transId : referenceTransMap.keySet()) {
                Integer oldSquidId = referenceTransMap.get(transId);
                if (squidMap.containsKey(oldSquidId)) {
                    int newSquidId = squidMap.get(oldSquidId);
                    Transformation trans = transDao.getObjectById(transId,
                            Transformation.class);
                    if (trans != null && trans.getDictionary_squid_id() > 0) {
                        transDao.modifyRefSquidForTransId(transId, newSquidId,
                                0);
                    } else if (trans != null && trans.getModel_squid_id() > 0) {
                        transDao.modifyRefSquidForTransId(transId, newSquidId,
                                1);
                    }
                }
            }
        }
    }

    /**
     * 复制整个squidFlow
     *
     * @param info
     * @param out
     * @return
     */
    public Map<String, Object> copySquidFlowForSquidFlow(String info,
                                                         ReturnValue out) {
        Map<String, Object> outputMap = new HashMap<String, Object>();
        IRelationalDataManager adapter3 = null;
        Timer timer = new Timer();
        final Map<String, Object> returnMap = new HashMap<String, Object>();
        returnMap.put("1", 1);
        key = TokenUtil.getKey();
        token = TokenUtil.getToken();
        try {
            //定时推送，目的是复制大量的数据的时候，始终保持前后台的通信
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    logger.info("复制squid定时推送");
                    PushMessagePacket.pushMap(returnMap, DSObjectType.SQUIDLINK, "1011",
                            "0101", key, token, MessageCode.BATCH_CODE.value());
                }
            }, 25 * 1000, 25 * 1000);
            Map<String, Object> paramsMap = JsonUtil.toHashMap(info);

            int squidFlowId = Integer.parseInt(paramsMap.get("SquidFlowId")
                    + "");
            String squidFlowName = paramsMap.get("SquidFlowName") + "";
            int projectId = Integer.parseInt(paramsMap.get("ProjectId") + "");
            int repositoryId = Integer.parseInt(paramsMap.get("RepositoryId")
                    + "");
            if (squidFlowId > 0 && !ValidateUtils.isEmpty(squidFlowName)
                    && projectId > 0) {
                adapter3 = DataAdapterFactory.getDefaultDataManager();
                adapter3.openSession();
                // 判断是否超过限制
                String sql = "select count(1) from ds_squid where squid_flow_id="+squidFlowId;
                Map<String,Object> countMap = adapter3.query2Object(true,sql,null);
                validateNum(adapter3,repositoryId,Integer.parseInt(countMap.values().iterator().next()+""));
                ISquidDao squidDao = new SquidDaoImpl(adapter3);
                ISquidFlowDao squidFlowDao = new SquidFlowDaoImpl(adapter3);
                ISquidLinkDao squidLinkDao = new SquidLinkDaoImpl(adapter3);
                IVariableDao variableDao = new VariableDaoImpl(adapter3);
                SquidFlow oldSquidFlow = squidFlowDao.getObjectById(
                        squidFlowId, SquidFlow.class);
                if (oldSquidFlow != null) {
                    SquidFlowProcess process = new SquidFlowProcess(TokenUtil.getToken());
                    oldSquidFlow.setId(0);
                    oldSquidFlow.setKey(StringUtils.generateGUID());
                    oldSquidFlow.setName(process.getSquidFlowNameForCopy(
                            adapter3, squidFlowName, projectId, 1));
                    oldSquidFlow.setProject_id(projectId);
                    int n = adapter3.insert2(oldSquidFlow);
                    if (n > 0) {
                        oldSquidFlow.setId(n);
                    } else {
                        adapter3.rollback();
                        //返回一个空的squidflow对象
                        outputMap.put("SquidFlow", new SquidFlow());
                        return outputMap;
                    }
                    int newSquidFlowId = oldSquidFlow.getId();
                    List<Integer> squidList = new ArrayList<Integer>();
                    List<Integer> squidIds = squidDao
                            .getSquidIdsForNoLinkBySquidFlow(squidFlowId);
                    List<Integer> squidLinkIds = squidLinkDao
                            .orderSquidLinkIdsForSquidFlowId(squidFlowId);
                    /*
                     * Map<String, Object> map = new HashMap<String, Object>();
					 * map.put("SquidFlowId", newSquidFlowId);
					 * map.put("SquidIds", squidIds); map.put("SquidLinkIds",
					 * squidLinkIds); String newInfo =
					 * JsonUtil.toJSONString(map); if
					 * (!ValidateUtils.isEmpty(newInfo)){ newInfo =
					 * newInfo.substring(1); newInfo = newInfo.substring(0,
					 * newInfo.length()-1); }
					 */
                    this.initCopySquid(out, adapter3, squidList,
                            newSquidFlowId, squidIds, squidLinkIds,
                            repositoryId, true);

                    // 复制SquidFlow下面的变量
                    List<DSVariable> varList = variableDao
                            .getDSVariableByScope(1, squidFlowId);
                    if (varList != null && varList.size() > 0) {
                        for (DSVariable dsVariable : varList) {
                            copyVariableById(adapter3, dsVariable, 0,
                                    newSquidFlowId, projectId);
                        }
                    }
                    String name = process.getSquidFlowNameForCopy(adapter3, oldSquidFlow.getName(), projectId, 1);
                    if (name != null && name.equals(oldSquidFlow.getName())) {
                        outputMap.put("SquidFlow", oldSquidFlow);
                    } else {
                        oldSquidFlow.setName(name);
                        adapter3.update2(oldSquidFlow);
                        outputMap.put("SquidFlow", oldSquidFlow);
                    }

                    return outputMap;
                } else {
                    outputMap.put("SquidFlow", new SquidFlow());
                    return outputMap;
                }
            } else {
                out.setMessageCode(MessageCode.NODATA);
                return null;
            }
        } catch (BeyondSquidException e) {
            try {
                if (adapter3 != null)
                    adapter3.rollback();
            } catch (SQLException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            } finally {
                timer.cancel();
                timer.purge();
            }
            out.setMessageCode(MessageCode.ERR_SQUID_COUNT_MAX);
            logger.error("copySquidFlowForSquidFlow异常", e);
        } catch (Exception e) {
            // TODO: handle exception
            out.setMessageCode(MessageCode.SQL_ERROR);
            if((MessageCode.ERR_SQUID_OVER_LIMIT.value()+"").equals(e.getMessage())){
                out.setMessageCode(MessageCode.ERR_SQUID_OVER_LIMIT);
            }
            try {
                if (adapter3 != null)
                    adapter3.rollback();
            } catch (SQLException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            } finally {
                timer.cancel();
                timer.purge();
            }
            e.printStackTrace();

        } finally {
            if (adapter3 != null) {
                adapter3.closeSession();
            }
            timer.cancel();
            timer.purge();
        }
        outputMap.put("SquidFlow", new SquidFlow());
        return outputMap;
    }

    /**
     * 按类型分组，同时进行单个复制的类型验证，分类型进行调度处理方法
     *
     * @param out
     * @param adapter3
     * @param squidFlowId
     * @param newSquid
     * @param squidList
     * @param squidId
     * @param type
     * @param squidLink
     * @param referenceColumnMap
     * @param squidMap
     * @param transMap
     * @return
     * @throws Exception
     * @throws SQLException
     */
    private Squid getSquidByType(ReturnValue out,
                                 IRelationalDataManager adapter3, int squidFlowId, Squid newSquid,
                                 List<Integer> squidList, Integer squidId, int type,
                                 SquidLink squidLink, Map<String, Integer> referenceColumnMap,
                                 Map<Integer, Integer> squidMap, Map<Integer, Integer> transMap,
                                 Map<Integer, Integer> referenceTransMap, Integer repositoryId,
                                 boolean isValidation) throws Exception, SQLException {
        Squid squid;
        ISquidDao squidDao = new SquidDaoImpl(adapter3);
        ISourceColumnDao sourceColumnDao = new SourceColumnDaoImpl(adapter3);
        ISquidLinkDao squidLinkDao = new SquidLinkDaoImpl(adapter3);
        // 根据Id获取到相关的Squid集合。

        //squid = squidDao.getObjectById(squidId, SquidModelBase.class);
        String sql = "select * from ds_squid where id=" + squidId;
        squid = adapter3.query2Object(true, sql, null, Squid.class);
        if (squid == null) {
            out.setMessageCode(MessageCode.NODATA);
            return null;
        }
        SquidTypeEnum squidType = SquidTypeEnum.parse(squid.getSquid_type());
        int status = 0;
        // 根据squid类型进行分组，根据分组的值进行调用相关方法
        switch (squidType) {
            case EXTRACT:
            case REPORT:
            case EXCEPTION:
            case DOC_EXTRACT:
            case XML_EXTRACT:
            case WEBLOGEXTRACT:
            case WEBEXTRACT:
            case WEIBOEXTRACT:
            case GISMAP:
            case WEBSERVICEEXTRACT:
            case HTTPEXTRACT:
            case MONGODBEXTRACT:
            case DESTES:
            case DEST_HDFS:
            case DESTCLOUDFILE:
            case DEST_IMPALA:
            case DEST_HIVE:
            case DEST_CASSANDRA:
            case KAFKAEXTRACT:
            case HBASEEXTRACT:
            case HIVEEXTRACT:
            case CASSANDRA_EXTRACT:
                status = 1;
                break;
            case STREAM_STAGE:
            case STAGE:
            case DESTWS:
            case DBDESTINATION:
            case FACT:
            case GROUPTAGGING:
            case SAMPLINGSQUID:
            case PIVOTSQUID:
                status = 2;
                break;
            case DBSOURCE:
            case CLOUDDB:
            case TRAININGDBSQUID:
            case HIVE:
            case CASSANDRA:
                status = 3;
                break;
            case MONGODB:
            case FILEFOLDER:
            case FTP:
            case HDFS:
            case WEB:
            case WEIBO:
            case HTTP:
            case WEBSERVICE:
            case ANNOTATION:
            case KAFKA:
            case HBASE:
            case SOURCECLOUDFILE:
            case TRAINNINGFILESQUID:
                status = 4;
                break;
            case LOGREG:
            case NAIVEBAYES:
            case SVM:
            case KMEANS:
            case ALS:
            case LINEREG:
            case RIDGEREG:
            case QUANTIFY:
            case DISCRETIZE:
            case DECISIONTREE:
            case ASSOCIATION_RULES:
            case LASSO:
            case RANDOMFORESTCLASSIFIER:
            case RANDOMFORESTREGRESSION:
            case MULTILAYERPERCEPERONCLASSIFIER:
            case PLS:
            case NORMALIZER:
            case DATAVIEW:
            case COEFFICIENT:
            case DECISIONTREEREGRESSION:
            case DECISIONTREECLASSIFICATION:
            case BISECTINGKMEANSSQUID:
                status = 5;
                break;
            case USERDEFINED:
            case STATISTICS:
                status = 6;
                break;
            default:
                break;
        }
        // 特殊squid进行过滤
        if (status <= 1) {
            // 如果是Link信息，特殊的squid过滤恢复
            if ((type == 1 && status == 1) || isValidation) {
                status = 2;
            } else {
                out.setMessageCode(MessageCode.ERR_SQUIDLINK_DATA_INCOMPLETE);
                return null;
            }
        }
        // dataSquid的处理逻辑
        if (status == 2) {
            // 报表squid特殊处理
            if (squidType == SquidTypeEnum.REPORT) {
                newSquid = this.copyReportSquid(adapter3, squid, squidFlowId);
            } else if (squidType == SquidTypeEnum.GISMAP) {
                newSquid = this.copyGISMapSquid(adapter3, squid, squidFlowId);
            } else if (squidType == SquidTypeEnum.DESTWS) {
                newSquid = this.copyOtherSquid(adapter3, squid, squidFlowId);
            } else if (squidType == SquidTypeEnum.DOC_EXTRACT) {
                newSquid = this.copyDocExtractSquid(adapter3, squid,
                        squidFlowId, squidMap, referenceColumnMap, transMap,
                        referenceTransMap);
            } else if (squidType == SquidTypeEnum.KAFKAEXTRACT) {
                newSquid = this.copyKafkaExtractSquid(adapter3, squid,
                        squidFlowId, squidMap, referenceColumnMap, transMap,
                        referenceTransMap);
            } else if (squidType == SquidTypeEnum.HBASEEXTRACT) {
                newSquid = this.copyHBaseExtractSquid(adapter3, squid,
                        squidFlowId, squidMap, referenceColumnMap, transMap, referenceTransMap);
            } else if (squidType == SquidTypeEnum.DESTES) {
                if (squidLink != null) {
                    newSquid = this.copyDestESSquidFromSquidLink(adapter3, squid, squidFlowId, squidMap.get(squidLink.getFrom_squid_id()), squid.getId());
                } else {
                    newSquid = this.copyDestESSquid(adapter3, squid, squidFlowId, squid.getId());
                }
            } else if (squidType == SquidTypeEnum.DEST_HIVE) {
                newSquid = this.copyDestHiveFromSquidLink(adapter3, squid, squidFlowId, squidMap, referenceColumnMap);
            } else if (squidType == SquidTypeEnum.DEST_CASSANDRA) {
                newSquid = this.copyDestCassandraFromSquidLink(adapter3, squid, squidFlowId, squidMap, referenceColumnMap);
            } else if (squidType == SquidTypeEnum.DEST_HDFS || squid.getSquid_type() == SquidTypeEnum.DESTCLOUDFILE.value()) {
                if (squidLink != null) {
                    newSquid = this.copyDestHDFSSquidFromSquidLink(adapter3, squid, squidFlowId, squidMap.get(squidLink.getFrom_squid_id()), squid.getId());
                } else {
                    newSquid = this.copyDestHDFSSquid(adapter3, squid, squidFlowId, squid.getId());
                }
            } else if (squidType == SquidTypeEnum.DEST_IMPALA) {
                if (squidLink != null) {
                    newSquid = this.copyDestIMPALASquidFromSquidLink(adapter3, squid, squidFlowId, squidMap.get(squidLink.getFrom_squid_id()), squid.getId());
                } else {
                    newSquid = this.copyDestIMPALASquid(adapter3, squid, squidFlowId, squid.getId());
                }
            } else if (squidType == SquidTypeEnum.STREAM_STAGE) {
                newSquid = this.copyDataSquid(adapter3, squid, squidFlowId,
                        squidMap, referenceColumnMap, transMap,
                        referenceTransMap, StreamStageSquid.class);
            } else if (squidType == SquidTypeEnum.HIVEEXTRACT) {
                newSquid = this.copyDataSquid(adapter3, squid, squidFlowId, squidMap, referenceColumnMap, transMap, referenceTransMap, SystemHiveExtractSquid.class);
            } else if (squidType == SquidTypeEnum.CASSANDRA_EXTRACT) {
                newSquid = this.copyDataSquid(adapter3, squid, squidFlowId,
                        squidMap, referenceColumnMap, transMap,
                        referenceTransMap, CassandraExtractSquid.class);
            } else if(squidType == SquidTypeEnum.PIVOTSQUID){
                newSquid = this.copyDataSquid(adapter3, squid, squidFlowId,
                        squidMap, referenceColumnMap, transMap,
                        referenceTransMap, PivotSquid.class);
            }else {
                newSquid = this.copyDataSquid(adapter3, squid, squidFlowId,
                        squidMap, referenceColumnMap, transMap,
                        referenceTransMap, DataSquid.class);
            }
            if (type == 1 && squidLink != null) {
                int oldFromSquidId = squidLink.getFrom_squid_id();
                int newFromSquidId = squidMap.get(oldFromSquidId);
                squidLink.setId(0);
                squidLink.setFrom_squid_id(newFromSquidId);
                squidLink.setTo_squid_id(newSquid.getId());
                squidLink.setKey(StringUtils.generateGUID());
                squidLink.setSquid_flow_id(squidFlowId);
                squidLinkDao.insert2(squidLink);
                //实现所有生成squidJoin，referenceGroup，referenceCloumn，Transformation,TransformationLink（包括）
                copyDataSquidForLink(adapter3, squidId, oldFromSquidId,
                        newSquid.getId(), squidMap, referenceColumnMap,
                        transMap);
                //修改extractSquid的checkColumn属性
                if (SquidTypeEnum.isExtractBySquidType(squidType.value())) {
                    DataSquid cassandraSquid = (DataSquid) newSquid;
                    if (referenceColumnMap.containsKey(oldFromSquidId + "_" + ((DataSquid) newSquid).getCheck_column_id())) {
                        cassandraSquid.setCheck_column_id(referenceColumnMap.get(oldFromSquidId + "_" + ((DataSquid) newSquid).getCheck_column_id()));
                        adapter3.update2(cassandraSquid);
                    }

                }
//                if (SquidTypeEnum.isDestSquid(squidType.value())) {
//                    DestCassandraSquid destCassandraSquid = (DestCassandraSquid) newSquid;
//                }
                // 如果链接的是地图squid，那么重新构建下模板
                if (squidType == SquidTypeEnum.GISMAP) {
                    GISMapSquid gisMap = squidDao.getSquidForCond(
                            newSquid.getId(), GISMapSquid.class);
                    if (StringUtils.isNotNull(gisMap.getMap_template())) {
                        ReportMapParams rm = JSON
                                .parseObject(gisMap.getMap_template(),
                                        ReportMapParams.class);
                        if (StringUtils.isNotNull(rm)) {
                            if (referenceColumnMap.containsKey(oldFromSquidId
                                    + "_" + rm.getKey_column_id())) {
                                rm.setKey_column_id(referenceColumnMap
                                        .get(oldFromSquidId + "_"
                                                + rm.getKey_column_id()));
                            } else {
                                rm.setKey_column_id(0);
                            }
                            if (referenceColumnMap.containsKey(oldFromSquidId
                                    + "_" + rm.getVal_column_id())) {
                                rm.setVal_column_id(referenceColumnMap
                                        .get(oldFromSquidId + "_"
                                                + rm.getVal_column_id()));
                            } else {
                                rm.setVal_column_id(0);
                            }
                            if (referenceColumnMap.containsKey(oldFromSquidId
                                    + "_" + rm.getLeft_point_id())) {
                                rm.setLeft_point_id(referenceColumnMap
                                        .get(oldFromSquidId + "_"
                                                + rm.getLeft_point_id()));
                            } else {
                                rm.setLeft_point_id(0);
                            }
                            if (referenceColumnMap.containsKey(oldFromSquidId
                                    + "_" + rm.getRight_point_id())) {
                                rm.setRight_point_id(referenceColumnMap
                                        .get(oldFromSquidId + "_"
                                                + rm.getRight_point_id()));
                            } else {
                                rm.setRight_point_id(0);
                            }
                            gisMap.setMap_template(JSON.toJSONString(rm));
                            squidDao.update(gisMap);
                        }
                    }
                }
            }
            // sqlConnection处理
        } else if (status == 3) {
            if (squidType == SquidTypeEnum.HIVE) {
                newSquid = this.copyHiveConnectionSquid(adapter3, squid, squidFlowId);
                if (type == 1 && newSquid != null && newSquid.getId() > 0) {
                    sourceColumnDao.copyDbSourceForData(squidId, newSquid.getId());
                }
            } else if (squidType == SquidTypeEnum.CASSANDRA) {
                newSquid = this.copyCassandraConnectionSquid(adapter3, squid, squidFlowId);
                if (type == 1 && newSquid != null && newSquid.getId() > 0) {
                    sourceColumnDao.copyDbSourceForData(squidId, newSquid.getId());
                }
                squidMap.put(squid.getId(), newSquid.getId());
                if (!destSquidMap.isEmpty()) {
                    for (Integer key : destSquidMap.keySet()) {
                        if (squidMap.containsKey(key)) {
                            DestCassandraSquid destCassandraSquid = squidDao.getSquidForCond(destSquidMap.get(key),
                                    DestCassandraSquid.class);
                            destCassandraSquid.setDest_squid_id(squidMap.get(key));
                            squidDao.update(destCassandraSquid);
                            destSquidMap.remove(key);
                        }
                    }
                }
            } else {
                newSquid = this.copyDbSourceSquid(adapter3, squid, squidFlowId);
                if (type == 1 && newSquid != null && newSquid.getId() > 0) {
                    sourceColumnDao.copyDbSourceForData(squidId, newSquid.getId());
                }
            }

        } else if (status == 4) {
            newSquid = this.copyOtherConnectionSquid(adapter3, squid,
                    squidFlowId);
            if (type == 1 && newSquid != null && newSquid.getId() > 0) {
                sourceColumnDao.copyDbSourceForData(squidId, newSquid.getId());
            }
        } else if (status == 5) {
            newSquid = this.copyDataMiningSquid(adapter3, squid, squidFlowId,
                    referenceColumnMap, transMap, referenceTransMap,
                    repositoryId);
            if (type == 1 && squidLink != null) {
                int oldFromSquidId = squidLink.getFrom_squid_id();
                int newFromSquidId = squidMap.get(oldFromSquidId);
                squidLink.setId(0);
                squidLink.setFrom_squid_id(newFromSquidId);
                squidLink.setTo_squid_id(newSquid.getId());
                squidLink.setKey(StringUtils.generateGUID());
                squidLink.setSquid_flow_id(squidFlowId);
                squidLinkDao.insert2(squidLink);
                copyDataSquidForLink(adapter3, squidId, oldFromSquidId,
                        newSquid.getId(), squidMap, referenceColumnMap,
                        transMap);
            }
        } else if (status == 6) {
            //复制userdefined,squid和column信息
            newSquid = this.copyUserDefinedSquid(adapter3, squid, squidFlowId,
                    referenceColumnMap, transMap, referenceTransMap,
                    repositoryId);
            //复制squidlink
            if (type == 1 && squidLink != null) {
                int oldFromSquidId = squidLink.getFrom_squid_id();
                int newFromSquidId = squidMap.get(oldFromSquidId);
                squidLink.setId(0);
                squidLink.setFrom_squid_id(newFromSquidId);
                squidLink.setTo_squid_id(newSquid.getId());
                squidLink.setKey(StringUtils.generateGUID());
                squidLink.setSquid_flow_id(squidFlowId);
                squidLinkDao.insert2(squidLink);
                //复制referenceColumn,dataMaping,parameterMap,Transformation信息
                copyUserDefinedDataForLink(adapter3, squidId, oldFromSquidId,
                        newSquid.getId(), newSquid.getSquid_type(), squidMap, referenceColumnMap,
                        transMap);
            } else {
                //直接复制referenceColumn，dataMaping,Transformation
                ITransformationDao transDao = new TransformationDaoImpl(adapter3);
                TransformationService service = new TransformationService(TokenUtil.getToken());
                ITransformationLinkDao transLinkDao = new TransformationLinkDaoImpl(adapter3);
                //查询出dataMap
                Map<String, Object> paramMap = new HashMap<>();
                paramMap.put("squid_id", squidId);
                if (newSquid.getSquid_type() == SquidTypeEnum.USERDEFINED.value()) {
                    List<UserDefinedMappingColumn> dataMapColumn = adapter3.query2List2(true, paramMap, UserDefinedMappingColumn.class);
                    if (dataMapColumn != null) {
                        for (UserDefinedMappingColumn mapColumn : dataMapColumn) {
                            mapColumn.setSquid_id(newSquid.getId());
                            mapColumn.setColumn_id(0);
                            adapter3.insert2(mapColumn);
                        }
                    }
                } else if (newSquid.getSquid_type() == SquidTypeEnum.STATISTICS.value()) {
                    List<StatisticsDataMapColumn> dataMapColumn = adapter3.query2List2(true, paramMap, StatisticsDataMapColumn.class);
                    if (dataMapColumn != null) {
                        for (StatisticsDataMapColumn mapColumn : dataMapColumn) {
                            mapColumn.setSquid_id(newSquid.getId());
                            mapColumn.setColumn_id(0);
                            adapter3.insert2(mapColumn);
                        }
                    }
                }
                //复制Transformation，TransformationLink
                List<Transformation> trans = transDao.getTransListBySquidId(squidId);
                if (trans != null && trans.size() > 0) {
                    for (Transformation transformation : trans) {
                        int transId = transformation.getId();
                        int newFromTransId = 0;
                        int oldColumnId = transformation.getColumn_id();
                        int newColumnId = 0;
                        //复制左边的
                        if (referenceColumnMap != null
                                && referenceColumnMap.containsKey(transformation.getSquid_id() + "_"
                                + oldColumnId)) {
                            newColumnId = referenceColumnMap.get(transformation.getSquid_id() + "_"
                                    + oldColumnId);
                            Transformation newTrans = service.initTransformation(
                                    adapter3, newSquid.getId(), newColumnId,
                                    transformation.getTranstype(),
                                    transformation.getOutput_data_type(), 1, 0);
                            newFromTransId = newTrans.getId();
                            transMap.put(transId, newFromTransId);
                        }
                        List<TransformationLink> links = transLinkDao
                                .getTransLinkByFromTrans(transId);
                        if (links != null && links.size() > 0 && newFromTransId > 0) {
                            for (TransformationLink link : links) {
                                if (transMap
                                        .containsKey(link.getTo_transformation_id())) {
                                    int oldToTrans = link.getTo_transformation_id();
                                    int newToTrans = transMap.get(oldToTrans);
                                    link.setTo_transformation_id(newToTrans);
                                    link.setFrom_transformation_id(newFromTransId);
                                    link.setKey(StringUtils.generateGUID());
                                    transLinkDao.insert2(link);
                                    //transInputMaps.put(oldToTrans, newToTrans);
                                }
                            }
                        }
                    }
                }
            }
        }

        // 复制变量
        IVariableDao variableDao = new VariableDaoImpl(adapter3);
        ISquidFlowDao squidFlowDao = new SquidFlowDaoImpl(adapter3);
        List<DSVariable> list = variableDao.getDSVariableByScope(2, squidId);
        if (list != null && list.size() > 0) {
            SquidFlow newSquidFlow = squidFlowDao.getObjectById(squidFlowId,
                    SquidFlow.class);
            for (DSVariable dsVariable : list) {
                variableDao.copyVariableById(dsVariable, newSquid.getId(),
                        newSquidFlow.getId(), newSquidFlow.getProject_id());
            }
        }
        squidList.add(newSquid.getId());
        return newSquid;
    }

    /**
     * 拷贝UserDefinedSquid信息
     * referenceColumn,dataMapColumn,parameterColumn
     *
     * @param adapter3
     * @param squidId
     * @param fromSquidId
     * @param newSquidId
     * @param squidMap
     * @param referenceColumnMap
     * @param transMap
     */
    public void copyUserDefinedDataForLink(IRelationalDataManager adapter3,
                                           int squidId, int fromSquidId, int newSquidId, int squidType,
                                           Map<Integer, Integer> squidMap,
                                           Map<String, Integer> referenceColumnMap,
                                           Map<Integer, Integer> transMap) throws Exception {
        Map<Integer, Integer> transInputMaps = new HashMap<Integer, Integer>();
        TransformationService service = new TransformationService(TokenUtil.getToken());
        int newFromSquidId = squidMap.get(fromSquidId);
        // 规则匹配的squidId, 在ExceptionSquid中会不同
        int tempSquidId = fromSquidId;

        ISquidDao squidDao = new SquidDaoImpl(adapter3);
        IReferenceColumnDao refColumnDao = new ReferenceColumnDaoImpl(adapter3);
        IColumnDao columnDao = new ColumnDaoImpl(adapter3);
        ITransformationDao transDao = new TransformationDaoImpl(adapter3);
        ITransformationLinkDao transLinkDao = new TransformationLinkDaoImpl(adapter3);
        //获取老的squid的referenceColumn信息
        List<ReferenceColumnGroup> groupList = refColumnDao.getRefColumnGroupListBySquidId(
                squidId);
        int oldGroupId = 0;
        if (groupList != null && groupList.size() > 0) {
            for (ReferenceColumnGroup group : groupList) {
                oldGroupId = group.getId();
                group.setId(0);
                group.setReference_squid_id(newSquidId);
                group.setKey(StringUtils.generateGUID());
                int newGroupId = adapter3.insert2(group);
                group.setId(newGroupId);

                // 首先添加新的column映射关系，在创建新的ReferenceColumn和Transformation的时候用到
                Squid fromSquid = squidDao.getObjectById(squidId, Squid.class);
                //获取oldGroupId下的ReferenceColumn列表
                List<ReferenceColumn> referenceColumns = refColumnDao
                        .getRefColumnListByGroupId(oldGroupId);
                if (referenceColumns != null && referenceColumns.size() > 0) {
                    Squid newToSquid = squidDao.getObjectById(newSquidId,
                            Squid.class);
                    for (ReferenceColumn referenceColumn : referenceColumns) {
                        int oldColumnId = referenceColumn.getColumn_id();
                        int newColumnId = 0;
                        // 只有完全符合referenceColumn的下游列  才能进行复制
                        if (referenceColumnMap.containsKey(tempSquidId + "_"
                                + oldColumnId)) {
                            newColumnId = referenceColumnMap.get(tempSquidId + "_"
                                    + oldColumnId);
                            referenceColumn.setColumn_id(newColumnId);
                            referenceColumn.setReference_squid_id(newSquidId);
                            referenceColumn.setHost_squid_id(newFromSquidId);
                            referenceColumn.setGroup_id(newGroupId);
                            refColumnDao.insert2(referenceColumn);
                        }

                    }
                }
            }
        }

        //复制Transformation，TransformationLink
        List<Transformation> trans = transDao.getTransListBySquidId(squidId);
        if (trans != null && trans.size() > 0) {
            for (Transformation transformation : trans) {
                int transId = transformation.getId();
                int newFromTransId = 0;
                int oldColumnId = transformation.getColumn_id();
                int newColumnId = 0;
                //复制右边的
                if (referenceColumnMap != null
                        && referenceColumnMap.containsKey(tempSquidId + "_"
                        + oldColumnId)) {
                    newColumnId = referenceColumnMap.get(tempSquidId + "_"
                            + oldColumnId);
                    Transformation newTrans = service.initTransformation(
                            adapter3, newSquidId, newColumnId,
                            transformation.getTranstype(),
                            transformation.getOutput_data_type(), 1, 0);
                    newFromTransId = newTrans.getId();
                    transMap.put(transId, newFromTransId);
                }
                //复制左边的
                if (referenceColumnMap != null
                        && referenceColumnMap.containsKey(transformation.getSquid_id() + "_"
                        + oldColumnId)) {
                    newColumnId = referenceColumnMap.get(transformation.getSquid_id() + "_"
                            + oldColumnId);
                    Transformation newTrans = service.initTransformation(
                            adapter3, newSquidId, newColumnId,
                            transformation.getTranstype(),
                            transformation.getOutput_data_type(), 1, 0);
                    newFromTransId = newTrans.getId();
                    transMap.put(transId, newFromTransId);
                }
                List<TransformationLink> links = transLinkDao
                        .getTransLinkByFromTrans(transId);
                if (links != null && links.size() > 0 && newFromTransId > 0) {
                    for (TransformationLink link : links) {
                        if (transMap
                                .containsKey(link.getTo_transformation_id())) {
                            int oldToTrans = link.getTo_transformation_id();
                            int newToTrans = transMap.get(oldToTrans);
                            link.setTo_transformation_id(newToTrans);
                            link.setFrom_transformation_id(newFromTransId);
                            link.setKey(StringUtils.generateGUID());
                            transLinkDao.insert2(link);
                            // service.updTransInputs(adapter3, link, null);
                            transInputMaps.put(oldToTrans, newToTrans);
                        }
                    }
                }
            }
        }

        //复制dataMap
        Map<String, Object> paramMap = new HashMap<>();
        paramMap.put("squid_id", squidId);
        if (squidType == SquidTypeEnum.USERDEFINED.value()) {
            List<UserDefinedMappingColumn> dataMapColumnList = adapter3.query2List2(true, paramMap, UserDefinedMappingColumn.class);
            if (dataMapColumnList != null) {
                for (UserDefinedMappingColumn dataMapColumn : dataMapColumnList) {
                    dataMapColumn.setSquid_id(newSquidId);
                    if (referenceColumnMap.containsKey(tempSquidId + "_" + dataMapColumn.getColumn_id())) {
                        dataMapColumn.setColumn_id(referenceColumnMap.get(tempSquidId + "_" + dataMapColumn.getColumn_id()));
                        adapter3.insert2(dataMapColumn);
                    }
                    if (dataMapColumn.getColumn_id() == 0) {
                        adapter3.insert2(dataMapColumn);
                    }
                }
            }
        } else if (squidType == SquidTypeEnum.STATISTICS.value()) {
            List<StatisticsDataMapColumn> dataMapColumnList = adapter3.query2List2(true, paramMap, StatisticsDataMapColumn.class);
            if (dataMapColumnList != null) {
                for (StatisticsDataMapColumn dataMapColumn : dataMapColumnList) {
                    dataMapColumn.setSquid_id(newSquidId);
                    if (referenceColumnMap.containsKey(tempSquidId + "_" + dataMapColumn.getColumn_id())) {
                        dataMapColumn.setColumn_id(referenceColumnMap.get(tempSquidId + "_" + dataMapColumn.getColumn_id()));
                        adapter3.insert2(dataMapColumn);
                    }
                    if (dataMapColumn.getColumn_id() == 0) {
                        adapter3.insert2(dataMapColumn);
                    }
                }
            }
        }

    }

    /**
     * 复制SqlConnection Squid的处理
     *
     * @param adapter3
     * @param squid
     * @param squidFlowId
     * @return
     * @throws Exception
     */
    public Squid copyDbSourceSquid(IRelationalDataManager adapter3,
                                   Squid squid, int squidFlowId) throws Exception {
        if (squid != null) {
            int squidId = squid.getId();
            ISquidDao squidDao = new SquidDaoImpl(adapter3);
            DbSquid db = squidDao.getSquidForCond(squidId, DbSquid.class);
            int x = squid.getLocation_x() + getOffset_x();
            int y = squid.getLocation_y() + getOffset_y();
            db.setLocation_x(x);
            db.setLocation_y(y);
            db.setId(0);// 清空id
            db.setName(this.getSquidName(adapter3, squid.getName(), squidFlowId));
            db.setSquidflow_id(squidFlowId);// 给squidFlowId复制
            db.setKey(StringUtils.generateGUID());
            db.setId(adapter3.insert2(db));
            return db;
        }
        return null;
    }

    /**
     * 拷贝hive类型的squid
     */
    public Squid copyHiveConnectionSquid(IRelationalDataManager adapter3,
                                         Squid squid, int squidFlowId) throws Exception {
        if (squid != null) {
            int squidId = squid.getId();
            ISquidDao squidDao = new SquidDaoImpl(adapter3);
            SystemHiveConnectionSquid hiveConnectionSquid = squidDao.getSquidForCond(squidId, SystemHiveConnectionSquid.class);
            int x = squid.getLocation_x() + getOffset_x();
            int y = squid.getLocation_y() + getOffset_y();
            hiveConnectionSquid.setLocation_x(x);
            hiveConnectionSquid.setLocation_y(y);
            hiveConnectionSquid.setId(0);// 清空id
            hiveConnectionSquid.setName(this.getSquidName(adapter3, squid.getName(), squidFlowId));
            hiveConnectionSquid.setSquidflow_id(squidFlowId);// 给squidFlowId复制
            hiveConnectionSquid.setKey(StringUtils.generateGUID());
            hiveConnectionSquid.setId(adapter3.insert2(hiveConnectionSquid));
            return hiveConnectionSquid;
        }
        return null;
    }

    /**
     * 拷贝connection类型的squid
     */
    public Squid copyCassandraConnectionSquid(IRelationalDataManager adapter3,
                                              Squid squid, int squidFlowId) throws Exception {
        if (squid != null) {
            int squidId = squid.getId();
            ISquidDao squidDao = new SquidDaoImpl(adapter3);
            CassandraConnectionSquid connectionSquid = squidDao.getSquidForCond(squidId, CassandraConnectionSquid.class);
            int x = squid.getLocation_x() + getOffset_x();
            int y = squid.getLocation_y() + getOffset_y();
            connectionSquid.setLocation_x(x);
            connectionSquid.setLocation_y(y);
            connectionSquid.setId(0);// 清空id
            connectionSquid.setName(this.getSquidName(adapter3, squid.getName(), squidFlowId));
            connectionSquid.setSquidflow_id(squidFlowId);// 给squidFlowId复制
            connectionSquid.setKey(StringUtils.generateGUID());
            connectionSquid.setId(adapter3.insert2(connectionSquid));
            return connectionSquid;
        }
        return null;
    }

    /**
     * 复制其他类型的 Connection
     *
     * @param adapter3
     * @param squid
     * @param squidFlowId
     * @return
     * @throws Exception
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Squid copyOtherConnectionSquid(IRelationalDataManager adapter3,
                                          Squid squid, int squidFlowId) throws Exception {
        if (squid != null) {
            int squidId = squid.getId();
            Class c = SquidTypeEnum.classOfValue(squid.getSquid_type());
            ISquidDao squidDao = new SquidDaoImpl(adapter3);
            Map<String, Object> map = squidDao.getMapForCond(squidId, c);
            int x = squid.getLocation_x() + getOffset_x();
            int y = squid.getLocation_y() + getOffset_y();
            map.put("LOCATION_X", x);
            map.put("LOCATION_Y", y);
            map.put("ID", 0);// 清空id
            map.put("NAME",
                    this.getSquidName(adapter3, squid.getName(), squidFlowId));
            map.put("SQUID_FLOW_ID", squidFlowId);
            map.put("KEY", StringUtils.generateGUID());
            int newId = adapter3
                    .insert2(AnnotationHelper.result2Object(map, c));
            map.put("ID", newId);
            Squid newSquid = (Squid) AnnotationHelper.result2Object(map, c);

            // 复制抓取信息
            List<Url> urls = squidDao.getDSUrlsBySquidId(squidId);
            if (urls != null && urls.size() > 0) {
                for (Url url : urls) {
                    url.setSquid_id(newId);
                    squidDao.insert2(url);
                }
            }
            return newSquid;
        }
        return null;
    }

    /**
     * 复制 data squid的处理，兼容任何从DataSquid派生出来的子类
     *
     * @param adapter3
     * @param squid
     * @param squidFlowId
     * @param squidMap
     * @param referenceColumnMap
     * @param transMap
     * @param referenceTransMap
     * @param dataSquidType
     * @return
     * @throws Exception
     */
    public <T extends DataSquid> DataSquid copyDataSquid(IRelationalDataManager adapter3, Squid squid,
                                                         int squidFlowId, Map<Integer, Integer> squidMap,
                                                         Map<String, Integer> referenceColumnMap,
                                                         Map<Integer, Integer> transMap,
                                                         Map<Integer, Integer> referenceTransMap, Class<T> dataSquidType) throws Exception {
        ISquidDao squidDao = new SquidDaoImpl(adapter3);
        ISourceTableDao sourceTableDao = new SourceTableDaoImpl(adapter3);
        int newSquidId = 0;
        if (squid != null) {
            int squidId = squid.getId();
            DataSquid newSquid=null;
            if(squid.getSquid_type()==SquidTypeEnum.SAMPLINGSQUID.value()){
                newSquid=squidDao.getSquidForCond(squidId, SamplingSquid.class);
            }else {
             newSquid = squidDao.getSquidForCond(squidId,
                    dataSquidType);
            }
            int x = squid.getLocation_x() + getOffset_x();
            int y = squid.getLocation_y() + getOffset_y();
            newSquid.setLocation_x(x);
            newSquid.setLocation_y(y);
            newSquid.setId(0);// 清空id
            newSquid.setName(this.getSquidName(adapter3, squid.getName(),
                    squidFlowId));
            newSquid.setSquidflow_id(squidFlowId);// 给squidFlowId复制
            newSquid.setKey(StringUtils.generateGUID());
            if (newSquid != null && newSquid.getSource_table_id() > 0) {
                DBSourceTable dbSourceTable = sourceTableDao.getObjectById(
                        newSquid.getSource_table_id(), DBSourceTable.class);
                if (dbSourceTable != null) {
                    String table_name = dbSourceTable.getTable_name();
                    int source_squid_id = dbSourceTable.getSource_squid_id();
                    if (squidMap.containsKey(source_squid_id)) {
                        int newId = squidMap.get(source_squid_id);
                        DBSourceTable newDbSourceTable = sourceTableDao
                                .getDBSourceTableByParams(newId, table_name);
                        if (newDbSourceTable != null) {
                            newSquid.setSource_table_id(newDbSourceTable
                                    .getId());
                        }
                    }
                }
            }
            newSquid.setId(squidDao.insert2(newSquid));
            if (newSquid.getId() > 0) {
                newSquidId = newSquid.getId();
                if (newSquid != null) {
                    copyDataSquidForPart(adapter3, newSquidId, squidId,
                            referenceColumnMap, transMap, referenceTransMap, newSquid);
                }
            }

            return newSquid;
        }
        return null;
    }

    /**
     * 拷贝	单个DestESSquid
     *
     * @param adapter3
     * @param squid
     * @param squidFlowId // * @param fromSquidId
     * @param oldSquidId
     * @return
     * @throws Exception
     */
    private Squid copyDestESSquid(IRelationalDataManager adapter3, Squid squid, int squidFlowId, int oldSquidId) throws Exception {
        ISquidDao squidDao = new SquidDaoImpl(adapter3);
        if (squid != null) {
            int squidId = squid.getId();
            DestESSquid newSquid = squidDao.getSquidForCond(squidId,
                    DestESSquid.class);
            int x = squid.getLocation_x() + getOffset_x();
            int y = squid.getLocation_y() + getOffset_y();
            newSquid.setLocation_x(x);
            newSquid.setLocation_y(y);
            newSquid.setId(0);// 清空id
            newSquid.setName(this.getSquidName(adapter3, squid.getName(), squidFlowId));//squidName:squidName副本
            newSquid.setSquidflow_id(squidFlowId);// 给squidFlowId复制
            newSquid.setKey(StringUtils.generateGUID());
//			if (newSquid != null && newSquid.getFolder_id() > 0) {
//				newSquid.setFolder_id(0);
//			}
            int newSquidId = adapter3.insert2(newSquid);
            newSquid.setId(newSquidId);
            List<EsColumn> listEsColumn = copyDeatESForPart(adapter3, newSquidId, oldSquidId);//创建复制过来的EsColumn集合
            for (EsColumn esColumn : listEsColumn) {
                esColumn.setId(adapter3.insert2(esColumn));
            }
            if (newSquid.getId() > 0) {
                return newSquid;
            }
        }
        return null;
    }

    public Squid copyDestSquid(IRelationalDataManager adapter3, Squid squid, int squidFlowId, int oldSquidId) throws Exception {
        ISquidDao squidDao = new SquidDaoImpl(adapter3);
        if (squid != null) {
            int squidId = squid.getId();
            DestHiveSquid newSquid = squidDao.getSquidForCond(squidId,
                    DestHiveSquid.class);
            int x = squid.getLocation_x() + getOffset_x();
            int y = squid.getLocation_y() + getOffset_y();
            newSquid.setLocation_x(x);
            newSquid.setLocation_y(y);
            newSquid.setId(0);// 清空id
            newSquid.setName(this.getSquidName(adapter3, squid.getName(), squidFlowId));//squidName:squidName副本
            newSquid.setSquidflow_id(squidFlowId);// 给squidFlowId复制
            newSquid.setKey(StringUtils.generateGUID());
//			if (newSquid != null && newSquid.getFolder_id() > 0) {
//				newSquid.setFolder_id(0);
//			}
            int newSquidId = adapter3.insert2(newSquid);
            newSquid.setId(newSquidId);
            List<EsColumn> listEsColumn = copyDeatESForPart(adapter3, newSquidId, oldSquidId);//创建复制过来的EsColumn集合
            for (EsColumn esColumn : listEsColumn) {
                esColumn.setId(adapter3.insert2(esColumn));
            }
            if (newSquid.getId() > 0) {
                return newSquid;
            }
        }
        return null;
    }

    /**
     * 创建单个ESColumn
     *
     * @param adapter3
     * @param newSquidId
     * @return
     */


    private List<EsColumn> copyDeatESForPart(IRelationalDataManager adapter3, int newSquidId, int oldSquidId) {
        Map<String, Object> map = new HashMap<String, Object>();
        //取出新的column和旧的EsColumn 因为我们

        map.put("squid_id", oldSquidId);
        IEsColumnDao esColumnDao = new EsColumnDaoImpl(adapter3);
        List<EsColumn> listEsColumn = esColumnDao.getEsColumnsBySquidId(true, oldSquidId);
//		List<EsColumn> listEsColumn = adapter3.query2List2(true,map ,EsColumn.class);
        //map.put("squid_id",fromSquidId);
        // 生成一个包含Column表单的集合对象
        List<Column> listColumn = adapter3.query2List2(true, map, Column.class);
        for (Column column : listColumn) {
            for (EsColumn esColumn : listEsColumn) {
                // 通过EScolumn对象中的column属性调用getName方法
                if (esColumn.getColumn().getName().equals(column.getName())) {
                    esColumn.setId(0);
                    esColumn.setColumn_id(column.getId());
                    esColumn.setSquid_id(newSquidId);
                    break;
                }
            }
        }
        return listEsColumn;
    }

    /**
     * 创建ESColumn
     *
     * @param adapter3
     * @param newSquidId
     * @param fromSquidId
     * @return
     */
    private List<EsColumn> copyDeatESForPart(IRelationalDataManager adapter3, int newSquidId, int fromSquidId, int oldSquidId) {
        Map<String, Object> map = new HashMap<String, Object>();
        //取出新的column和旧的EsColumn 因为我们

//		map.put("squid_id",oldSquidId);
        IEsColumnDao esColumnDao = new EsColumnDaoImpl(adapter3);
        List<EsColumn> listEsColumn = esColumnDao.getEsColumnsBySquidId(true, oldSquidId);
//		List<EsColumn> listEsColumn = adapter3.query2List2(true,map ,EsColumn.class);
        map.put("squid_id", fromSquidId);
        // 生成一个包含Column表单的集合对象
        List<Column> listColumn = adapter3.query2List2(true, map, Column.class);
        for (Column column : listColumn) {
            for (EsColumn esColumn : listEsColumn) {
                // 通过EScolumn对象中的column属性调用getName方法
                if (esColumn.getColumn().getName().equals(column.getName())) {
                    esColumn.setId(0);
                    esColumn.setColumn_id(column.getId());
                    esColumn.setSquid_id(newSquidId);
                    break;
                }
            }
        }
        return listEsColumn;
    }

    /**
     * 拷贝hiveColumn
     *
     * @param adapter3
     * @param newSquidId
     * @param oldSquidId
     * @return
     */
    public List<DestHiveColumn> copyDestHiveForPart(IRelationalDataManager adapter3, int newSquidId, int oldSquidId, Map<String, Integer> referenceColumnMap) throws SQLException {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("squid_id", oldSquidId);
        List<DestHiveColumn> oldHiveColumn = adapter3.query2List2(true, map, DestHiveColumn.class);
        ISquidLinkDao squidLinkDao = new SquidLinkDaoImpl(adapter3);
        List<SquidLink> squidLinks = squidLinkDao.getFormSquidLinkBySquidId(oldSquidId);
        if (oldHiveColumn != null) {
            if (squidLinks == null || squidLinks.size() == 0) {
                for (DestHiveColumn hiveColumn : oldHiveColumn) {
                    hiveColumn.setSquid_id(newSquidId);
                    hiveColumn.setColumn_id(0);
                }
            } else {
                SquidLink squidLink = squidLinks.get(0);
                for (DestHiveColumn hiveColumn : oldHiveColumn) {
                    hiveColumn.setSquid_id(newSquidId);
                    if (referenceColumnMap.containsKey(squidLink.getFrom_squid_id() + "_" + hiveColumn.getColumn_id())) {
                        hiveColumn.setColumn_id(referenceColumnMap.get(squidLink.getFrom_squid_id() + "_" + hiveColumn.getColumn_id()));
                    } else {
                        hiveColumn.setColumn_id(0);
                    }
                }
            }

        }
        return oldHiveColumn;
    }

    /**
     * 拷贝cassandraColumn
     *
     * @param adapter3
     * @param newSquidId
     * @param oldSquidId
     * @return
     */
    public List<DestCassandraColumn> copyDestCassandraForPart(IRelationalDataManager adapter3, int newSquidId, int oldSquidId, Map<String, Integer> referenceColumnMap) throws SQLException {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("squid_id", oldSquidId);
        List<DestCassandraColumn> oldCassandraColumn = adapter3.query2List2(true, map, DestCassandraColumn.class);
        ISquidLinkDao squidLinkDao = new SquidLinkDaoImpl(adapter3);
        List<SquidLink> squidLinks = squidLinkDao.getFormSquidLinkBySquidId(oldSquidId);
        if (oldCassandraColumn != null) {
            if (squidLinks == null || squidLinks.size() == 0) {
                for (DestCassandraColumn cassandraColumn : oldCassandraColumn) {
                    cassandraColumn.setSquid_id(newSquidId);
                    cassandraColumn.setColumn_id(0);
                }
            } else {
                SquidLink squidLink = squidLinks.get(0);
                for (DestCassandraColumn cassandraColumn : oldCassandraColumn) {
                    cassandraColumn.setSquid_id(newSquidId);
                    if (referenceColumnMap.containsKey(squidLink.getFrom_squid_id() + "_" + cassandraColumn.getColumn_id())) {
                        cassandraColumn.setColumn_id(referenceColumnMap.get(squidLink.getFrom_squid_id() + "_" + cassandraColumn.getColumn_id()));
                    } else {
                        cassandraColumn.setColumn_id(0);
                    }
                }
            }

        }
        return oldCassandraColumn;
    }

    /**
     * 拷贝DestESSquid
     *
     * @param adapter3
     * @param squid
     * @param squidFlowId
     * @param fromSquidId
     * @param oldSquidId
     * @return
     * @throws Exception
     */
    private Squid copyDestESSquidFromSquidLink(IRelationalDataManager adapter3, Squid squid,
                                               int squidFlowId, int fromSquidId, int oldSquidId) throws Exception {
        ISquidDao squidDao = new SquidDaoImpl(adapter3);
        if (squid != null) {
            int squidId = squid.getId();
            DestESSquid newSquid = squidDao.getSquidForCond(squidId,
                    DestESSquid.class);
            int x = squid.getLocation_x() + getOffset_x();
            int y = squid.getLocation_y() + getOffset_y();
            newSquid.setLocation_x(x);
            newSquid.setLocation_y(y);
            newSquid.setId(0);// 清空id
            newSquid.setName(this.getSquidName(adapter3, squid.getName(), squidFlowId));//squidName:squidName副本
            newSquid.setSquidflow_id(squidFlowId);// 给squidFlowId复制
            newSquid.setKey(StringUtils.generateGUID());
//			if (newSquid != null && newSquid.getFolder_id() > 0) {
//				newSquid.setFolder_id(0);
//			}
            int newSquidId = adapter3.insert2(newSquid);
            newSquid.setId(newSquidId);
            List<EsColumn> listEsColumn = copyDeatESForPart(adapter3, newSquidId, fromSquidId, oldSquidId);//创建复制过来的EsColumn集合
            for (EsColumn esColumn : listEsColumn) {
                esColumn.setId(adapter3.insert2(esColumn));
            }
            if (newSquid.getId() > 0) {
                return newSquid;
            }
        }
        return null;
    }

    /**
     * 拷贝DestSquidLink
     *
     * @param adapter3
     * @param squid
     * @param squidFlowId
     * @return
     */
    public Squid copyDestHiveFromSquidLink(IRelationalDataManager adapter3, Squid squid,
                                           int squidFlowId, Map<Integer, Integer> squidMap, Map<String, Integer> referenceColumnMap) throws Exception {
        ISquidDao squidDao = new SquidDaoImpl(adapter3);
        if (squid != null) {
            int squidId = squid.getId();
            DestHiveSquid newSquid = squidDao.getSquidForCond(squidId,
                    DestHiveSquid.class);
            int x = squid.getLocation_x() + getOffset_x();
            int y = squid.getLocation_y() + getOffset_y();
            newSquid.setLocation_x(x);
            newSquid.setLocation_y(y);
            newSquid.setId(0);// 清空id
            newSquid.setName(this.getSquidName(adapter3, squid.getName(), squidFlowId));//squidName:squidName副本
            newSquid.setSquidflow_id(squidFlowId);// 给squidFlowId复制
            newSquid.setKey(StringUtils.generateGUID());
            int newSquidId = adapter3.insert2(newSquid);
            squidMap.put(newSquid.getId(), newSquidId);
            newSquid.setId(newSquidId);
            List<DestHiveColumn> listEsColumn = copyDestHiveForPart(adapter3, newSquidId, squidId, referenceColumnMap);//创建复制过来的EsColumn集合
            for (DestHiveColumn esColumn : listEsColumn) {
                esColumn.setId(adapter3.insert2(esColumn));
            }
            if (newSquid.getId() > 0) {
                return newSquid;
            }
        }
        return null;
    }

    /**
     * 拷贝DestSquidLink
     *
     * @param adapter3
     * @param squid
     * @param squidFlowId
     * @return
     */
    public Squid copyDestCassandraFromSquidLink(IRelationalDataManager adapter3, Squid squid,
                                                int squidFlowId, Map<Integer, Integer> squidMap, Map<String, Integer> referenceColumnMap) throws Exception {
        ISquidDao squidDao = new SquidDaoImpl(adapter3);
        if (squid != null) {
            int squidId = squid.getId();
            DestCassandraSquid newSquid = squidDao.getSquidForCond(squidId,
                    DestCassandraSquid.class);
            if (squidMap.containsKey(newSquid.getDest_squid_id())) {
                newSquid.setDest_squid_id(squidMap.get(newSquid.getDest_squid_id()));
            }
            int x = squid.getLocation_x() + getOffset_x();
            int y = squid.getLocation_y() + getOffset_y();
            newSquid.setLocation_x(x);
            newSquid.setLocation_y(y);
            newSquid.setId(0);// 清空id
            newSquid.setName(this.getSquidName(adapter3, squid.getName(), squidFlowId));//squidName:squidName副本
            newSquid.setSquidflow_id(squidFlowId);// 给squidFlowId复制
            newSquid.setKey(StringUtils.generateGUID());
            int newSquidId = adapter3.insert2(newSquid);
            squidMap.put(newSquid.getId(), newSquidId);
            destSquidMap.put(newSquid.getDest_squid_id(), newSquidId);
            newSquid.setId(newSquidId);
            List<DestCassandraColumn> listEsColumn = copyDestCassandraForPart(adapter3, newSquidId, squidId, referenceColumnMap);//创建复制过来的EsColumn集合
            for (DestCassandraColumn esColumn : listEsColumn) {
                esColumn.setId(adapter3.insert2(esColumn));
            }
            if (newSquid.getId() > 0) {
                return newSquid;
            }
        }
        return null;
    }

    //	/**
//	 * 创建单个HDFSColumn
//	 * @param adapter3
//	 * @param newSquidId
//
//	 * @return
//	 */
    private List<DestHDFSColumn> copyDeatHDFSForPart(IRelationalDataManager adapter3, int newSquidId, int oldSquidId) {
        Map<String, Object> map = new HashMap<String, Object>();
        //取出新的column和旧的hdfsColumn
        map.put("squid_id", oldSquidId);
        IHdfsColumnDao hdfsColumnDao = new HdfsColumnDaoImpl(adapter3);
        List<DestHDFSColumn> listHDFSColumn = hdfsColumnDao.getHdfsColumnBySquidId(true, oldSquidId);
//		List<EsColumn> listEsColumn = adapter3.query2List2(true,map ,EsColumn.class);
        //map.put("squid_id",fromSquidId);
        // 生成一个包含Column表单的集合对象
        List<Column> listColumn = adapter3.query2List2(true, map, Column.class);
        for (Column column : listColumn) {
            for (DestHDFSColumn hdfsColumn : listHDFSColumn) {
                // 通过EScolumn对象中的column属性调用getName方法
                if (hdfsColumn.getColumn().getName().equals(column.getName())) {
                    hdfsColumn.setId(0);
                    hdfsColumn.setColumn_id(column.getId());
                    hdfsColumn.setSquid_id(newSquidId);
                    break;
                }
            }
        }
        return listHDFSColumn;
    }

    /**
     * 创建HDFSColumn
     *
     * @param adapter3
     * @param newSquidId
     * @param fromSquidId
     * @return
     */
    private List<DestHDFSColumn> copyDeatHDFSForPart(IRelationalDataManager adapter3, int newSquidId, int fromSquidId, int oldSquidId) {
        Map<String, Object> map = new HashMap<String, Object>();
        //取出新的column和旧的EsColumn

        IHdfsColumnDao hdfsColumnDao = new HdfsColumnDaoImpl(adapter3);
        List<DestHDFSColumn> listHdfsColumn = hdfsColumnDao.getHdfsColumnBySquidId(true, oldSquidId);
        map.put("squid_id", fromSquidId);
        // 生成一个包含Column表单的集合对象
        List<Column> listColumn = adapter3.query2List2(true, map, Column.class);
        for (Column column : listColumn) {
            for (DestHDFSColumn hdfsColumn : listHdfsColumn) {
                // 通过EScolumn对象中的column属性调用getName方法
                if (hdfsColumn.getColumn().getName().equals(column.getName())) {
                    hdfsColumn.setId(0);
                    hdfsColumn.setColumn_id(column.getId());
                    hdfsColumn.setSquid_id(newSquidId);
                    break;
                }
            }
        }
        return listHdfsColumn;
    }

    /**
     * 拷贝DestHDFSSquid
     *
     * @param adapter3
     * @param squid
     * @param squidFlowId
     * @param fromSquidId
     * @param oldSquidId
     * @return
     * @throws Exception
     */
    private Squid copyDestHDFSSquidFromSquidLink(IRelationalDataManager adapter3, Squid squid,
                                                 int squidFlowId, int fromSquidId, int oldSquidId) throws Exception {
        ISquidDao squidDao = new SquidDaoImpl(adapter3);
        if (squid != null) {
            int squidId = squid.getId();
            DestHDFSSquid newSquid = squidDao.getSquidForCond(squidId,
                    DestHDFSSquid.class);
            int x = squid.getLocation_x() + getOffset_x();
            int y = squid.getLocation_y() + getOffset_y();
            newSquid.setLocation_x(x);
            newSquid.setLocation_y(y);
            newSquid.setId(0);// 清空id
            newSquid.setName(this.getSquidName(adapter3, squid.getName(), squidFlowId));//squidName:squidName副本
            newSquid.setSquidflow_id(squidFlowId);// 给squidFlowId复制
            newSquid.setKey(StringUtils.generateGUID());
            int newSquidId = adapter3.insert2(newSquid);
            newSquid.setId(newSquidId);
            List<DestHDFSColumn> listHdfsColumn = copyDeatHDFSForPart(adapter3, newSquidId, fromSquidId, oldSquidId);//创建复制过来的EsColumn集合
            for (DestHDFSColumn hdfsColumn : listHdfsColumn) {
                hdfsColumn.setId(adapter3.insert2(hdfsColumn));
            }
            if (newSquid.getId() > 0) {
                return newSquid;
            }
        }
        return null;
    }

    /**
     * 拷贝	单个DestHDFSSquid
     *
     * @param adapter3
     * @param squid
     * @param squidFlowId // * @param fromSquidId
     * @param oldSquidId
     * @return
     * @throws Exception
     */
    private Squid copyDestHDFSSquid(IRelationalDataManager adapter3, Squid squid, int squidFlowId, int oldSquidId) throws Exception {
        ISquidDao squidDao = new SquidDaoImpl(adapter3);
        if (squid != null) {
            int squidId = squid.getId();
            DestHDFSSquid newSquid = squidDao.getSquidForCond(squidId,
                    DestHDFSSquid.class);
            int x = squid.getLocation_x() + getOffset_x();
            int y = squid.getLocation_y() + getOffset_y();
            newSquid.setLocation_x(x);
            newSquid.setLocation_y(y);
            newSquid.setId(0);// 清空id
            newSquid.setName(this.getSquidName(adapter3, squid.getName(), squidFlowId));//squidName:squidName副本
            newSquid.setSquidflow_id(squidFlowId);// 给squidFlowId复制
            newSquid.setKey(StringUtils.generateGUID());
//			if (newSquid != null && newSquid.getFolder_id() > 0) {
//				newSquid.setFolder_id(0);
//			}
            int newSquidId = adapter3.insert2(newSquid);
            newSquid.setId(newSquidId);
            List<DestHDFSColumn> listHdfsColumn = copyDeatHDFSForPart(adapter3, newSquidId, oldSquidId);//创建复制过来的EsColumn集合
            for (DestHDFSColumn hdfsColumn : listHdfsColumn) {
                hdfsColumn.setId(adapter3.insert2(hdfsColumn));
            }
            if (newSquid.getId() > 0) {
                return newSquid;
            }
        }
        return null;
    }

    //-----------------------------------------华丽的分割线-------------------------------------------------

    /**
     * 创建单个IMPALAColumn
     *
     * @param adapter3
     * @param newSquidId
     * @return
     */
    private List<DestImpalaColumn> copyDeatIMPALAForPart(IRelationalDataManager adapter3, int newSquidId, int oldSquidId) {
        Map<String, Object> map = new HashMap<String, Object>();
        //取出新的column和旧的hdfsColumn
        map.put("squid_id", oldSquidId);
        ImpalaColumnDao impalaColumnDao = new ImpalaColumnDaoImpl(adapter3);
        List<DestImpalaColumn> listImpalaColumn = impalaColumnDao.getImpalaColumnBySquidId(true, oldSquidId);
//		List<EsColumn> listEsColumn = adapter3.query2List2(true,map ,EsColumn.class);
        //map.put("squid_id",fromSquidId);
        // 生成一个包含Column表单的集合对象
        List<Column> listColumn = adapter3.query2List2(true, map, Column.class);
        for (Column column : listColumn) {
            for (DestImpalaColumn impalaColumn : listImpalaColumn) {
                // 通过ImpalaColumn对象中的column属性调用getName方法
                if (impalaColumn.getColumn().getName().equals(column.getName())) {
                    impalaColumn.setId(0);
                    impalaColumn.setColumn_id(column.getId());
                    impalaColumn.setSquid_id(newSquidId);
                    break;
                }
            }
        }
        return listImpalaColumn;
    }

    /**
     * 创建IMPALAColumn
     *
     * @param adapter3
     * @param newSquidId
     * @param fromSquidId
     * @return
     */
    private List<DestImpalaColumn> copyDeatIMPALAForPart(IRelationalDataManager adapter3, int newSquidId, int fromSquidId, int oldSquidId) {
        Map<String, Object> map = new HashMap<String, Object>();
        //取出新的column和旧的ImpalaColumn

        ImpalaColumnDao impalaColumnDao = new ImpalaColumnDaoImpl(adapter3);
        List<DestImpalaColumn> listImpalaColumn = impalaColumnDao.getImpalaColumnBySquidId(true, oldSquidId);
        map.put("squid_id", fromSquidId);
        // 生成一个包含Column表单的集合对象
        List<Column> listColumn = adapter3.query2List2(true, map, Column.class);
        for (Column column : listColumn) {
            for (DestImpalaColumn impalaColumn : listImpalaColumn) {
                // 通过IMPALAcolumn对象中的column属性调用getName方法
                if (impalaColumn.getColumn().getName().equals(column.getName())) {
                    impalaColumn.setId(0);
                    impalaColumn.setColumn_id(column.getId());
                    impalaColumn.setSquid_id(newSquidId);
                    break;
                }
            }
        }
        return listImpalaColumn;
    }

    /**
     * 拷贝DestIMPALASquid
     *
     * @param adapter3
     * @param squid
     * @param squidFlowId
     * @param fromSquidId
     * @param oldSquidId
     * @return
     * @throws Exception
     */
    private Squid copyDestIMPALASquidFromSquidLink(IRelationalDataManager adapter3, Squid squid,
                                                   int squidFlowId, int fromSquidId, int oldSquidId) throws Exception {
        ISquidDao squidDao = new SquidDaoImpl(adapter3);
        if (squid != null) {
            int squidId = squid.getId();
            DestImpalaSquid newSquid = squidDao.getSquidForCond(squidId,
                    DestImpalaSquid.class);
            int x = squid.getLocation_x() + getOffset_x();
            int y = squid.getLocation_y() + getOffset_y();
            newSquid.setLocation_x(x);
            newSquid.setLocation_y(y);
            newSquid.setId(0);// 清空id
            newSquid.setName(this.getSquidName(adapter3, squid.getName(), squidFlowId));//squidName:squidName副本
            newSquid.setSquidflow_id(squidFlowId);// 给squidFlowId复制
            newSquid.setKey(StringUtils.generateGUID());
            int newSquidId = adapter3.insert2(newSquid);
            newSquid.setId(newSquidId);
            List<DestImpalaColumn> listImpalaColumn = copyDeatIMPALAForPart(adapter3, newSquidId, fromSquidId, oldSquidId);//创建复制过来的EsColumn集合
            for (DestImpalaColumn impalaColumn : listImpalaColumn) {
                impalaColumn.setId(adapter3.insert2(impalaColumn));
            }
            if (newSquid.getId() > 0) {
                return newSquid;
            }
        }
        return null;
    }

    /**
     * 拷贝	单个DestIMPALASquid
     *
     * @param adapter3
     * @param squid
     * @param squidFlowId // * @param fromSquidId
     * @param oldSquidId
     * @return
     * @throws Exception
     */
    private Squid copyDestIMPALASquid(IRelationalDataManager adapter3, Squid squid, int squidFlowId, int oldSquidId) throws Exception {
        ISquidDao squidDao = new SquidDaoImpl(adapter3);
        if (squid != null) {
            int squidId = squid.getId();
            DestImpalaSquid newSquid = squidDao.getSquidForCond(squidId,
                    DestImpalaSquid.class);
            int x = squid.getLocation_x() + getOffset_x();
            int y = squid.getLocation_y() + getOffset_y();
            newSquid.setLocation_x(x);
            newSquid.setLocation_y(y);
            newSquid.setId(0);// 清空id
            newSquid.setName(this.getSquidName(adapter3, squid.getName(), squidFlowId));//squidName:squidName副本
            newSquid.setSquidflow_id(squidFlowId);// 给squidFlowId复制
            newSquid.setKey(StringUtils.generateGUID());
//			if (newSquid != null && newSquid.getFolder_id() > 0) {
//				newSquid.setFolder_id(0);
//			}
            int newSquidId = adapter3.insert2(newSquid);
            newSquid.setId(newSquidId);
            List<DestImpalaColumn> listImpalaColumn = copyDeatIMPALAForPart(adapter3, newSquidId, oldSquidId);//创建复制过来的EsColumn集合
            for (DestImpalaColumn impalaColumn : listImpalaColumn) {
                impalaColumn.setId(adapter3.insert2(impalaColumn));
            }
            if (newSquid.getId() > 0) {
                return newSquid;
            }
        }
        return null;
    }

    /**
     * 拷贝hiveExtract的数据
     *
     * @param adapter3
     * @param squid
     * @param squidFlowId
     * @return
     */
    public Squid copyHiveDataSquid(IRelationalDataManager adapter3, Squid squid,
                                   int squidFlowId, Map<Integer, Integer> squidMap,
                                   Map<String, Integer> referenceColumnMap,
                                   Map<Integer, Integer> transMap,
                                   Map<Integer, Integer> referenceTransMap, Class<?> dataSquidType) throws Exception {
        int newSquidId = 0;
        ISquidDao squidDao = new SquidDaoImpl(adapter3);
        ISourceTableDao sourceTableDao = new SourceTableDaoImpl(adapter3);
        Map<String, String> tagColumnMap = new HashMap<>();
        if (squid != null) {
            int squidId = squid.getId();
            SystemHiveExtractSquid newSquid = squidDao.getSquidForCond(squidId,
                    SystemHiveExtractSquid.class);
            int x = squid.getLocation_x() + getOffset_x();
            int y = squid.getLocation_y() + getOffset_y();
            newSquid.setLocation_x(x);
            newSquid.setLocation_y(y);
            newSquid.setId(0);// 清空id
            newSquid.setName(this.getSquidName(adapter3, squid.getName(),
                    squidFlowId));
            newSquid.setSquidflow_id(squidFlowId);// 给squidFlowId复制
            newSquid.setKey(StringUtils.generateGUID());
            if (newSquid != null && newSquid.getDestination_squid_id() > 0) {
                if (squidMap.containsKey(newSquid.getDestination_squid_id())) {
                    int newId = squidMap
                            .get(newSquid.getDestination_squid_id());
                    newSquid.setDestination_squid_id(newId);
                }
            }
            if (newSquid != null && newSquid.getSource_table_id() > 0) {
                DBSourceTable dbSourceTable = sourceTableDao.getObjectById(
                        newSquid.getSource_table_id(), DBSourceTable.class);
                if (dbSourceTable != null) {
                    String table_name = dbSourceTable.getTable_name();
                    int source_squid_id = dbSourceTable.getSource_squid_id();
                    if (squidMap.containsKey(source_squid_id)) {
                        int newId = squidMap.get(source_squid_id);
                        DBSourceTable newDbSourceTable = sourceTableDao
                                .getDBSourceTableByParams(newId, table_name);
                        if (newDbSourceTable != null) {
                            newSquid.setSource_table_id(newDbSourceTable
                                    .getId());
                        }
                    }
                }
            }
            newSquid.setId(squidDao.insert2(newSquid));
            if (newSquid.getId() > 0) {
                newSquidId = newSquid.getId();
                if (newSquid != null) {
                    copyDataSquidForPart(adapter3, newSquidId, squidId,
                            referenceColumnMap, transMap, referenceTransMap, newSquid);
                }
            }
            return newSquid;
        }
        return null;
    }

    public Squid copyCassandraDataSquid(IRelationalDataManager adapter3, Squid squid,
                                        int squidFlowId, Map<Integer, Integer> squidMap,
                                        Map<String, Integer> referenceColumnMap,
                                        Map<Integer, Integer> transMap,
                                        Map<Integer, Integer> referenceTransMap, Class<?> dataSquidType) throws Exception {
        int newSquidId = 0;
        ISquidDao squidDao = new SquidDaoImpl(adapter3);
        ISourceTableDao sourceTableDao = new SourceTableDaoImpl(adapter3);
        if (squid != null) {
            int squidId = squid.getId();
            CassandraExtractSquid newSquid = squidDao.getSquidForCond(squidId,
                    CassandraExtractSquid.class);
            int x = squid.getLocation_x() + getOffset_x();
            int y = squid.getLocation_y() + getOffset_y();
            newSquid.setLocation_x(x);
            newSquid.setLocation_y(y);
            newSquid.setId(0);// 清空id
            newSquid.setName(this.getSquidName(adapter3, squid.getName(),
                    squidFlowId));
            newSquid.setSquidflow_id(squidFlowId);// 给squidFlowId复制
            newSquid.setKey(StringUtils.generateGUID());
            if (newSquid != null && newSquid.getDestination_squid_id() > 0) {
                if (squidMap.containsKey(newSquid.getDestination_squid_id())) {
                    int newId = squidMap
                            .get(newSquid.getDestination_squid_id());
                    newSquid.setDestination_squid_id(newId);
                }
            }
            if (newSquid != null && newSquid.getSource_table_id() > 0) {
                DBSourceTable dbSourceTable = sourceTableDao.getObjectById(
                        newSquid.getSource_table_id(), DBSourceTable.class);
                if (dbSourceTable != null) {
                    String table_name = dbSourceTable.getTable_name();
                    int source_squid_id = dbSourceTable.getSource_squid_id();
                    if (squidMap.containsKey(source_squid_id)) {
                        int newId = squidMap.get(source_squid_id);
                        DBSourceTable newDbSourceTable = sourceTableDao
                                .getDBSourceTableByParams(newId, table_name);
                        if (newDbSourceTable != null) {
                            newSquid.setSource_table_id(newDbSourceTable
                                    .getId());
                        }
                    }
                }
            }
            newSquid.setId(squidDao.insert2(newSquid));
            if (newSquid.getId() > 0) {
                newSquidId = newSquid.getId();
                if (newSquid != null) {
                    copyDataSquidForPart(adapter3, newSquidId, squidId,
                            referenceColumnMap, transMap, referenceTransMap, newSquid);
                }
            }
            return newSquid;
        }
        return null;
    }
    //-----------------------------------------华丽的分割线-------------------------------------------------

    /**
     * report数据表的复制
     *
     * @param adapter3
     * @param squid
     * @param squidFlowId
     * @throws Exception
     */
    private Squid copyReportSquid(IRelationalDataManager adapter3, Squid squid,
                                  int squidFlowId) throws Exception {
        ISquidDao squidDao = new SquidDaoImpl(adapter3);
        if (squid != null) {
            int squidId = squid.getId();
            ReportSquid newSquid = squidDao.getSquidForCond(squidId,
                    ReportSquid.class);
            int x = squid.getLocation_x() + getOffset_x();
            int y = squid.getLocation_y() + getOffset_y();
            newSquid.setLocation_x(x);
            newSquid.setLocation_y(y);
            newSquid.setId(0);// 清空id
            newSquid.setName(this.getSquidName(adapter3, squid.getName(),
                    squidFlowId));
            newSquid.setSquidflow_id(squidFlowId);// 给squidFlowId复制
            newSquid.setKey(StringUtils.generateGUID());
            if (newSquid != null && newSquid.getFolder_id() > 0) {
                newSquid.setFolder_id(0);
            }
            newSquid.setId(adapter3.insert2(newSquid));
            if (newSquid.getId() > 0) {
                return newSquid;
            }
        }
        return null;
    }

    /**
     * gismap数据表的复制
     *
     * @param adapter3
     * @param squid
     * @param squidFlowId
     * @throws Exception
     */
    private Squid copyGISMapSquid(IRelationalDataManager adapter3, Squid squid,
                                  int squidFlowId) throws Exception {
        ISquidDao squidDao = new SquidDaoImpl(adapter3);
        if (squid != null) {
            int squidId = squid.getId();
            GISMapSquid newSquid = squidDao.getSquidForCond(squidId,
                    GISMapSquid.class);
            int x = squid.getLocation_x() + getOffset_x();
            int y = squid.getLocation_y() + getOffset_y();
            newSquid.setLocation_x(x);
            newSquid.setLocation_y(y);
            newSquid.setId(0);// 清空id
            newSquid.setName(this.getSquidName(adapter3, squid.getName(),
                    squidFlowId));
            newSquid.setSquidflow_id(squidFlowId);// 给squidFlowId复制
            newSquid.setKey(StringUtils.generateGUID());
            if (newSquid != null && newSquid.getFolder_id() > 0) {
                newSquid.setFolder_id(0);
            }
            if (newSquid != null
                    && StringUtils.isNotNull(newSquid.getMap_template())) {

            }
            newSquid.setId(adapter3.insert2(newSquid));
            if (newSquid.getId() > 0) {
                return newSquid;
            }
        }
        return null;
    }

    /**
     * 其他类型的数据表的复制
     *
     * @param adapter3
     * @param squid
     * @param squidFlowId
     * @throws Exception
     */
    private Squid copyOtherSquid(IRelationalDataManager adapter3, Squid squid,
                                 int squidFlowId) throws Exception {
        if (squid != null) {
            int squidId = squid.getId();
            Class c = SquidTypeEnum.classOfValue(squid.getSquid_type());
            ISquidDao squidDao = new SquidDaoImpl(adapter3);
            Map<String, Object> map = squidDao.getMapForCond(squidId, c);
            int x = squid.getLocation_x() + getOffset_x();
            int y = squid.getLocation_y() + getOffset_y();
            map.put("LOCATION_X", x);
            map.put("LOCATION_Y", y);
            map.put("ID", 0);// 清空id
            map.put("NAME",
                    this.getSquidName(adapter3, squid.getName(), squidFlowId));
            map.put("SQUID_FLOW_ID", squidFlowId);
            map.put("KEY", StringUtils.generateGUID());
            int newId = adapter3
                    .insert2(AnnotationHelper.result2Object(map, c));
            map.put("ID", newId);
            Squid newSquid = (Squid) AnnotationHelper.result2Object(map, c);
            return newSquid;
        }
        return null;
    }

    /**
     * 特殊的类型DocExtract
     *
     * @param adapter3
     * @param squid
     * @param squidFlowId
     * @return
     * @throws Exception
     */
    private Squid copyDocExtractSquid(IRelationalDataManager adapter3,
                                      Squid squid, int squidFlowId, Map<Integer, Integer> squidMap,
                                      Map<String, Integer> referenceColumnMap,
                                      Map<Integer, Integer> transMap,
                                      Map<Integer, Integer> referenceTransMap) throws Exception {
        int newSquidId = 0;
        ISquidDao squidDao = new SquidDaoImpl(adapter3);
        ISourceTableDao sourceTableDao = new SourceTableDaoImpl(adapter3);
        Map<String, String> tagColumnMap = new HashMap<>();
        if (squid != null) {
            int squidId = squid.getId();
            DocExtractSquid newSquid = squidDao.getSquidForCond(squidId,
                    DocExtractSquid.class);
            int x = squid.getLocation_x() + getOffset_x();
            int y = squid.getLocation_y() + getOffset_y();
            newSquid.setLocation_x(x);
            newSquid.setLocation_y(y);
            newSquid.setId(0);// 清空id
            newSquid.setName(this.getSquidName(adapter3, squid.getName(),
                    squidFlowId));
            newSquid.setSquidflow_id(squidFlowId);// 给squidFlowId复制
            newSquid.setKey(StringUtils.generateGUID());
            if (newSquid != null && newSquid.getDestination_squid_id() > 0) {
                if (squidMap.containsKey(newSquid.getDestination_squid_id())) {
                    int newId = squidMap
                            .get(newSquid.getDestination_squid_id());
                    newSquid.setDestination_squid_id(newId);
                }
            }
            if (newSquid != null && newSquid.getSource_table_id() > 0) {
                DBSourceTable dbSourceTable = sourceTableDao.getObjectById(
                        newSquid.getSource_table_id(), DBSourceTable.class);
                if (dbSourceTable != null) {
                    String table_name = dbSourceTable.getTable_name();
                    int source_squid_id = dbSourceTable.getSource_squid_id();
                    if (squidMap.containsKey(source_squid_id)) {
                        int newId = squidMap.get(source_squid_id);
                        DBSourceTable newDbSourceTable = sourceTableDao
                                .getDBSourceTableByParams(newId, table_name);
                        if (newDbSourceTable != null) {
                            newSquid.setSource_table_id(newDbSourceTable
                                    .getId());
                        }
                    }
                }
            }
            newSquid.setId(squidDao.insert2(newSquid));
            if (newSquid.getId() > 0) {
                newSquidId = newSquid.getId();
                if (newSquid != null) {
                    copyDataSquidForPart(adapter3, newSquidId, squidId,
                            referenceColumnMap, transMap, referenceTransMap, newSquid);
                }
            }
            return newSquid;
        }
        return null;
    }

    /**
     * 复制KafkaExtract
     *
     * @param adapter3
     * @param squid
     * @param squidFlowId
     * @param squidMap
     * @param referenceColumnMap
     * @param transMap
     * @param referenceTransMap
     * @return
     * @throws Exception
     */
    private Squid copyKafkaExtractSquid(IRelationalDataManager adapter3,
                                        Squid squid, int squidFlowId, Map<Integer, Integer> squidMap,
                                        Map<String, Integer> referenceColumnMap,
                                        Map<Integer, Integer> transMap,
                                        Map<Integer, Integer> referenceTransMap) throws Exception {
        int newSquidId = 0;
        ISquidDao squidDao = new SquidDaoImpl(adapter3);
        ISourceTableDao sourceTableDao = new SourceTableDaoImpl(adapter3);
        if (squid != null) {
            int squidId = squid.getId();
            KafkaExtractSquid newSquid = squidDao.getSquidForCond(squidId,
                    KafkaExtractSquid.class);
            int x = squid.getLocation_x() + getOffset_x();
            int y = squid.getLocation_y() + getOffset_y();
            newSquid.setLocation_x(x);
            newSquid.setLocation_y(y);
            newSquid.setId(0);// 清空id
            newSquid.setName(this.getSquidName(adapter3, squid.getName(),
                    squidFlowId));
            newSquid.setSquidflow_id(squidFlowId);// 给squidFlowId复制
            newSquid.setKey(StringUtils.generateGUID());
            if (newSquid != null && newSquid.getDestination_squid_id() > 0) {
                if (squidMap.containsKey(newSquid.getDestination_squid_id())) {
                    int newId = squidMap
                            .get(newSquid.getDestination_squid_id());
                    newSquid.setDestination_squid_id(newId);
                }
            }
            if (newSquid != null && newSquid.getSource_table_id() > 0) {
                DBSourceTable dbSourceTable = sourceTableDao.getObjectById(
                        newSquid.getSource_table_id(), DBSourceTable.class);
                if (dbSourceTable != null) {
                    String table_name = dbSourceTable.getTable_name();
                    int source_squid_id = dbSourceTable.getSource_squid_id();
                    if (squidMap.containsKey(source_squid_id)) {
                        int newId = squidMap.get(source_squid_id);
                        DBSourceTable newDbSourceTable = sourceTableDao
                                .getDBSourceTableByParams(newId, table_name);
                        if (newDbSourceTable != null) {
                            newSquid.setSource_table_id(newDbSourceTable
                                    .getId());
                        }
                    }
                }
            }
            newSquid.setId(squidDao.insert2(newSquid));
            if (newSquid.getId() > 0) {
                newSquidId = newSquid.getId();
                if (newSquid != null) {
                    copyDataSquidForPart(adapter3, newSquidId, squidId,
                            referenceColumnMap, transMap, referenceTransMap, newSquid);
                }
            }
            return newSquid;
        }
        return null;
    }

    /**
     * 复制HBaseExtract
     *
     * @param adapter3
     * @param squid
     * @param squidFlowId
     * @param squidMap
     * @param referenceColumnMap
     * @param transMap
     * @param referenceTransMap
     * @return
     * @throws Exception
     */
    private Squid copyHBaseExtractSquid(IRelationalDataManager adapter3,
                                        Squid squid, int squidFlowId, Map<Integer, Integer> squidMap,
                                        Map<String, Integer> referenceColumnMap,
                                        Map<Integer, Integer> transMap,
                                        Map<Integer, Integer> referenceTransMap) throws Exception {
        int newSquidId = 0;
        ISquidDao squidDao = new SquidDaoImpl(adapter3);
        ISourceTableDao sourceTableDao = new SourceTableDaoImpl(adapter3);
        if (squid != null) {
            int squidId = squid.getId();
            HBaseExtractSquid newSquid = squidDao.getSquidForCond(squidId,
                    HBaseExtractSquid.class);
            int x = squid.getLocation_x() + getOffset_x();
            int y = squid.getLocation_y() + getOffset_y();
            newSquid.setLocation_x(x);
            newSquid.setLocation_y(y);
            newSquid.setId(0);// 清空id
            newSquid.setName(this.getSquidName(adapter3, squid.getName(),
                    squidFlowId));
            newSquid.setSquidflow_id(squidFlowId);// 给squidFlowId复制
            newSquid.setKey(StringUtils.generateGUID());
            if (newSquid != null && newSquid.getDestination_squid_id() > 0) {
                if (squidMap.containsKey(newSquid.getDestination_squid_id())) {
                    int newId = squidMap
                            .get(newSquid.getDestination_squid_id());
                    newSquid.setDestination_squid_id(newId);
                }
            }
            if (newSquid != null && newSquid.getSource_table_id() > 0) {
                DBSourceTable dbSourceTable = sourceTableDao.getObjectById(
                        newSquid.getSource_table_id(), DBSourceTable.class);
                if (dbSourceTable != null) {
                    String table_name = dbSourceTable.getTable_name();
                    int source_squid_id = dbSourceTable.getSource_squid_id();
                    if (squidMap.containsKey(source_squid_id)) {
                        int newId = squidMap.get(source_squid_id);
                        DBSourceTable newDbSourceTable = sourceTableDao
                                .getDBSourceTableByParams(newId, table_name);
                        if (newDbSourceTable != null) {
                            newSquid.setSource_table_id(newDbSourceTable
                                    .getId());
                        }
                    }
                }
            }
            newSquid.setId(squidDao.insert2(newSquid));
            if (newSquid.getId() > 0) {
                newSquidId = newSquid.getId();
                if (newSquid != null) {
                    copyDataSquidForPart(adapter3, newSquidId, squidId,
                            referenceColumnMap, transMap, referenceTransMap, newSquid);
                }
            }
            return newSquid;
        }
        return null;
    }

    /**
     * DataMining
     *
     * @param adapter3
     * @param squid
     * @param squidFlowId
     * @return
     * @throws Exception
     */
    private Squid copyDataMiningSquid(IRelationalDataManager adapter3,
                                      Squid squid, int squidFlowId,
                                      Map<String, Integer> referenceColumnMap,
                                      Map<Integer, Integer> transMap,
                                      Map<Integer, Integer> referenceTransMap, Integer repositoryId)
            throws Exception {
        int newSquidId = 0;
        if (squid != null) {
            int squidId = squid.getId();
            ISquidDao squidDao = new SquidDaoImpl(adapter3);
            DataMiningSquid newSquid = squidDao.getSquidForCond(squidId,
                    DataMiningSquid.class);
            int x = squid.getLocation_x() + getOffset_x();
            int y = squid.getLocation_y() + getOffset_y();
            newSquid.setLocation_x(x);
            newSquid.setLocation_y(y);
            newSquid.setId(0);// 清空id
            newSquid.setName(this.getSquidName(adapter3, squid.getName(),
                    squidFlowId));
            newSquid.setSquidflow_id(squidFlowId);// 给squidFlowId复制
            newSquid.setKey(StringUtils.generateGUID());
            newSquid.setId(squidDao.insert2(newSquid));
            if (newSquid.getId() > 0) {
                newSquidId = newSquid.getId();
                if (newSquid != null) {
                    copyDataSquidForPart(adapter3, newSquidId, squidId,
                            referenceColumnMap, transMap, referenceTransMap, newSquid);
                    // 创建创建模型落地表
                    /*
                     * String tableName =
					 * HbaseUtil.genTrainModelTableName(repositoryId,
					 * squidFlowId, newSquidId); DBConnectionInfo dbs = new
					 * DBConnectionInfo(); dbs.setHost("e231");
					 * dbs.setPort(2181); dbs.setDbType(DataBaseType.HBASE_PHOENIX);
					 * Connection con =
					 * AdapterDataSourceManager.createConnection(dbs);
					 * HbaseUtil.createModelTable(con, tableName);
					 * if(!con.isClosed()){ con.close(); }
					 */
                }
            }
            return newSquid;
        }
        return null;
    }

    /**
     * 复制userdefined相关信息
     *
     * @param adapter3
     * @param squid
     * @param squidFlowId
     * @param referenceColumnMap
     * @param transMap
     * @param referenceTransMap
     * @param repositoryId
     * @return
     */
    public Squid copyUserDefinedSquid(IRelationalDataManager adapter3,
                                      Squid squid, int squidFlowId,
                                      Map<String, Integer> referenceColumnMap,
                                      Map<Integer, Integer> transMap,
                                      Map<Integer, Integer> referenceTransMap, Integer repositoryId) throws Exception {
        int newSquidId = 0;
        if (squid != null) {
            int squidId = squid.getId();
            ISquidDao squidDao = new SquidDaoImpl(adapter3);
            Squid newSquid = (Squid) squidDao.getSquidForCond(squidId,
                    SquidTypeEnum.classOfValue(squid.getSquid_type()));
            int x = squid.getLocation_x() + getOffset_x();
            int y = squid.getLocation_y() + getOffset_y();
            if (squid.getSquid_type() == SquidTypeEnum.USERDEFINED.value()) {
                newSquid = (UserDefinedSquid) newSquid;
            } else if (squid.getSquid_type() == SquidTypeEnum.STATISTICS.value()) {
                newSquid = (StatisticsSquid) newSquid;
            }
            newSquid.setLocation_x(x);
            newSquid.setLocation_y(y);
            newSquid.setId(0);// 清空id
            newSquid.setName(this.getSquidName(adapter3, squid.getName(),
                    squidFlowId));
            newSquid.setSquidflow_id(squidFlowId);// 给squidFlowId复制
            newSquid.setKey(StringUtils.generateGUID());
            newSquid.setId(squidDao.insert2(newSquid));
            if (newSquid.getId() > 0) {
                newSquidId = newSquid.getId();
                if (newSquid != null) {
                    /*copyDataSquidForPart(adapter3, newSquidId, squidId,
                            referenceColumnMap, transMap, referenceTransMap, newSquid);*/
                    //复制column和parameterMap
                    copyUserDefinedDataForPart(adapter3, newSquidId, squidId, squid.getSquid_type(),
                            referenceColumnMap, transMap, referenceTransMap);
                }
            }
            return newSquid;
        }
        return null;
    }

    /**
     * 根据变量的id
     *
     * @return
     * @throws SQLException
     */
    public DSVariable copyVariableById(IRelationalDataManager adapter3,
                                       DSVariable oldVariable, int newSquidId, int newSquidFlowId,
                                       int newProjectId) throws SQLException {
        if (oldVariable != null && oldVariable.getId() > 0) {
            IVariableDao variableDao = new VariableDaoImpl(adapter3);
            return variableDao.copyVariableById(oldVariable, newSquidId,
                    newSquidFlowId, newProjectId);
        }
        return oldVariable;
    }

    /**
     * 部分复制：其中包括（datasquid、column、VTrans、Trans、TransLink、TransInput等等）
     *
     * @param adapter3
     * @param newSquidId
     * @param squidId
     * @param referenceColumnMap 记录变更column的集合，用于添加ReferenceColumn
     * @param transMap           记录变更Transformation的集合
     * @throws SQLException
     * @throws Exception
     */
    private void copyDataSquidForPart(IRelationalDataManager adapter3,
                                      int newSquidId, int squidId,
                                      Map<String, Integer> referenceColumnMap,
                                      Map<Integer, Integer> transMap,
                                      Map<Integer, Integer> referenceTransMap, DataSquid newSquid) throws SQLException,
            Exception {
        ITransformationInputsDao transInputsDao = new TransformationInputsDaoImpl(
                adapter3);
        IColumnDao columnDao = new ColumnDaoImpl(adapter3);
        ITransformationDao transDao = new TransformationDaoImpl(adapter3);
        ITransformationLinkDao transLinkDao = new TransformationLinkDaoImpl(
                adapter3);
        ISquidIndexesDao squidIndexesDao = new SquidIndexesDaoImpl(adapter3);
        ISquidLinkDao squidLinkDao = new SquidLinkDaoImpl(adapter3);

        TransformationService service = new TransformationService(TokenUtil.getToken());
        Map<String, Object> nameColumn = new HashMap<String, Object>();
        Map<Integer, Integer> transInputMaps = new HashMap<Integer, Integer>();
        // key为column集合：记录column和Transformation的关系集合
        // value为trans集合：记录当前上游column集合，在集合中才能添加新的Column。
        Map<Integer, Integer> columnForTransMap = new HashMap<Integer, Integer>();

        // 获取需要添加的TransformationLink集合，并且记录变更的trans的Id
        List<TransformationLink> links = new ArrayList<TransformationLink>();
        //
        List<Map<String, Object>> objList = columnDao
                .getColumnTransBySquidId(squidId);
        if (objList != null) {
            for (Map<String, Object> map : objList) {
                int fromTransId = Integer.parseInt(map.get("ID") + "");
                int columnId = Integer.parseInt(map.get("COLUMN_ID") + "");
                this.getTransLinks(adapter3, fromTransId, links);
                columnForTransMap.put(columnId, fromTransId);
            }
        }

        // 根据squid_id获取到源Column集合，添加新的column及Transformation
        List<Column> columns = columnDao.getColumnListBySquidId(squidId);
        ISquidDao squidDao = new SquidDaoImpl(adapter3);
        List<SquidLink> squidLinks = squidLinkDao.getFormSquidLinkBySquidId(squidId);
        int fromSquidId = 0;
        if (squidLinks != null && squidLinks.size() > 0) {
            fromSquidId = squidLinks.get(0).getFrom_squid_id();
        }
        int squidType = squidDao.getSquidTypeById(squidId);
        if (columns != null && columns.size() > 0) {
            for (int i = 0; i < columns.size(); i++) {
                int oldId = columns.get(i).getId();
				/*if(squidType==SquidTypeEnum.GROUPTAGGING.value()
						&& ConstantsUtil.CN_GROUP_TAG.equals(columns.get(i).getName())){
					Column newColumn = service.initColumn(adapter3,
							columns.get(i), i + 1, newSquidId, nameColumn);
				}*/
                if (columnForTransMap.containsKey(oldId)) {
                    int oldTransId = columnForTransMap.get(oldId);
                    Column newColumn = service.initColumn(adapter3,
                            columns.get(i), i + 1, newSquidId, nameColumn);
                    Transformation newTrans = service.initTransformation(
                            adapter3, newSquidId, newColumn.getId(),
                            TransformationTypeEnum.VIRTUAL.value(),
                            newColumn.getData_type(), 1, i);
                    transMap.put(oldTransId, newTrans.getId());
                    referenceColumnMap.put(squidId + "_" + oldId,
                            newColumn.getId());
                }
                //DataViewSquid单独添加Column
                if (newSquid.getSquid_type() == SquidTypeEnum.DATAVIEW.value() || newSquid.getSquid_type() == SquidTypeEnum.COEFFICIENT.value()) {
                    Column newColumn = service.initColumn(adapter3,
                            columns.get(i), i + 1, newSquidId, nameColumn);
                    referenceColumnMap.put(squidId + "_" + oldId,
                            newColumn.getId());
                }
            }
            //将dataSquid中的sortColumns等替换掉
            if (squidType == SquidTypeEnum.GROUPTAGGING.value()) {
                String groupColumn = newSquid.getGroupColumnIds();
                StringBuffer groupBuffer = new StringBuffer("");
                if (StringUtils.isNotNull(groupColumn)) {
                    String[] groupColumns = groupColumn.split(",");
                    for (int i = 0; i < groupColumns.length; i++) {
                        if (referenceColumnMap.containsKey(fromSquidId + "_" + groupColumns[i])) {
                            groupColumns[i] = referenceColumnMap.get(fromSquidId + "_" + groupColumns[i]) + "";
                        }

                        groupBuffer.append(groupColumns[i]);
                        if (i < groupColumns.length - 1) {
                            groupBuffer.append(",");
                        }
                    }

                }
                String sortColumn = newSquid.getSortingColumnIds();
                StringBuffer sortBuffer = new StringBuffer("");
                if (StringUtils.isNotNull(sortColumn)) {
                    String[] sortColumns = sortColumn.split(",");
                    for (int i = 0; i < sortColumns.length; i++) {
                        if (referenceColumnMap.containsKey(fromSquidId + "_" + sortColumns[i])) {
                            sortColumns[i] = referenceColumnMap.get(fromSquidId + "_" + sortColumns[i]) + "";
                        }
                        sortBuffer.append(sortColumns[i]);
                        if (i < sortColumns.length - 1) {
                            sortBuffer.append(",");
                        }
                    }
                }
                String tagColumn = newSquid.getTaggingColumnIds();
                StringBuffer tagBuffer = new StringBuffer("");
                if (StringUtils.isNotNull(tagColumn)) {
                    String[] tagColumns = tagColumn.split(",");
                    for (int i = 0; i < tagColumns.length; i++) {
                        if (referenceColumnMap.containsKey(fromSquidId + "_" + tagColumns[i])) {
                            tagColumns[i] = referenceColumnMap.get(fromSquidId + "_" + tagColumns[i]) + "";
                        }
                        tagBuffer.append(tagColumns[i]);
                        if (i < tagColumns.length - 1) {
                            tagBuffer.append(",");
                        }
                    }
                }
                newSquid.setGroupColumnIds(groupBuffer.toString());
                newSquid.setTaggingColumnIds(tagBuffer.toString());
                newSquid.setSortingColumnIds(sortBuffer.toString());
                adapter3.update2(newSquid);
            }

            //更新复制后的pivotSquid的groupByColumnIds,pivotColumnId,valumnColumnId
            if(squidType == SquidTypeEnum.PIVOTSQUID.value()){
                PivotSquid oldPivotSquid = squidDao.getSquidForCond(squidId,PivotSquid.class);
                List<SquidLink> squidLinks1 = squidLinkDao.getSquidLinkListByToSquid(squidId);
                if(squidLinks1.size() > 0){
                    SquidLink squidLink = squidLinks1.get(0);
                    Integer oldfromSquidId = squidLink.getFrom_squid_id();
                    Integer oldPivotColumnId = oldPivotSquid.getPivotColumnId();
                    Integer oldValueColumnId = oldPivotSquid.getValueColumnId();
                    String oldGroupByColumnIds = oldPivotSquid.getGroupByColumnIds();
                    Map<String,String> param = new Hashtable<>();
                    param.put("id",newSquidId+"");
                    PivotSquid newPivot = adapter3.query2Object2(true,param,PivotSquid.class);
                    if(referenceColumnMap.containsKey(oldfromSquidId+"_"+oldPivotColumnId)){
                        newPivot.setPivotColumnId(referenceColumnMap.get(oldfromSquidId+"_"+oldPivotColumnId));
                    }
                    if(referenceColumnMap.containsKey(oldfromSquidId+"_"+oldValueColumnId)){
                        newPivot.setValueColumnId(referenceColumnMap.get(oldfromSquidId+"_"+oldValueColumnId));
                    }
                    StringBuffer groupByBuffer = new StringBuffer("");
                    if (StringUtils.isNotNull(oldGroupByColumnIds)) {
                        String[] gruopByColumns = oldGroupByColumnIds.split(",");
                        for (int i = 0; i < gruopByColumns.length; i++) {
                            if (referenceColumnMap.containsKey(oldfromSquidId + "_" + gruopByColumns[i])) {
                                gruopByColumns[i] = referenceColumnMap.get(oldfromSquidId + "_" + gruopByColumns[i]) + "";
                            }
                            groupByBuffer.append(gruopByColumns[i]);
                            if (i < gruopByColumns.length - 1) {
                                groupByBuffer.append(",");
                            }
                        }
                    }
                    newPivot.setGroupByColumnIds(groupByBuffer.toString());
                    adapter3.update2(newPivot);
                }
            }

        }

        // 查询indexes集合
        List<SquidIndexes> squidIndexes = squidIndexesDao
                .getSquidIndexesBySquidId(squidId);
        if (squidIndexes != null && squidIndexes.size() > 0) {
            for (SquidIndexes indexes : squidIndexes) {
                indexes.setId(0);
                indexes.setColumn_id1(referenceColumnMap.get(squidId + "_"
                        + indexes.getColumn_id1()) == null ? 0
                        : referenceColumnMap.get(squidId + "_"
                        + indexes.getColumn_id1()));
                indexes.setColumn_id2(referenceColumnMap.get(squidId + "_"
                        + indexes.getColumn_id2()) == null ? 0
                        : referenceColumnMap.get(squidId + "_"
                        + indexes.getColumn_id2()));
                indexes.setColumn_id3(referenceColumnMap.get(squidId + "_"
                        + indexes.getColumn_id3()) == null ? 0
                        : referenceColumnMap.get(squidId + "_"
                        + indexes.getColumn_id3()));
                indexes.setColumn_id4(referenceColumnMap.get(squidId + "_"
                        + indexes.getColumn_id4()) == null ? 0
                        : referenceColumnMap.get(squidId + "_"
                        + indexes.getColumn_id4()));
                indexes.setColumn_id5(referenceColumnMap.get(squidId + "_"
                        + indexes.getColumn_id5()) == null ? 0
                        : referenceColumnMap.get(squidId + "_"
                        + indexes.getColumn_id5()));
                indexes.setColumn_id6(referenceColumnMap.get(squidId + "_"
                        + indexes.getColumn_id6()) == null ? 0
                        : referenceColumnMap.get(squidId + "_"
                        + indexes.getColumn_id6()));
                indexes.setColumn_id7(referenceColumnMap.get(squidId + "_"
                        + indexes.getColumn_id7()) == null ? 0
                        : referenceColumnMap.get(squidId + "_"
                        + indexes.getColumn_id7()));
                indexes.setColumn_id8(referenceColumnMap.get(squidId + "_"
                        + indexes.getColumn_id8()) == null ? 0
                        : referenceColumnMap.get(squidId + "_"
                        + indexes.getColumn_id8()));
                indexes.setColumn_id9(referenceColumnMap.get(squidId + "_"
                        + indexes.getColumn_id9()) == null ? 0
                        : referenceColumnMap.get(squidId + "_"
                        + indexes.getColumn_id9()));
                indexes.setColumn_id10(referenceColumnMap.get(squidId + "_"
                        + indexes.getColumn_id10()) == null ? 0
                        : referenceColumnMap.get(squidId + "_"
                        + indexes.getColumn_id10()));
                indexes.setSquid_id(newSquidId);
                adapter3.insert2(indexes);
            }
        }

        // 添加 第三方参数集合
        IThirdPartyParamsDao paramsDao = new ThirdPartyParamsDaoImpl(adapter3);
        List<ThirdPartyParams> paramsList = paramsDao
                .findThirdPartyParamsByWSEID(squidId);
        if (paramsList != null && paramsList.size() > 0) {
            for (ThirdPartyParams params : paramsList) {
                params.setId(0);
                params.setSquid_id(newSquidId);
                adapter3.insert2(params);
            }
        }

        // 单个 实体 Transformation，上个步骤没有进行创建的
        List<Transformation> transList = transDao
                .getEntityTransBySquidId(squidId);
        if (transList != null && transList.size() > 0) {
            for (Transformation transformation : transList) {
                int transId = transformation.getId();
                if (!transMap.containsKey(transId)) {
                    this.copyTransForInputByParms(adapter3, transformation,
                            newSquidId);
                    if (transformation.getDictionary_squid_id() > 0) {
                        referenceTransMap.put(transformation.getId(),
                                transformation.getDictionary_squid_id());
                    }
                    if (transformation.getModel_squid_id() > 0) {
                        referenceTransMap.put(transformation.getId(),
                                transformation.getModel_squid_id());
                    }
                    transMap.put(transId, transformation.getId());
                    this.getTransLinks(adapter3, transId, links);
                }
            }
        }

        // 根据生成好的新的对象，实现TransformationLink的链接
        if (links.size() > 0) {
            for (TransformationLink link : links) {
                int fromId = link.getFrom_transformation_id();
                Transformation trans = transDao.getObjectById(fromId,
                        Transformation.class);
                if (trans != null) {
                    int newTransId = 0;
                    if (!transMap.containsKey(fromId)
                            && trans.getTranstype() != TransformationTypeEnum.VIRTUAL
                            .value()) {
                        this.copyTransForInputByParms(adapter3, trans,
                                newSquidId);
                        newTransId = trans.getId();
                        transMap.put(fromId, newTransId);
                    } else {
                        newTransId = transMap.get(fromId);
                    }
                    if (newTransId == 0) {
                        continue;
                    }
                    int old_to_id = link.getTo_transformation_id();
                    int new_to_id = transMap.get(old_to_id);
                    link.setId(0);
                    link.setFrom_transformation_id(newTransId);
                    link.setTo_transformation_id(new_to_id);
                    link.setKey(StringUtils.generateGUID());
                    int newLinkId = transLinkDao.insert2(link);
                    link.setId(newLinkId);
                    // service.updTransInputs(adapter3, link, null);
                    transInputMaps.put(old_to_id, new_to_id);
                    // System.out.println(newLinkId);
                }
            }
        }

        // 复制input
        if (transInputMaps != null && transInputMaps.size() > 0) {
            for (Integer oldToTransId : transInputMaps.keySet()) {
                int newToTransId = transInputMaps.get(oldToTransId);
                transInputsDao.copyTransInputByLink(newToTransId, transMap,
                        oldToTransId);
            }
        }
    }

    /**
     * 复制UserDefinedSquid相关信息
     *
     * @param newSquidId
     */
    public void copyUserDefinedDataForPart(IRelationalDataManager adapter3,
                                           int newSquidId, int squidId, int squidType,
                                           Map<String, Integer> referenceColumnMap,
                                           Map<Integer, Integer> transMap,
                                           Map<Integer, Integer> referenceTransMap) throws Exception {
        IColumnDao columnDao = new ColumnDaoImpl(adapter3);
        ISquidLinkDao squidLinkDao = new SquidLinkDaoImpl(adapter3);
        TransformationService service = new TransformationService(TokenUtil.getToken());
        Map<String, Object> nameColumn = new HashMap<String, Object>();
        // 根据squid_id获取到源Column集合，添加新的column
        List<Column> columns = columnDao.getColumnListBySquidId(squidId);
        if (columns != null && columns.size() > 0) {
            for (int i = 0; i < columns.size(); i++) {
                int oldId = columns.get(i).getId();
                Column newColumn = service.initColumn(adapter3,
                        columns.get(i), i + 1, newSquidId, nameColumn);
                referenceColumnMap.put(squidId + "_" + oldId,
                        newColumn.getId());
            }
        }
        //复制parameterMap
        Map<String, Object> paramMap = new HashMap<>();
        paramMap.put("squid_id", squidId);
        if (squidType == SquidTypeEnum.USERDEFINED.value()) {
            List<UserDefinedParameterColumn> parameterColumns = adapter3.query2List2(true, paramMap, UserDefinedParameterColumn.class);
            if (parameterColumns != null) {
                if (squidType == SquidTypeEnum.USERDEFINED.value()) {
                    for (UserDefinedParameterColumn parameterColumn : parameterColumns) {
                        parameterColumn.setSquid_id(newSquidId);
                        adapter3.insert2(parameterColumn);
                    }
                }
            }
        } else if (squidType == SquidTypeEnum.STATISTICS.value()) {
            List<StatisticsParameterColumn> parameterColumns = adapter3.query2List2(true, paramMap, StatisticsParameterColumn.class);
            if (parameterColumns != null) {
                if (squidType == SquidTypeEnum.STATISTICS.value()) {
                    for (StatisticsParameterColumn parameterColumn : parameterColumns) {
                        parameterColumn.setSquid_id(newSquidId);
                        adapter3.insert2(parameterColumn);
                    }
                }
            }
        }

    }

    /**
     * 复制Transformation和Input，表中存在存储信息，只能通过修改对象来进行添加
     *
     * @param adapter3
     * @param oldTrans
     * @param newSquidId
     * @throws Exception
     */
    public void copyTransForInputByParms(IRelationalDataManager adapter3,
                                         Transformation oldTrans, int newSquidId) throws Exception {
        ITransformationInputsDao transInputsDao = new TransformationInputsDaoImpl(
                adapter3);
        if (oldTrans != null) {
			/* 获取老Inputs准备复制 */
            List<TransformationInputs> transInputs = transInputsDao.getTransInputListByTransId(oldTrans.getId());
            oldTrans.setSquid_id(newSquidId);
            oldTrans.setKey(StringUtils.generateGUID());
            oldTrans.setId(0);
            oldTrans.setModel_version(-1);
            oldTrans.setId(adapter3.insert2(oldTrans));
            // 添加的Transformation根据原有的信息，进行初始化，下次连线时，在更改对应的SourceTransId
            int dataType = 0;
            if (oldTrans.getTranstype() == TransformationTypeEnum.VIRTUAL
                    .value()) {
                dataType = oldTrans.getOutput_data_type();
            }
            List<TransformationInputs> newInputs = transInputsDao.initTransInputs1(oldTrans, dataType);
            // 判断transInputs是否为空,(常量trans的transInputs为空)  为空则跳过
            if (transInputs != null && transInputs.size() > 0) {
                //遍历新的inputs中的数据,以transInputs中数据与之匹配,获取inputs值
                for (TransformationInputs item : newInputs) {
                    int index = newInputs.indexOf(item);
                    TransformationInputs newInput = transInputs.get(index);
                    newInput.setInput_value(item.getInput_value());
                    transInputsDao.update(newInput);
                }
            }
            oldTrans.setInputs(newInputs);
        }
    }

    /**
     * 根据squidLink的信息 进行copy右侧功能：如join、group、ReferenceColumn等等
     *
     * @param adapter3
     * @param squidId
     * @param fromSquidId
     * @param newSquidId
     * @param squidMap
     * @param referenceColumnMap
     * @param transMap
     * @throws Exception
     */
    private void copyDataSquidForLink(IRelationalDataManager adapter3,
                                      int squidId, int fromSquidId, int newSquidId,
                                      Map<Integer, Integer> squidMap,
                                      Map<String, Integer> referenceColumnMap,
                                      Map<Integer, Integer> transMap) throws Exception {

        Map<Integer, Integer> transInputMaps = new HashMap<Integer, Integer>();
        TransformationService service = new TransformationService(TokenUtil.getToken());
        int newFromSquidId = squidMap.get(fromSquidId);
        // 规则匹配的squidId, 在ExceptionSquid中会不同
        int tempSquidId = fromSquidId;

        ISquidJoinDao squidJoinDao = new SquidJoinDaoImpl(adapter3);
        ISquidDao squidDao = new SquidDaoImpl(adapter3);
        IReferenceColumnDao refColumnDao = new ReferenceColumnDaoImpl(adapter3);
        ISourceColumnDao sourceColumnDao = new SourceColumnDaoImpl(adapter3);
        IColumnDao columnDao = new ColumnDaoImpl(adapter3);
        ITransformationDao transDao = new TransformationDaoImpl(adapter3);
        ITransformationLinkDao transLinkDao = new TransformationLinkDaoImpl(
                adapter3);
        ITransformationInputsDao transInputsDao = new TransformationInputsDaoImpl(
                adapter3);

        SquidJoin join = squidJoinDao
                .getSquidJoinByParams(fromSquidId, squidId);
        if (join != null) {
            join.setTarget_squid_id(newFromSquidId);
            join.setJoined_squid_id(newSquidId);
            join.setKey(StringUtils.generateGUID());
            squidJoinDao.insert2(join);
        }
        List<ReferenceColumnGroup> groupList = refColumnDao.getRefColumnGroupListBySquidId(
                squidId);
        int oldGroupId = 0;
        if (groupList != null && groupList.size() > 0) {
            for (ReferenceColumnGroup group : groupList) {
                oldGroupId = group.getId();
                group.setId(0);
                group.setReference_squid_id(newSquidId);
                group.setKey(StringUtils.generateGUID());
                int newGroupId = adapter3.insert2(group);
                group.setId(newGroupId);

                // 如果是DBSource link EXTRACT
                // 首先添加新的column映射关系，在创建新的ReferenceColumn和Transformation的时候用到
                Squid fromSquid = squidDao.getObjectById(squidId, Squid.class);
                String tableName = sourceColumnDao.getTableNameBySquidId(squidId,
                        fromSquid.getSquid_type());
                if (!ValidateUtils.isEmpty(tableName)) {
                    // 根据oldSquidId和复制之后生成的id，获取sourceColumn的依赖
                    List<Map<String, Object>> sourceColumns = sourceColumnDao
                            .getCopyedSourceParms(fromSquidId, newFromSquidId,
                                    tableName);
                    if (sourceColumns != null && sourceColumns.size() > 0) {
                        for (int tmpIndex = 0; tmpIndex < sourceColumns.size(); tmpIndex++) {
                            // 目标ExtractSquid引用列（变换面板右边，引用db_source_squid）
                            Map<String, Object> columnMap = sourceColumns
                                    .get(tmpIndex);
                            String oldColumnId = columnMap.get("SOURCEID") + "";
                            int newColumnId = Integer.parseInt(columnMap.get("ID")
                                    + "");
                            referenceColumnMap.put(fromSquidId + "_" + oldColumnId,
                                    newColumnId);
                        }
                    }
                }

                //获取oldGroupId下的ReferenceColumn列表
                List<ReferenceColumn> referenceColumns = refColumnDao
                        .getRefColumnListByGroupId(oldGroupId);
                if (referenceColumns != null && referenceColumns.size() > 0) {
                    Squid newToSquid = squidDao.getObjectById(newSquidId,
                            Squid.class);
                    if (newToSquid != null
                            && (newToSquid.getSquid_type() == SquidTypeEnum.EXCEPTION
                            .value())) {
                        Column oldColumn = columnDao.getObjectById(referenceColumns.get(0).getColumn_id(), Column.class);
                        if (oldColumn == null) {
						/*
							如果oldColumn为空，该RefColumn的ColumnId是通过上游的ColumnId赋值而来，
							就是因为当前RefColumn的上游是SourceColumn，所以查询DS_Column表是无法获取数据的。
						 */
                            SourceColumn sourceColumn = columnDao.getObjectById(Math.abs(referenceColumns.get(0).getColumn_id()), SourceColumn.class);
                            DBSourceTable tempDbSourceTable = columnDao.getObjectById(sourceColumn.getSource_table_id(), DBSourceTable.class);
                            tempSquidId = tempDbSourceTable.getSource_squid_id();
                        } else {
                            tempSquidId = oldColumn.getSquid_id();
                        }
                    }
                    for (ReferenceColumn referenceColumn : referenceColumns) {
                        int oldColumnId = referenceColumn.getColumn_id();
                        // System.out.println(squidId+"_"+oldColumnId);
                        int newColumnId = 0;
                        // 只有完全符合referenceColumn的下游列  才能进行复制
                        if (referenceColumnMap.containsKey(tempSquidId + "_"
                                + oldColumnId)) {
                            newColumnId = referenceColumnMap.get(tempSquidId + "_"
                                    + oldColumnId);
                            referenceColumn.setColumn_id(newColumnId);
                            referenceColumn.setReference_squid_id(newSquidId);
                            referenceColumn.setHost_squid_id(newFromSquidId);
                            referenceColumn.setGroup_id(newGroupId);
                            refColumnDao.insert2(referenceColumn);
                        } else {
                            newColumnId = -oldColumnId;
                        }

                    }
                }


                // 根据旧的GroupId和squid，获取所有的下游squid的虚拟Transformation
                List<Transformation> trans = transDao.getVTransBySquidId(oldGroupId,
                        squidId);
                if (trans != null && trans.size() > 0) {
                    for (Transformation transformation : trans) {
                        int transId = transformation.getId();
                        int newFromTransId = 0;
                        int oldColumnId = transformation.getColumn_id();
                        int newColumnId = 0;
                        //判断stageSquid中上游表在stage Group中的数据  将完全符合的trans进行复制
                        if (referenceColumnMap != null
                                && referenceColumnMap.containsKey(tempSquidId + "_"
                                + oldColumnId)) {
                            newColumnId = referenceColumnMap.get(tempSquidId + "_"
                                    + oldColumnId);
                            Transformation newTrans = service.initTransformation(
                                    adapter3, newSquidId, newColumnId,
                                    transformation.getTranstype(),
                                    transformation.getOutput_data_type(), 1, 0);
                            newFromTransId = newTrans.getId();
                            transMap.put(transId, newFromTransId);
                        } else {
                            newColumnId = -oldColumnId;
                        }
                        List<TransformationLink> links = transLinkDao
                                .getTransLinkByFromTrans(transId);
                        if (links != null && links.size() > 0 && newFromTransId > 0) {
                            for (TransformationLink link : links) {
                                if (transMap
                                        .containsKey(link.getTo_transformation_id())) {
                                    int oldToTrans = link.getTo_transformation_id();
                                    int newToTrans = transMap.get(oldToTrans);
                                    link.setTo_transformation_id(newToTrans);
                                    link.setFrom_transformation_id(newFromTransId);
                                    link.setKey(StringUtils.generateGUID());
                                    transLinkDao.insert2(link);
                                    // service.updTransInputs(adapter3, link, null);
                                    transInputMaps.put(oldToTrans, newToTrans);
                                }
                            }
                        }
                    }
                }
            }
            // 复制input
            if (transInputMaps != null && transInputMaps.size() > 0) {
                for (Integer oldToTransId : transInputMaps.keySet()) {
                    int newToTransId = transInputMaps.get(oldToTransId);
                    transInputsDao.copyTransInputByLink(newToTransId, transMap,
                            oldToTransId);
                }
            }
        }
    }

    /**
     * 设置重复名称后自动创建序号
     *
     * @param adapter3
     * @param squidName
     * @param squidFlowId
     * @return
     */
    private synchronized String getSquidName(IRelationalDataManager adapter3,
                                             String squidName, int squidFlowId) {
        Map<String, Object> params = new HashMap<String, Object>();
        int cnt = 1;
        String tempName = squidName;
        while (true) {
            params.put("name", tempName);
            params.put("squid_flow_id", squidFlowId);
            Squid squid = adapter3.query2Object(true, params, Squid.class);
            if (squid == null) {
                return tempName;
            } else {
                if (cnt == 1) {
                    tempName = squidName + "_副本";
                } else {
                    tempName = squidName + "_副本" + cnt + "";
                }
                cnt++;
            }
        }
    }

    /**
     * 获取左侧 column对应的 transformation的 连线， 从下往上递归查询
     *
     * @param adapter3
     * @return
     * @throws SQLException
     */
    public void getTransLinks(IRelationalDataManager adapter3, int toTransId,
                              List<TransformationLink> links) throws SQLException {
        ITransformationLinkDao transLinkDao = new TransformationLinkDaoImpl(
                adapter3);
        List<TransformationLink> transLinkList = transLinkDao
                .getTransLinkByToTransId(toTransId);

        //判断是否是死循环，如果tranformationLink是一个循环，则会造成程序无限递归，最终导致
        //栈内存溢出(至少要有两个id相同，才能产生死循环) 1.79->78 78->77 77->79 2.79->78 78-77 77-78
        if (links.size() > 1) {
            //相同的次数
            int i = 0;
            for (TransformationLink translink : links) {
                for (TransformationLink translink2 : links) {
                    if (translink.getFrom_transformation_id() == translink2.getTo_transformation_id()) {
                        i++;
                        break;
                    }
                }
            }
            if (i == links.size()) {
                return;
            }
        }

        if (transLinkList != null && transLinkList.size() > 0) {
            for (TransformationLink transLink : transLinkList) {
                int tempId = transLink.getFrom_transformation_id();
                if (!links.contains(transLink)) {
                    links.add(transLink);
                }
                getTransLinks(adapter3, tempId, links);
            }
        }
        transLinkList.clear();
        transLinkList = null;
    }

    /**
     * 推送数据
     *
     * @param squidIds
     * @param squid_flow_id
     * @throws Exception
     */
    public void pushCopySquidData(List<Integer> squidIds, int squid_flow_id)
            throws Exception {
        ReturnValue out = new ReturnValue();
        // params.put("id", squid_flow_id);
        DataAdapterFactory adapterFactory = DataAdapterFactory.newInstance();
        IRelationalDataManager adapter3 = DataAdapterFactory.getDefaultDataManager();
        adapter3.openSession();
        String ids = JsonUtil.toJSONString(squidIds);
        if (!ValidateUtils.isEmpty(ids)) {
            ids = ids.substring(1);
            ids = ids.substring(0, ids.length() - 1);
        }
        ISquidFlowDao squidFlowDao = new SquidFlowDaoImpl(adapter3);
        List<Map<String, Object>> typeObj = squidFlowDao
                .getSquidTypeBySquidids(squid_flow_id, ids);
        if (typeObj != null && typeObj.size() > 0) {
            SquidFlowServicesub service = new SquidFlowServicesub(TokenUtil.getToken());
            service.pushSquidData(out, typeObj, 0, squid_flow_id, TokenUtil.getKey(),
                    adapter3, "1011", "0100", ids/*,null*/);
        }
        adapter3.closeSession();
    }

    /**
     * 推送数据（复制squid集合后）
     *
     * @param squidIds
     * @param squid_flow_id
     * @throws EnumException
     * @throws Exception
     */
    public void pushCopySquidLinks(List<Integer> squidIds, int squid_flow_id)
            throws EnumException, Exception {
        // params.put("id", squid_flow_id);
        //DataAdapterFactory adapterFactory = DataAdapterFactory.newInstance();
        IRelationalDataManager adapter3 = DataAdapterFactory.getDefaultDataManager();
        adapter3.openSession();
        String ids = JsonUtil.toJSONString(squidIds);
        if (!ValidateUtils.isEmpty(ids)) {
            ids = ids.substring(1);
            ids = ids.substring(0, ids.length() - 1);
        }
        Map<String, Object> map = new HashMap<String, Object>();

        // 查询变量集合
        IVariableDao variableDao = new VariableDaoImpl(adapter3);
        List<DSVariable> variables = variableDao.getDSVariableByScope(1,
                squid_flow_id, ids);
        if (variables == null) {
            variables = new ArrayList<DSVariable>();
        }
        map.put("Variables", variables);

        ISquidLinkDao squidLinkDao = new SquidLinkDaoImpl(adapter3);
        List<SquidLink> squidLinks = squidLinkDao.getSquidLinkBySquidIds(
                squid_flow_id, ids);
        if (squidLinks != null && squidLinks.size() > 0) {
            System.out.println(squidLinks.size());
            map.put("SquidLinks", squidLinks);
            PushMessagePacket.pushMap(map, DSObjectType.SQUIDLINK, "1011",
                    "0100", TokenUtil.getKey(), TokenUtil.getToken());
        }
        adapter3.closeSession();
    }

    /**
     * 创建落地squid数据表
     *
     * @param info
     * @param out
     */
    public Map<String, Object> createPersistTableByIds(String info,
                                                       ReturnValue out) {
        DataAdapterFactory adapterFactory = null;
        IRelationalDataManager adapter3 = null;
        Map<String, Object> output = new HashMap<String, Object>();
        List<Integer> list = new ArrayList<Integer>();
        try {
            Map<String, Object> inParmsMap = JsonUtil.toHashMap(info);
            List<Integer> squidIds = JsonUtil.toGsonList(
                    inParmsMap.get("SquidIds") + "", Integer.class);
            int repositoryId = Integer.parseInt(inParmsMap.get("RepositoryId")
                    + "");
            if (squidIds != null && squidIds.size() > 0) {
                adapter3 = DataAdapterFactory.getDefaultDataManager();
                adapter3.openSession();
                String sql = "";
                boolean isExists = false;
                for (Integer squidId : squidIds) {
                    sql = "select * from ds_squid where id=" + squidId;
                    Squid squid = adapter3.query2Object(true, sql, null,
                            Squid.class);
                    if (squid == null) {
                        continue;
                    }
                    Class c = SquidTypeEnum.classOfValue(squid.getSquid_type());
                    boolean is_persisted = false;
                    int destination_squid_id = 0;
                    String tableName = "";
                    String persist_sql = "";
                    DataSquid currentSquid = null;
                    if (c == DataSquid.class
                            || c == WebserviceExtractSquid.class
                            || c == HttpExtractSquid.class
                            || c == StageSquid.class
                            || c == ExceptionSquid.class
                            || c == StreamStageSquid.class) {
                        sql = "select * from ds_squid ds where ds.id=" + squidId;
                        DataSquid dataSquid = adapter3.query2Object(true, sql,
                                null, DataSquid.class);
                        if (dataSquid != null) {
                            is_persisted = dataSquid.isIs_persisted();
                            destination_squid_id = dataSquid.getDestination_squid_id();
                            tableName = dataSquid.getTable_name();
                            persist_sql = dataSquid.getPersist_sql();
                            currentSquid = dataSquid;
                        }
                    } else if (c == DataMiningSquid.class) {
                        DBConnectionInfo dbs = new DBConnectionInfo();
                        dbs.setHost(SysConf.getValue("hbase.host"));
                        dbs.setPort(Integer.parseInt(SysConf
                                .getValue("hbase.port")));
                        dbs.setDbType(DataBaseType.MYSQL);
                        dbs.setDbName(SysConf.getValue("hbase.dbname"));
                        dbs.setPassword(SysConf.getValue("hbase.password"));
                        dbs.setPort(Integer.parseInt(SysConf.getValue("hbase.port")));
                        dbs.setUserName(SysConf.getValue("hbase.username"));
                        tableName = DataMiningUtil.genTrainModelTableName(
                                repositoryId, squid.getSquidflow_id(), squidId);
                        INewDBAdapter adaoterSource = AdapterDataSourceManager
                                .getAdapter(dbs);
                        createSquidTable(out, adapter3, list, squidId,
                                tableName, adaoterSource, dbs.getDbType(), null);
                        isExists = true;
                        continue;
                    } else if (c == DocExtractSquid.class) {
                        sql = "select * from ds_squid where id=" + squidId;
                        DocExtractSquid docSquid = adapter3.query2Object(true,
                                sql, null, DocExtractSquid.class);
                        if (docSquid != null) {
                            is_persisted = docSquid.isIs_persisted();
                            destination_squid_id = docSquid.getDestination_squid_id();
                            tableName = docSquid.getTable_name();
                            persist_sql = docSquid.getPersist_sql();
                            currentSquid = docSquid;
                        }
                    } else if (c == KafkaExtractSquid.class) {
                        //为Kafka Extract SquidModelBase 落地，因为对应的表不同，所以需要重新写Sql语句
                        sql = "select * from DS_SQUID where id = " + squidId;
                        KafkaExtractSquid kafkaExtractSquid = adapter3.query2Object(true, sql, null, KafkaExtractSquid.class);
                        if (kafkaExtractSquid != null) {
                            is_persisted = kafkaExtractSquid.isIs_persisted();
                            tableName = kafkaExtractSquid.getTable_name();
                            destination_squid_id = kafkaExtractSquid.getDestination_squid_id();
                            persist_sql = kafkaExtractSquid.getPersist_sql();
                            currentSquid = kafkaExtractSquid;
                        }
                    } else if (c == HBaseExtractSquid.class) {
                        //为Hbase Extract SquidModelBase 落地，因为对应的表不同，所以需要重新写Sql语句
                        sql = "select * from DS_SQUID where id = " + squidId;
                        HBaseExtractSquid HBaseExtractSquid = adapter3.query2Object(true, sql, null, HBaseExtractSquid.class);
                        if (HBaseExtractSquid != null) {
                            is_persisted = HBaseExtractSquid.isIs_persisted();
                            tableName = HBaseExtractSquid.getTable_name();
                            destination_squid_id = HBaseExtractSquid.getDestination_squid_id();
                            persist_sql = HBaseExtractSquid.getPersist_sql();
                            currentSquid = HBaseExtractSquid;
                        }
                    } else if (c == SystemHiveExtractSquid.class) {
                        sql = "select * from ds_squid where id=" + squidId;
                        SystemHiveExtractSquid dataSquid = adapter3.query2Object(true, sql,
                                null, SystemHiveExtractSquid.class);
                        if (dataSquid != null) {
                            is_persisted = dataSquid.isIs_persisted();
                            destination_squid_id = dataSquid.getDestination_squid_id();
                            tableName = dataSquid.getTable_name();
                            persist_sql = dataSquid.getPersist_sql();
                            currentSquid = dataSquid;
                        }
                    } else if (c == CassandraExtractSquid.class) {
                        sql = "select * from ds_squid where id=" + squidId;
                        CassandraExtractSquid dataSquid = adapter3.query2Object(true, sql,
                                null, CassandraExtractSquid.class);
                        if (dataSquid != null) {
                            is_persisted = dataSquid.isIs_persisted();
                            destination_squid_id = dataSquid.getDestination_squid_id();
                            tableName = dataSquid.getTable_name();
                            persist_sql = dataSquid.getPersist_sql();
                            currentSquid = dataSquid;
                        }
                    }

                    // 验证表名
                    String regex = "[a-zA-Z][_a-zA-Z0-9]*(\\.[a-zA-Z][_a-zA-Z0-9]*)?";
                    Pattern pattern = Pattern.compile(regex);
                    if (ValidateUtils.isEmpty(tableName)
                            || !pattern.matcher(tableName).matches()
                            ) {
                        list.add(squidId);
                        out.setMessageCode(MessageCode.ERR_PERSISTTABLE_TABLE_NAME);
                        break;
                    } else if (tableName.length() > 30) {
                        list.add(squidId);
                        out.setMessageCode(MessageCode.ERR_PERSISTTABLE_OVER_LENGTH);
                        break;
                    }

                    if (is_persisted) {
                        NOSQLConnectionSquid nosql = null;
                        // 如果传过来的对象里面有sql语句，则调用通过sql语句创建落地表
                        if (persist_sql != null) {
                            PersistManagerProcess process = new PersistManagerProcess(TokenUtil.getToken(), TokenUtil.getToken());
                            process.createTableBySQL(currentSquid, persist_sql, out, list);
                            isExists = true;
                        } else {
                            if (destination_squid_id > 0) {
                                Squid dbSquid = adapter3.query2Object(true,
                                        "select * from ds_squid ds where ds.id="
                                                + destination_squid_id, null,
                                        DbSquid.class);
                                INewDBAdapter adaoterSource = null;
                                DBConnectionInfo dbs = null;
                                if (dbSquid != null) {
                                    if (dbSquid.getSquid_type() == SquidTypeEnum.DBSOURCE.value()
                                            || dbSquid.getSquid_type() == SquidTypeEnum.CLOUDDB.value()
                                            || dbSquid.getSquid_type() == SquidTypeEnum.TRAININGDBSQUID.value()) {
                                        sql = "select * from ds_squid where id=" + destination_squid_id;
                                        DbSquid db = adapter3.query2Object(true, sql,
                                                null, DbSquid.class);
                                        if((SysConf.getValue("cloud_host")+":3306").equals(db.getHost())
                                                || SysConf.getValue("train_db_host").equals(db.getHost())){
                                            RepositoryService service = new RepositoryService();
                                            String dbUrl = service.getDbUrlByRepositoryId(repositoryId);
                                            db.setHost(dbUrl);
                                        }
                                        if (db != null) {
                                            // 获取数据源
                                            dbs = DBConnectionInfo.fromDBSquid(db);
                                            adaoterSource = AdapterDataSourceManager
                                                    .getAdapter(dbs);
                                            if (dbs.getDbType() == DataBaseType.HANA
                                                    && !tableName.contains(".")) {
                                                tableName = dbs.getDbName() + "."
                                                        + tableName;
                                            }
                                        }
                                    } else if (dbSquid.getSquid_type() == SquidTypeEnum.MONGODB
                                            .value()) {
                                        // sql =
                                        // "select * from ds_squid ds inner join ds_sql_connection dsc "
                                        // +
                                        // " on ds.id=dsc.id where ds.id="+destination_squid_id;
                                        sql = "select * from ds_squid where id=" + destination_squid_id;
                                        nosql = adapter3
                                                .query2Object(true, sql, null,
                                                        NOSQLConnectionSquid.class);
                                        if (nosql != null) {
                                            // 获取数据源
                                            dbs = DBConnectionInfo
                                                    .fromNOSQLSquid(nosql);
                                            adaoterSource = AdapterDataSourceManager
                                                    .getAdapter(dbs);
                                        }
                                    }
                                    createSquidTable(out, adapter3, list, squidId,
                                            tableName, adaoterSource, dbs.getDbType(), nosql);
                                    isExists = true;
                                }
                            } else {
                                DBConnectionInfo dbs = new DBConnectionInfo();
                                dbs.setHost(SysConf.getValue("hbase.host"));
                                dbs.setPort(Integer.parseInt(SysConf
                                        .getValue("hbase.port")));
                                dbs.setDbType(DataBaseType.HBASE_PHOENIX);
                                INewDBAdapter adaoterSource = AdapterDataSourceManager
                                        .getAdapter(dbs);
                                createSquidTable(out, adapter3, list, squidId,
                                        tableName, adaoterSource, dbs.getDbType(), null);
                                isExists = true;
                            }
                        }
                    }
                }
                if (out.getMessageCode() != MessageCode.SUCCESS) {
                    output.put("ids", list);
                    return output;
                }
                if (!isExists) {
                    out.setMessageCode(MessageCode.ERR_PERSISTTABLE_NOT_CHECKED);
                }
            } else {
                out.setMessageCode(MessageCode.NODATA);
            }
        } catch (Exception e) {
            try {
                if (adapter3 != null)
                    adapter3.rollback();
            } catch (SQLException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            if (e instanceof SystemErrorException) {
                //out.setMessageCode(((SystemErrorException) e).getCode());
                out.setMessageCode(MessageCode.ERR_DBSOURCE_CONNECTION);
            } else {
                out.setMessageCode(MessageCode.SQL_ERROR);
                if (e instanceof RuntimeException) {
                    if (list.size() > 0) {
                        out.setMessageCode(MessageCode.SQL_ERROR);
                    }
                    output.put("ids", list);
                    if (e.getCause().getCause() instanceof com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException) {
                        MySQLSyntaxErrorException ex = (MySQLSyntaxErrorException) e.getCause().getCause();
                        if (ex.getSQLState().equals("42000")) {
                            out.setMessageCode(MessageCode.ERR_CREATE_DEST_TABLE_COLUMN_ISILLEAGEL);
                        }
                    }
                    if (e.getCause().getCause() instanceof SQLServerException) {
                        SQLServerException ex = (SQLServerException) e.getCause().getCause();
                        if (ex.getSQLState().equals("S0001")) {
                            out.setMessageCode(MessageCode.ERR_CREATE_DEST_TABLE_COLUMN_ISILLEAGEL);
                        }
                    }
                    if(e.getCause().getCause() instanceof SqlException){
                        SqlException ex = (SqlException) e.getCause().getCause();
                        if(ex.getSQLState().equals("42611")){
                            out.setMessageCode(MessageCode.ERR_CREATE_DEST_TABLE__PRECISION);
                        }
                    }
                    return output;
                }
            }

        } finally {
            if (adapter3 != null)
                adapter3.closeSession();
        }
        return output;
    }

    /**
     * 创建指定落地目标
     *
     * @param out
     * @param adapter3
     * @param list
     * @param squidId
     * @param tableName
     * @param adaoterSource
     */
    private void createSquidTable(ReturnValue out,
                                  IRelationalDataManager adapter3, List<Integer> list,
                                  Integer squidId, String tableName, INewDBAdapter adaoterSource,
                                  DataBaseType dtType, NOSQLConnectionSquid nosql) {
        try {
            BasicTableInfo table = this.getBasicTableInfo(adapter3, squidId,
                    tableName);
            table.setDbType(dtType);
            if (dtType == DataBaseType.SQLSERVER) {
                List<ColumnInfo> columnList = table.getColumnList();
                if (columnList != null && columnList.size() > 0) {
                    //当数据库类型为sqlserver的时候，将datatime类型datetime2类型
                   /* for (ColumnInfo info : columnList) {
                        if (info.getSystemDatatype() == SystemDatatype.DATETIME) {
							*//*info.setSystemDatatype(SystemDatatype.DATETIME2);
							info.setTypeName("DATETIME2");*//*
                        }
                    }*/
                }
            }
            if (!table.isPersistTable()) {
                out.setMessageCode(MessageCode.ERR_PERSISTTABLE_UNKNOWN);
            } else {
                Set<String> origColls = null;
                if (nosql != null) {
                    if (nosql.getDb_type() == NoSQLDataBaseType.MONGODB.value()) {
                        DB mongoDB = NoSqlConnectionUtil.createMongoDBConnection(nosql);
                        origColls = mongoDB.getCollectionNames();
                        if (origColls == null && origColls.size() == 0) {
                            out.setMessageCode(MessageCode.ERR_CONNECTION_FAIL);
                        }
                        //判断表名是否存在
                        for (String collectionName : origColls) {
                            if (collectionName.toUpperCase().equals(tableName.toUpperCase())) {
                                out.setMessageCode(MessageCode.WARN_PERSISTTABLEALREADYEXIST);
                                list.add(squidId);
                                return;
                            }
                        }
                    }
                } else {
                    List<BasicTableInfo> tableInfoList = adaoterSource.getAllTables("");
                    for (BasicTableInfo info : tableInfoList) {
                        if (info.getTableName().toUpperCase().equals(tableName.toUpperCase())) {
                            out.setMessageCode(MessageCode.WARN_PERSISTTABLEALREADYEXIST);
                            list.add(squidId);
                            return;
                        }
                    }
                }
                adaoterSource.createTable(table, out);
            }
            if (out.getMessageCode() != MessageCode.SUCCESS) {
                //out.setMessageCode(MessageCode.SUCCESS);
                list.clear();
                if (out.getMessageCode() != MessageCode.ERR_DBSOURCE_CONNECTION) {
                    out.setMessageCode(MessageCode.SUCCESS);
                    list.add(squidId);
                }
            }
        } catch (Exception e) {
            logger.error(e);
            list.add(squidId);
            throw e;
        } finally {
            adaoterSource.close();
        }

        /**
         try {
         int i = adaoterSource.getRecordCount(tableName);
         if (dtType == null && i == 0) {
         is_exists = false;
         }
         } catch (Exception e) {
         // e.printStackTrace();
         logger.error(e.getMessage());
         is_exists = false;
         }
         if (is_exists) {
         list.add(squidId);
         } else {
         BasicTableInfo table = this.getBasicTableInfo(adapter3, squidId,
         tableName);
         table.setDbType(dtType);
         if (!table.isPersistTable()) {
         out.setMessageCode(MessageCode.ERR_PERSISTTABLE_UNKNOWN);
         } else {
         adaoterSource.createTable(table, out);
         }
         if (out.getMessageCode() != MessageCode.SUCCESS) {
         list.clear();
         list.add(squidId);
         }
         }
         adaoterSource.close();
         */
    }

    /**
     * 删除落地squid数据表
     *
     * @param info
     * @param out
     */
    public Map<String, Object> dropPersistTableByIds(String info,
                                                     ReturnValue out) {
        DataAdapterFactory adapterFactory = null;
        IRelationalDataManager adapter3 = null;
        List<Integer> list = new ArrayList<Integer>();
        Map<String, Object> outputMap = new HashMap<String, Object>();
        try {
            Map<String, Object> inParmsMap = JsonUtil.toHashMap(info);
            List<Integer> squidIds = JsonUtil.toGsonList(
                    inParmsMap.get("SquidIds") + "", Integer.class);
            int repositoryId = Integer.parseInt(inParmsMap.get("RepositoryId")
                    + "");
            String username = inParmsMap.get("UserName") + "";
            int spaceId = Integer.parseInt(inParmsMap.get("SpaceId")+"");
            int fieldType = Integer.parseInt(inParmsMap.get("FieldType") + "");
            if (squidIds != null && squidIds.size() > 0) {
                adapterFactory = DataAdapterFactory.newInstance();
                adapter3 = DataAdapterFactory.getDefaultDataManager();
                adapter3.openSession();
                String sql = "";
                boolean isExists = false;
                for (Integer squidId : squidIds) {
                    boolean isDel = false;
                    sql = "select * from ds_squid where id=" + squidId;
                    Squid squid = adapter3.query2Object(true, sql, null,
                            Squid.class);
                    if (squid == null) {
                        continue;
                    }
                    Class c = SquidTypeEnum.classOfValue(squid.getSquid_type());
                    boolean is_persisted = false;
                    int destination_squid_id = 0;
                    String tableName = "";
                    if (c == DataSquid.class
                            || c == WebserviceExtractSquid.class
                            || c == HttpExtractSquid.class
                            || c == StageSquid.class
                            || c == ExceptionSquid.class
                            || c == StreamStageSquid.class) {
                        sql = "select * from ds_squid where id=" + squidId;
                        DataSquid dataSquid = adapter3.query2Object(true, sql,
                                null, DataSquid.class);
                        if (dataSquid != null) {
                            is_persisted = dataSquid.isIs_persisted();
                            destination_squid_id = dataSquid
                                    .getDestination_squid_id();
                            tableName = dataSquid.getTable_name();
                        }
                    } else if (c == SystemHiveExtractSquid.class) {
                        sql = "select * from ds_squid where id=" + squidId;
                        SystemHiveExtractSquid dataSquid = adapter3.query2Object(true, sql, null, SystemHiveExtractSquid.class);
                        if (dataSquid != null) {
                            is_persisted = dataSquid.isIs_persisted();
                            destination_squid_id = dataSquid
                                    .getDestination_squid_id();
                            tableName = dataSquid.getTable_name();
                        }
                    } else if (c == DataMiningSquid.class) {
                        DBConnectionInfo dbs = new DBConnectionInfo();
                        dbs.setHost(SysConf.getValue("hbase.host"));
                        dbs.setPort(Integer.parseInt(SysConf
                                .getValue("hbase.port")));
                        dbs.setDbType(DataBaseType.MYSQL);
                        dbs.setDbName(SysConf.getValue("hbase.dbname"));
                        dbs.setPassword(SysConf.getValue("hbase.password"));
                        dbs.setPort(Integer.parseInt(SysConf.getValue("hbase.port")));
                        dbs.setUserName(SysConf.getValue("hbase.username"));
                        tableName = DataMiningUtil.genTrainModelTableName(
                                repositoryId, squid.getSquidflow_id(), squidId);
                        isDel = dropSquidTable(spaceId,username, tableName, fieldType, dbs, out);

                        if (isDel) {
                            isExists = true;
                        } else {
                            isExists = false;
                        }
                        //isExists = true;
                        if (!isDel) {
                            list.add(squidId);
                        }
                        continue;
                    } else if (c == DocExtractSquid.class) {
                        sql = "select * from ds_squid where id=" + squidId;
                        DocExtractSquid docSquid = adapter3.query2Object(true,
                                sql, null, DocExtractSquid.class);
                        if (docSquid != null) {
                            is_persisted = docSquid.isIs_persisted();
                            destination_squid_id = docSquid
                                    .getDestination_squid_id();
                            tableName = docSquid.getTable_name();
                        }
                    } else if (c == KafkaExtractSquid.class) {
                        sql = "select * from DS_SQUID where id = " + squidId;
                        KafkaExtractSquid kafkaExtractSquid = adapter3.query2Object(true,
                                sql, null, KafkaExtractSquid.class);
                        if (kafkaExtractSquid != null) {
                            is_persisted = kafkaExtractSquid.isIs_persisted();
                            destination_squid_id = kafkaExtractSquid
                                    .getDestination_squid_id();
                            tableName = kafkaExtractSquid.getTable_name();
                        }
                    } else if (c == HBaseExtractSquid.class) {
                        sql = "select * from DS_SQUID where id = " + squidId;
                        HBaseExtractSquid HBaseExtractSquid = adapter3.query2Object(true,
                                sql, null, HBaseExtractSquid.class);
                        if (HBaseExtractSquid != null) {
                            is_persisted = HBaseExtractSquid.isIs_persisted();
                            destination_squid_id = HBaseExtractSquid
                                    .getDestination_squid_id();
                            tableName = HBaseExtractSquid.getTable_name();
                        }
                    } else if (c == CassandraExtractSquid.class) {
                        sql = "select * from ds_squid where id=" + squidId;
                        CassandraExtractSquid dataSquid = adapter3.query2Object(true, sql, null, CassandraExtractSquid.class);
                        if (dataSquid != null) {
                            is_persisted = dataSquid.isIs_persisted();
                            destination_squid_id = dataSquid
                                    .getDestination_squid_id();
                            tableName = dataSquid.getTable_name();
                        }
                    }

                    if (is_persisted) {
                        if (destination_squid_id > 0) {
                            sql = "select * from ds_squid where id="
                                    + destination_squid_id;
                            DbSquid db = adapter3.query2Object(true, sql, null,
                                    DbSquid.class);
                            INewDBAdapter adaoterSource = null;
                            DBConnectionInfo dbs = null;

                            if (db != null) {
                                if (db.getSquid_type() == SquidTypeEnum.DBSOURCE.value()
                                        || db.getSquid_type() == SquidTypeEnum.CLOUDDB.value()
                                        || db.getSquid_type()==SquidTypeEnum.TRAININGDBSQUID.value()) {
                                    sql = "select * from ds_squid ds where ds.id=" + destination_squid_id;
                                    db = adapter3.query2Object(true, sql, null,
                                            DbSquid.class);
                                    if(db.getHost().equals(SysConf.getValue("cloud_host")+":3306")
                                            || db.getHost().equals(SysConf.getValue("train_db_host"))){
                                        RepositoryService service = new RepositoryService();
                                        String dbUrl = service.getDbUrlByRepositoryId(repositoryId);
                                        db.setHost(dbUrl);
                                    }
                                    /*if(db.getHost().equals(SysConf.getValue("train_db_host"))){
                                        db.setHost(SysConf.getValue("train_db_real_host"));
                                    }*/
                                    // 获取数据源
                                    dbs = DBConnectionInfo.fromDBSquid(db);
                                    if (dbs.getDbType() == DataBaseType.HANA
                                            && !tableName.contains(".")) {
                                        tableName = dbs.getDbName() + "."
                                                + tableName;
                                    }
                                } else if (db.getSquid_type() == SquidTypeEnum.MONGODB
                                        .value()) {
                                    sql = "select * from ds_squid ds where ds.id=" + destination_squid_id;
                                    NOSQLConnectionSquid nosql = adapter3
                                            .query2Object(true, sql, null,
                                                    NOSQLConnectionSquid.class);
                                    // 获取数据源
                                    // dbs =
                                    // DBConnectionInfo.fromNOSQLSquid(nosql);
                                    if (nosql != null) {
                                        // 获取数据源
                                        dbs = DBConnectionInfo
                                                .fromNOSQLSquid(nosql);
                                    }
                                }
                                isDel = dropSquidTable(spaceId,username, tableName, fieldType, dbs, out);
                                //删除成功，
                                if (isDel) {
                                    isExists = true;
                                } else {
                                    isExists = false;
                                }
                                //isExists = true;

                            }
                        } else if (is_persisted && destination_squid_id == 0) {
                            DBConnectionInfo dbs = new DBConnectionInfo();
                            dbs.setHost(SysConf.getValue("hbase.host"));
                            dbs.setPort(Integer.parseInt(SysConf
                                    .getValue("hbase.port")));
                            dbs.setDbType(DataBaseType.HBASE_PHOENIX);
                            isDel = dropSquidTable(spaceId,username, tableName, fieldType, dbs, out);
                            //删除成功，
                            if (isDel) {
                                isExists = true;
                            } else {
                                isExists = false;
                            }

                        }
                    }
                    if (!isDel) {
                        list.add(squidId);
                    }
                }
				/*if (!isExists ) {
					if(out.getMessageCode()!=MessageCode.ERR_DBSOURCE_CONNECTION) {
						out.setMessageCode(MessageCode.ERR_PERSISTTABLE_NOT_TABLENAME);
						outputMap.put("ids", list);
						return outputMap;
					}
				} else*/
                if (list.size() > 0) {
                    if (out.getMessageCode() != MessageCode.ERR_DBSOURCE_CONNECTION) {
                        out.setMessageCode(MessageCode.ERR_PERSISTTABLE_NOT_TABLENAME);
                    }
                    outputMap.put("ids", list);
                    return outputMap;
                }
            } else {
                out.setMessageCode(MessageCode.NODATA);
            }
        } catch (Exception e) {
            try {
                if (adapter3 != null)
                    adapter3.rollback();
            } catch (Exception e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            if (e instanceof SystemErrorException) {
                if (((SystemErrorException) e).getCode() == MessageCode.ERR_CONNECTION_NULL) {
                    out.setMessageCode(MessageCode.ERR_DBSOURCE_CONNECTION);
                }
                if (((SystemErrorException) e).getCode() == MessageCode.ERR_DS_DB_TYPE_Enum) {
                    out.setMessageCode(MessageCode.ERR_DBSOURCE_CONNECTION);
                }
            } else {
                out.setMessageCode(MessageCode.SQL_ERROR);
            }
            logger.error(e.getMessage());
        } finally {
            if (adapter3 != null)
                adapter3.closeSession();
        }
        return outputMap;
    }

    /**
     * 删除指定落地目标
     *
     * @param tableName
     * @param dbs
     * @throws Exception
     */
    private boolean dropSquidTable(int spaceId,String username, String tableName, int fieldType, DBConnectionInfo dbs, ReturnValue out)
            throws Exception {
        long current = System.currentTimeMillis();

        INewDBAdapter adaoterSource = AdapterDataSourceManager.getAdapter(dbs);
        boolean is_exists = true;
        try {
            if (dbs.getIsNoSql() != null && dbs.getIsNoSql()) {
                is_exists = adaoterSource.deleteMongoTable(tableName);
            } else {
                adaoterSource.deleteTable(tableName);
            }
            //调用接口删除云上面的表
            if (StringUtils.isNotNull(username)) {
                Map<String, Object> params = new HashMap<>();
                params.put("username", username);
                params.put("tableName", tableName);
                params.put("delTable", 0);
                params.put("fieldType", fieldType);
                params.put("spaceId",spaceId);
                //需要增加一个数猎场类型
                long time = System.currentTimeMillis();
                String result = HttpClientUtils.doPost(SysConf.getValue("del_dest"), params);
                long endTime = System.currentTimeMillis();
                logger.info("调用删除接口，所需时间:"+(endTime-time));
                if (!"1".equals(result)) {
                    logger.error("删除报表中的数据库表失败");
                } else {
                    logger.info("删除报表中的数据库表成功");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e);
            if (e instanceof MongoTimeoutException) {
                out.setMessageCode(MessageCode.ERR_DBSOURCE_CONNECTION);
            }
            is_exists = false;
        } finally {
            adaoterSource.close();
        }

        /** 通过count来判断表是否存在的方法太差，碰到大表，删除超级慢
         try {
         int i = adaoterSource.getRecordCount(tableName);
         if (i == 0 && dbs.getIsNoSql() != null && dbs.getIsNoSql()) {
         throw new Exception("落地表不存在");
         }
         } catch (Exception e) {
         // e.printStackTrace();
         logger.error(e.getMessage());
         is_exists = false;
         }
         if (is_exists) {
         adaoterSource.deleteTable(tableName);
         }
         adaoterSource.close();
         */

        logger.info("==================\n"
                + "\t==================\n"
                + "\t==================\n"
                + "\t==================\n"
                + "\t==================\n"
                + "\t 删除表耗时：" + (System.currentTimeMillis() - current) / 1000.0 + "(秒) ===========");
        return is_exists;
    }

    private BasicTableInfo getBasicTableInfo(IRelationalDataManager adapter3,
                                             int squidId, String tableName) {
        BasicTableInfo table = new BasicTableInfo(tableName);
        ISquidDao squidDao = new SquidDaoImpl(adapter3);
        IColumnDao columnDao = new ColumnDaoImpl(adapter3);
        Column col = null;
        Squid squid = null;
        Class c = null;
        try {
            squid = squidDao.getSquidForCond(squidId, Squid.class);
            c = SquidTypeEnum.classOfValue(squid.getSquid_type());
            if (c == DataMiningSquid.class) {
                col = columnDao.getColumnKeyForDataMingSquid(squidId);
            }
        } catch (Exception e) {
        }
        List<ColumnInfo> columnList = new ArrayList<ColumnInfo>();
        ColumnInfo e = new ColumnInfo();
        String sql = "select * from ds_column where squid_id=" + squidId
                + " order by RELATIVE_ORDER asc";
        List<Column> squidColumns = adapter3.query2List(true, sql, null,
                Column.class);
        if (squidColumns != null && squidColumns.size() > 0) {
            int i = 1;
            for (Column column : squidColumns) {
                if (squid != null) {
                    if (c == DataMiningSquid.class
                            && column.getName().equals("KEY") && col == null) {
                        continue;
                    }
                    e = new ColumnInfo();
                    e.setTableName(tableName);
                    e.setName(column.getName());
                    e.setIdentity(column.isIsAutoIncrement());
                    e.setPrimary(column.isIsPK());
                    e.setUniqueness(column.isIsUnique());
                    if (column.getData_type() == SystemDatatype.UNKNOWN.value()) {
                        table.setPersistTable(false);
                    }
                    if (column.getData_type() == DbBaseDatatype.STRING.value()) {
                        e.setSystemDatatype(SystemDatatype.NVARCHAR);
                    }
                    if (column.getData_type() == DbBaseDatatype.STRUCT.value()) {
                        e.setSystemDatatype(SystemDatatype.NVARCHAR);
                    }
                    if (column.getData_type() == DbBaseDatatype.UNIONTYPE.value()) {
                        e.setSystemDatatype(SystemDatatype.NVARCHAR);
                    }
                    e.setSystemDatatype(SystemDatatype.parse(column
                            .getData_type()));
                    e.setTypeName(SystemDatatype.parse(column.getData_type())
                            .toString());
                    e.setOrderNumber(i++);
                    e.setNullable(column.isNullable());
                    e.setLength(column.getLength());
                    e.setScale(column.getScale());
                    e.setPrecision(column.getPrecision());
                    columnList.add(e);
                }
            }
        }
        // columnList.add(new ColumnInfo("names",SystemDatatype.NVARCHAR,100));
        table.setColumnList(columnList);
        return table;
    }

    private List<String> getSquidTableInfo(IRelationalDataManager adapter3,
                                           DataBaseType dtType, int squidId, String tableName) {
        BasicTableInfo table = this.getBasicTableInfo(adapter3, squidId, tableName);
        List<ColumnInfo> columnList = null;
        List<String> rsList = new ArrayList<String>();
        if (table != null && table.getColumnList() != null) {
            columnList = table.getColumnList();
        }
        for (ColumnInfo info : columnList) {
            String tm = DataTypeConvertor.getOutTypeByColumn(dtType.value(), info);
            rsList.add(tm);
        }
        return rsList;
    }

    private List<String> getPersistBasicTableInfo(DataBaseType dtType,
                                                  INewDBAdapter adaoterSource, String tableName, String dataBaseName) {
        List<String> rsList = new ArrayList<String>();
        List<ColumnInfo> columnList = null;
        try {
            columnList = adaoterSource.getTableColumns(tableName,
                    dataBaseName);
        } catch (TException e) {
            e.printStackTrace();
        }
        // ColumnInfo e= new ColumnInfo();
        if (columnList != null && columnList.size() > 0) {
            for (ColumnInfo columnInfo : columnList) {
                SourceColumn source = DataTypeConvertor.getSourceColumnByColumnInfo(columnInfo);
                String tm = DataTypeConvertor.getOutTypeBySourceColumn(dtType.value(), source);
                rsList.add(tm);
            }
        }
        return rsList;
    }

    public void checkTypeData() {

    }

    /**
     * 根据输入参数在数据中进行查询，并且排序(复制squidFlow时用，不过滤）
     *
     * @param adapter3
     * @param toSquidId
     * @param links
     * @throws SQLException
     */
    private void getLinkIdsForList(IRelationalDataManager adapter3,
                                   int toSquidId, List<Integer> links) throws SQLException {
        getLinkIdsForList(adapter3, toSquidId, links, null);
    }

    /**
     * 根据输入参数在数据中进行查询，并且排序(复制squid时用，过滤，只对输入Link集合排序）
     *
     * @param adapter3
     * @param toSquidId
     * @param links
     * @throws SQLException
     */
    private void getLinkIdsForList(IRelationalDataManager adapter3,
                                   int toSquidId, List<Integer> links, List<Integer> oldLinks)
            throws SQLException {
        String sql = "select * from ds_squid_link where from_squid_id="
                + toSquidId;
        List<SquidLink> squidLinks = adapter3.query2List(true, sql, null,
                SquidLink.class);
        if (squidLinks != null && squidLinks.size() > 0) {
            for (SquidLink squidLink : squidLinks) {
                int tempId = squidLink.getTo_squid_id();
                if ((oldLinks == null || oldLinks.contains(squidLink.getId()))
                        && !links.contains(squidLink.getId())) {
                    links.add(squidLink.getId());
                }
                getLinkIdsForList(adapter3, tempId, links, oldLinks);
            }
        }
    }

    public Map<String, Object> getConvertedColumns(String info, ReturnValue out) {
        Map<String, Object> outputMap = new HashMap<String, Object>();
        try {
            Map<String, Object> inParmsMap = JsonUtil.toHashMap(info);
            List<Column> columns = new ArrayList<Column>();
            List<ReferenceColumn> refColumns = JsonUtil.toGsonList(
                    inParmsMap.get("ReferenceColumns") + "",
                    ReferenceColumn.class);
            int dbType = Integer.parseInt(inParmsMap.get("DbType") + "");
            if (refColumns != null && refColumns.size() > 0) {
                for (ReferenceColumn referenceColumn : refColumns) {
                    SourceColumn col = DataTypeConvertor
                            .getSourceColumnByReferenceColumns(referenceColumn);
                    SourceColumn newSourceColumn = DataTypeConvertor
                            .getInColumnBySourceColumn(dbType, col);
                    Column column = DataTypeConvertor
                            .getColumnBySourceColumn(newSourceColumn);
                    columns.add(column);
                }
            }
            outputMap.put("Columns", columns);
        } catch (Exception e) {
            e.printStackTrace();
            out.setMessageCode(MessageCode.ERR);
        }
        return outputMap;
    }

    /**
     * 根据task_id查询日志
     *
     * @param task_id
     * @return
     */
    public String getSquidFlowLog(String task_id) {
        ReturnValue out = new ReturnValue();
        int timeSpan = Integer.parseInt(SysConf.getValue("timer"));
        CommonConsts.scheduleMap.put(task_id, TokenUtil.getKey() + TokenUtil.getToken());
		/*
		 * // 定时任务推送日志给前端 Timer timer = new Timer();
		 */
        JobLogProcess jobLogProcess = new JobLogProcess();
        // JobScheduleProcess jobScheduleProcess=new JobScheduleProcess();
        // 获取所有的repository-squidflow的name组合
        // Map<String, Object> nameMap=jobScheduleProcess.getAllName(out);
		/*
		 * //每次启动任务时,将取消状态设置为false JobLogService.setCancleFlag(false);
		 */
        // 启动定时任务的时候需要绑定token & task_id
		/*
		 * TaskUtils task = null; if
		 * (!CommonConsts.squidTimer.containsKey(token+"&"+task_id)){
		 * CommonConsts.squidTimer.put(token+"&"+task_id, new
		 * Timer(token+"&"+task_id));
		 * CommonConsts.stopTimer.put(token+"&"+task_id, false); task = new
		 * TaskUtils(out, token,jobLogProcess,key,null,0,task_id,-1);
		 * CommonConsts.timerTaskMap.put(token+"&"+task_id, task); }else{ task =
		 * CommonConsts.timerTaskMap.get(token+"&"+task_id); }
		 * CommonConsts.squidTimer.get(token+"&"+task_id).schedule(task, 0,
		 * timeSpan * 1000);
		 */
        return null;
    }

    /**
     * 创建落地数据表后的index
     *
     * @param info
     * @return
     */
    public Map<String, Object> createIndexBySquidId(String info, ReturnValue out) {
        IRelationalDataManager adapter3 = null;
        DataAdapterFactory dataFactory = null;
        Map<String, Object> outputMap = new HashMap<String, Object>();
        try {
            Map<String, Object> infoMap = JsonUtil.toHashMap(info);
            List<Integer> squidIds = JsonUtil.toGsonList(
                    (infoMap.get("SquidIds") + ""), Integer.class);
            Map<String, String> paramsMap = new HashMap<String, String>();
            if (squidIds != null && squidIds.size() > 0) {
                adapter3 = DataAdapterFactory.getDefaultDataManager();
                adapter3.openSession();
                List<Integer> squidIndexIds = new ArrayList<Integer>();
                for (Integer squidId : squidIds) {
                    paramsMap.clear();
                    paramsMap.put("squid_id", String.valueOf(squidId));
                    List<SquidIndexes> squidIndexs = adapter3.query2List(true,
                            paramsMap, SquidIndexes.class);
                    if (squidIndexs != null && squidIndexs.size() > 0) {
                        SquidIndexsServiceSub service = new SquidIndexsServiceSub(
                                TokenUtil.getToken());
                        List<Integer> ids = service.createSquidIndexsToDB(
                                adapter3, squidIndexs, out);
                        if (ids != null) {
                            squidIndexIds.addAll(ids);
                        } else {
                            if (out.getMessageCode() != MessageCode.SUCCESS) {
                                return null;
                            }
                        }
                    } else {
                        out.setMessageCode(MessageCode.ERR_SQUIDINDEXS_NOT_INDEXS);
                        return null;
                    }
                }
                if (squidIndexIds.size() > 0) {
                    outputMap.put("SquidIndexIds", squidIndexIds);
                    return outputMap;
                } else {
                    out.setMessageCode(MessageCode.INSERT_ERROR);
                }
            } else {
                out.setMessageCode(MessageCode.NODATA);
            }
        } catch (Exception e) {
            logger.error("删除SquidIndexs异常", e);
            try {
                if (adapter3 != null)
                    adapter3.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!", e1);
            }
            out.setMessageCode(MessageCode.SQL_ERROR);
        } finally {
            if (adapter3 != null)
                adapter3.closeSession();
        }
        return null;
    }

    /**
     * 删除落地数据表后的index
     *
     * @param info
     * @return
     */
    public Map<String, Object> dropIndexBySquidId(String info, ReturnValue out) {
        IRelationalDataManager adapter3 = null;
        DataAdapterFactory dataFactory = null;
        try {
            Map<String, Object> infoMap = JsonUtil.toHashMap(info);
            List<Integer> squidIds = JsonUtil.toGsonList(
                    (infoMap.get("SquidIds") + ""), Integer.class);
            Map<String, String> paramsMap = new HashMap<String, String>();
            if (squidIds != null && squidIds.size() > 0) {
                adapter3 = DataAdapterFactory.getDefaultDataManager();
                adapter3.openSession();
                for (Integer squidId : squidIds) {
                    paramsMap.clear();
                    paramsMap.put("squid_id", String.valueOf(squidId));
                    List<SquidIndexes> squidIndexs = adapter3.query2List(true,
                            paramsMap, SquidIndexes.class);
                    if (squidIndexs != null && squidIndexs.size() > 0) {
                        boolean isIndex = false;
                        SquidIndexsServiceSub service = new SquidIndexsServiceSub(
                                TokenUtil.getToken());
                        for (SquidIndexes squidIndexes : squidIndexs) {
                            boolean flag = service.dropSquidIndexsFromDB(
                                    adapter3, squidIndexes, out);
                            if (!flag
                                    && out.getMessageCode() != MessageCode.SUCCESS) {
                                return null;
                            }
                            if (flag) {
                                isIndex = true;
                            }
                        }
                        if (!isIndex) {
                            out.setMessageCode(MessageCode.ERR_SQUIDINDEXS_NOT_INDEXNAME);
                            return null;
                        }
                    } else {
                        out.setMessageCode(MessageCode.ERR_SQUIDINDEXS_NOT_INDEXS);
                        return null;
                    }
                }
            } else {
                out.setMessageCode(MessageCode.NODATA);
            }
        } catch (Exception e) {
            logger.error("删除SquidIndexs异常", e);
            try {
                if (adapter3 != null)
                    adapter3.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!", e1);
            }
            out.setMessageCode(MessageCode.SQL_ERROR);
        } finally {
            if (adapter3 != null)
                adapter3.closeSession();
        }
        return null;
    }

    /**
     * 创建指定落地目标
     *
     * @param out
     * @param adapter3
     * @param list
     * @param squidId
     * @param adaoterSource
     * @throws Exception
     */
    private Integer checkSquidTable(ReturnValue out,
                                    IRelationalDataManager adapter3, List<Integer> list,
                                    Integer squidId, String tableName, INewDBAdapter adaoterSource,
                                    DataBaseType dtType, String dataBaseName) throws Exception {

        try {
            // tableName = "AAA_REPORT_11";
            // tableName = "DBO_S_TABLE_23";
            int i = adaoterSource.getRecordCount(tableName);
            if (dtType == null && i == 0) {
                adaoterSource.close();
                CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(),
                        MessageBubbleService.setMessageBubble(squidId, squidId,
                                null, MessageBubbleCode.ERROR_NOT_PERSISTENT_TABLE
                                        .value()), false, "未创建落地对象"));
                return SyncStatusEnum.NOT_PERSISTENT.value();
            }
        } catch (Exception e) {
            // e.printStackTrace();
            logger.error(e.getMessage());
            adaoterSource.close();
            CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(),
                    MessageBubbleService.setMessageBubble(squidId, squidId,
                            null, MessageBubbleCode.ERROR_NOT_PERSISTENT_TABLE
                                    .value()), false, "未创建落地对象"));
            return SyncStatusEnum.NOT_PERSISTENT.value();
        }
        CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(),
                MessageBubbleService.setMessageBubble(squidId, squidId, null,
                        MessageBubbleCode.ERROR_NOT_PERSISTENT_TABLE.value()),
                true, "未创建落地对象"));
        if (dtType == null) {
            return SyncStatusEnum.SYNCHRONIZED.value();
        }
        // 1.比较落地后修改的信息和落地表中的信息
        Integer pStatus = compareSquidTable2PersistTable(adaoterSource,
                adapter3, squidId, tableName, dataBaseName, dtType, out);
        // 2.比较索引

        Integer indexStatus = comparDBIndexes2SquidIndexes(adaoterSource,
                adapter3, squidId, tableName, dataBaseName);
        // Sql Server:sp_helpindex 'sys.trace_xe_action_map'
        // DB2:select name from sysibm.sysindexes where tbname = 'ACT'
        if (out.getMessageCode() != MessageCode.SUCCESS) {
            list.clear();
            list.add(squidId);
        }
        adaoterSource.close();
        if (pStatus >= indexStatus
                || SyncStatusEnum.UNKNOWN.value() == indexStatus) {
            return pStatus;
        } else {
            return indexStatus;
        }
    }

    /**
     * 取得DB中的索引
     *
     * @param adaoterSource
     * @param tableName
     * @return
     * @author bo.dang
     */
    public List<String> getDBSquidIndexes(INewDBAdapter adaoterSource,
                                          String tableName, String dataBaseName) {
        List<SquidIndexes> dbSquidIndexesList = adaoterSource.getTableIndexes(
                tableName, dataBaseName);
        List<String> indexesList = new ArrayList<String>();
        if (dbSquidIndexesList != null && dbSquidIndexesList.size() > 0) {
            String temp = "";
            for (SquidIndexes indexes : dbSquidIndexesList) {
                if (!ValidateUtils.isEmpty(indexes.getColumn_name_1())) {
                    temp += "," + indexes.getColumn_name_1();
                }
                if (!ValidateUtils.isEmpty(indexes.getColumn_name_2())) {
                    temp += "," + indexes.getColumn_name_2();
                }
                if (!ValidateUtils.isEmpty(indexes.getColumn_name_3())) {
                    temp += "," + indexes.getColumn_name_3();
                }
                if (!ValidateUtils.isEmpty(indexes.getColumn_name_4())) {
                    temp += "," + indexes.getColumn_name_4();
                }
                if (!ValidateUtils.isEmpty(indexes.getColumn_name_5())) {
                    temp += "," + indexes.getColumn_name_5();
                }
                if (!ValidateUtils.isEmpty(indexes.getColumn_name_6())) {
                    temp += "," + indexes.getColumn_name_6();
                }
                if (!ValidateUtils.isEmpty(indexes.getColumn_name_7())) {
                    temp += "," + indexes.getColumn_name_7();
                }
                if (!ValidateUtils.isEmpty(indexes.getColumn_name_8())) {
                    temp += "," + indexes.getColumn_name_1();
                }
                if (!ValidateUtils.isEmpty(indexes.getColumn_name_9())) {
                    temp += "," + indexes.getColumn_name_9();
                }
                if (!ValidateUtils.isEmpty(indexes.getColumn_name_10())) {
                    temp += "," + indexes.getColumn_name_10();
                }
                if (temp != "") {
                    temp = temp.substring(1);
                }
                String key = indexes.getIndex_name() + "(" + temp + ")";
                indexesList.add(key.toLowerCase());
            }
        }
        return indexesList;
    }

    public List<String> initConvertStringByIndexes(IRelationalDataManager adapter3,
                                                   List<SquidIndexes> dbSquidIndexesList) {
        List<String> indexesList = new ArrayList<String>();
        if (StringUtils.isNotNull(dbSquidIndexesList)
                && !dbSquidIndexesList.isEmpty()) {
            for (int i = 0; i < dbSquidIndexesList.size(); i++) {
                setColumnName(adapter3, dbSquidIndexesList.get(i));
            }
        }
        if (dbSquidIndexesList != null && dbSquidIndexesList.size() > 0) {
            String temp = "";
            for (SquidIndexes indexes : dbSquidIndexesList) {
                if (!ValidateUtils.isEmpty(indexes.getColumn_name_1())) {
                    temp += "," + indexes.getColumn_name_1();
                }
                if (!ValidateUtils.isEmpty(indexes.getColumn_name_2())) {
                    temp += "," + indexes.getColumn_name_2();
                }
                if (!ValidateUtils.isEmpty(indexes.getColumn_name_3())) {
                    temp += "," + indexes.getColumn_name_3();
                }
                if (!ValidateUtils.isEmpty(indexes.getColumn_name_4())) {
                    temp += "," + indexes.getColumn_name_4();
                }
                if (!ValidateUtils.isEmpty(indexes.getColumn_name_5())) {
                    temp += "," + indexes.getColumn_name_5();
                }
                if (!ValidateUtils.isEmpty(indexes.getColumn_name_6())) {
                    temp += "," + indexes.getColumn_name_6();
                }
                if (!ValidateUtils.isEmpty(indexes.getColumn_name_7())) {
                    temp += "," + indexes.getColumn_name_7();
                }
                if (!ValidateUtils.isEmpty(indexes.getColumn_name_8())) {
                    temp += "," + indexes.getColumn_name_1();
                }
                if (!ValidateUtils.isEmpty(indexes.getColumn_name_9())) {
                    temp += "," + indexes.getColumn_name_9();
                }
                if (!ValidateUtils.isEmpty(indexes.getColumn_name_10())) {
                    temp += "," + indexes.getColumn_name_10();
                }
                if (temp != "") {
                    temp = temp.substring(1);
                }
                String key = indexes.getIndex_name() + "(" + temp + ")";
                indexesList.add(key.toLowerCase());
            }
        }
        return indexesList;
    }

    /**
     * 比较DB中索引和Squid中的索引
     *
     * @return
     * @throws Exception
     * @author bo.dang
     */
    public Integer comparDBIndexes2SquidIndexes(INewDBAdapter adaoterSource,
                                                IRelationalDataManager adapter3, Integer squidId, String tableName,
                                                String dataBaseName) throws Exception {

        // 取得落地表中的索引
        List<String> dbSquidIndexesList = getDBSquidIndexes(
                adaoterSource, tableName, dataBaseName);
        // 取得Squid中的索引
        List<SquidIndexes> squidIndexes = new SquidIndexsServiceSub(TokenUtil.getToken())
                .getSquidIndexsListBySquidId(squidId);

        List<String> squidIndexesList = initConvertStringByIndexes(adapter3, squidIndexes);

        Map<String, Object> paramsMap = new HashMap<String, Object>();
        paramsMap.put("isunique", "Y");
        paramsMap.put("squid_id", squidId);
        List<Column> uq_columns = adapter3.query2List2(true, paramsMap, Column.class);
        if (uq_columns != null && uq_columns.size() > 0) {
            String temp = "";
            for (int i = 0; i < uq_columns.size(); i++) {
                temp += "," + uq_columns.get(i).getName();
            }
            if (temp != "") {
                temp = temp.substring(1);
            }
            String key = "uq_" + tableName + "_temp" + "(" + temp + ")";
            squidIndexesList.add(key.toLowerCase());
        }

        paramsMap.clear();
        paramsMap.put("ispk", "Y");
        paramsMap.put("squid_id", squidId);
        List<Column> pk_columns = adapter3.query2List2(true, paramsMap, Column.class);
        if (pk_columns != null && pk_columns.size() > 0) {
            String temp = "";
            for (int i = 0; i < pk_columns.size(); i++) {
                temp += "," + pk_columns.get(i).getName();
            }
            if (temp != "") {
                temp = temp.substring(1);
            }
            String key = "pk_" + tableName + "_temp" + "(" + temp + ")";
            squidIndexesList.add(key.toLowerCase());
        }

        // 如果都为空的话，说明没有创建索引
        if ((StringUtils.isNull(dbSquidIndexesList) || dbSquidIndexesList
                .isEmpty())
                && (StringUtils.isNull(squidIndexesList) || squidIndexesList
                .isEmpty())) {
            // TODO
            return SyncStatusEnum.UNKNOWN.value();
        }
        // 落地索引已创建，SquidModelBase Index未经创建
        if ((!StringUtils.isNull(dbSquidIndexesList) && !dbSquidIndexesList
                .isEmpty())
                && (StringUtils.isNull(squidIndexesList) || squidIndexesList
                .isEmpty())) {
            CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(),
                    MessageBubbleService.setMessageBubble(squidId, squidId,
                            null,
                            MessageBubbleCode.ERROR_INDEXES_NOT_SYNCHRONIZED
                                    .value()), false, "索引未同步"));
            return SyncStatusEnum.NOT_SYNCHRONIZED_HISTORY_DATA.value();
        }
        // 落地索引未创建，SquidModelBase Index已经创建
        if ((StringUtils.isNull(dbSquidIndexesList) || dbSquidIndexesList
                .isEmpty())
                && (!StringUtils.isNull(squidIndexesList) && !squidIndexesList
                .isEmpty())) {
            CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(),
                    MessageBubbleService.setMessageBubble(squidId, squidId,
                            null,
                            MessageBubbleCode.ERROR_INDEXES_NOT_SYNCHRONIZED
                                    .value()), false, "索引未同步"));
            return SyncStatusEnum.NOT_SYNCHRONIZED_NO_HISTORY_DATA.value();
        }

        if (dbSquidIndexesList.size() != squidIndexesList.size()) {
            CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(),
                    MessageBubbleService.setMessageBubble(squidId, squidId,
                            null,
                            MessageBubbleCode.ERROR_INDEXES_NOT_SYNCHRONIZED
                                    .value()), false, "索引未同步"));
            return SyncStatusEnum.NOT_SYNCHRONIZED_HISTORY_DATA.value();
        } else {
            if (dbSquidIndexesList.toString().equals(squidIndexesList.toString())) {
                return SyncStatusEnum.SYNCHRONIZED.value();
            } else {
                System.out.println("dbSquidIndexesList:" + dbSquidIndexesList.toString());
                System.out.println("squidIndexesList:" + squidIndexesList.toString());
                return SyncStatusEnum.NOT_SYNCHRONIZED_HISTORY_DATA.value();
            }
//			Boolean eqFlag = true;
//			for (int i = 0; i < squidIndexesList.size(); i++) {
//				if (!eqFlag) {
//					break;
//				}
//				for (int j = 0; j < dbSquidIndexesList.size(); j++) {
//					if (squidIndexesList
//							.get(i)
//							.getIndex_name()
//							.toLowerCase()
//							.equals(dbSquidIndexesList.get(j).getIndex_name()
//									.toLowerCase())) {
//
//						if ((StringUtils.isNotNull(squidIndexesList.get(i)
//								.getColumn_name_1()) && StringUtils
//								.isNotNull(dbSquidIndexesList.get(j)
//										.getColumn_name_1()))
//								&& (!squidIndexesList
//										.get(i)
//										.getColumn_name_1()
//										.toLowerCase()
//										.equals(dbSquidIndexesList.get(j)
//												.getColumn_name_1()))) {
//							eqFlag = false;
//							break;
//						}
//						if ((StringUtils.isNotNull(squidIndexesList.get(i)
//								.getColumn_name_2()) && StringUtils
//								.isNotNull(dbSquidIndexesList.get(j)
//										.getColumn_name_2()))
//								&& (!squidIndexesList
//										.get(i)
//										.getColumn_name_2()
//										.toLowerCase()
//										.equals(dbSquidIndexesList.get(j)
//												.getColumn_name_2()))) {
//							eqFlag = false;
//							break;
//						}
//						if ((StringUtils.isNotNull(squidIndexesList.get(i)
//								.getColumn_name_3()) && StringUtils
//								.isNotNull(dbSquidIndexesList.get(j)
//										.getColumn_name_3()))
//								&& (!squidIndexesList
//										.get(i)
//										.getColumn_name_3()
//										.toLowerCase()
//										.equals(dbSquidIndexesList.get(j)
//												.getColumn_name_3()))) {
//							eqFlag = false;
//							break;
//						}
//						if ((StringUtils.isNotNull(squidIndexesList.get(i)
//								.getColumn_name_4()) && StringUtils
//								.isNotNull(dbSquidIndexesList.get(j)
//										.getColumn_name_4()))
//								&& (!squidIndexesList
//										.get(i)
//										.getColumn_name_4()
//										.toLowerCase()
//										.equals(dbSquidIndexesList.get(j)
//												.getColumn_name_4()))) {
//							eqFlag = false;
//							break;
//						}
//						if ((StringUtils.isNotNull(squidIndexesList.get(i)
//								.getColumn_name_5()) && StringUtils
//								.isNotNull(dbSquidIndexesList.get(j)
//										.getColumn_name_5()))
//								&& (!squidIndexesList
//										.get(i)
//										.getColumn_name_5()
//										.toLowerCase()
//										.equals(dbSquidIndexesList.get(j)
//												.getColumn_name_5()))) {
//							eqFlag = false;
//							break;
//						}
//						if ((StringUtils.isNotNull(squidIndexesList.get(i)
//								.getColumn_name_6()) && StringUtils
//								.isNotNull(dbSquidIndexesList.get(j)
//										.getColumn_name_6()))
//								&& (!squidIndexesList
//										.get(i)
//										.getColumn_name_6()
//										.toLowerCase()
//										.equals(dbSquidIndexesList.get(j)
//												.getColumn_name_6()))) {
//							eqFlag = false;
//							break;
//						}
//						if ((StringUtils.isNotNull(squidIndexesList.get(i)
//								.getColumn_name_7()) && StringUtils
//								.isNotNull(dbSquidIndexesList.get(j)
//										.getColumn_name_7()))
//								&& (!squidIndexesList
//										.get(i)
//										.getColumn_name_7()
//										.toLowerCase()
//										.equals(dbSquidIndexesList.get(j)
//												.getColumn_name_7()))) {
//							eqFlag = false;
//							break;
//						}
//						if ((StringUtils.isNotNull(squidIndexesList.get(i)
//								.getColumn_name_8()) && StringUtils
//								.isNotNull(dbSquidIndexesList.get(j)
//										.getColumn_name_8()))
//								&& (!squidIndexesList
//										.get(i)
//										.getColumn_name_8()
//										.toLowerCase()
//										.equals(dbSquidIndexesList.get(j)
//												.getColumn_name_8()))) {
//							eqFlag = false;
//							break;
//						}
//						if ((StringUtils.isNotNull(squidIndexesList.get(i)
//								.getColumn_name_9()) && StringUtils
//								.isNotNull(dbSquidIndexesList.get(j)
//										.getColumn_name_9()))
//								&& (!squidIndexesList
//										.get(i)
//										.getColumn_name_9()
//										.toLowerCase()
//										.equals(dbSquidIndexesList.get(j)
//												.getColumn_name_9()))) {
//							eqFlag = false;
//							break;
//						}
//						if ((StringUtils.isNotNull(squidIndexesList.get(i)
//								.getColumn_name_10()) && StringUtils
//								.isNotNull(dbSquidIndexesList.get(j)
//										.getColumn_name_10()))
//								&& (!squidIndexesList
//										.get(i)
//										.getColumn_name_10()
//										.toLowerCase()
//										.equals(dbSquidIndexesList.get(j)
//												.getColumn_name_10()))) {
//							eqFlag = false;
//							break;
//						}
//					}
//				}
//			}
//			if (eqFlag) {
//				CommonConsts
//						.addValidationTask(new SquidValidationTask(
//								token,
//								MessageBubbleService
//										.setMessageBubble(
//												squidId,
//												squidId,
//												null,
//												MessageBubbleCode.ERROR_INDEXES_NOT_SYNCHRONIZED
//														.value()), true,
//								"索引未同步"));
//				return SyncStatusEnum.SYNCHRONIZED.value();
//			} else {
//				CommonConsts
//						.addValidationTask(new SquidValidationTask(
//								token,
//								MessageBubbleService
//										.setMessageBubble(
//												squidId,
//												squidId,
//												null,
//												MessageBubbleCode.ERROR_INDEXES_NOT_SYNCHRONIZED
//														.value()), false,
//								"索引未同步"));
//				return SyncStatusEnum.NOT_SYNCHRONIZED_HISTORY_DATA.value();
//
//			}
        }
    }

    /**
     * 比较落地后修改的信息和落地表中的信息
     *
     * @param out
     * @param adapter3
     * @param squidId
     * @param tableName
     * @param adaoterSource
     * @param dtType
     * @return
     * @author bo.dang
     */
    public Integer compareSquidTable2PersistTable(INewDBAdapter adaoterSource,
                                                  IRelationalDataManager adapter3, Integer squidId, String tableName,
                                                  String dataBaseName, DataBaseType dtType, ReturnValue out) {
        // 获取落地表中的信息
        List<String> persistColumnInfoList = this.getPersistBasicTableInfo(dtType,
                adaoterSource, tableName, dataBaseName);
        // 如果落地表为空，说明未创建落地对象
        if (StringUtils.isNull(persistColumnInfoList)) {
            out.setMessageCode(MessageCode.ERR_PERSISTTABLE_NOT_TABLENAME);
            return SyncStatusEnum.UNKNOWN.value();
            //return SyncStatusEnum.NOT_SYNCHRONIZED_NO_HISTORY_DATA.value();
        }
        // 获取修改后的Squid信息
        List<String> squidColumnInfoList = this.getSquidTableInfo(adapter3, dtType, squidId,
                tableName);
        // 如果列数目不同
        if (squidColumnInfoList.size() != persistColumnInfoList.size()) {
            CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(),
                    MessageBubbleService.setMessageBubble(squidId,
                            squidId, null,
                            MessageBubbleCode.ERROR_SYNCHRONIZED
                                    .value()), false, "列未同步"));
            return SyncStatusEnum.NOT_SYNCHRONIZED_HISTORY_DATA.value();
        } else {
            // 如果两个list相同
            if (persistColumnInfoList.toString().equals(
                    squidColumnInfoList.toString())) {
                return SyncStatusEnum.SYNCHRONIZED.value();
            }
            // 如果两个list不同
            else {
                System.out.println("persistColumnInfoList : " + persistColumnInfoList.toString());
                System.out.println("squidColumnInfoList : " + squidColumnInfoList.toString());
                // Squid中Column的列名
//				String columnName = null;
//				// 落地表中Column的列名
//				String pColumnName = null;
//				// Squid中Column信息
//				ColumnInfo columnInfo = null;
//				// 落地表中Column的信息
//				ColumnInfo pColumnInfo = null;
//				int columnId = 0;
//				for (int i = 0; i < squidColumnInfoList.size(); i++) {
//					columnInfo = squidColumnInfoList.get(i);
//					columnName = columnInfo.getName();
//					columnId = columnInfo.getId();
//					for (int j = 0; j < persistColumnInfoList.size(); j++) {
//						pColumnInfo = persistColumnInfoList.get(i);
//						pColumnName = pColumnInfo.getName();
//						// 如果列相等
//						if (columnName.equals(pColumnName)) {
//							// 如果Type不相等
//							if (columnInfo.getSystemDatatype() != pColumnInfo
//									.getSystemDatatype()) {
//								CommonConsts
//										.addValidationTask(new SquidValidationTask(
//												token,
//												MessageBubbleService
//														.setMessageBubble(
//																squidId,
//																columnId,
//																null,
//																MessageBubbleCode.ERROR_SYNCHRONIZED
//																		.value()),
//												false, columnName + "类型已改变"));
//							}
//							// 如果长度不相同
//							if (columnInfo.getLength() != pColumnInfo
//									.getLength()) {
//								CommonConsts
//										.addValidationTask(new SquidValidationTask(
//												token,
//												MessageBubbleService
//														.setMessageBubble(
//																squidId,
//																columnId,
//																null,
//																MessageBubbleCode.ERROR_SYNCHRONIZED
//																		.value()),
//												false, columnName + "长度已改变"));
//							}
//							// 如果精度不相同
//							if (columnInfo.getPrecision() != pColumnInfo
//									.getPrecision()) {
//								CommonConsts
//										.addValidationTask(new SquidValidationTask(
//												token,
//												MessageBubbleService
//														.setMessageBubble(
//																squidId,
//																columnId,
//																null,
//																MessageBubbleCode.ERROR_SYNCHRONIZED
//																		.value()),
//												false, columnName + "精度已改变"));
//							}
//							// 如果不为空改变
//							if (columnInfo.isNullable() != pColumnInfo
//									.isNullable()) {
//								CommonConsts
//										.addValidationTask(new SquidValidationTask(
//												token,
//												MessageBubbleService
//														.setMessageBubble(
//																squidId,
//																columnId,
//																null,
//																MessageBubbleCode.ERROR_SYNCHRONIZED
//																		.value()),
//												false, columnName + "是否为空已改变"));
//							}
//							// 如果Scale不相同
//							if (columnInfo.getScale() != pColumnInfo.getScale()) {
//								CommonConsts
//										.addValidationTask(new SquidValidationTask(
//												token,
//												MessageBubbleService
//														.setMessageBubble(
//																squidId,
//																columnId,
//																null,
//																MessageBubbleCode.ERROR_SYNCHRONIZED
//																		.value()),
//												false, columnName + "Scale已改变"));
//							}
//						}
//
//					}
//				}
                return SyncStatusEnum.NOT_SYNCHRONIZED_HISTORY_DATA.value();
            }
        }
    }

    public void compareIndexes() {

    }

    /**
     * 创建指定落地目标
     *
     * @param out
     * @param adapter3
     * @param list
     * @param squidId
     * @param adaoterSource
     * @throws Exception
     */
    private Boolean synchronizeSquidTable(ReturnValue out,
                                          IRelationalDataManager adapter3, List<Integer> list,
                                          Integer squidId, String tableName, INewDBAdapter adaoterSource,
                                          DataBaseType dtType, String dataBaseName) throws Exception {
        boolean is_exists = true;
        try {
            int i = adaoterSource.getRecordCount(tableName);
            if (dtType == null && i == 0) {
                is_exists = false;
            }
        } catch (Exception e) {
            // e.printStackTrace();
            logger.error(e.getMessage());
            is_exists = false;
        }
        // 消除未创建落地对象的消息泡
        // 同步
        BasicTableInfo table = this.getBasicTableInfo(adapter3, squidId,
                tableName);
        table.setDbType(dtType);
		/*
		 * if (!table.isPersistTable()){
		 * out.setMessageCode(MessageCode.ERR_PERSISTTABLE_UNKNOWN); }else{
		 */
        // 1.如果存在历史数据，需要删除原来的落地表
        if (is_exists) {
            adaoterSource.deleteTable(tableName);
        }
        // 创建落地表
        adaoterSource.createTable(table, out);
        CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(),
                MessageBubbleService.setMessageBubble(squidId, squidId, null,
                        MessageBubbleCode.ERROR_SYNCHRONIZED.value()), true,
                "未创建落地对象"));
		/*
		 * if (out.getMessageCode()!=MessageCode.SUCCESS){ list.clear();
		 * list.add(squidId); }
		 */
        // }
        // 2.创建索引
        if (dtType != null) {
            SquidIndexsServiceSub squidIndexsServiceSub = new SquidIndexsServiceSub(
                    TokenUtil.getToken());
            List<SquidIndexes> squidIndexesList = squidIndexsServiceSub
                    .getSquidIndexsListBySquidId(squidId);

            Integer indexStatus = comparDBIndexes2SquidIndexes(adaoterSource,
                    adapter3, squidId, tableName, dataBaseName);
            if (SyncStatusEnum.NOT_PERSISTENT.value() == indexStatus
                    || SyncStatusEnum.NOT_SYNCHRONIZED_HISTORY_DATA.value() == indexStatus
                    || SyncStatusEnum.NOT_SYNCHRONIZED_NO_HISTORY_DATA.value() == indexStatus) {
                // 创建落地数据库索引
                createSquidIndexsToDB(squidIndexsServiceSub, adapter3,
                        adaoterSource, dtType, tableName, squidIndexesList);
                CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(),
                        MessageBubbleService.setMessageBubble(squidId, squidId,
                                null,
                                MessageBubbleCode.ERROR_INDEXES_NOT_SYNCHRONIZED
                                        .value()), true, "索引未同步"));
            }
        }
        adaoterSource.close();
        return true;
    }

    /**
     * 在落地数据库中创建索引
     *
     * @param squidIndexsServiceSub
     * @param adapter3
     * @param adaoterSource
     * @param dtType
     * @param tableName
     * @param squidIndexesList
     * @author bo.dang
     */
    public void createSquidIndexsToDB(
            SquidIndexsServiceSub squidIndexsServiceSub,
            IRelationalDataManager adapter3, INewDBAdapter adaoterSource,
            DataBaseType dtType, String tableName,
            List<SquidIndexes> squidIndexesList) {
        for (int i = 0; i < squidIndexesList.size(); i++) {

            TableIndex index = squidIndexsServiceSub.getColumnNameByIndex(
                    adapter3, dtType.value(), squidIndexesList.get(i));
            if (index != null) {
                try {
                    adaoterSource.deleteIndex(tableName, index.getIndexName());
                } catch (Exception e) {
                    logger.error("删除未存在的index：" + index.getIndexName());
                }
                adaoterSource.addIndex(tableName, index);
            }
        }
    }
    /**
     * 重写获取sourceTable方法，防止大数据量的时候，通信超时
     */
}

