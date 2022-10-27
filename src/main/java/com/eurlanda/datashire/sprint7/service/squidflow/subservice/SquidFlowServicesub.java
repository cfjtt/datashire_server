package com.eurlanda.datashire.sprint7.service.squidflow.subservice;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.dao.IReferenceColumnDao;
import com.eurlanda.datashire.dao.ISourceTableDao;
import com.eurlanda.datashire.dao.ISquidDao;
import com.eurlanda.datashire.dao.ISquidFlowDao;
import com.eurlanda.datashire.dao.ITransformationInputsDao;
import com.eurlanda.datashire.dao.ITransformationLinkDao;
import com.eurlanda.datashire.dao.IUrlDao;
import com.eurlanda.datashire.dao.IVariableDao;
import com.eurlanda.datashire.dao.impl.ReferenceColumnDaoImpl;
import com.eurlanda.datashire.dao.impl.SourceTableDaoImpl;
import com.eurlanda.datashire.dao.impl.SquidDaoImpl;
import com.eurlanda.datashire.dao.impl.SquidFlowDaoImpl;
import com.eurlanda.datashire.dao.impl.TransformationInputsDaoImpl;
import com.eurlanda.datashire.dao.impl.TransformationLinkDaoImpl;
import com.eurlanda.datashire.dao.impl.UrlDaoImpl;
import com.eurlanda.datashire.dao.impl.VariableDaoImpl;
import com.eurlanda.datashire.entity.*;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.enumeration.MessageBubbleCode;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.server.model.PivotSquid;
import com.eurlanda.datashire.server.model.SamplingSquid;
import com.eurlanda.datashire.server.utils.TokenUtil;
import com.eurlanda.datashire.sprint7.packet.PushMessagePacket;
import com.eurlanda.datashire.sprint7.service.squidflow.AbstractRepositoryService;
import com.eurlanda.datashire.sprint7.service.squidflow.JobScheduleProcess;
import com.eurlanda.datashire.sprint7.service.squidflow.LockSquidFlowProcess;
import com.eurlanda.datashire.entity.StatisticsDataMapColumn;
import com.eurlanda.datashire.entity.StatisticsParameterColumn;
import com.eurlanda.datashire.entity.StatisticsSquid;
import com.eurlanda.datashire.entity.UserDefinedMappingColumn;
import com.eurlanda.datashire.entity.UserDefinedParameterColumn;
import com.eurlanda.datashire.entity.UserDefinedSquid;
import com.eurlanda.datashire.utility.CalcSquidFlowStatus;
import com.eurlanda.datashire.utility.CommonConsts;
import com.eurlanda.datashire.utility.EnumException;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import com.eurlanda.datashire.utility.StringUtils;
import com.eurlanda.datashire.validator.SquidValidationTask;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * SquidFlow处理类
 *
 * @author lei.bin
 */
public class SquidFlowServicesub extends AbstractRepositoryService implements ISquidFlowService {
    private String token;

    public SquidFlowServicesub(String token) {
        super(token);
        this.token = token;
    }

    public SquidFlowServicesub(IRelationalDataManager adapter) {
        super(adapter);
    }

    public SquidFlowServicesub(String token, IRelationalDataManager adapter) {
        super(token, adapter);
        this.token = token;
    }

    public static String getReportNodeName(IRelationalDataManager adapter, int folderId) {
        String nodeName = "";
        List<String> list = new ArrayList<String>();
        try {
            //adapter.openSession();
            Map<Integer, Integer> tempMap = new HashMap<Integer, Integer>();
            //tempMap.put(folderId, folderId);
            getReportFolderById(adapter, folderId, list, tempMap);
            if (list != null && list.size() > 0) {
                int cnt = list.size() - 1;
                for (int i = cnt; i >= 0; i--) {
                    nodeName += "/" + list.get(i);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return nodeName;
    }

    public static void getReportFolderById(IRelationalDataManager adapter, int id,
                                           List<String> list, Map<Integer, Integer> tempMap) throws SQLException {
        if (tempMap.containsKey(id) || id == -1) {
            return;
        }
        String sql = "select folder_name,pid from ds_sys_report_folder where id=" + id;
        Map<String, Object> map = adapter.query2Object(true, sql, null);
        if (map.containsKey("PID") && StringUtils.isNotNull(map.get("PID"))) {
            int pid = Integer.parseInt(map.get("PID") + "");
            String name = map.get("FOLDER_NAME") + "";
            list.add(name);
            tempMap.put(id, id);
            getReportFolderById(adapter, pid, list, tempMap);
        } else if (map.containsKey("FOLDER_NAME")) {
            String name = map.get("FOLDER_NAME") + "";
            list.add(name);
        }
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    /**
     * 根据squidFlowId删除squidflow以及关联子类
     *
     * @param squidFlowId
     * @param out
     * @return
     */
    public boolean deleteSquidFlow(int squidFlowId, int repositoryId, ReturnValue out) {
        boolean delete = true;
        Map<String, String> paramMap = new HashMap<String, String>();
        List<SquidFlowStatus> statusList = null;
        try {
            if (0 != repositoryId) {
                // 根据repositoryId和squidFlowId去系统表查询squidflow的状态
                LockSquidFlowProcess lockSquidFlowProcess = new LockSquidFlowProcess();
                statusList = lockSquidFlowProcess.getSquidFlowStatus2(
                        repositoryId, squidFlowId, out, adapter);
                Map<String, Object> map = CalcSquidFlowStatus
                        .calcStatus(statusList);
                if (Integer.parseInt(map.get("schedule").toString()) > 0
                        || Integer.parseInt(map.get("checkout").toString()) > 0) {// 调度的个数大于0或者已经加锁
                    out.setMessageCode(MessageCode.WARN_DELETESQUIDFLOW);
                    return true;
                } else {
                    // 查询出squid的id
                    paramMap.put("squid_flow_id", String.valueOf(squidFlowId));
                    List<Squid> squids = adapter.query2List(true, paramMap,
                            Squid.class);
                    if (null != squids && squids.size() > 0) {
                        SquidServicesub squidService = new SquidServicesub(
                                TokenUtil.getToken(), adapter);
                        for (Squid squid : squids) {
                            delete = squidService.deleteSquid(squid.getId(),
                                    out);
                        }
                    }
                    if (delete) {
                        // 删除squidflow
                        paramMap.clear();
                        paramMap.put("id", String.valueOf(squidFlowId));
                        return adapter.delete(paramMap, SquidFlow.class) > 0 ? true
                                : false;
                    }
                }
            }
        } catch (Exception e) {
            logger.error(
                    "[删除deleteSquidFlow=========================================exception]",
                    e);
            try {
                adapter.rollback();
            } catch (SQLException e1) {
                logger.error("rollback err!", e1);
            }
            out.setMessageCode(MessageCode.SQL_ERROR);
            return false;
        } finally {
            adapter.closeSession();
        }
        return delete;
    }

    /**
     * 根据squid_flow_id获取当前squid flow详细信息
     * 运行SquidFlow时调用，将SquidFlow传送至变换引擎
     *
     * @param squid_flow_id
     * @return
     */
    public SquidFlow getSquidFlow(int squid_flow_id) {
        int sourceCnt = 0; // source database squid counts
        int extractCnt = 0; // extract squid counts
        int stageCnt = 0; // stage squid counts
        int linkCnt = 0; // squid link counts

        List<Integer> squidId = new ArrayList<Integer>();
        Map<String, Object> params = new HashMap<String, Object>(); // 查询参数

        adapter.openSession();
        params.put("squid_flow_id", squid_flow_id);
        List<SquidLink> link = adapter.query2List2(true, params, SquidLink.class);
        if (link != null && !link.isEmpty()) {
            linkCnt = link.size();
            for (int i = 0; i < linkCnt; i++) {
                if (!squidId.contains(link.get(i).getFrom_squid_id())) {
                    squidId.add(link.get(i).getFrom_squid_id());
                }
                if (!squidId.contains(link.get(i).getTo_squid_id())) {
                    squidId.add(link.get(i).getTo_squid_id());
                }
            }
        } else {
            logger.warn("no squid link! no data to translate. squid_flow_id = " + squid_flow_id);
            return null;
        }
        logger.debug("squid id list with link: " + squidId);

        params.clear();
        params.put("id", squidId);
        params.put("squid_type_id", SquidTypeEnum.EXTRACT.value());
        List<Squid> extract = adapter.query2List2(true, params, Squid.class);
        if (extract != null && !extract.isEmpty()) {
            extractCnt = extract.size();
            ExtractServicesub extractService = new ExtractServicesub(TokenUtil.getToken());
            for (int i = 0; i < extractCnt; i++) {
                TableExtractSquid es = new TableExtractSquid(extract.get(i));
                extractService.setExtractSquidData(null, es, true, adapter);
                es.setSquid_type(SquidTypeEnum.EXTRACT.value());
                extract.set(i, es);
            }
        }
        logger.debug("extract: " + extract);

        params.put("squid_type_id", SquidTypeEnum.STAGE.value());
        List<Squid> stage = adapter.query2List2(true, params, Squid.class);
        if (stage != null && !stage.isEmpty()) {
            stageCnt = stage.size();
            StageSquidServicesub stageSquidService = new StageSquidServicesub(TokenUtil.getToken());
            for (int i = 0; i < stageCnt; i++) {
                StageSquid ss = new StageSquid();
                ss.setId(stage.get(i).getId());

                stageSquidService.setStageSquidData(null, true, adapter, ss);
                ss.setSquid_type(SquidTypeEnum.STAGE.value());
                stage.set(i, ss);
            }
        }

        logger.debug("stage: " + stage);

        params.clear();
        params.put("id", squidId);
        List<DbSquid> dbSquid = adapter.query2List2(true, params, DbSquid.class);
        adapter.closeSession();

        if (dbSquid != null && !dbSquid.isEmpty()) {
            sourceCnt = dbSquid.size();
            for (int i = 0; i < sourceCnt; i++) {
                DbSquid s = dbSquid.get(i);
                if (s != null) {
                    s.setSquid_type(SquidTypeEnum.DBSOURCE.value());
                }
            }
        }

        List<Squid> squidList = new ArrayList<Squid>();
        squidList.addAll(extract);
        squidList.addAll(stage);
        squidList.addAll(dbSquid);

        SquidFlow squidFlow = new SquidFlow();
        squidFlow.setId(squid_flow_id);
        squidFlow.setSquidLinkList(link);
        squidFlow.setSquidList(squidList);

        logger.debug("load squid flow ok! sourceCnt=" + sourceCnt + ", extractCnt=" + extractCnt + ", stageCnt=" + stageCnt + ", linkCnt=" + linkCnt);
        return squidFlow;
    }

    /**
     * 获取ExceptionSquid数据
     *
     * @param adapter3
     * @param squidFlowId
     * @param squidIds
     * @param out
     * @return
     * @throws Exception
     */
    public List<ExceptionSquid> getExceptionSquidListByTp(IRelationalDataManager adapter3,
                                                          int squidFlowId, String squidIds, ReturnValue out) throws Exception {
        List<ExceptionSquid> rsList = new ArrayList<ExceptionSquid>();
        ISquidDao squidDao = new SquidDaoImpl(adapter3);
        rsList = squidDao.getSquidListForParams(SquidTypeEnum.EXCEPTION.value(),
                squidFlowId, squidIds, ExceptionSquid.class);
        if (rsList != null && rsList.size() > 0) {
            for (ExceptionSquid exceptionSquid : rsList) {
                StageSquidService.setExceptionSquidData(adapter3, exceptionSquid);
            }
        }
        return rsList;
    }

    /**
     * 获取ExceptionSquid数据
     *
     * @param adapter3
     * @param squidFlowId
     * @param squidIds
     * @param out
     * @return
     * @throws Exception
     */
    public static List<ExceptionSquid> getExceptionSquidListLimit(IRelationalDataManager adapter3,
                                                                  int squidFlowId, String squidIds, ReturnValue out) throws Exception {
        //获得总的数据个数
        List<ExceptionSquid> rsList = new ArrayList<ExceptionSquid>();
        ISquidDao squidDao = new SquidDaoImpl(adapter3);
        rsList = squidDao.getSquidListForParams(SquidTypeEnum.EXCEPTION.value(),
                squidFlowId, squidIds, ExceptionSquid.class);
        return rsList;
    }

    /**
     * 加载squid flow 推送所有squid
     * extract 分批推送（5个/次）
     * source 1个/次 (table list可能数据量较大，所以单独推送)
     * stage、destination 一次推送所有
     *
     * @param key
     * @param squid_flow_id
     */
    public void pushAllSquid(final String key, final int squid_flow_id, final int repository_id) throws Exception {
        ReturnValue out = new ReturnValue();
        IRelationalDataManager adapter = null;
        try {
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            ISquidFlowDao squidFlowDao = new SquidFlowDaoImpl(adapter);
            List<Map<String, Object>> typeObj = squidFlowDao.getSquidTypeBySquidFlow(squid_flow_id);
            logger.info("squidType list : " + typeObj.toString());
            if (typeObj != null && typeObj.size() > 0) {
                this.pushSquidData(out, typeObj, repository_id, squid_flow_id, key, adapter, "1001", "0111", "");
            }
        } catch (Exception e) {
            logger.error("load squid flow error", e);
            out.setMessageCode(MessageCode.SQL_ERROR);
        } finally {
            adapter.closeSession();
        }
    }

    /**
     * 推送所有的消息泡
     *
     * @param key
     * @param squid_flow_id
     * @author bo.dang
     * @date 2014年6月11日
     */
    public void pushAllMessageBubble(final String key, final int squid_flow_id) {
        adapter.openSession();
        // 是否执行消息泡校验逻辑(删除SquidFlow的时候不需要推送消息泡)
        CommonConsts.executeValidationTaskFlag = true;
        // SquidFlow打开的时候，如果消息泡的状态是true,那么不向客户端发送请求
        CommonConsts.executePushMessageBubbleFlag = false;
        // 孤立消息泡验证
        // 2.的 filter 过滤条件
        CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(), adapter, MessageBubbleService.setMessageBubble(0, squid_flow_id, null, MessageBubbleCode.ERROR_SQL_SYNTAX_FILTER.value())));
        // 2.校验Join
        CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(), adapter, MessageBubbleService.setMessageBubble(0, squid_flow_id, null, MessageBubbleCode.ERROR_SQL_SYNTAX_JOIN.value())));
        // 2.32.33incremental expression 校验增量抽取表达式
        CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(), adapter, MessageBubbleService.setMessageBubble(0, squid_flow_id, null, MessageBubbleCode.ERROR_SQL_SYNTAX_INCREMENT.value())));
        // 3.校验孤立的Squid
        CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(), adapter, MessageBubbleService.setMessageBubble(0, squid_flow_id, null, MessageBubbleCode.WARN_SQUID_NO_LINK.value())));
        // 4.校验transformation是否孤立
        CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(), adapter, MessageBubbleService.setMessageBubble(0, squid_flow_id, null, MessageBubbleCode.WARN_TRANSFORMATION_NO_LINK.value())));
        // 5.StageSquid中没定义transformation的column
        CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(), adapter, MessageBubbleService.setMessageBubble(0, squid_flow_id, null, MessageBubbleCode.WARN_COLUMN_NO_TRANSFORMATION.value())));
        // 6.dataSquid落地的表名
        CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(), adapter, MessageBubbleService.setMessageBubble(0, squid_flow_id, null, MessageBubbleCode.ERROR_NO_DB_TABLE_NAME.value())));
        // 8.29.dataSquid索引
        CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(), adapter, MessageBubbleService.setMessageBubble(0, squid_flow_id, null, MessageBubbleCode.WARN_DATASQUID_NO_SQUIDINDEXES.value())));
        // 10.校验HostAddress
        CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(), adapter, MessageBubbleService.setMessageBubble(0, squid_flow_id, null, MessageBubbleCode.ERROR_SQUID_CONNECTION_NO_HOST.value())));
        // 11.校验RDBMSType
        CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(), adapter, MessageBubbleService.setMessageBubble(0, squid_flow_id, null, MessageBubbleCode.ERROR_SQUID_CONNECTION_NO_RDBMSTYPE.value())));
        // 12.校验DataBaseName
        CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(), adapter, MessageBubbleService.setMessageBubble(0, squid_flow_id, null, MessageBubbleCode.ERROR_SQUID_CONNECTION_NO_DATABASE_NAME.value())));
        // 15.18.21.校验FilePath
        CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(), adapter, MessageBubbleService.setMessageBubble(0, squid_flow_id, null, MessageBubbleCode.ERROR_SQUID_CONNECTION_NO_FILE_PATH.value())));
        // 23.校验ServiceProvider微博提供商设置
        CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(), adapter, MessageBubbleService.setMessageBubble(0, squid_flow_id, null, MessageBubbleCode.ERROR_WEIBO_CONNECTION_NO_SERVICE_PROVIDER.value())));
        // 25.校验WebConnection的URL未设置
        CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(), adapter, MessageBubbleService.setMessageBubble(0, squid_flow_id, null, MessageBubbleCode.WARN_WEB_CONNECTION_NO_URL_LIST.value())));
        // 31.异常squid已定义
        CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(), adapter, MessageBubbleService.setMessageBubble(0, squid_flow_id, null, MessageBubbleCode.WARN_DATA_SQUID_NO_EXCEPTION_SQUID.value())));
        // 34.文档抽取Squid(DocExtractSquid)验证Delimiter
        CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(), adapter, MessageBubbleService.setMessageBubble(0, squid_flow_id, null, MessageBubbleCode.ERROR_DOC_EXTRACT_SQUID_NO_DELIMITER.value())));
        // 35.FieldLength字段长度设置非法
        CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(), adapter, MessageBubbleService.setMessageBubble(0, squid_flow_id, null, MessageBubbleCode.ERROR_DOC_EXTRACT_SQUID_NO_FIELD_LENGTH.value())));
        // 36.校验WebLogExtractSquid是否完成获取元数据
        CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(), adapter, MessageBubbleService.setMessageBubble(0, squid_flow_id, null, MessageBubbleCode.ERROR_WEBLOG_EXTRACT_SQUID_NO_SOURCE_DATA.value())));
        // 37.校验显式SquidLink直接构成闭环和复杂闭环
        CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(), adapter, MessageBubbleService.setMessageBubble(0, squid_flow_id, null, MessageBubbleCode.ERROR_SQUID_LINK_LOOP.value())));
        // 39.40.TranLink显式和阴式闭环校验
        CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(), adapter, MessageBubbleService.setMessageBubble(0, squid_flow_id, null, MessageBubbleCode.ERROR_TRAN_LINK_LOOP.value())));
        // 41.Transform的实参个数和其定义的形参个数
        CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(), adapter, MessageBubbleService.setMessageBubble(0, squid_flow_id, null, MessageBubbleCode.ERROR_TRAN_PARAMETERS.value())));
        // 42.校验TermExtract表达式设置语法错误
        CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(), adapter, MessageBubbleService.setMessageBubble(0, squid_flow_id, null, MessageBubbleCode.ERROR_TERMEXTRACT_REG_EXPRESSION_SYNTAX.value())));
        // 43.校验发布路径未设置
        CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(), adapter, MessageBubbleService.setMessageBubble(0, squid_flow_id, null, MessageBubbleCode.WARN_REPORT_SQUID_NO_PUBLISHING_FOLDER.value())));
        // 44.校验报表模板定义
        CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(), adapter, MessageBubbleService.setMessageBubble(0, squid_flow_id, null, MessageBubbleCode.ERROR_REPORT_SQUID_NO_TEMPLATE_DEFINITION.value())));
        // 47.校验Training_percentage训练集所占比例
        CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(), adapter, MessageBubbleService.setMessageBubble(0, squid_flow_id, null, MessageBubbleCode.WARN_DATA_MININGSQUID_TRAINING_PERCENTAGE.value())));
        // 48.校验ModelSquidId
        CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(), adapter, MessageBubbleService.setMessageBubble(0, squid_flow_id, null, MessageBubbleCode.ERROR_PREDICT_MODEL_SQUID_BY_DELETED.value())));
        // 49.报表squid的源数据被修改，可能导致报表运行不正常
        CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(), adapter, MessageBubbleService.setMessageBubble(0, squid_flow_id, null, MessageBubbleCode.WARN_REPORT_SQUID_COLUMNS_BY_DELETED.value())));
        // 50.校验Column的映射类型
        CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(), adapter, MessageBubbleService.setMessageBubble(0, squid_flow_id, null, MessageBubbleCode.ERROR_COLUMN_DATA_TYPE.value())));
        // 51.Aggregation或者Group设置
        CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(), adapter, MessageBubbleService.setMessageBubble(0, squid_flow_id, null, MessageBubbleCode.ERROR_AGGREGATION_OR_GROUP.value())));
        adapter.closeSession();
        //CommonConsts.executePushMessageBubbleFlag = true;

    }

    /**
     * 推送SquidFlow里面的Squid的数据，封装为单独方法方便多次调用
     *
     * @param out
     * @param typeObj
     * @param squid_flow_id
     * @param key
     * @throws EnumException
     * @throws Exception
     */
    public void pushSquidData(ReturnValue out, List<Map<String, Object>> typeObj,
                              final int repository_id, final int squid_flow_id, final String key,
                              IRelationalDataManager adapter, final String cmd1, final String cmd2
            , final String squidIds) throws EnumException, Exception {
        final Map<String, Object> outputMap = new HashMap<String, Object>();
        long start = System.currentTimeMillis();
        ISquidDao squidDao = new SquidDaoImpl(adapter);
        ISourceTableDao sourceTableDao = new SourceTableDaoImpl(adapter);
        IVariableDao variableDao = new VariableDaoImpl(adapter);
        for (Map<String, Object> map : typeObj) {
            final int squidTypeId = Integer.valueOf(map.get("SQUID_TYPE_ID") + "");
            if (squidTypeId == SquidTypeEnum.EXTRACT.value()
                    || squidTypeId == SquidTypeEnum.XML_EXTRACT.value()
                    || squidTypeId == SquidTypeEnum.WEBLOGEXTRACT.value()
                    || squidTypeId == SquidTypeEnum.WEBEXTRACT.value()
                    || squidTypeId == SquidTypeEnum.WEIBOEXTRACT.value()) {
                DSObjectType obj = DSObjectType.parse(squidTypeId);
                List<DataSquid> squidList = squidDao.getSquidListForParams(squidTypeId,
                        squid_flow_id, squidIds, DataSquid.class);
                List<DataSquid> dsList = new ArrayList<>();
                for (DataSquid dataSquid : squidList) {
                    if (dsList.size() > 0 && dsList.size() % 20 == 0) {
                        outputMap.clear();
                        outputMap.put("Squids", dsList);
                        PushMessagePacket.pushMap(outputMap, obj, cmd1, cmd2, key, TokenUtil.getToken());
                        logger.info("ExtractSquid 加载时间:" + (System.currentTimeMillis() - start));
                        start = System.currentTimeMillis();
                        dsList.clear();
                    }
                    ExtractService.setAllExtractSquidData(adapter, dataSquid);
                    dsList.add(dataSquid);
                }
                if (dsList.size() != 0) {
                    outputMap.clear();
                    outputMap.put("Squids", dsList);
                    PushMessagePacket.pushMap(outputMap, obj, cmd1, cmd2, key, TokenUtil.getToken());
                    logger.info("ExtractSquid 加载时间:" + (System.currentTimeMillis() - start));
                    start = System.currentTimeMillis();
                }

            } else if (squidTypeId == SquidTypeEnum.HIVEEXTRACT.value()) {
                DSObjectType obj = DSObjectType.parse(squidTypeId);
                List<SystemHiveExtractSquid> squidList = squidDao.getSquidListForParams(squidTypeId,
                        squid_flow_id, squidIds, SystemHiveExtractSquid.class);
                List<SystemHiveExtractSquid> dsList = new ArrayList<>();
                for (SystemHiveExtractSquid dataSquid : squidList) {
                    if (dsList.size() > 0 && dsList.size() % 20 == 0) {
                        outputMap.clear();
                        outputMap.put("Squids", dsList);
                        PushMessagePacket.pushMap(outputMap, obj, cmd1, cmd2, key, TokenUtil.getToken());
                        logger.info("ExtractSquid 加载时间:" + (System.currentTimeMillis() - start));
                        start = System.currentTimeMillis();
                        dsList.clear();
                    }
                    ExtractService.setAllExtractSquidData(adapter, dataSquid);
                    dsList.add(dataSquid);
                }
                if (dsList.size() != 0) {
                    outputMap.clear();
                    outputMap.put("Squids", dsList);
                    PushMessagePacket.pushMap(outputMap, obj, cmd1, cmd2, key, TokenUtil.getToken());
                    logger.info("Hive ExtractSquid 加载时间:" + (System.currentTimeMillis() - start));
                    start = System.currentTimeMillis();
                }
            } else if (squidTypeId == SquidTypeEnum.CASSANDRA_EXTRACT.value()) {
                DSObjectType obj = DSObjectType.parse(squidTypeId);
                List<CassandraExtractSquid> squidList = squidDao.getSquidListForParams(squidTypeId,
                        squid_flow_id, squidIds, CassandraExtractSquid.class);
                List<CassandraExtractSquid> dsList = new ArrayList<>();
                for (CassandraExtractSquid dataSquid : squidList) {
                    if (dsList.size() > 0 && dsList.size() % 20 == 0) {
                        outputMap.clear();
                        outputMap.put("Squids", dsList);
                        PushMessagePacket.pushMap(outputMap, obj, cmd1, cmd2, key, TokenUtil.getToken());
                        logger.info("Cassandra ExtractSquid 加载时间:" + (System.currentTimeMillis() - start));
                        start = System.currentTimeMillis();
                        dsList.clear();
                    }
                    ExtractService.setAllExtractSquidData(adapter, dataSquid);
                    dsList.add(dataSquid);
                }
                if (dsList.size() != 0) {
                    outputMap.clear();
                    outputMap.put("Squids", dsList);
                    PushMessagePacket.pushMap(outputMap, obj, cmd1, cmd2, key, TokenUtil.getToken());
                    logger.info("ExtractSquid 加载时间:" + (System.currentTimeMillis() - start));
                    start = System.currentTimeMillis();
                }
            } else if (squidTypeId == SquidTypeEnum.HBASEEXTRACT.value()) {
                List<HBaseExtractSquid> squidList = squidDao.getSquidListForParams(squidTypeId, squid_flow_id, squidIds, HBaseExtractSquid.class);
                List<HBaseExtractSquid> returnSquidList = new ArrayList<>();
                //50推送一个
                for (int i = 0; i < squidList.size(); i++) {
                    HBaseExtractSquid HbaseSquid = squidList.get(i);
                    ExtractService.setAllExtractSquidData(adapter, HbaseSquid);
                    if (i > 0 && i % 50 == 0) {
                        outputMap.clear();
                        outputMap.put("Squids", returnSquidList);
                        PushMessagePacket.pushMap(outputMap, DSObjectType.HBASEEXTRACT, cmd1, cmd2, key, TokenUtil.getToken());
                        returnSquidList.clear();
                    }
                    returnSquidList.add(HbaseSquid);
                }
                if (returnSquidList.size() > 0) {
                    outputMap.clear();
                    outputMap.put("Squids", returnSquidList);
                    PushMessagePacket.pushMap(outputMap, DSObjectType.HBASEEXTRACT, cmd1, cmd2, key, TokenUtil.getToken());
                }
                returnSquidList.clear();
                squidList.clear();
                logger.info("DocExtractSquid 加载时间:" + (System.currentTimeMillis() - start));
                start = System.currentTimeMillis();
            } else if (squidTypeId == SquidTypeEnum.KAFKAEXTRACT.value()) {
                List<KafkaExtractSquid> squidList = squidDao.getSquidListForParams(squidTypeId, squid_flow_id, squidIds, KafkaExtractSquid.class);
                List<KafkaExtractSquid> returnSquidList = new ArrayList<>();
                //50推送一个
                for (int i = 0; i < squidList.size(); i++) {
                    KafkaExtractSquid kafkaSquid = squidList.get(i);
                    ExtractService.setAllExtractSquidData(adapter, kafkaSquid);
                    if (i > 0 && i % 50 == 0) {
                        outputMap.clear();
                        outputMap.put("Squids", returnSquidList);
                        PushMessagePacket.pushMap(outputMap, DSObjectType.KAFKAEXTRACT, cmd1, cmd2, key, TokenUtil.getToken());
                        returnSquidList.clear();
                    }
                    returnSquidList.add(kafkaSquid);
                }
                if (returnSquidList.size() > 0) {
                    outputMap.clear();
                    outputMap.put("Squids", returnSquidList);
                    PushMessagePacket.pushMap(outputMap, DSObjectType.KAFKAEXTRACT, cmd1, cmd2, key, TokenUtil.getToken());
                }
                returnSquidList.clear();
                squidList.clear();
                logger.info("DocExtractSquid 加载时间:" + (System.currentTimeMillis() - start));
                start = System.currentTimeMillis();
            } else if (squidTypeId == SquidTypeEnum.DOC_EXTRACT.value()) {
                List<DocExtractSquid> squidList = squidDao.getSquidListForParams(squidTypeId, squid_flow_id, squidIds, DocExtractSquid.class);
                List<DocExtractSquid> returnSquidList = new ArrayList<>();
                //50个推送一次
                for (int i = 0; i < squidList.size(); i++) {
                    DocExtractSquid docExtractSquid = squidList.get(i);
                    ExtractService.setDocExtractSquidData(adapter, docExtractSquid);
                    if (i > 0 && i % 50 == 0) {
                        outputMap.clear();
                        outputMap.put("Squids", returnSquidList);
                        PushMessagePacket.pushMap(outputMap, DSObjectType.DOC_EXTRACT, cmd1, cmd2, key, TokenUtil.getToken());
                        returnSquidList.clear();
                    }
                    returnSquidList.add(docExtractSquid);
                }
                if (returnSquidList.size() > 0) {
                    outputMap.clear();
                    outputMap.put("Squids", returnSquidList);
                    PushMessagePacket.pushMap(outputMap, DSObjectType.DOC_EXTRACT, cmd1, cmd2, key, TokenUtil.getToken());
                }
                returnSquidList.clear();
                squidList.clear();
                logger.info("DocExtractSquid 加载时间:" + (System.currentTimeMillis() - start));
                start = System.currentTimeMillis();
            } else if (squidTypeId == SquidTypeEnum.MONGODBEXTRACT.value()) {
                List<MongodbExtractSquid> squidList = squidDao.getSquidListForParams(squidTypeId, squid_flow_id, squidIds, MongodbExtractSquid.class);
                List<MongodbExtractSquid> returnSquidList = new ArrayList<>();
                //50个推送一次
                for (int i = 0; i < squidList.size(); i++) {
                    MongodbExtractSquid mongodb = squidList.get(i);
                    ExtractService.setMongodbExtractSquidData(adapter, mongodb);
                    if (i > 0 && i % 50 == 0) {
                        outputMap.clear();
                        outputMap.put("Squids", returnSquidList);
                        PushMessagePacket.pushMap(outputMap, DSObjectType.MONGODBEXTRACT, cmd1, cmd2, key, TokenUtil.getToken());
                        returnSquidList.clear();
                    }
                    returnSquidList.add(mongodb);

                }
                if (returnSquidList.size() > 0) {
                    outputMap.clear();
                    outputMap.put("Squids", returnSquidList);
                    PushMessagePacket.pushMap(outputMap, DSObjectType.MONGODBEXTRACT, cmd1, cmd2, key, TokenUtil.getToken());
                }
                returnSquidList.clear();
                squidList.clear();
                logger.info("MONGODBEXTRACT 加载时间:" + (System.currentTimeMillis() - start));
                start = System.currentTimeMillis();
            } else if (squidTypeId == SquidTypeEnum.WEBSERVICEEXTRACT.value()) {
                List<WebserviceExtractSquid> webserviceExtractSquid = squidDao.getSquidListForParams(squidTypeId, squid_flow_id, squidIds, WebserviceExtractSquid.class);
                List<WebserviceExtractSquid> returnSquidList = new ArrayList<>();
                if (webserviceExtractSquid != null && webserviceExtractSquid.size() > 0) {
                    //50推送一次
                    for (int i = 0; i < webserviceExtractSquid.size(); i++) {
                        WebserviceExtractSquid wes = webserviceExtractSquid.get(i);
                        ExtractService.setAllExtractSquidData(adapter, wes);
                        ExtractService.setDSFCS(adapter, wes);
                        if (i > 0 && i % 50 == 0) {
                            outputMap.clear();
                            outputMap.put("Squids", returnSquidList);
                            PushMessagePacket.pushMap(outputMap, DSObjectType.valueOf(squidTypeId), cmd1, cmd2, key, TokenUtil.getToken());
                            returnSquidList.clear();
                        }
                        returnSquidList.add(wes);
                    }
                    if (returnSquidList.size() > 0) {
                        outputMap.clear();
                        outputMap.put("Squids", returnSquidList);
                        PushMessagePacket.pushMap(outputMap, DSObjectType.valueOf(squidTypeId), cmd1, cmd2, key, TokenUtil.getToken());
                    }
                    returnSquidList.clear();
                    webserviceExtractSquid.clear();
                }
                logger.info("dataMiningSquidList 加载时间:" + (System.currentTimeMillis() - start));
                start = System.currentTimeMillis();
            } else if (squidTypeId == SquidTypeEnum.HTTPEXTRACT.value()) {
                List<HttpExtractSquid> listHttpExtractSquid = squidDao.getSquidListForParams(squidTypeId, squid_flow_id, squidIds, HttpExtractSquid.class);
                List<HttpExtractSquid> returnSquidList = new ArrayList<>();
                if (listHttpExtractSquid != null && listHttpExtractSquid.size() > 0) {
                    for (int i = 0; i < listHttpExtractSquid.size(); i++) {
                        HttpExtractSquid wes = listHttpExtractSquid.get(i);
                        ExtractService.setAllExtractSquidData(adapter, wes);
                        ExtractService.setDSFCS(adapter, wes);
                        if (i > 0 && i % 50 == 0) {
                            outputMap.clear();
                            outputMap.put("Squids", returnSquidList);
                            PushMessagePacket.pushMap(outputMap, DSObjectType.valueOf(squidTypeId), cmd1, cmd2, key, TokenUtil.getToken());
                            returnSquidList.clear();
                        }
                        returnSquidList.add(wes);
                    }
                    if (returnSquidList.size() > 0) {
                        outputMap.clear();
                        outputMap.put("Squids", returnSquidList);
                        PushMessagePacket.pushMap(outputMap, DSObjectType.valueOf(squidTypeId), cmd1, cmd2, key, TokenUtil.getToken());
                    }
                    returnSquidList.clear();
                    listHttpExtractSquid.clear();
                }
                logger.info("dataMiningSquidList 加载时间:" + (System.currentTimeMillis() - start));
                start = System.currentTimeMillis();
            } else if (squidTypeId == SquidTypeEnum.DBSOURCE.value() || squidTypeId == SquidTypeEnum.CLOUDDB.value() || squidTypeId == SquidTypeEnum.TRAININGDBSQUID.value()) {
                List<DBSourceSquid> squidList = squidDao.getSquidListForParams(squidTypeId, squid_flow_id, squidIds, DBSourceSquid.class);
                List<DBSourceSquid> returnSquidList = new ArrayList<DBSourceSquid>();
                //每50个推送一次
                for (int i = 0; i < squidList.size(); i++) {
                    DBSourceSquid dbSourceSquid = squidList.get(i);
                    List<SourceTable> sourceTableList = sourceTableDao.getSourceTableBySquidId(dbSourceSquid.getId());
                    dbSourceSquid.setSourceTableList(sourceTableList);
                    dbSourceSquid.setType(DSObjectType.valueOf(squidTypeId).value());
                    //查询变量集合
                    List<DSVariable> variables = variableDao.getDSVariableByScope(2, dbSourceSquid.getId());
                    if (variables == null) {
                        variables = new ArrayList<DSVariable>();
                    }
                    if (i > 0 && i % 5 == 0) {
                        outputMap.clear();
                        outputMap.put("Squids", returnSquidList);
                        PushMessagePacket.pushMap(outputMap, DSObjectType.valueOf(squidTypeId), cmd1, cmd2, key, TokenUtil.getToken());
                        returnSquidList.clear();
                    }
                    returnSquidList.add(dbSourceSquid);

                }

                if (returnSquidList.size() > 0) {
                    outputMap.clear();
                    outputMap.put("Squids", returnSquidList);
                    PushMessagePacket.pushMap(outputMap, DSObjectType.valueOf(squidTypeId), cmd1, cmd2, key, TokenUtil.getToken());
                }
                squidList.clear();
                returnSquidList.clear();
                logger.info("dbSourceSquid 加载时间:" + (System.currentTimeMillis() - start));
                start = System.currentTimeMillis();
            } else if (squidTypeId == SquidTypeEnum.MONGODB.value()) {
                List<NOSQLConnectionSquid> squidList = squidDao.getSquidListForParams(squidTypeId, squid_flow_id, squidIds, NOSQLConnectionSquid.class);
                for (NOSQLConnectionSquid dbSourceSquid : squidList) {
                    List<SourceTable> sourceTableList = sourceTableDao.getSourceTableBySquidId(dbSourceSquid.getId());
                    dbSourceSquid.setSourceTableList(sourceTableList);
                    dbSourceSquid.setType(DSObjectType.MONGODB.value());
                    //查询变量集合
                    List<DSVariable> variables = variableDao.getDSVariableByScope(2, dbSourceSquid.getId());
                    if (variables == null) {
                        variables = new ArrayList<DSVariable>();
                    }
                    dbSourceSquid.setVariables(variables);


                }
                outputMap.clear();
                outputMap.put("Squids", squidList);
                PushMessagePacket.pushMap(outputMap, DSObjectType.MONGODB, cmd1, cmd2, key, TokenUtil.getToken());
                logger.info("NOSQLConnectionSquid 加载时间:" + (System.currentTimeMillis() - start));
                start = System.currentTimeMillis();
            } else if (squidTypeId == SquidTypeEnum.USERDEFINED.value()) {
                List<UserDefinedSquid> squidList = squidDao.getSquidListForParams(squidTypeId, squid_flow_id, squidIds, UserDefinedSquid.class);
                ITransformationInputsDao transInputDao = new TransformationInputsDaoImpl(adapter);
                ITransformationLinkDao transLinkDao = new TransformationLinkDaoImpl(adapter);
                IReferenceColumnDao refColumnDao = new ReferenceColumnDaoImpl(adapter);
                for (UserDefinedSquid definedSquid : squidList) {
                    //查找出所有的className
                    String sql = "select alias_name from THIRD_JAR_DEFINITION";
                    List<Map<String, Object>> classNames = adapter.query2List(true, sql, null);
                    List<String> classNameList = new ArrayList<>();
                    for (Map<String, Object> className : classNames) {
                        classNameList.add(className.get("ALIAS_NAME") + "");
                    }
                    definedSquid.setClassNames(classNameList);
                    //查找出dataMapColumn
                    Map<String, String> params = new HashMap<>();
                    params.put("squid_id", definedSquid.getId() + "");
                    List<UserDefinedMappingColumn> dataMapList = adapter.query2List(params, UserDefinedMappingColumn.class);
                    definedSquid.setUserDefinedMappingColumns(dataMapList);
                    //查找出parameterMap
                    List<UserDefinedParameterColumn> parameterColumns = adapter.query2List(params, UserDefinedParameterColumn.class);
                    definedSquid.setUserDefinedParameterColumns(parameterColumns);
                    //查找referenceColumn
                    List<ReferenceColumn> referenceColumns = refColumnDao.getRefColumnListByRefSquidId(definedSquid.getId());
                    definedSquid.setSourceColumns(referenceColumns);
                    //查找出Transformation
                    params.clear();
                    params.put("squid_id", definedSquid.getId() + "");
                    List<Transformation> transformations = adapter.query2List(params, Transformation.class);
                    if (transformations != null) {
                        Map<Integer, List<TransformationInputs>> rsMap = transInputDao.getTransInputsBySquidId(definedSquid.getId());
                        for (int i = 0; i < transformations.size(); i++) {
                            int trans_id = transformations.get(i).getId();
                            if (rsMap != null && rsMap.containsKey(trans_id)) {
                                transformations.get(i).setInputs(rsMap.get(trans_id));
                            }
                        }
                    }
                    definedSquid.setTransformations(transformations);

                    // 所有transformation link
                    List<TransformationLink> transformationLinks = transLinkDao.getTransLinkListBySquidId(definedSquid.getId());
                    definedSquid.setTransformationLinks(transformationLinks);

                    //查询出column
                    List<Column> columns = adapter.query2List(params, Column.class);
                    definedSquid.setColumns(columns);

                    //查询出变量
                    List<DSVariable> variables = variableDao.getDSVariableByScope(2, definedSquid.getId());
                    if (variables == null) {
                        variables = new ArrayList<DSVariable>();
                    }
                    definedSquid.setVariables(variables);
                }
                outputMap.clear();
                outputMap.put("Squids", squidList);
                PushMessagePacket.pushMap(outputMap, DSObjectType.USERDEFINED, cmd1, cmd2, key, TokenUtil.getToken());
                logger.info("UserDefined 加载时间:" + (System.currentTimeMillis() - start));
                start = System.currentTimeMillis();
            } else if (squidTypeId == SquidTypeEnum.STATISTICS.value()) {
                List<StatisticsSquid> squidList = squidDao.getSquidListForParams(squidTypeId, squid_flow_id,squidIds, StatisticsSquid.class);
                ITransformationInputsDao transInputDao = new TransformationInputsDaoImpl(adapter);
                ITransformationLinkDao transLinkDao = new TransformationLinkDaoImpl(adapter);
                IReferenceColumnDao refColumnDao = new ReferenceColumnDaoImpl(adapter);
                for (StatisticsSquid squid : squidList) {
                    //查找出所有的className
                    String sql = "select statistics_name from ds_statistics_definition";
                    List<Map<String, Object>> classNames = adapter.query2List(true, sql, null);
                    List<String> classNameList = new ArrayList<>();
                    for (Map<String, Object> className : classNames) {
                        classNameList.add(className.get("STATISTICS_NAME") + "");
                    }
                    squid.setStatisticsNames(classNameList);
                    //查找出dataMapColumn
                    Map<String, Object> params = new HashMap<>();
                    params.put("squid_id", squid.getId() + "");
                    List<StatisticsDataMapColumn> dataMapList = adapter.query2List2(true, params, StatisticsDataMapColumn.class);
                    squid.setStatisticsDataMapColumns(dataMapList);
                    //查找出parameterMap
                    List<StatisticsParameterColumn> parameterColumns = adapter.query2List2(true, params, StatisticsParameterColumn.class);
                    squid.setStatisticsParametersColumns(parameterColumns);
                    //查找referenceColumn
                    List<ReferenceColumn> referenceColumns = refColumnDao.getRefColumnListByRefSquidId(squid.getId());
                    squid.setSourceColumns(referenceColumns);
                    //查找出Transformation
                    params.clear();
                    params.put("squid_id", squid.getId() + "");
                    List<Transformation> transformations = adapter.query2List2(true, params, Transformation.class);
                    if (transformations != null) {
                        Map<Integer, List<TransformationInputs>> rsMap = transInputDao.getTransInputsBySquidId(squid.getId());
                        for (int i = 0; i < transformations.size(); i++) {
                            int trans_id = transformations.get(i).getId();
                            if (rsMap != null && rsMap.containsKey(trans_id)) {
                                transformations.get(i).setInputs(rsMap.get(trans_id));
                            }
                        }
                    }
                    squid.setTransformations(transformations);

                    // 所有transformation link
                    List<TransformationLink> transformationLinks = transLinkDao.getTransLinkListBySquidId(squid.getId());
                    squid.setTransformationLinks(transformationLinks);

                    //查询出column
                    List<Column> columns = adapter.query2List2(true, params, Column.class);
                    squid.setColumns(columns);
                    //查询出变量
                    List<DSVariable> variables = variableDao.getDSVariableByScope(2, squid.getId());
                    if (variables == null) {
                        variables = new ArrayList<>();
                    }
                    squid.setVariables(variables);
                }
                outputMap.clear();
                outputMap.put("Squids", squidList);
                PushMessagePacket.pushMap(outputMap, DSObjectType.STATISTICS, cmd1, cmd2, key, token);
                logger.info("Statistics 加载时间:" + (System.currentTimeMillis() - start));
                start = System.currentTimeMillis();
            } else {
                //查询出所有的调度任务
                Map<Integer, JobSchedule> scheduleMap = new JobScheduleProcess().getJobSchedules(repository_id, squid_flow_id, null, true, out);
                switch (SquidTypeEnum.valueOf(squidTypeId)) {
                    // FileFolderSquid added by lei.bin
                    case FILEFOLDER:
                        List<FileFolderSquid> folderSquids = squidDao.getSquidListForParams(squidTypeId, squid_flow_id, squidIds, FileFolderSquid.class);
                        if (null != folderSquids && folderSquids.size() > 0) {
                            for (FileFolderSquid rSquid : folderSquids) {
                                List<SourceTable> list = sourceTableDao.getSourceTableBySquidId(rSquid.getId());
                                rSquid.setSourceTableList(list);

                                //查询变量集合
                                List<DSVariable> variables = variableDao.getDSVariableByScope(2, rSquid.getId());
                                if (variables == null) {
                                    variables = new ArrayList<DSVariable>();
                                }
                                rSquid.setVariables(variables);
                            }
                            outputMap.clear();
                            outputMap.put("Squids", folderSquids);
                            PushMessagePacket.pushMap(outputMap, DSObjectType.FILEFOLDER, cmd1, cmd2, key, TokenUtil.getToken());
                        }
                        break;
                    case FTP:
                        // FtpSquid
                        List<FtpSquid> ftpList = squidDao.getSquidListForParams(squidTypeId, squid_flow_id, squidIds, FtpSquid.class);
                        if (null != ftpList && ftpList.size() > 0) {
                            for (FtpSquid rSquid : ftpList) {
                                List<SourceTable> list = sourceTableDao.getSourceTableBySquidId(rSquid.getId());
                                rSquid.setSourceTableList(list);

                                //查询变量集合
                                List<DSVariable> variables = variableDao.getDSVariableByScope(2, rSquid.getId());
                                if (variables == null) {
                                    variables = new ArrayList<DSVariable>();
                                }
                                rSquid.setVariables(variables);
                            }
                            outputMap.clear();
                            outputMap.put("Squids", ftpList);
                            PushMessagePacket.pushMap(outputMap, DSObjectType.FTP, cmd1, cmd2, key, TokenUtil.getToken());
                        }
                        break;
                    case HDFS:
                    case SOURCECLOUDFILE: case TRAINNINGFILESQUID:
                        //HdfsSquid
                        List<HdfsSquid> hdfs = squidDao.getSquidListForParams(squidTypeId, squid_flow_id, squidIds, HdfsSquid.class);
                        if (null != hdfs && hdfs.size() > 0) {
                            for (HdfsSquid rSquid : hdfs) {
                                List<SourceTable> list = sourceTableDao.getSourceTableBySquidId(rSquid.getId());
                                rSquid.setSourceTableList(list);

                                //查询变量集合
                                List<DSVariable> variables = variableDao.getDSVariableByScope(2, rSquid.getId());
                                if (variables == null) {
                                    variables = new ArrayList<DSVariable>();
                                }
                                rSquid.setVariables(variables);
                            }
                            outputMap.clear();
                            outputMap.put("Squids", hdfs);
                            PushMessagePacket.pushMap(outputMap, DSObjectType.valueOf(squidTypeId), cmd1, cmd2, key, TokenUtil.getToken());

                        }
                        break;

                    case WEIBO:
                        //WeiboSquid
                        try {
                            List<WeiboSquid> weibo = squidDao.getSquidListForParams(squidTypeId, squid_flow_id, squidIds, WeiboSquid.class);
                            if (null != weibo && weibo.size() > 0) {
                                for (WeiboSquid rSquid : weibo) {
                                    if (null != scheduleMap.get(rSquid.getId())) {
                                        rSquid.setJobSchedule(scheduleMap.get(rSquid.getId()));
                                    }
                                    List<SourceTable> list = sourceTableDao.getSourceTableBySquidId(rSquid.getId());
                                    rSquid.setSourceTableList(list);

                                    //查询变量集合
                                    List<DSVariable> variables = variableDao.getDSVariableByScope(2, rSquid.getId());
                                    if (variables == null) {
                                        variables = new ArrayList<DSVariable>();
                                    }
                                    rSquid.setVariables(variables);
                                }
                                outputMap.clear();
                                outputMap.put("Squids", weibo);
                                PushMessagePacket.pushMap(outputMap, DSObjectType.WEIBO, cmd1, cmd2, key, TokenUtil.getToken());
                            }
                            break;
                        } catch (Exception e) {
                            // TODO: handle exception
                            e.printStackTrace();
                        }
                    case WEB:
                        //WebSquid
                        try {
                            List<WebSquid> web = squidDao.getSquidListForParams(squidTypeId, squid_flow_id, squidIds, WebSquid.class);
                            if (null != web && web.size() > 0) {
                                for (WebSquid rSquid : web) {
                                    if (null != scheduleMap.get(rSquid.getId())) {
                                        rSquid.setJobSchedule(scheduleMap.get(rSquid.getId()));
                                    }
                                    List<SourceTable> list = sourceTableDao.getSourceTableBySquidId(rSquid.getId());
                                    rSquid.setSourceTableList(list);
                                    IUrlDao urlDao = new UrlDaoImpl(adapter);
                                    List<Url> webUrls = urlDao.getUrlsBySquidId(rSquid.getId());
                                    rSquid.setUrlList(webUrls);

                                    //查询变量集合
                                    List<DSVariable> variables = variableDao.getDSVariableByScope(2, rSquid.getId());
                                    if (variables == null) {
                                        variables = new ArrayList<DSVariable>();
                                    }
                                    rSquid.setVariables(variables);
                                }
                                outputMap.clear();
                                outputMap.put("Squids", web);
                                PushMessagePacket.pushMap(outputMap, DSObjectType.WEB, cmd1, cmd2, key, TokenUtil.getToken());
                            }
                            break;
                        } catch (Exception e) {
                            // TODO: handle exception
                            e.printStackTrace();
                        }
                    case DBDESTINATION:
                        // DBDestinationSquid
                        outputMap.clear();
                        outputMap.put("Squids", squidDao.getSquidListForParams(squidTypeId, squid_flow_id, squidIds, DBDestinationSquid.class));
                        PushMessagePacket.pushMap(outputMap, DSObjectType.DBDESTINATION, cmd1, cmd2, key, TokenUtil.getToken());
                        logger.info("DBDestinationSquid 加载时间:" + (System.currentTimeMillis() - start));
                        start = System.currentTimeMillis();
                        break;
                    case STAGE:
                        // StageSquid
                        List<StageSquid> squids = squidDao.getSquidListForParams(squidTypeId, squid_flow_id, squidIds, StageSquid.class);
                        List<StageSquid> returnSquids = new ArrayList<>();
                        if (squids != null && squids.size() > 0) {
                            //每50个推送一次
                            for (int i = 0; i < squids.size(); i++) { // 获得StagSquid
                                StageSquid stageSquid = squids.get(i);
                                StageSquidService.setStageSquidData(adapter, stageSquid);
                                if (i > 0 && i % 30 == 0) {
                                    outputMap.clear();
                                    outputMap.put("Squids", returnSquids);
                                    PushMessagePacket.pushMap(outputMap, DSObjectType.STAGE, cmd1, cmd2, key, TokenUtil.getToken());
                                    returnSquids.clear();
                                }
                                returnSquids.add(stageSquid);
                            }
                            if (returnSquids.size() > 0) {
                                outputMap.clear();
                                outputMap.put("Squids", returnSquids);
                                PushMessagePacket.pushMap(outputMap, DSObjectType.STAGE, cmd1, cmd2, key, TokenUtil.getToken());

                            }
                            returnSquids.clear();
                            squids.clear();
                        }
                        logger.info("StageSquid 加载时间:" + (System.currentTimeMillis() - start));
                        start = System.currentTimeMillis();
                        break;
                    case GROUPTAGGING:
                        List<GroupTaggingSquid> taggingSquids = squidDao.getSquidListForParams(squidTypeId, squid_flow_id, squidIds, GroupTaggingSquid.class);
                        List<GroupTaggingSquid> returnTagSquids = new ArrayList<>();
                        if (taggingSquids != null && taggingSquids.size() > 0) {
                            //每50个推送一次
                            for (int i = 0; i < taggingSquids.size(); i++) { // 获得StagSquid
                                GroupTaggingSquid stageSquid = taggingSquids.get(i);
                                StageSquidService.setGroupTaggingSquidData(adapter, stageSquid);
                                if (i > 0 && i % 30 == 0) {
                                    outputMap.clear();
                                    outputMap.put("Squids", returnTagSquids);
                                    PushMessagePacket.pushMap(outputMap, DSObjectType.GROUPTAGGING, cmd1, cmd2, key, TokenUtil.getToken());
                                    returnTagSquids.clear();
                                }
                                returnTagSquids.add(stageSquid);
                            }
                            if (returnTagSquids.size() > 0) {
                                outputMap.clear();
                                outputMap.put("Squids", returnTagSquids);
                                PushMessagePacket.pushMap(outputMap, DSObjectType.GROUPTAGGING, cmd1, cmd2, key, TokenUtil.getToken());

                            }
                            returnTagSquids.clear();
                            taggingSquids.clear();
                        }
                        logger.info("GroupTaggingSquid 加载时间:" + (System.currentTimeMillis() - start));
                        start = System.currentTimeMillis();
                        break;
                    case SAMPLINGSQUID:
                        List<SamplingSquid> samplingSquids = squidDao.getSquidListForParams(squidTypeId, squid_flow_id, squidIds, SamplingSquid.class);
                        List<SamplingSquid> returnSampSquids = new ArrayList<>();
                        if (samplingSquids != null && samplingSquids.size() > 0) {
                            //每50个推送一次
                            for (int i = 0; i < samplingSquids.size(); i++) { // 获得StagSquid
                                SamplingSquid stageSquid = samplingSquids.get(i);
                                StageSquidService.setSamplingSquidData(adapter, stageSquid);
                                if (i > 0 && i % 30 == 0) {
                                    outputMap.clear();
                                    outputMap.put("Squids", returnSampSquids);
                                    PushMessagePacket.pushMap(outputMap, DSObjectType.SAMPLINGSQUID, cmd1, cmd2, key, TokenUtil.getToken());
                                    returnSampSquids.clear();
                                }
                                returnSampSquids.add(stageSquid);
                            }
                            if (returnSampSquids.size() > 0) {
                                outputMap.clear();
                                outputMap.put("Squids", returnSampSquids);
                                PushMessagePacket.pushMap(outputMap, DSObjectType.SAMPLINGSQUID, cmd1, cmd2, key, TokenUtil.getToken());

                            }
                            returnSampSquids.clear();
                            samplingSquids.clear();
                        }
                        logger.info("SamplingSquid 加载时间:" + (System.currentTimeMillis() - start));
                        start = System.currentTimeMillis();
                        break;
                    case PIVOTSQUID:
                        List<PivotSquid> pivotSquids = squidDao.getSquidListForParams(squidTypeId, squid_flow_id, squidIds, PivotSquid.class);
                        List<PivotSquid> returnPivotSquids = new ArrayList<>();
                        if (pivotSquids != null && pivotSquids.size() > 0) {
                            //每50个推送一次
                            for (int i = 0; i < pivotSquids.size(); i++) { // 获得StagSquid
                                PivotSquid stageSquid = pivotSquids.get(i);
                                StageSquidService.setPivotSquidData(adapter, stageSquid);
                                if (i > 0 && i % 30 == 0) {
                                    outputMap.clear();
                                    outputMap.put("Squids", returnPivotSquids);
                                    PushMessagePacket.pushMap(outputMap, DSObjectType.PIVOTSQUID, cmd1, cmd2, key, TokenUtil.getToken());
                                    returnPivotSquids.clear();
                                }
                                returnPivotSquids.add(stageSquid);
                            }
                            if (returnPivotSquids.size() > 0) {
                                outputMap.clear();
                                outputMap.put("Squids", returnPivotSquids);
                                PushMessagePacket.pushMap(outputMap, DSObjectType.PIVOTSQUID, cmd1, cmd2, key, TokenUtil.getToken());

                            }
                            returnPivotSquids.clear();
                            pivotSquids.clear();
                        }
                        logger.info("PivotSquid 加载时间:" + (System.currentTimeMillis() - start));
                        start = System.currentTimeMillis();
                        break;
                    case STREAM_STAGE:
                        // StreamStageSquid
                        List<StreamStageSquid> StreamSquid = squidDao.getSquidListForParams(squidTypeId, squid_flow_id, squidIds, StreamStageSquid.class);
                        List<StreamStageSquid> returnSquidList = new ArrayList<>();
                        if (StreamSquid != null && StreamSquid.size() > 0) {
                            //50个推送一次
                            for (int i = 0; i < StreamSquid.size(); i++) { // 获得StreamStageSquid
                                StreamStageSquid StreamStageSquid = StreamSquid.get(i);
                                StageSquidService.setStreamStageSquid(adapter, StreamStageSquid);
                                if (i > 0 && i % 30 == 0) {
                                    outputMap.clear();
                                    outputMap.put("Squids", returnSquidList);
                                    PushMessagePacket.pushMap(outputMap, DSObjectType.STREAM_STAGE, cmd1, cmd2, key, TokenUtil.getToken());
                                    returnSquidList.clear();
                                }
                                returnSquidList.add(StreamStageSquid);

                            }
                            if (returnSquidList.size() > 0) {
                                outputMap.clear();
                                outputMap.put("Squids", returnSquidList);
                                PushMessagePacket.pushMap(outputMap, DSObjectType.STREAM_STAGE, cmd1, cmd2, key, TokenUtil.getToken());
                            }
                            returnSquidList.clear();
                            StreamSquid.clear();

                        }
                        logger.info("StageSquid 加载时间:" + (System.currentTimeMillis() - start));
                        start = System.currentTimeMillis();
                        break;

                    case HTTP:
                        List<HttpSquid> httpSquid = squidDao.getSquidListForParams(squidTypeId, squid_flow_id, squidIds, HttpSquid.class);
                        if (httpSquid != null && httpSquid.size() > 0) {
                            for (HttpSquid httpSquidItem : httpSquid) { // 获得DataMining
                                List<SourceTable> list = sourceTableDao.getSourceTableBySquidId(httpSquidItem.getId());
                                httpSquidItem.setSourceTableList(list);

                                //查询变量集合
                                List<DSVariable> variables = variableDao.getDSVariableByScope(2, httpSquidItem.getId());
                                if (variables == null) {
                                    variables = new ArrayList<DSVariable>();
                                }
                                httpSquidItem.setVariables(variables);
                            }
                            outputMap.clear();
                            outputMap.put("Squids", httpSquid);
                            PushMessagePacket.pushMap(outputMap, DSObjectType.valueOf(squidTypeId), cmd1, cmd2, key, TokenUtil.getToken());
                        }

                        logger.info("dataMiningSquidList 加载时间:" + (System.currentTimeMillis() - start));
                        start = System.currentTimeMillis();
                        break;
                    case WEBSERVICE:
                        List<WebserviceSquid> webServiceSquid = squidDao.getSquidListForParams(squidTypeId, squid_flow_id, squidIds, WebserviceSquid.class);
                        if (webServiceSquid != null && webServiceSquid.size() > 0) {
                            for (WebserviceSquid wsi : webServiceSquid) { // 获得DataMining
                                List<SourceTable> list = sourceTableDao.getSourceTableBySquidId(wsi.getId());
                                wsi.setSourceTableList(list);

                                //查询变量集合
                                List<DSVariable> variables = variableDao.getDSVariableByScope(2, wsi.getId());
                                if (variables == null) {
                                    variables = new ArrayList<DSVariable>();
                                }
                                wsi.setVariables(variables);
                            }
                            outputMap.clear();
                            outputMap.put("Squids", webServiceSquid);
                            PushMessagePacket.pushMap(outputMap, DSObjectType.valueOf(squidTypeId), cmd1, cmd2, key, TokenUtil.getToken());
                        }
                        logger.info("dataMiningSquidList 加载时间:" + (System.currentTimeMillis() - start));
                        start = System.currentTimeMillis();
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
                        // DataMining SquidModelBase
                        List<DataMiningSquid> dataMiningSquidList = squidDao.getSquidListForParams(squidTypeId, squid_flow_id, squidIds, DataMiningSquid.class);
                        if (dataMiningSquidList != null && dataMiningSquidList.size() > 0) {
                            for (DataMiningSquid dataMiningSquid : dataMiningSquidList) { // 获得DataMining
                                StageSquidService.setDataMiningSquidData(adapter, dataMiningSquid);
                            }
                            outputMap.clear();
                            outputMap.put("Squids", dataMiningSquidList);
                            PushMessagePacket.pushMap(outputMap, DSObjectType.valueOf(squidTypeId), cmd1, cmd2, key, TokenUtil.getToken());
                        }
                        logger.info("dataMiningSquidList 加载时间:" + (System.currentTimeMillis() - start));
                        start = System.currentTimeMillis();
                        break;
                    case REPORT:
                        // ReportSquid added by bo.dang
                        List<ReportSquid> reportSquidArray = squidDao.getSquidListForParams(squidTypeId,
                                squid_flow_id, squidIds, ReportSquid.class);
                        if (null != reportSquidArray && reportSquidArray.size() > 0) {
                            for (ReportSquid reportSquid : reportSquidArray) {
                                if (reportSquid.getFolder_id() > 0) {
                                    String path = getReportNodeName(adapter, reportSquid.getFolder_id());
                                    reportSquid.setPublishing_path(path);
                                }

                                //查询变量集合
                                List<DSVariable> variables = variableDao.getDSVariableByScope(2, reportSquid.getId());
                                if (variables == null) {
                                    variables = new ArrayList<DSVariable>();
                                }
                                reportSquid.setVariables(variables);
                            }
                            outputMap.clear();
                            outputMap.put("Squids", reportSquidArray);
                            PushMessagePacket.pushMap(outputMap, DSObjectType.REPORT, cmd1, cmd2, key, TokenUtil.getToken());
                        }
                        break;
                    case GISMAP:
                        // ReportSquid added by bo.dang
                        List<GISMapSquid> gisMapSquidArray = squidDao.getSquidListForParams(squidTypeId,
                                squid_flow_id, squidIds, GISMapSquid.class);
                        if (null != gisMapSquidArray && gisMapSquidArray.size() > 0) {
                            for (GISMapSquid gisMapSquid : gisMapSquidArray) {
                                if (gisMapSquid.getFolder_id() > 0) {
                                    String path = getReportNodeName(adapter, gisMapSquid.getFolder_id());
                                    gisMapSquid.setPublishing_path(path);
                                }

                                //查询变量集合
                                List<DSVariable> variables = variableDao.getDSVariableByScope(2, gisMapSquid.getId());
                                if (variables == null) {
                                    variables = new ArrayList<DSVariable>();
                                }
                                gisMapSquid.setVariables(variables);
                            }
                            outputMap.clear();
                            outputMap.put("Squids", gisMapSquidArray);
                            PushMessagePacket.pushMap(outputMap, DSObjectType.GISMAP, cmd1, cmd2, key, TokenUtil.getToken());
                        }
                        break;
                    case EXCEPTION:
                        //当Exception数据量很大时，可以分页查询，一次查出50个
                        List<ExceptionSquid> exceptionList = this.getExceptionSquidListLimit(adapter, squid_flow_id, squidIds, out);
                        List<ExceptionSquid> returnExceptionList = new ArrayList<>();
                        if (exceptionList != null && exceptionList.size() > 0) {
                            for (int i = 0; i < exceptionList.size(); i++) {
                                ExceptionSquid exceptionSquid = exceptionList.get(i);
                                StageSquidService.setExceptionSquidData(adapter, exceptionSquid);
                                //查询变量集合
                                List<DSVariable> variables = variableDao.getDSVariableByScope(2, exceptionSquid.getId());
                                if (variables == null) {
                                    variables = new ArrayList<DSVariable>();
                                }
                                exceptionSquid.setVariables(variables);
                                //50个推送一次
                                if (i > 0 && i % 50 == 0) {
                                    outputMap.clear();
                                    outputMap.put("Squids", returnExceptionList);
                                    PushMessagePacket.pushMap(outputMap, DSObjectType.EXCEPTION, cmd1, cmd2, key, TokenUtil.getToken());
                                    returnExceptionList.clear();
                                }
                                returnExceptionList.add(exceptionSquid);

                            }
                            if (returnExceptionList.size() > 0) {
                                outputMap.clear();
                                outputMap.put("Squids", returnExceptionList);
                                PushMessagePacket.pushMap(outputMap, DSObjectType.EXCEPTION, cmd1, cmd2, key, TokenUtil.getToken());
                            }
                            returnExceptionList.clear();
                            exceptionList.clear();

                        }
                        logger.info("ExceptionSquid 加载时间:" + (System.currentTimeMillis() - start));
                        start = System.currentTimeMillis();
                        break;
                    case KAFKA:
                        List<KafkaSquid> kafkaSquidList = squidDao.getSquidListForParams(squidTypeId, squid_flow_id, squidIds, KafkaSquid.class);
                        if (kafkaSquidList != null && kafkaSquidList.size() > 0) {
                            for (KafkaSquid kafkaSquid : kafkaSquidList) {
                                List<SourceTable> tableList = sourceTableDao.getSourceTableBySquidId(kafkaSquid.getId());
                                if (tableList == null)
                                    tableList = new ArrayList<SourceTable>();
                                kafkaSquid.setSourceTableList(tableList);
                            }
                            outputMap.clear();
                            outputMap.put("Squids", kafkaSquidList);
                            PushMessagePacket.pushMap(outputMap, DSObjectType.KAFKA, cmd1, cmd2, key, TokenUtil.getToken());
                        }
                        logger.info("Kafka Connection SquidModelBase 加载时间:" + (System.currentTimeMillis() - start));
                        start = System.currentTimeMillis();
                        break;
                    case HIVE:
                        List<SystemHiveConnectionSquid>
                                hiveConnectionSquids = squidDao.getSquidListForParams(squidTypeId, squid_flow_id, squidIds, SystemHiveConnectionSquid.class);
                        if (hiveConnectionSquids != null && hiveConnectionSquids.size() > 0) {
                            for (SystemHiveConnectionSquid hbaseConnectionSquid : hiveConnectionSquids) {
                                //查询出变量信息
                                List<DSVariable> variables = variableDao.getDSVariableByScope(2, hbaseConnectionSquid.getId());
                                List<SourceTable> tableList = sourceTableDao.getSourceTableBySquidId(
                                        hbaseConnectionSquid.getId());
                                if (tableList == null)
                                    tableList = new ArrayList<SourceTable>();
                                if (variables == null) {
                                    variables = new ArrayList<>();
                                }
                                hbaseConnectionSquid.setVariables(variables);
                                hbaseConnectionSquid.setSourceTableList(tableList);
                            }
                            outputMap.clear();
                            outputMap.put("Squids", hiveConnectionSquids);
                            PushMessagePacket.pushMap(outputMap, DSObjectType.HIVE, cmd1, cmd2, key, TokenUtil.getToken());
                        }
                        logger.info("Hive Connection SquidModelBase 加载时间:" + (System.currentTimeMillis() - start));
                        start = System.currentTimeMillis();
                        break;
                    case CASSANDRA:
                        List<CassandraConnectionSquid>
                                connectionSquids = squidDao.getSquidListForParams(squidTypeId, squid_flow_id, squidIds, CassandraConnectionSquid.class);
                        if (connectionSquids != null && connectionSquids.size() > 0) {
                            for (CassandraConnectionSquid connectionSquid : connectionSquids) {
                                //查询出变量信息
                                List<DSVariable> variables = variableDao.getDSVariableByScope(2, connectionSquid.getId());
                                List<SourceTable> tableList = sourceTableDao.getSourceTableBySquidId(
                                        connectionSquid.getId());
                                if (tableList == null)
                                    tableList = new ArrayList<SourceTable>();
                                if (variables == null) {
                                    variables = new ArrayList<>();
                                }
                                connectionSquid.setVariables(variables);
                                connectionSquid.setSourceTableList(tableList);
                            }
                            outputMap.clear();
                            outputMap.put("Squids", connectionSquids);
                            PushMessagePacket.pushMap(outputMap, DSObjectType.CASSANDRA, cmd1, cmd2, key, TokenUtil.getToken());
                        }
                        logger.info("Cassandra Connection SquidModelBase 加载时间:" + (System.currentTimeMillis() - start));
                        start = System.currentTimeMillis();
                        break;
                    case HBASE:
                        List<HBaseConnectionSquid>
                                hbaseConnectionSquidList = squidDao.getSquidListForParams(squidTypeId, squid_flow_id, squidIds, HBaseConnectionSquid.class);
                        if (hbaseConnectionSquidList != null && hbaseConnectionSquidList.size() > 0) {
                            for (HBaseConnectionSquid hbaseConnectionSquid : hbaseConnectionSquidList) {
                                List<SourceTable> tableList = sourceTableDao.getSourceTableBySquidId(
                                        hbaseConnectionSquid.getId());
                                if (tableList == null)
                                    tableList = new ArrayList<SourceTable>();
                                hbaseConnectionSquid.setSourceTableList(tableList);
                            }
                            outputMap.clear();
                            outputMap.put("Squids", hbaseConnectionSquidList);
                            PushMessagePacket.pushMap(outputMap, DSObjectType.HBASE, cmd1, cmd2, key, TokenUtil.getToken());
                        }
                        logger.info("Hbase Connection SquidModelBase 加载时间:" + (System.currentTimeMillis() - start));
                        start = System.currentTimeMillis();
                        break;
                    default:
                        //其他类型的
                        @SuppressWarnings("unchecked")
                        List<?> list = squidDao.getSquidListForParams(squidTypeId,
                                squid_flow_id, squidIds, SquidTypeEnum.classOfValue(squidTypeId));
                        if (list != null && list.size() > 0) {
                            outputMap.clear();
                            //添加Squid下面的variables
                            for (int i = 0; i < list.size(); i++) {
                                Squid squid = (Squid) list.get(i);
                                List<DSVariable> dsVariables = variableDao.getDSVariableByScope(2, squid.getId());
                                squid.setVariables(dsVariables);
                            }
                            outputMap.put("Squids", list);
                            PushMessagePacket.pushMap(outputMap,
                                    DSObjectType.valueOf(squidTypeId), cmd1, cmd2, key, TokenUtil.getToken());
                        }
                        logger.info(DSObjectType.parse(squidTypeId).name() + " 加载时间:" + (System.currentTimeMillis() - start));
                        start = System.currentTimeMillis();
                        break;
                }
            }
        }
    }

    /**
     * 位置信息更新
     *
     * @return
     */
    public void updateLocation(String info, ReturnValue out) {
        IRelationalDataManager adapter = DataAdapterFactory.getDefaultDataManager();
        try {
            HashMap<String, Object> data = JsonUtil.toHashMap(info);
            List<CommonLocationInfo> list = JsonUtil.toGsonList(data.get("SquidPositions").toString(), CommonLocationInfo.class);
//			if (list != null && !list.isEmpty()) {
//				adapter.openSession();
//				String sql = "";
//				for (CommonLocationInfo loc : list) {
//					if(loc.getId()>0){
//						sql += "UPDATE DS_SQUID SET location_X="+loc.getX()+", location_Y="+loc.getY()+
//								", squid_Width="+loc.getW()+", squid_Height="+loc.getH()+
//								" WHERE ID="+loc.getId()+";";
//					}
//				}
//				if (sql!=""){
//					adapter.execute(sql);
//					logger.info("[update location sys]\t" + "\t"+ sql);
//				}
//			}

            if (list != null && !list.isEmpty()) {
                adapter.openSession();
                String sql = "UPDATE DS_SQUID SET location_X=?, location_Y=?, squid_Width=?, squid_Height=? WHERE ID=?;";
                List<List<Object>> params = new ArrayList<>();
                for (CommonLocationInfo loc : list) {
                    List<Object> item = new ArrayList<>();
                    if (loc.getId() > 0) {
                        item.add(loc.getX());
                        item.add(loc.getY());
                        item.add(loc.getW());
                        item.add(loc.getH());
                        item.add(loc.getId());
                        params.add(item);
                    }
                }
                if (params.size() > 0) {
                    adapter.executeBatch(sql, params);
                    logger.info("[update location sys]\t" + "\t" + sql);
                }
            }
        } catch (Exception e) {
            try {
                adapter.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
            out.setMessageCode(MessageCode.UPDATE_ERROR);
            logger.error("更新位置异常", e);
        } finally {
            adapter.closeSession();
        }

    }

}
