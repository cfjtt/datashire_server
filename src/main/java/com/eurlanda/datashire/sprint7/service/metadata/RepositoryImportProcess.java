package com.eurlanda.datashire.sprint7.service.metadata;

import cn.com.jsoft.jframe.utils.FileUtils;
import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.dao.ISquidDao;
import com.eurlanda.datashire.dao.ITransformationDao;
import com.eurlanda.datashire.dao.IVariableDao;
import com.eurlanda.datashire.dao.impl.SquidDaoImpl;
import com.eurlanda.datashire.dao.impl.TransformationDaoImpl;
import com.eurlanda.datashire.dao.impl.VariableDaoImpl;
import com.eurlanda.datashire.entity.*;
import com.eurlanda.datashire.entity.dest.*;
import com.eurlanda.datashire.entity.operation.*;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.enumeration.DataShireFieldTypeEnum;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.enumeration.TransformationTypeEnum;
import com.eurlanda.datashire.exception.ImportVersionException;
import com.eurlanda.datashire.server.model.PivotSquid;
import com.eurlanda.datashire.server.utils.Constants;
import com.eurlanda.datashire.server.utils.TokenUtil;
import com.eurlanda.datashire.sprint7.packet.PushMessagePacket;
import com.eurlanda.datashire.sprint7.service.squidflow.RepositoryService;
import com.eurlanda.datashire.utility.*;
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.DomDriver;
import org.apache.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import util.ZipStrUtil;

import java.io.File;
import java.sql.SQLException;
import java.util.*;

public class RepositoryImportProcess {
    /**
     * 对RepositoryExportProcess的日志进行记录
     */
    static Logger logger = Logger.getLogger(RepositoryImportProcess.class);
    private String token;
    private String key;
    private static Map<String, Object> importModuleGuidMap;
    private static Map<String, Object> importIOFlowMap;

    static {
        try {
            importModuleGuidMap = new HashMap<String, Object>();
            importIOFlowMap = new HashMap<String, Object>();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public RepositoryImportProcess() {
    }

    public RepositoryImportProcess(String token, String key) {
        this.token = token;
        this.key = key;
    }

    /**
     * 导入对象
     *
     * @param info
     * @param out
     * @return
     */
    @SuppressWarnings("unchecked")
    public Map<String, Object> importSquidFlows(String info, ReturnValue out) {
        Map<String, Object> outputMap = new HashMap<String, Object>();
        IRelationalDataManager adapter = DataAdapterFactory.getDefaultDataManager();
        Timer timer = new Timer();
        final Map<String, Object> returnMap = new HashMap<>();
        returnMap.put("1", 1);
        key = TokenUtil.getKey();
        token = TokenUtil.getToken();
        try {
            //定时器，防止大数据量时超时
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    PushMessagePacket.pushMap(returnMap, DSObjectType.SQUID_FLOW, "1023", "0001", key, token, MessageCode.BATCH_CODE.value());
                }
            }, 25 * 1000, 25 * 1000);
            // 判断license
            // 查询出所有的repository
            adapter.openSession(true);
            Map<String, Object> parasMap = JsonUtil.toHashMap(info);
            List<IOFlow> ioFlows = JsonUtil.toGsonList(parasMap.get("IOFlows") + "", IOFlow.class);
            FlowUnit flowUnit = JsonUtil.toGsonBean(parasMap.get("FlowUnit") + "", FlowUnit.class);
            //int fieldType = Integer.parseInt(parasMap.get("DataShireFieldType") + "");
            int userId = Integer.parseInt(parasMap.get("UserId") + "");
            if (ioFlows != null && ioFlows.size() > 0) {
                importIOFlowMap.put(key, ioFlows);
            }
            if (StringUtils.isNotNull(parasMap.get("ProjectId"))) {
                int project_id = Integer.parseInt(parasMap.get("ProjectId") + "");
                if (project_id > 0) {
                    importIOFlowMap.put(key + "_Project", project_id);
                }
            }
            if (flowUnit != null) {
                if (importModuleGuidMap.containsKey(key)) {
                    List<FlowUnit> flowList = (ArrayList<FlowUnit>) importModuleGuidMap.get(key);
                    flowList.add(flowUnit);
                    importModuleGuidMap.put(key, flowList);
                } else {
                    List<FlowUnit> flowList = new ArrayList<FlowUnit>();
                    flowList.add(flowUnit);
                    importModuleGuidMap.put(key, flowList);
                }
                List<FlowUnit> flowList = (ArrayList<FlowUnit>) importModuleGuidMap.get(key);
                Collections.sort(flowList, new Comparator<FlowUnit>() {
                    public int compare(FlowUnit arg0, FlowUnit arg1) {
                        return arg0.getIndex().compareTo(arg1.getIndex());
                    }
                });
                int cnt = flowUnit.getCount();
                int listIndex = flowList.get(flowList.size() - 1).getIndex();
                //保证接收了所有的压缩数据之后，才往下执行
                if (flowList.size() == cnt && (cnt - 1) == listIndex) {
                    logger.info("导入数据开始");
                    System.out.println(key);
                    List<IOFlow> ioflowList = (ArrayList<IOFlow>) importIOFlowMap.get(key);
                    if (ioflowList == null || ioflowList.size() == 0) {
                        out.setMessageCode(MessageCode.NODATA);
                        return outputMap;
                    }
                    int projectId = Integer.parseInt(importIOFlowMap.get(key + "_Project") + "");
                    Map<Integer, Integer> filterMap = filterIOFlowListForMap(ioflowList);
                    importSquidFlowForProject(adapter, filterMap, flowList, projectId, userId, out);
                    importIOFlowMap.remove(key);
                    importIOFlowMap.remove(key + "_Project");
                    importModuleGuidMap.remove(key);
                    logger.info("导入数据结束");
                }
            }
        } catch (ImportVersionException e) {
            timer.cancel();
            timer.purge();
            out.setMessageCode(MessageCode.ERR_IMPORT_VERSION);
            try {
                adapter.rollback();
            } catch (SQLException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            logger.error("importSquidFlows err : ", e);
        } catch (Exception e) {
            timer.cancel();
            timer.purge();
            out.setMessageCode(MessageCode.ERR_IMPORT_DATA_FORMAT);
            if(e.getMessage().equals(MessageCode.ERR_SQUID_OVER_LIMIT.value()+"")){
                out.setMessageCode(MessageCode.ERR_SQUID_OVER_LIMIT);
            }
            try {
                adapter.rollback();
            } catch (SQLException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            logger.error("importSquidFlows err : ", e);
        } finally {
            timer.cancel();
            timer.purge();
            adapter.closeSession();
        }
        return outputMap;
    }

    /**
     * 格式客户端传入参数，生成方便查询的map对象
     *
     * @param ioflowList
     * @return
     */
    public Map<Integer, Integer> filterIOFlowListForMap(List<IOFlow> ioflowList) {
        Map<Integer, Integer> map = new HashMap<Integer, Integer>();
        if (ioflowList != null && ioflowList.size() > 0) {
            for (IOFlow ioFlow : ioflowList) {
                int level = 0;
                if (ioFlow.isAllSquids() && ioFlow.isAllVariables()) {
                    level = 1;
                } else if (ioFlow.isAllSquids() && !ioFlow.isAllVariables()) {
                    level = 2;
                } else {
                    level = 3;
                }
                map.put(ioFlow.getId(), level);
            }
        }
        return map;
    }

    /**
     * 导入squid信息
     *
     * @param adapter
     * @param filterMap
     * @param flowUnitList
     * @param projectId
     * @throws Exception
     */
    public void importSquidFlowForProject(IRelationalDataManager adapter,
                                          Map<Integer, Integer> filterMap,
                                          List<FlowUnit> flowUnitList,
                                          int projectId,
                                          int userId,
                                         /* int datashireFieldType,*/
                                          ReturnValue out) throws Exception {
        Map<Integer, Integer> squidFlowMap = new HashMap<Integer, Integer>();
        Map<Integer, Integer> squidMap = new HashMap<Integer, Integer>();
        Map<Integer, Integer> sourceTableMap = new HashMap<Integer, Integer>();

        if (flowUnitList != null && flowUnitList.size() > 0) {
            byte[] tempByte = new byte[0];
            for (FlowUnit flowUnit : flowUnitList) {
                //先解压，后拼接
                byte[] merger = ZipStrUtil.unCompress(flowUnit.getData());
                tempByte = byteMerger(tempByte, merger);
            }
            //保证转换成字符串的XML格式规范化
            //String dataXml=new String(tempByte,"utf-8").replaceAll("\n","");
            //修改bug5321
            String dataXml = new String(tempByte, "utf-8");
            XStream xStream = new XStream(new DomDriver("utf-8"));//new DomDriver("utf-8")
            //xStream.autodetectAnnotations(true);
            xStream.alias("SquidFlowModuleInfo", SquidFlowModuleInfo.class);
            xStream.alias("SquidModuleInfo", SquidModuleInfo.class);
            xStream.alias("DSVariable", DSVariable.class);
            xStream.alias("SquidLink", SquidLink.class);
            xStream.alias("SquidJoin", SquidJoin.class);
            xStream.alias("DBSourceTable", DBSourceTable.class);
            xStream.alias("SourceColumn", SourceColumn.class);
            xStream.alias("Transformation", Transformation.class);
            xStream.alias("TransformationInputs", TransformationInputs.class);
            xStream.alias("TransformationLink", TransformationLink.class);
            xStream.alias("Column", Column.class);
            xStream.alias("ReferenceColumn", ReferenceColumn.class);
            xStream.alias("ReferenceColumnGroup", ReferenceColumnGroup.class);
            xStream.alias("SquidLink", SquidLink.class);
            xStream.alias("ThirdPartyParams", ThirdPartyParams.class);
            xStream.alias("ExportModelInfo", ExportModelInfo.class);
            xStream.alias("MetaModuleInfo", MetaModuleInfo.class);
            xStream.alias("IOFlow", IOFlow.class);
            xStream.alias("SquidIndexes", SquidIndexes.class);
            xStream.alias("SquidUrl", Url.class);
            xStream.alias("EsColumn", EsColumn.class);
            xStream.alias("DestHDFSColumn", DestHDFSColumn.class);
            xStream.alias("DestImpalaColumn", DestImpalaColumn.class);
            xStream.alias("UserDefinedMappingColumn", UserDefinedMappingColumn.class);
            xStream.alias("UserDefinedParameterColumn", UserDefinedParameterColumn.class);
            xStream.alias("DestHiveColumn", DestHiveColumn.class);
            xStream.alias("StatisticsDataMapColumn", StatisticsDataMapColumn.class);
            xStream.alias("StatisticsParameterColumn", StatisticsParameterColumn.class);
            xStream.alias("DestCassandraColumn", DestCassandraColumn.class);
            ExportModelInfo exportModuleInfo = (ExportModelInfo) xStream.fromXML(dataXml);
            Map<String,Object> paramMap = new HashMap<>();
            if (exportModuleInfo != null) {
                if (exportModuleInfo.getMeta() != null) {
                    String version = exportModuleInfo.getMeta().getVersion();
                    // 导入导出版本匹配规则， a.b.c   a,b匹配即可,数据结构发生不兼容时a,b不相等
                    String[] ivArray = version.split("\\.");
                    String[] vArray = CommonConsts.VERSION.split("\\.");
                    if (!(ivArray[0].equals(vArray[0]) && ivArray[1].equals(vArray[1]))) {
                        throw new ImportVersionException("系统导入版本不匹配,系统版本为：" + CommonConsts.VERSION);
                    } else {
                        //查找出当前导入的project的所对应的套餐等级
                        String sql="select dsr.packageId as packageId from ds_project dp,ds_sys_repository dsr where dp.REPOSITORY_ID=dsr.id and dp.id="+projectId;
                        Map<String,Object> rspMap = adapter.query2Object(true,sql,null);
                        int currentPackStep=-1;
                        int squidNumLimit = 0;
                        int alreadyNum = 0;
                        if(rspMap.get("PACKAGEID")==null || Integer.parseInt(rspMap.get("PACKAGEID")+"")==0){
                            currentPackStep=-1;
                        } else {
                            //判断是否导入squid
                            if(filterMap!=null){
                                //查询出当前repository存在的squid数量
                                String reSql = "select repository_id from ds_project where id=" + projectId;
                                // 查询出课程信息
                                Map<String, Object> resMap = adapter.query2Object(true, reSql, null);
                                paramMap.put("repositoryId",resMap.get("REPOSITORY_ID"));
                                String countSql = "select count(1) from ds_squid ds,ds_project dp,ds_sys_repository dsr,ds_squid_flow dsf where ds.squid_flow_id = dsf.id and dsf.project_id = dp.id and dp.repository_id = dsr.id and dsr.id = " + resMap.get("REPOSITORY_ID");
                                Map<String, Object> countMap = adapter.query2Object(true, countSql, null);
                                alreadyNum = Integer.parseInt(countMap.values().iterator().next() + "");
                                //查询出squid数量限制
                                JdbcTemplate cloudTemplate = (JdbcTemplate) Constants.context.getBean("cloudTemplate");
                                Map<String, Object> map = cloudTemplate.queryForMap("select * from packages where id=" + rspMap.get("PACKAGEID"));
                                currentPackStep = Integer.parseInt(map.get("step") + "");
                                if(currentPackStep<0){
                                    //因为deep中，step为-1
                                    currentPackStep = 1;
                                }
                                squidNumLimit = Integer.parseInt(map.get("squid_num_limit") + "");
                                //查询出课程信息
                                sql = "select cu.id as courseId from datashire_space ds,course cu  where ds.repositoryId = "+Integer.parseInt(resMap.get("REPOSITORY_ID")+"")+" and ds.id = cu.spaceId";
                                List<Map<String,Object>> courseMsgMapList = cloudTemplate.queryForList(sql);
                                int courseId = 0;
                                if(courseMsgMapList != null && courseMsgMapList.size()>0){
                                    Map<String,Object> courseMap = courseMsgMapList.get(0);
                                    courseId = Integer.parseInt(courseMap.get("courseId")+"");
                                }
                                paramMap.put("courseId",courseId);
                                for(Map.Entry<Integer,Integer> filter : filterMap.entrySet()){
                                    int squidflow_id = filter.getKey();
                                    int filterFlag = filter.getValue();
                                    if (filterFlag ==1 || filterFlag ==2) {
                                        //等于3，表示只导入variable
                                        if(squidNumLimit>-1){
                                            //查找出已经导出文件中存在的squid数量
                                            List<SquidFlowModuleInfo> squidFlowModuleInfos = exportModuleInfo.getSquidflowList();
                                            if (squidFlowModuleInfos != null) {
                                                for (SquidFlowModuleInfo flowModule : squidFlowModuleInfos) {
                                                    if(flowModule.getId()==squidflow_id) {
                                                        if (flowModule.getSquidList() != null) {
                                                            alreadyNum += flowModule.getSquidList().size();
                                                        }
                                                    }
                                                }
                                            }
                                            if(squidNumLimit>-1) {
                                                if (alreadyNum > squidNumLimit) {
                                                    throw new RuntimeException(MessageCode.ERR_SQUID_OVER_LIMIT.value() + "");
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            //Integer filterFlag = filterMap.get(id);
                            /*if (filterFlag ==1 || filterFlag ==2) {
                                //等于3，表示只导入variable
                                int packId = Integer.parseInt(rspMap.get("PACKAGEID") + "");
                                JdbcTemplate cloudTemplate = (JdbcTemplate) Constants.context.getBean("cloudTemplate");
                                Map<String, Object> map = cloudTemplate.queryForMap("select * from packages where id=" + packId);
                                currentPackStep = Integer.parseInt(map.get("step") + "");
                                squidNumLimit = Integer.parseInt(map.get("squid_num_limit") + "");
                                //查询出当前repository存在的squid数量
                                //查询出repositoryId的id
                                String reSql = "select repository_id from ds_project where id=" + projectId;
                                Map<String, Object> resMap = adapter.query2Object(true, reSql, null);
                                String countSql = "select count(1) from ds_squid ds,ds_project dp,ds_sys_repository dsr,ds_squid_flow dsf where ds.squid_flow_id = dsf.id and dsf.project_id = dp.id and dp.repository_id = dsr.id and dsr.id = " + resMap.get("REPOSITORY_ID");
                                Map<String, Object> countMap = adapter.query2Object(true, countSql, null);
                                alreadyNum = Integer.parseInt(countMap.values().iterator().next() + "");
                                //查找出已经导出文件中存在的squid数量
                                List<SquidFlowModuleInfo> squidFlowModuleInfos = exportModuleInfo.getSquidflowList();
                                if (squidFlowModuleInfos != null) {
                                    for (SquidFlowModuleInfo flowModule : squidFlowModuleInfos) {
                                        if (flowModule.getSquidList() != null) {
                                            alreadyNum += flowModule.getSquidList().size();
                                        }
                                    }
                                }
                                if(squidNumLimit>-1) {
                                    if (alreadyNum > squidNumLimit) {
                                        throw new RuntimeException(MessageCode.ERR_SQUID_OVER_LIMIT.value() + "");
                                    }
                                }
                            }*/
                        }
                        List<SquidFlowModuleInfo> squidFlowModuleInfos = exportModuleInfo.getSquidflowList();
                        if (squidFlowModuleInfos != null && squidFlowModuleInfos.size() > 0) {
                            for (SquidFlowModuleInfo flowModule : squidFlowModuleInfos) {
                                int filterLv = 0;
                                if(!filterMap.containsKey(flowModule.getId())){
                                    continue;
                                } else {
                                    filterLv = filterMap.get(flowModule.getId());
                                }
                                /**
                                 * 判断是否符合导入规则
                                 * 1.本地场只能导入本地场的squidflow（是否是本地的标准:是否存在套餐packageid=0）
                                 * 2.低套餐(packageid)
                                 * 3.私有数猎场能够导入所有类型的squidflow
                                 */
                                String oldPackSteps = DesUtils.decryptBasedDes(flowModule.getDatashireFieldType());
                                oldPackSteps=oldPackSteps.substring(36,oldPackSteps.length()-36);
                                int oldPackStep = Integer.parseInt(oldPackSteps);
                                //如果所有数猎场是本地，检查squid合法性
                                if (currentPackStep == DataShireFieldTypeEnum.LOCAL_FIELD_SQUIDFLOW.getValue()) {
                                    //如果不导入squid，那么不对squid合法性进行校验
                                    if(filterLv == 1 || filterLv == 2) {
                                        List<SquidModuleInfo> infos = flowModule.getSquidList();
                                        if (infos != null) {
                                            for (SquidModuleInfo info : infos) {
                                                Map<String, Object> attrs = info.getAttributes();
                                                if (attrs != null) {
                                                    int squidType = Integer.parseInt(attrs.get("SQUID_TYPE_ID") + "");
                                                    //deep导入课程包 允许导入 tansFile,db,CloudDB,CloudFile
                                                    if (!SquidTypeEnum.isLocalSquidValid(squidType)) {
                                                        out.setMessageCode(MessageCode.ERR_SQUIDFLOW_SQUID);
                                                        return;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                //如果不是本地数猎场
                                if(currentPackStep!=DataShireFieldTypeEnum.LOCAL_FIELD_SQUIDFLOW.getValue()
                                        && oldPackStep!=DataShireFieldTypeEnum.LOCAL_FIELD_SQUIDFLOW.getValue()){
                                    //判断套餐等级(高套餐的可以导入低套餐的)
                                    if(oldPackStep>currentPackStep){
                                        out.setMessageCode(MessageCode.ERR_SQUIDFLOW_TYPE);
                                        return;
                                    }
                                }
                                //如果是云端数猎场，校验squid是否合法
                                if(currentPackStep > 0){
                                    if(filterLv == 1 || filterLv == 2) {
                                        List<SquidModuleInfo> infos = flowModule.getSquidList();
                                        if (infos != null) {
                                            for (SquidModuleInfo info : infos) {
                                                Map<String, Object> attrs = info.getAttributes();
                                                if (attrs != null) {
                                                    int squidType = Integer.parseInt(attrs.get("SQUID_TYPE_ID") + "");
                                                    //因为deep中套餐的step为-1,项目数猎场中为-2
                                                    if(oldPackStep == DataShireFieldTypeEnum.LOCAL_FIELD_SQUIDFLOW.getValue()){
                                                        //deep数猎场
                                                        if (!SquidTypeEnum.isDeepValidSquid(squidType)) {
                                                            out.setMessageCode(MessageCode.ERR_SQUIDFLOW_SQUID);
                                                            return;
                                                        }
                                                    } else {
                                                        if (!SquidTypeEnum.isCloudValidSquid(squidType)) {
                                                            out.setMessageCode(MessageCode.ERR_SQUIDFLOW_SQUID);
                                                            return;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                if (filterMap.containsKey(flowModule.getId())) {
                                    SquidFlow newSquidFlow = new SquidFlow();
                                    newSquidFlow.setSquidflow_type(flowModule.getSquidFlow_type());
                                    newSquidFlow.setKey(StringUtils.generateGUID());
                                    newSquidFlow.setCompilation_status(flowModule.getCompilation_status());
                                    newSquidFlow.setModification_date(flowModule.getModification_date());
                                    newSquidFlow.setProject_id(projectId);
                                    newSquidFlow.setDataShireFieldType(currentPackStep);
                                    String newName = getSequenceName(adapter, flowModule.getName(),
                                            "project_id", projectId, SquidFlow.class);
                                    newSquidFlow.setName(newName);
                                    int newId = adapter.insert2(newSquidFlow);
                                    squidFlowMap.put(flowModule.getId(), newId);
                                    if (filterLv == 1 || filterLv == 2) {
                                        importSquidParmasByFlow(adapter, newId, projectId,
                                                flowModule.getSquidLinkList(),
                                                flowModule.getSquidList(),
                                                squidMap,
                                                sourceTableMap,
                                                userId,
                                                oldPackStep,currentPackStep,paramMap);
                                    }
                                    if (filterLv == 1 || filterLv == 3) {
                                        importVariableByScope(adapter,
                                                flowModule.getVariableList(),
                                                squidFlowMap,
                                                squidMap,
                                                projectId);
                                    }
                                }
                                logger.info("squidFlow:" + flowModule.getId() + " 导入成功");
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * 导入squid相关信息
     *
     * @param adapter
     * @param squidlinkList
     * @param squidList
     * @throws Exception
     * @throws SQLException
     */
    public void importSquidParmasByFlow(IRelationalDataManager adapter,
                                        Integer newSquidFlowId,
                                        Integer newProjectId,
                                        List<SquidLink> squidlinkList,
                                        List<SquidModuleInfo> squidList,
                                        Map<Integer, Integer> squidMap,
                                        Map<Integer, Integer> sourceTableMap,
                                        int userId,
                                        int oldPackStep,int currentPackStep,Map<String,Object> paramMap) throws SQLException, Exception {

        Map<Integer, Integer> columnMap = new HashMap<Integer, Integer>();
        Map<String, Integer> refColumnMap = new HashMap<String, Integer>();
        Map<String, Integer> sourceColumnMap = new HashMap<String, Integer>();
        Map<Integer, Integer> transMap = new HashMap<Integer, Integer>();
        Map<Integer, Integer> referenceTransMap = new HashMap<Integer, Integer>();
        Map<Integer, Integer> groupTaggingMap = new HashMap<>();
        Map<Integer,Integer> destCassandraMap=new HashMap<Integer, Integer>();
        ISquidDao squidDao = new SquidDaoImpl(adapter);
        IVariableDao variableDao = new VariableDaoImpl(adapter);
        ITransformationDao transDao = new TransformationDaoImpl(adapter);

        if (squidList != null && squidList.size() > 0) {
            //查找出当前repository的id
            Repository repository = adapter.query2Object(true,"select * from ds_project dp,ds_sys_repository dsr where dp.repository_id=dsr.id and dp.id="+newProjectId,null,Repository.class);
            for (SquidModuleInfo module : squidList) {
                if (module.getAttributes() != null && module.getAttributes().size() > 0) {
                    int squid_type = Integer.parseInt(String.valueOf(module.getAttributes().get("SQUID_TYPE_ID")));
                    int id = Integer.parseInt(String.valueOf(module.getAttributes().get("ID")));
                    Class cz = SquidTypeEnum.classOfValue(squid_type);
                    //特殊处理一些属性
                    module.getAttributes().put("SQUID_FLOW_ID", newSquidFlowId);
                    module.getAttributes().put("KEY", StringUtils.generateGUID());
                    module.getAttributes().remove("ID");
                    //报表中的信息进行格式化
                    if (module.getAttributes().containsKey("REPORT_TEMPLATE")) {
                        if (StringUtils.isNotNull(module.getAttributes().get("REPORT_TEMPLATE"))) {
                            byte[] strs = (byte[]) module.getAttributes().get("REPORT_TEMPLATE");
                            String template = new String(ZipStrUtil.unCompress(strs), "utf-8");
                            module.getAttributes().put("REPORT_TEMPLATE", template);
                        }
                    }
                    //地图报表中的信息进行格式化
                    if (module.getAttributes().containsKey("MAP_TEMPLATE")) {
                        if (StringUtils.isNotNull(module.getAttributes().get("MAP_TEMPLATE"))) {
                            byte[] strs = (byte[]) module.getAttributes().get("MAP_TEMPLATE");
                            String template = new String(ZipStrUtil.unCompress(strs), "utf-8");
                            module.getAttributes().put("MAP_TEMPLATE", template);
                        }
                    }

                    //修改extractSquid的check——column——id属性
                    if (module.getAttributes().containsKey("CHECK_COLUMN_ID")) {
                        if (squidlinkList != null) {
                            for (SquidLink squidLink : squidlinkList) {
                                if (squidLink.getTo_squid_id() == id) {
                                    if (sourceColumnMap.containsKey(squidLink.getFrom_squid_id() + "_" + module.getAttributes().get("CHECK_COLUMN_ID"))) {
                                        module.getAttributes().put("CHECK_COLUMN_ID", sourceColumnMap.get(squidLink.getFrom_squid_id() + "_" + module.getAttributes().get("CHECK_COLUMN_ID")));
                                    }
                                }
                            }
                        }
                    }
                    if (SquidTypeEnum.CLOUDDB.value() == squid_type) {
                        if (currentPackStep == DataShireFieldTypeEnum.PUBLIC_FIELD_SQUIDFLOW.getValue()) {
                            //因为公有数猎场只能导入公有数猎场的squidflow，所以此时能够导入说明信息就是公有数猎场的信息,无需进行操作
                            //所以此时只需要修改数据库名字和数据库用户名
                            if (module.getAttributes().containsKey("USER_NAME")) {
                                String userName = module.getAttributes().get("USER_NAME") + "";
                                if (StringUtils.isNotNull(userName)) {
                                    String regex = "u{1}[a-zA-Z0-9]+";
                                    if (userName.matches(regex)) {
                                        module.getAttributes().put("USER_NAME", "u" + repository.getId());
                                    }

                                }
                            }
                            if (module.getAttributes().containsKey("DATABASE_NAME")) {
                                String databaseName = module.getAttributes().get("DATABASE_NAME") + "";
                                if (StringUtils.isNotNull(databaseName)) {
                                    String regex = "u{1}[a-zA-Z0-9]+(db){1}";
                                    if (databaseName.matches(regex)) {
                                        module.getAttributes().put("DATABASE_NAME", "u" + repository + "db");
                                    }
                                }
                            }
                        } else if (currentPackStep >= DataShireFieldTypeEnum.PRIVATE_FILED_SQUIDFLOW.getValue()) {
                            //私有数猎场
                            if (module.getAttributes().containsKey("HOST")) {
                                String host = module.getAttributes().get("HOST") + "";
                                if (StringUtils.isNotNull(host)) {
                                    if(host.equals(SysConf.getValue("cloud_host")+":3306")){
                                        //现在都是动态配置，所以不需要来进行连接信息
                                       /* RepositoryService service = new RepositoryService();
                                        //查询出repository_id
                                        String sql = "select repository_id from ds_project where id = "+newProjectId;
                                        */
                                        //module.getAttributes().put("HOST", SysConf.getValue("cloud_db_ip_port"));
                                        RepositoryService service = new RepositoryService();
                                        String dbUrl = service.getDbUrlByRepositoryId(repository.getId());
                                        module.getAttributes().put("HOST",dbUrl);
                                    }

                                }
                            }
                            if (module.getAttributes().containsKey("USER_NAME")) {
                                String userName = module.getAttributes().get("USER_NAME") + "";
                                if (StringUtils.isNotNull(userName))
                                    module.getAttributes().put("USER_NAME", "u"+repository.getId());
                            }
                            if (module.getAttributes().containsKey("PASSWORD")) {
                                String password = module.getAttributes().get("PASSWORD") + "";
                                if (StringUtils.isNotNull(password)) {
                                    module.getAttributes().put("PASSWORD", SysConf.getValue("cloud_password"));
                                }
                            }
                            if (module.getAttributes().containsKey("DATABASE_NAME")) {
                                String databaseName = module.getAttributes().get("DATABASE_NAME") + "";
                                if (StringUtils.isNotNull(databaseName)) {
                                    module.getAttributes().put("DATABASE_NAME", "u"+repository.getId()+"db");
                                }
                            }
                        }
                    }  else if(SquidTypeEnum.TRAININGDBSQUID.value() == squid_type){
                        if (module.getAttributes().containsKey("HOST")) {
                            String host = module.getAttributes().get("HOST") + "";
                            if (StringUtils.isNotNull(host)) {
                                if(host.equals(SysConf.getValue("train_db_host")+":3306")){
                                    RepositoryService service = new RepositoryService();
                                    module.getAttributes().put("HOST",SysConf.getValue("train_db_real_host"));
                                }

                            }
                        }
                        if (module.getAttributes().containsKey("DATABASE_NAME")) {
                            String databaseName = module.getAttributes().get("DATABASE_NAME") + "";
                            if (StringUtils.isNotNull(databaseName)) {
                                module.getAttributes().put("DATABASE_NAME", "c"+paramMap.get("courseId"));
                            }
                        }
                        module.getAttributes().put("COURSE_ID",paramMap.get("courseId"));
                    } else if(SquidTypeEnum.TRAINNINGFILESQUID.value() == squid_type){
                        if (module.getAttributes().containsKey("FILE_PATH")) {
                            String path = module.getAttributes().get("FILE_PATH") + "";
                            if (StringUtils.isNotNull(path)) {
                                String regex = "(/cdata/c){1}[A-Za-z0-9]+";
                                if (path.matches(regex)) {
                                    module.getAttributes().put("FILE_PATH", "/cdata/c" + paramMap.get("courseId")+"/");
                                }
                            }
                        }
                        if (module.getAttributes().containsKey("HOST")) {
                            String host = module.getAttributes().get("HOST") + "";
                            if (StringUtils.isNotNull(host)) {
                                if(host.equals(SysConf.getValue("train_file_host"))){
                                    module.getAttributes().put("HOST", SysConf.getValue("train_file_real_host"));
                                }
                            }
                        }
                        module.getAttributes().put("COURSE_ID",paramMap.get("courseId"));
                    } else if (SquidTypeEnum.SOURCECLOUDFILE.value() == squid_type || SquidTypeEnum.DESTCLOUDFILE.value()==squid_type) {
                        //if (datashireFieldType == DataShireFieldTypeEnum.PUBLIC_FIELD_SQUIDFLOW.getValue()) {
                        if (module.getAttributes().containsKey("FILE_PATH")) {
                            String path = module.getAttributes().get("FILE_PATH") + "";
                            if (StringUtils.isNotNull(path)) {
                                String regex = "(/udata/u){1}[A-Za-z0-9]+/";
                                if (path.matches(regex)) {
                                    module.getAttributes().put("FILE_PATH", "/udata/u" + repository.getId() + "/");
                                }
                            }
                        }
                        if (module.getAttributes().containsKey("HOST")) {
                            String host = module.getAttributes().get("HOST") + "";
                            if (StringUtils.isNotNull(host)) {
                                if (host.equals(SysConf.getValue("hdfs_host"))) {
                                    module.getAttributes().put("HOST", SysConf.getValue("hdfsIpAndPort"));
                                }
                            }
                        }
                    }
                    int newId = adapter.insert2(AnnotationHelper.result2Object(module.getAttributes(), cz));
                    if (squid_type == SquidTypeEnum.GROUPTAGGING.value()) {
                        groupTaggingMap.put(id, newId);
                    }

                    if(squid_type==SquidTypeEnum.PIVOTSQUID.value()){
                        //根据老的id查询PivotSquid的 pivot列和value列
                        Map<String,String> param=new HashMap<>();
                        param.put("id",id+"");
                        PivotSquid oldPivotSquid=(PivotSquid) AnnotationHelper.result2Object(module.getAttributes(),cz);
                        Integer oldPivot=oldPivotSquid.getPivotColumnId();
                        Integer oldValue=oldPivotSquid.getValueColumnId();
                        String oldGroupColumnId=oldPivotSquid.getGroupByColumnIds();
                        String[] oldGroups=null;
                        String newGroups="";
                        if(StringUtils.isNotNull(oldGroupColumnId) && oldGroupColumnId.length()>0){
                            if(oldGroupColumnId.contains(",")){
                                oldGroups=oldGroupColumnId.split(",");
                            }else{
                                newGroups=columnMap.get(Integer.parseInt(oldGroupColumnId))+"";
                            }
                        }
                        if(oldGroups!=null){
                            for(int i=0;i<oldGroups.length;i++){
                                Integer columnId=columnMap.get(Integer.parseInt(oldGroups[i]));
                                if(columnId!=null){
                                    newGroups+=(columnId+",");
                                }
                            }
                        }
                        Integer newPivot=columnMap.get(oldPivot);
                        Integer newValue=columnMap.get(oldValue);
                        param=new HashMap<>();
                        param.put("id",newId+"");
                        PivotSquid newPivotSquid=adapter.query2Object2(true,param,PivotSquid.class);
                        newPivotSquid.setPivotColumnId(newPivot);
                        newPivotSquid.setValueColumnId(newValue);
                        newPivotSquid.setAggregationType(oldPivotSquid.getAggregationType());
                        if(newGroups.length()>0) {
                            if (newGroups.contains(",")) {
                                newPivotSquid.setGroupByColumnIds(newGroups.substring(0, newGroups.length()-1));
                            } else {
                                newPivotSquid.setGroupByColumnIds(newGroups);
                            }
                        }
                        adapter.update2(newPivotSquid);
                    }


                    module.getAttributes().put("ID", id);
                    squidMap.put(id, newId);
                    if(SquidTypeEnum.DEST_CASSANDRA.value()==squid_type){
                        if(module.getAttributes().containsKey("DEST_SQUID_ID")){
                            destCassandraMap.put(Integer.parseInt(module.getAttributes().get("DEST_SQUID_ID")+""),newId);
                        }
                    }
                    //导入时 要把cassandra落地对应的数据源更新过来
                    if(squidMap!=null&&squidMap.size()>0){
                        for (Integer key : squidMap.keySet()){
                            if(destCassandraMap.containsKey(key)){
                                DestCassandraSquid destCassandraSquid= squidDao.getSquidForCond(destCassandraMap.get(key), DestCassandraSquid.class);
                                destCassandraSquid.setDest_squid_id(squidMap.get(key));
                                squidDao.update(destCassandraSquid);
                                destCassandraMap.remove(key);
                            }
                        }
                    }

                    //循环添加Column及VTransformation
                    if (module.getColumnList() != null && module.getColumnList().size() > 0) {
                        for (Column column : module.getColumnList()) {
                            int columnId = column.getId();
                            column.setId(0);
                            column.setSquid_id(newId);
                            column.setKey(StringUtils.generateGUID());
                            int newColumnId = adapter.insert2(column);
                            columnMap.put(columnId, newColumnId);
                        }
                    }
                    //修改TagSquid的column
                    for (Map.Entry<Integer, Integer> groupMap : groupTaggingMap.entrySet()) {
                        Map<String, Object> params = new HashMap<>();
                        params.put("id", groupMap.getValue());
                        List<DataSquid> dataSquids = adapter.query2List2(true, params, DataSquid.class);
                        if (dataSquids != null && dataSquids.size() > 0) {
                            DataSquid dataSquid = dataSquids.get(0);
                            String groupColumn = dataSquid.getGroupColumnIds();
                            String tagColumn = dataSquid.getTaggingColumnIds();
                            String sortColumn = dataSquid.getSortingColumnIds();
                            if (StringUtils.isNotNull(groupColumn)) {
                                String[] groupColumns = groupColumn.split(",");
                                StringBuffer groupBuffer = new StringBuffer("");
                                for (int i = 0; i < groupColumns.length; i++) {
                                    if (columnMap.containsKey(Integer.parseInt(groupColumns[i]))) {
                                        groupColumns[i] = columnMap.get(Integer.parseInt(groupColumns[i])) + "";
                                    }
                                    groupBuffer.append(groupColumns[i]);
                                    if (i < groupColumns.length - 1) {
                                        groupBuffer.append(",");
                                    }
                                }
                                dataSquid.setGroupColumnIds(groupBuffer.toString());
                            }
                            if (StringUtils.isNotNull(tagColumn)) {
                                String[] tagColumns = tagColumn.split(",");
                                StringBuffer tagBuffer = new StringBuffer("");
                                for (int i = 0; i < tagColumns.length; i++) {
                                    if (columnMap.containsKey(Integer.parseInt(tagColumns[i]))) {
                                        tagColumns[i] = columnMap.get(Integer.parseInt(tagColumns[i])) + "";
                                    }
                                    tagBuffer.append(tagColumns[i]);
                                    if (i < tagColumns.length - 1) {
                                        tagBuffer.append(",");
                                    }
                                }
                                dataSquid.setTaggingColumnIds(tagBuffer.toString());
                            }
                            if (StringUtils.isNotNull(sortColumn)) {
                                String[] sortColumns = sortColumn.split(",");
                                StringBuffer sortBuffer = new StringBuffer("");
                                for (int i = 0; i < sortColumns.length; i++) {
                                    if (columnMap.containsKey(Integer.parseInt(sortColumns[i]))) {
                                        sortColumns[i] = columnMap.get(Integer.parseInt(sortColumns[i])) + "";
                                    }
                                    sortBuffer.append(sortColumns[i]);
                                    if (i < sortColumns.length - 1) {
                                        sortBuffer.append(",");
                                    }
                                }
                                dataSquid.setSortingColumnIds(sortBuffer.toString());
                            }
                            String sql = "UPDATE ds_squid SET GROUP_COLUMN=?,SORT_COLUMN=?,TAGGING_COLUMN=? WHERE ID=?";
                            List<Object> paramMaps = new ArrayList<>();
                            paramMaps.add(0, dataSquid.getGroupColumnIds());
                            paramMaps.add(1, dataSquid.getSortingColumnIds());
                            paramMaps.add(2, dataSquid.getTaggingColumnIds());
                            paramMaps.add(3, dataSquid.getId());
                            adapter.execute(sql, paramMaps);
                        }
                    }

                    //循环添加SourceTable及SourceColumn
                    if (module.getSourceTableList() != null && module.getSourceTableList().size() > 0) {
                        for (DBSourceTable stable : module.getSourceTableList()) {
                            int oldId = stable.getId();
                            stable.setId(0);
                            stable.setKey(StringUtils.generateGUID());
                            stable.setSource_squid_id(newId);
                            int newTableId = adapter.insert2(stable);
                            sourceTableMap.put(oldId, newTableId);
                            if (stable.getSourceColumns() != null && stable.getSourceColumns().size() > 0) {
                                for (SourceColumn column : stable.getSourceColumns()) {
                                    int oldSourceColumnId = column.getId();
                                    column.setId(0);
                                    column.setKey(StringUtils.generateGUID());
                                    column.setSource_table_id(newTableId);
                                    int newSourceColumnId = adapter.insert2(column);
                                    sourceColumnMap.put(id + "_" + oldSourceColumnId, newSourceColumnId);
                                }
                            }
                        }
                    }

                    //添加Squid级别的 变量
                    if (module.getVariableList() != null && module.getVariableList().size() > 0) {
                        for (DSVariable variable : module.getVariableList()) {
                            variable.setId(0);
                            variable.setProject_id(newProjectId);
                            variable.setSquid_flow_id(newSquidFlowId);
                            variable.setId(newId);
                            variable.setSquid_id(newId);
                            variableDao.resetVariableName(variable);
                            adapter.insert2(variable);
                        }
                    }
                }
            }
            for (SquidModuleInfo module : squidList) {
                if (module.getAttributes() != null && module.getAttributes().size() > 0) {
                    int id = Integer.parseInt(String.valueOf(module.getAttributes().get("ID")));
                    int newId = squidMap.get(id);
                    //处理落地对象
                    if (module.getAttributes().containsKey("DESTINATION_SQUID_ID") &&
                            !String.valueOf(module.getAttributes().get("DESTINATION_SQUID_ID")).equals("0") &&
                            !String.valueOf(module.getAttributes().get("DESTINATION_SQUID_ID")).equals("null")) {
                        int destinaction_squid_id = Integer.parseInt(String.valueOf(module.getAttributes().get("DESTINATION_SQUID_ID")));
                        if (destinaction_squid_id > 0 && squidMap.containsKey(destinaction_squid_id)) {
                            int newDestId = squidMap.get(destinaction_squid_id);
                            module.getAttributes().put("DESTINATION_SQUID_ID", newDestId);
                            int squid_type = Integer.parseInt(String.valueOf(module.getAttributes().get("SQUID_TYPE_ID")));
                            Class cz = SquidTypeEnum.classOfValue(squid_type);
                            module.getAttributes().put("ID", newId);
                            adapter.update2(AnnotationHelper.result2Object(module.getAttributes(), cz));
                            module.getAttributes().put("ID", id);
                        }
                    }
                    //添加落地对象中的column
                    int squid_type = Integer.parseInt(String.valueOf(module.getAttributes().get("SQUID_TYPE_ID")));
                    Class<?> cz = SquidTypeEnum.classOfValue(squid_type);
                    if (SquidTypeEnum.DEST_HDFS.value() == squid_type || SquidTypeEnum.DESTCLOUDFILE.value() == squid_type) {
                        if (module.getHdfsColumns() != null && module.getHdfsColumns().size() > 0) {
                            for (DestHDFSColumn hdfsColumn : module.getHdfsColumns()) {
                                hdfsColumn.setSquid_id(newId);
                                if (columnMap.containsKey(hdfsColumn.getColumn_id())) {
                                    hdfsColumn.setColumn_id(columnMap.get(hdfsColumn.getColumn_id()));
                                    hdfsColumn.setId(adapter.insert2(hdfsColumn));
                                }

                            }
                        }
                    } else if (SquidTypeEnum.DEST_IMPALA.value() == squid_type) {
                        if (module.getImpalaColumns() != null && module.getImpalaColumns().size() > 0) {
                            for (DestImpalaColumn impalaColumn : module.getImpalaColumns()) {
                                impalaColumn.setSquid_id(newId);
                                if (columnMap.containsKey(impalaColumn.getColumn_id())) {
                                    impalaColumn.setColumn_id(columnMap.get(impalaColumn.getColumn_id()));
                                    impalaColumn.setId(adapter.insert2(impalaColumn));
                                }

                            }
                        }
                    } else if (SquidTypeEnum.DESTES.value() == squid_type) {
                        if (module.getEsColumns() != null && module.getEsColumns().size() > 0) {
                            for (EsColumn esc : module.getEsColumns()) {
                                esc.setSquid_id(newId);
                                if (columnMap.containsKey(esc.getColumn_id())) {
                                    esc.setColumn_id(columnMap.get(esc.getColumn_id()));
                                }
                                int newEsColumnId = adapter.insert2(esc);
                                esc.setId(newEsColumnId);
                            }
                        }
                    } else if (SquidTypeEnum.DEST_HIVE.value() == squid_type) {
                        if (module.getHiveColumns() != null && module.getHiveColumns().size() > 0) {
                            for (DestHiveColumn esc : module.getHiveColumns()) {
                                esc.setSquid_id(newId);
                                if (columnMap.containsKey(esc.getColumn_id())) {
                                    esc.setColumn_id(columnMap.get(esc.getColumn_id()));
                                }
                                int newHiveColumnId = adapter.insert2(esc);
                                esc.setId(newHiveColumnId);
                            }
                        }
                    } else if (SquidTypeEnum.USERDEFINED.value() == squid_type) {
                        if (module.getUserDefinedMappingColumns() != null && module.getUserDefinedMappingColumns().size() > 0) {
                            for (UserDefinedMappingColumn mappingColumn : module.getUserDefinedMappingColumns()) {
                                mappingColumn.setSquid_id(newId);
                                if (columnMap.containsKey(mappingColumn.getColumn_id())) {
                                    mappingColumn.setColumn_id(columnMap.get(mappingColumn.getColumn_id()));
                                }
                                mappingColumn.setId(adapter.insert2(mappingColumn));
                            }
                        }
                        if (module.getUserDefinedParameterColumns() != null && module.getUserDefinedParameterColumns().size() > 0) {
                            for (UserDefinedParameterColumn parameterColumn : module.getUserDefinedParameterColumns()) {
                                parameterColumn.setSquid_id(newId);
                                parameterColumn.setId(adapter.insert2(parameterColumn));
                            }
                        }
                    } else if (squid_type == SquidTypeEnum.STATISTICS.value()) {
                        if (module.getStatisticsDataMapColumns() != null && module.getStatisticsDataMapColumns().size() > 0) {
                            for (StatisticsDataMapColumn mappingColumn : module.getStatisticsDataMapColumns()) {
                                mappingColumn.setSquid_id(newId);
                                if (columnMap.containsKey(mappingColumn.getColumn_id())) {
                                    mappingColumn.setColumn_id(columnMap.get(mappingColumn.getColumn_id()));
                                }
                                mappingColumn.setId(adapter.insert2(mappingColumn));
                            }
                        }
                        if (module.getStatisticsParameterColumns() != null && module.getStatisticsParameterColumns().size() > 0) {
                            for (StatisticsParameterColumn parameterColumn : module.getStatisticsParameterColumns()) {
                                parameterColumn.setSquid_id(newId);
                                parameterColumn.setId(adapter.insert2(parameterColumn));
                            }
                        }
                    }else if (SquidTypeEnum.DEST_CASSANDRA.value() == squid_type) {
                        if (module.getCassandraColumns()!= null && module.getCassandraColumns().size() > 0) {
                            for (DestCassandraColumn esc : module.getCassandraColumns()) {
                                esc.setSquid_id(newId);
                                if (columnMap.containsKey(esc.getColumn_id())) {
                                    esc.setColumn_id(columnMap.get(esc.getColumn_id()));
                                }
                                int newCassandraColumnId = adapter.insert2(esc);
                                esc.setId(newCassandraColumnId);
                            }
                        }
                    }
                    //重新计算map地图里面的引用column值
                    if (module.getAttributes().containsKey("MAP_TEMPLATE")) {
                        squidDao.modifyTemplateImportBySquidId(newId, columnMap);
                    }
                    //重新计算dataming中的引用对象
                    if (module.getAttributes().containsKey("CATEGORICAL_SQUID")) {
                        squidDao.modifyDataMiningBySquidId(newId, squidMap);
                    }
                    if (module.getAttributes().containsKey("SOURCE_TABLE_ID")) {
                        squidDao.modifySourceTableIdBySquidId(newId, sourceTableMap, cz);
                    }

                    //循环添加ReferenceColumn及Group
                    if (module.getColumnGroupList() != null && module.getColumnGroupList().size() > 0) {
                        for (ReferenceColumnGroup group : module.getColumnGroupList()) {
                            //int groupId = group.getId();
                            group.setId(0);
                            group.setKey(StringUtils.generateGUID());
                            group.setReference_squid_id(newId);
                            int newGroupId = adapter.insert2(group);
                            if (group.getReferenceColumnList() != null && group.getReferenceColumnList().size() > 0) {
                                for (ReferenceColumn refColumn : group.getReferenceColumnList()) {
                                    int oldRefColumnId = refColumn.getColumn_id();
                                    int oldRefSquidId = refColumn.getReference_squid_id();
                                    int newHostSquidId = squidMap.get(refColumn.getHost_squid_id());
                                    int newRefColumnId = -oldRefColumnId;
                                    if (sourceColumnMap.containsKey(refColumn.getHost_squid_id() + "_" + oldRefColumnId)) {
                                        newRefColumnId = sourceColumnMap.get(refColumn.getHost_squid_id() + "_" + oldRefColumnId);
                                    } else if (columnMap.containsKey(oldRefColumnId)) {
                                        newRefColumnId = columnMap.get(oldRefColumnId);
                                    }
                                    refColumn.setColumn_id(newRefColumnId);
                                    refColumn.setGroup_id(newGroupId);
                                    refColumn.setKey(StringUtils.generateGUID());
                                    refColumn.setReference_squid_id(newId);
                                    refColumn.setHost_squid_id(newHostSquidId);
                                    adapter.insert2(refColumn);
                                    refColumnMap.put(oldRefSquidId + "_" + oldRefColumnId, newRefColumnId);
                                }
                            }
                        }
                    }

                    //循环添加Transformation及inputs
                    if (module.getTransList() != null && module.getTransList().size() > 0) {
                        //先循环添加trans，为了记录input中存在的sourceTransId
                        for (Transformation trans : module.getTransList()) {
                            if (trans.getColumn_id() != 0) {
                                //判断，如果columnId和transformation相同，代表是refColumn的虚拟trans
                                String rkey = trans.getSquid_id() + "_" + trans.getColumn_id();
                                if (refColumnMap.containsKey(rkey)) {
                                    int newRefColumnId = refColumnMap.get(rkey);
                                    int transId = trans.getId();
                                    trans.setId(0);
                                    trans.setKey(StringUtils.generateGUID());
                                    trans.setSquid_id(newId);
                                    trans.setColumn_id(newRefColumnId);
                                    //引用的模型应该是当前squid Flow中的模型
                                    if(trans.getTranstype()== TransformationTypeEnum.PREDICT.value() || trans.getTranstype()== TransformationTypeEnum.INVERSENORMALIZER.value() ||
                                            trans.getTranstype()== TransformationTypeEnum.INVERSEQUANTIFY.value() || trans.getTranstype()== TransformationTypeEnum.RULESQUERY.value()){
                                        if(squidMap.get(trans.getModel_squid_id())!=null){
                                            trans.setModel_squid_id(squidMap.get(trans.getModel_squid_id()));
                                        }
                                    }
                                    int newTransId = adapter.insert2(trans);
                                    trans.setId(transId);
                                    transMap.put(transId, newTransId);
                                } else {
                                    //不存在，那么就进行column查询
                                    int newColumnId = -trans.getColumn_id();
                                    if (columnMap.containsKey(trans.getColumn_id())) {
                                        newColumnId = columnMap.get(trans.getColumn_id());
                                    }
                                    int transId = trans.getId();
                                    trans.setId(0);
                                    trans.setKey(StringUtils.generateGUID());
                                    trans.setSquid_id(newId);
                                    trans.setColumn_id(newColumnId);
                                    //引用的模型应该是当前squid Flow中的模型
                                    if(trans.getTranstype()== TransformationTypeEnum.PREDICT.value() || trans.getTranstype()== TransformationTypeEnum.INVERSENORMALIZER.value() ||
                                            trans.getTranstype()== TransformationTypeEnum.INVERSEQUANTIFY.value() || trans.getTranstype()== TransformationTypeEnum.RULESQUERY.value()){
                                        if(squidMap.get(trans.getModel_squid_id())!=null){
                                            trans.setModel_squid_id(squidMap.get(trans.getModel_squid_id()));
                                        }
                                    }
                                    int newTransId = adapter.insert2(trans);
                                    trans.setId(transId);
                                    transMap.put(transId, newTransId);
                                }
                                if (trans.getDictionary_squid_id() > 0) {
                                    referenceTransMap.put(trans.getId(), trans.getDictionary_squid_id());
                                }
                                if (trans.getModel_squid_id() > 0) {
                                    referenceTransMap.put(trans.getId(), trans.getModel_squid_id());
                                }
                            } else {
                                // 实体transformation的添加
                                int transId = trans.getId();
                                trans.setId(0);
                                trans.setKey(StringUtils.generateGUID());
                                trans.setSquid_id(newId);
                                trans.setColumn_id(0);
                                //引用的模型应该是当前squid Flow中的模型
                                if(trans.getTranstype()== TransformationTypeEnum.PREDICT.value() || trans.getTranstype()== TransformationTypeEnum.INVERSENORMALIZER.value() ||
                                        trans.getTranstype()== TransformationTypeEnum.INVERSEQUANTIFY.value() || trans.getTranstype()== TransformationTypeEnum.RULESQUERY.value()){
                                    if(squidMap.get(trans.getModel_squid_id())!=null){
                                        trans.setModel_squid_id(squidMap.get(trans.getModel_squid_id()));
                                    }
                                }
                                int newTransId = adapter.insert2(trans);
                                trans.setId(transId);

                                transMap.put(transId, newTransId);
                            }
                        }
                        //得到所有的trans对应关系后，进行transInput的添加
                        for (Transformation trans : module.getTransList()) {
                            int newTransId = transMap.get(trans.getId());
                            if (trans.getInputs() != null && trans.getInputs().size() > 0) {
                                for (TransformationInputs input : trans.getInputs()) {
                                    input.setId(0);
                                    input.setKey(StringUtils.generateGUID());
                                    input.setTransformationId(newTransId);
                                    int sourceId = input.getSource_transform_id();
                                    if (sourceId > 0 && transMap!=null && transMap.size()>0) {
                                        if(transMap.containsKey(sourceId)) {
                                            int newSourceId = transMap.get(sourceId);
                                            input.setSource_transform_id(newSourceId);
                                        }
                                    }
                                    adapter.insert2(input);
                                }
                            }
                        }
                    }

                    //根据获取到的trans信息，进行translink的创建
                    if (module.getTransLinkList() != null && module.getTransLinkList().size() > 0) {
                        for (TransformationLink link : module.getTransLinkList()) {
                            int newFromId = transMap.get(link.getFrom_transformation_id());
                            int newToId = transMap.get(link.getTo_transformation_id());
                            link.setFrom_transformation_id(newFromId);
                            link.setTo_transformation_id(newToId);
                            link.setKey(StringUtils.generateGUID());
                            link.setId(0);
                            adapter.insert2(link);
                        }
                    }

                    //添加join集合
                    if (module.getSquidJoinList() != null && module.getSquidJoinList().size() > 0) {
                        for (SquidJoin join : module.getSquidJoinList()) {
                            int fromSquidId = squidMap.get(join.getTarget_squid_id());
                            int toSquidId = squidMap.get(join.getJoined_squid_id());
                            join.setId(0);
                            join.setTarget_squid_id(fromSquidId);
                            join.setJoined_squid_id(toSquidId);
                            join.setKey(StringUtils.generateGUID());
                            adapter.insert2(join);
                        }
                    }

                    //添加SquidIndexes集合
                    if (module.getSquidIndexesList() != null && module.getSquidIndexesList().size() > 0) {
                        for (SquidIndexes indexes : module.getSquidIndexesList()) {
                            indexes.setId(0);
                            indexes.setColumn_id1(columnMap.get(indexes.getColumn_id1()) == null ? 0 :
                                    columnMap.get(indexes.getColumn_id1()));
                            indexes.setColumn_id2(columnMap.get(indexes.getColumn_id2()) == null ? 0 :
                                    columnMap.get(indexes.getColumn_id2()));
                            indexes.setColumn_id3(columnMap.get(indexes.getColumn_id3()) == null ? 0 :
                                    columnMap.get(indexes.getColumn_id3()));
                            indexes.setColumn_id4(columnMap.get(indexes.getColumn_id4()) == null ? 0 :
                                    columnMap.get(indexes.getColumn_id4()));
                            indexes.setColumn_id5(columnMap.get(indexes.getColumn_id5()) == null ? 0 :
                                    columnMap.get(indexes.getColumn_id5()));
                            indexes.setColumn_id6(columnMap.get(indexes.getColumn_id6()) == null ? 0 :
                                    columnMap.get(indexes.getColumn_id6()));
                            indexes.setColumn_id7(columnMap.get(indexes.getColumn_id7()) == null ? 0 :
                                    columnMap.get(indexes.getColumn_id7()));
                            indexes.setColumn_id8(columnMap.get(indexes.getColumn_id8()) == null ? 0 :
                                    columnMap.get(indexes.getColumn_id8()));
                            indexes.setColumn_id9(columnMap.get(indexes.getColumn_id9()) == null ? 0 :
                                    columnMap.get(indexes.getColumn_id9()));
                            indexes.setColumn_id10(columnMap.get(indexes.getColumn_id10()) == null ? 0 :
                                    columnMap.get(indexes.getColumn_id10()));
                            indexes.setSquid_id(newId);
                            adapter.insert2(indexes);
                        }
                    }

                    //添加 抓取信息集合
                    if (module.getSquidUrls() != null && module.getSquidUrls().size() > 0) {
                        for (Url url : module.getSquidUrls()) {
                            url.setId(0);
                            url.setSquid_id(newId);
                            adapter.insert2(url);
                        }
                    }

                    //添加 第三方参数集合
                    if (module.getParamsList() != null && module.getParamsList().size() > 0) {
                        for (ThirdPartyParams params : module.getParamsList()) {
                            params.setId(0);
                            params.setSquid_id(newId);
                            if (params.getRef_squid_id() > 0) {
                                int new_ref_squid_id = squidMap.get(params.getRef_squid_id()) == null
                                        ? 0 : squidMap.get(params.getRef_squid_id());
                                if (new_ref_squid_id > 0) params.setRef_squid_id(new_ref_squid_id);
                            }
                            if (params.getColumn_id() > 0) {
                                int new_column_id = columnMap.get(params.getColumn_id()) == null
                                        ? 0 : columnMap.get(params.getColumn_id());
                                if (new_column_id > 0) params.setColumn_id(new_column_id);
                            }
                            adapter.insert2(params);
                        }
                    }

                }
            }
            //添加squidLink集合
            if (squidlinkList != null && squidlinkList.size() > 0) {
                for (SquidLink squidLink : squidlinkList) {
                    int fromSquidId = squidMap.get(squidLink.getFrom_squid_id());
                    int toSquidId = squidMap.get(squidLink.getTo_squid_id());
                    squidLink.setId(0);
                    squidLink.setFrom_squid_id(fromSquidId);
                    squidLink.setTo_squid_id(toSquidId);
                    squidLink.setSquid_flow_id(newSquidFlowId);
                    squidLink.setKey(StringUtils.generateGUID());
                    adapter.insert2(squidLink);
                }
            }
        }

        //最后修改trans中引用的squidId
        if (referenceTransMap != null && referenceTransMap.size() > 0) {
            for (Integer transId : referenceTransMap.keySet()) {
                Integer oldSquidId = referenceTransMap.get(transId);
                if (squidMap.containsKey(oldSquidId)) {
                    int newSquidId = squidMap.get(oldSquidId);
                    Transformation trans = transDao.getObjectById(transId, Transformation.class);
                    if (trans != null && trans.getDictionary_squid_id() > 0) {
                        transDao.modifyRefSquidForTransId(transId, newSquidId, 0);
                    } else if (trans != null && trans.getModel_squid_id() > 0) {
                        transDao.modifyRefSquidForTransId(transId, newSquidId, 1);
                    }
                }
            }
        }
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
        map.put("squid_id", oldSquidId);
        List<EsColumn> listEsColumn = adapter3.query2List2(true, map, EsColumn.class);
        map.put("squid_id", fromSquidId);
        List<Column> listColumn = adapter3.query2List2(true, map, Column.class);
        for (Column column : listColumn) {
            for (EsColumn esColumn : listEsColumn) {
                if (esColumn.getField_name().equals(column.getName())) {
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
     * 抽取导入变量模板（根据不同的作用域）
     *
     * @param adapter
     * @param squidFlowMap
     * @param squidMap
     * @param projectId
     * @throws SQLException
     */
    public void importVariableByScope(IRelationalDataManager adapter,
                                      List<DSVariable> variableList,
                                      Map<Integer, Integer> squidFlowMap,
                                      Map<Integer, Integer> squidMap,
                                      int projectId) throws SQLException {
        //添加Squid级别的 变量
        IVariableDao variableDao = new VariableDaoImpl(adapter);
        if (variableList != null && variableList.size() > 0) {
            for (DSVariable variable : variableList) {
                int newSquidFlowId = 0;
                int newSquidId = 0;
                if (variable.getSquid_flow_id() > 0) {
                    newSquidFlowId = squidFlowMap.get(variable.getSquid_flow_id());
                }
                if (variable.getSquid_id() > 0) {
                    newSquidId = squidMap.get(variable.getSquid_id());
                }
                variable.setId(0);
                variable.setProject_id(projectId);
                variable.setSquid_flow_id(newSquidFlowId);
                variable.setId(newSquidId);
                variableDao.resetVariableName(variable);
                adapter.insert2(variable);
            }
        }
    }

    //java 合并两个byte数组
    public static byte[] byteMerger(byte[] byte_1, byte[] byte_2) {
        byte[] byte_3 = new byte[byte_1.length + byte_2.length];
        System.arraycopy(byte_1, 0, byte_3, 0, byte_1.length);
        System.arraycopy(byte_2, 0, byte_3, byte_1.length, byte_2.length);
        return byte_3;
    }

    /**
     * 设置重复名称后自动创建序号
     *
     * @param adapter3
     * @return
     */
    private <T> String getSequenceName(IRelationalDataManager adapter3, String name,
                                       String parentKey, int parentId, Class<T> c) {
        Map<String, Object> params = new HashMap<String, Object>();
        int cnt = 1;
        String tempName = name;
        while (true) {
            params.put("name", tempName);
            params.put(parentKey, parentId);
            T squid = adapter3.query2Object(true, params, c);
            if (squid == null) {
                return tempName;
            }
            if (cnt == 1) {
                tempName = name + "_副本";
            } else {
                tempName = name + "_副本" + (cnt - 1) + "";
            }
            cnt++;
        }
    }

    public static void main(String[] args) {
        List<IOFlow> list = new ArrayList<IOFlow>();
        IOFlow flow = new IOFlow();
        flow.setId(3);
        flow.setName("SquidFlow");
        flow.setAllSquids(true);
        flow.setAllVariables(true);
        list.add(flow);
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("IOFlows", list);
        map.put("ProjectId", 1);
        FlowUnit unit = new FlowUnit();
        unit.setCount(1);
        unit.setIndex(0);
        //XStream xstream = new XStream(new DomDriver("utf-8"));
        try {
            /*ExportModelInfo modelInfo = (ExportModelInfo)xstream.fromXML(
                    new FileInputStream("d:/app/xStram.xml"));
			String dataXml = xstream.toXML(modelInfo);
			byte[] strs = dataXml.getBytes();*/
            byte[] datas = FileUtils.readFileToByteArray(new File("d:/app/xStram.xml"));
            byte[] tempDatas = ZipStrUtil.compress(datas);
            unit.setData(tempDatas);
            map.put("FlowUnit", unit);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        String info = JsonUtil.object2Json(map);
        ReturnValue out = new ReturnValue();
        RepositoryImportProcess process = new RepositoryImportProcess();
        Map<String, Object> outputMap = process.importSquidFlows(info, out);
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }
}
