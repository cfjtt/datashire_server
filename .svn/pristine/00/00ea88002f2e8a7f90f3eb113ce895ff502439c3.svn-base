package com.eurlanda.datashire.complieValidate;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.dao.IDBSquidDao;
import com.eurlanda.datashire.dao.impl.DBSquidDaoImpl;
import com.eurlanda.datashire.entity.BuildInfo;
import com.eurlanda.datashire.entity.Column;
import com.eurlanda.datashire.entity.Squid;
import com.eurlanda.datashire.entity.Transformation;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.enumeration.SchedulerLogStatus;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.server.utils.TokenUtil;
import com.eurlanda.datashire.sprint7.packet.PushMessagePacket;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import org.apache.log4j.Logger;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 编译检查具体实现类,接受客户端发送的请求，调用相关的validateSquidProcess方法
 * Created by Eurlanda-dev on 2016/8/9.
 */
@Service
@Scope("prototype")
public class CompileValidateService {
    private static Logger logger = Logger.getLogger(CompileValidateService.class);// 记录日志
    //private ThreadLocal tokenThread = new ThreadLocal();
    private String token;//令牌根据令牌得到相应的连接信息
    private String key;//key值

    /**
     * 编译检查入口，负责调用检查各个squid的方法，然后将信息返回
     *
     * @param info
     * @return
     */
    public void compileValidate(String info) {
        IRelationalDataManager adapter = DataAdapterFactory.getDefaultDataManager();
        adapter.openSession();
        Map<String, Object> map = JsonUtil.toHashMap(info);
        Map<String, Object> params = new HashMap<String, Object>();
        int repositoryId = Integer.parseInt(map.get("RepositoryId") + "");
        int SquidFlowId = Integer.parseInt(map.get("SquidflowId") + "");
        params.put("squid_flow_id", SquidFlowId);
        boolean isLast = false;
        try {
            //获取当前squidFlow下面所有的squid
            List<Squid> squidList = adapter.query2List2(true, params, Squid.class);
            //验证是否是空squidflow
            if (squidList == null || squidList.size() == 0) {
                Map<String, Object> resultMap = new HashMap<String, Object>();
                BuildInfo buildInfo = new BuildInfo();
                buildInfo.setRepositoryId(repositoryId);
                buildInfo.setInfoType(SchedulerLogStatus.WARNING.getValue());
                buildInfo.setLastBuildInfo(true);
                buildInfo.setMessageCode(MessageCode.ERR_SQUIDFLOW_ISNULL.value());
                buildInfo.setSquidFlowId(SquidFlowId);
                List<BuildInfo> buildInfoList = new ArrayList<BuildInfo>();
                buildInfoList.add(buildInfo);
                resultMap.put("BuildInfo", buildInfoList);
                PushMessagePacket.pushMap(resultMap, DSObjectType.SQUID_FLOW,
                        "1037", "0001",
                        TokenUtil.getKey(), TokenUtil.getToken());
            }
            boolean flag = true;
            //遍历Squid，检查编译状态，多次推送
            for (Squid squid : squidList) {
                List<BuildInfo> buildInfoList = new ArrayList<BuildInfo>();
                List<Transformation> transformationList = new ArrayList<Transformation>();
                List<Column> columnList = new ArrayList<Column>();
                //判断是否是最后一个
                map.put("isLast", false);
                columnList = ValidateColumnProcess.getColumnListBySquidId(squid, adapter);
                //判断该squid是否是孤立的Squid(注释squid除外,stage,exception,datamingsquid,destination squid上游不合法,exception上游的上游不合法)
                if (squid.getSquid_type() != SquidTypeEnum.ANNOTATION.value()) {
                    buildInfoList = ValidateSquidProcess.validateSingleSquid(buildInfoList, map, squid, adapter);
                }
                //验证squid是否有效
                buildInfoList = ValidateSquidProcess.validateSquidIsAvail(buildInfoList, map, squid, adapter);
                //判断squid类型
                //判断该类型的连接信息是否正确
                if (SquidTypeEnum.DBSOURCE.value() == squid.getSquid_type()
                        || SquidTypeEnum.MONGODB.value() == squid.getSquid_type()
                        || SquidTypeEnum.HDFS.value() == squid.getSquid_type()
                           /* || SquidTypeEnum.SOURCECLOUDFILE.value()==squid.getSquid_type()*/
                        || SquidTypeEnum.FTP.value() == squid.getSquid_type()
                        || SquidTypeEnum.FILEFOLDER.value() == squid.getSquid_type()
                        || SquidTypeEnum.HBASE.value() == squid.getSquid_type()
                        || SquidTypeEnum.KAFKA.value() == squid.getSquid_type()
                        || SquidTypeEnum.HIVE.value() == squid.getSquid_type()
                        || SquidTypeEnum.CASSANDRA.value() == squid.getSquid_type()) {
                    if (SquidTypeEnum.DBSOURCE.value() == squid.getSquid_type()
                            || SquidTypeEnum.CLOUDDB.value() == squid.getSquid_type()) {
                        IDBSquidDao dbSquid = new DBSquidDaoImpl(adapter);
                        map.put("DbType", dbSquid.getDBTypeBySquidId(squid.getId()));
                    } else {
                        map.put("DbType", -1);
                    }
                    //验证connection
                    flag = ValidateSquidProcess.validateConnect(buildInfoList, map, squid, adapter);
                }
                //验证DataMiningSquid的信息
                if(SquidTypeEnum.isDataMingSquid(squid.getSquid_type())) {
                    buildInfoList = ValidateSquidProcess.validateDataMiningSquidIsCorrect(buildInfoList, map, squid, adapter);
                }
                //验证自定义squid信息是否正确
                buildInfoList = ValidateSquidProcess.validateUserDefinedSquidIsCorrect(buildInfoList, map, squid, adapter);
                //验证statisticSquid信息是否正确
                buildInfoList = ValidateSquidProcess.validateStatisticSquidIsCorrect(buildInfoList, map, squid, adapter);
                //验证GroupTagSquid的sortColumn,TagColumn是否存在
                buildInfoList = ValidateSquidProcess.validateGroupTagIsExist(flag, buildInfoList, map, squid, adapter);
                //验证SamplingSquid的SamplingPercent 不能为空且大于0
                buildInfoList = ValidateSquidProcess.validateSamplingSquid(flag, buildInfoList, map, squid, adapter);
                // 验证squid，落地表名不能为空(除了doc_extract,hbase_extract,Kafka_extract),其余的都是查询data_squid表
                buildInfoList = ValidateSquidProcess.validateExtractName(flag, buildInfoList, map, squid, adapter);
                // 验证落地Squid的信息是否合法
                buildInfoList = ValidateSquidProcess.validateDestSquid(buildInfoList, map, squid, adapter);
                // 验证增量抽取条件是否为空，换行符，delimited，fixedlength,定义文件，元数据路径不合法或者为空
                buildInfoList = ValidateSquidProcess.validateIncrementalCondition(buildInfoList, map, squid, adapter);
                // 验证训练集比例
                buildInfoList = ValidateSquidProcess.validateDataMinTrain(buildInfoList, map, squid, adapter);
                //验证Transformation
                transformationList = ValidateTransProcess.getTransformationList(squid, adapter);
                //验证表达式是否合法
                Map<String, Object> variableAndSquid = ValidateJoinProcess.validateJoinAndFilter(squid, buildInfoList, map, columnList, adapter);
                //验证孤立的trans
                buildInfoList = ValidateTransProcess.validateSingleTrans(buildInfoList, columnList, map, squid, adapter, transformationList, variableAndSquid);
                //验证Column
                buildInfoList = ValidateColumnProcess.validateColumnLink(columnList, buildInfoList, map, squid, adapter);
                //验证squid 下游连接的如果是samplingSquid 总的抽取百分比不能大于100
                buildInfoList=ValidateSquidProcess.validateToSquidSamplingSquidPercent(flag,buildInfoList,map,squid,adapter);
                //验证pivotSquid是数据是否合法
                buildInfoList = ValidateSquidProcess.validateToSquidPivotSquid(flag,buildInfoList,map,squid,adapter);
                //验证分片控制列是否正确
                buildInfoList = ValidateSquidProcess.validateSpitCol(buildInfoList,map,squid,adapter);
                //验证增量抽取时间类型是否正确
                buildInfoList = ValidateSquidProcess.validateTimeFormat(buildInfoList,map,squid,adapter);
                //校验trainfile和traindb是否有权限使用
                buildInfoList = ValidateSquidProcess.validateTrainFileorDBIsValid(buildInfoList,map,squid,adapter);
                //多次推送
                Map<String, Object> resultMap = new HashMap<String, Object>();
                //表示检查过程中没有出现错误,此时返回一个对的状态
                if (buildInfoList.size() == 0) {
                    BuildInfo buildInfo = new BuildInfo(squid, map);
                    buildInfoList.add(buildInfo);
                }
                resultMap.put("BuildInfo", buildInfoList);
                PushMessagePacket.pushMap(resultMap, DSObjectType.valueOf(squid.getSquid_type()),
                        "1037", "0001",
                        TokenUtil.getKey(), TokenUtil.getToken());
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            adapter.closeSession();
        }
    }

    /**
     * 调度的时候，编译状态检查
     *
     * @param info
     */
    public void compileJobSchedule(String info) {
        IRelationalDataManager adapter = DataAdapterFactory.getDefaultDataManager();
        adapter.openSession();
        Map<String, Object> map = JsonUtil.toHashMap(info);
        Map<String, Object> params = new HashMap<String, Object>();
        int repositoryId = Integer.parseInt(map.get("RepositoryId") + "");
        int SquidFlowId = Integer.parseInt(map.get("SquidflowId") + "");
        params.put("squid_flow_id", SquidFlowId);
        boolean isLast = false;
        try {
            //获取当前squidFlow下面所有的squid
            List<Squid> squidList = adapter.query2List2(true, params, Squid.class);
            Map<String, Object> resultMap = new HashMap<>();
            //验证是否是空squidflow
            if (squidList == null || squidList.size() == 0) {
                resultMap.put("Flag", true);
                PushMessagePacket.pushMap(resultMap, DSObjectType.SQUID_FLOW,
                        "1037", "0002",
                        TokenUtil.getKey(), TokenUtil.getToken());
            }
            boolean flag = true;
            //遍历Squid，检查编译状态，多次推送
            for (Squid squid : squidList) {
                List<BuildInfo> buildInfoList = new ArrayList<BuildInfo>();
                List<Transformation> transformationList = new ArrayList<Transformation>();
                List<Column> columnList = new ArrayList<Column>();
                //判断是否是最后一个
                map.put("isLast", false);
                columnList = ValidateColumnProcess.getColumnListBySquidId(squid, adapter);
                //判断该squid是否是孤立的Squid(注释squid除外,stage,exception,datamingsquid,destination squid上游不合法,exception上游的上游不合法)
                if (squid.getSquid_type() != SquidTypeEnum.ANNOTATION.value()) {
                    buildInfoList = ValidateSquidProcess.validateSingleSquid(buildInfoList, map, squid, adapter);
                }
                //验证squid是否有效
                buildInfoList = ValidateSquidProcess.validateSquidIsAvail(buildInfoList, map, squid, adapter);
                //判断squid类型
                //判断该类型的连接信息是否正确
                if (SquidTypeEnum.DBSOURCE.value() == squid.getSquid_type()
                        || SquidTypeEnum.MONGODB.value() == squid.getSquid_type()
                        || SquidTypeEnum.HDFS.value() == squid.getSquid_type()
                           /* || SquidTypeEnum.SOURCECLOUDFILE.value()==squid.getSquid_type()*/
                        || SquidTypeEnum.FTP.value() == squid.getSquid_type()
                        || SquidTypeEnum.FILEFOLDER.value() == squid.getSquid_type()
                        || SquidTypeEnum.HBASE.value() == squid.getSquid_type()
                        || SquidTypeEnum.KAFKA.value() == squid.getSquid_type()
                        || SquidTypeEnum.HIVE.value() == squid.getSquid_type()
                        || SquidTypeEnum.CASSANDRA.value() == squid.getSquid_type()) {
                    if (SquidTypeEnum.DBSOURCE.value() == squid.getSquid_type()
                            || SquidTypeEnum.CLOUDDB.value() == squid.getSquid_type()) {
                        IDBSquidDao dbSquid = new DBSquidDaoImpl(adapter);
                        map.put("DbType", dbSquid.getDBTypeBySquidId(squid.getId()));
                    } else {
                        map.put("DbType", -1);
                    }
                    //验证connection
                    flag = ValidateSquidProcess.validateConnect(buildInfoList, map, squid, adapter);
                }
                //验证DataMiningSquid的信息
                if(SquidTypeEnum.isDataMingSquid(squid.getSquid_type())) {
                    buildInfoList = ValidateSquidProcess.validateDataMiningSquidIsCorrect(buildInfoList, map, squid, adapter);
                }
                //验证自定义squid信息是否正确
                buildInfoList = ValidateSquidProcess.validateUserDefinedSquidIsCorrect(buildInfoList, map, squid, adapter);
                //验证statisticSquid信息是否正确
                buildInfoList = ValidateSquidProcess.validateStatisticSquidIsCorrect(buildInfoList, map, squid, adapter);
                //验证GroupTagSquid的sortColumn,TagColumn是否存在
                buildInfoList = ValidateSquidProcess.validateGroupTagIsExist(flag, buildInfoList, map, squid, adapter);
                // 验证squid，落地表名不能为空(除了doc_extract,hbase_extract,Kafka_extract),其余的都是查询data_squid表
                buildInfoList = ValidateSquidProcess.validateExtractName(flag, buildInfoList, map, squid, adapter);
                // 验证落地Squid的信息是否合法
                buildInfoList = ValidateSquidProcess.validateDestSquid(buildInfoList, map, squid, adapter);
                // 验证增量抽取条件是否为空，换行符，delimited，fixedlength,定义文件，元数据路径不合法或者为空
                buildInfoList = ValidateSquidProcess.validateIncrementalCondition(buildInfoList, map, squid, adapter);
                // 验证训练集比例
                buildInfoList = ValidateSquidProcess.validateDataMinTrain(buildInfoList, map, squid, adapter);
                //验证Transformation
                transformationList = ValidateTransProcess.getTransformationList(squid, adapter);
                //验证表达式是否合法
                Map<String, Object> variableAndSquid = ValidateJoinProcess.validateJoinAndFilter(squid, buildInfoList, map, columnList, adapter);
                //验证孤立的trans
                buildInfoList = ValidateTransProcess.validateSingleTrans(buildInfoList, columnList, map, squid, adapter, transformationList, variableAndSquid);
                //验证Column
                buildInfoList = ValidateColumnProcess.validateColumnLink(columnList, buildInfoList, map, squid, adapter);
                //验证分片控制列是否正确
                buildInfoList = ValidateSquidProcess.validateSpitCol(buildInfoList,map,squid,adapter);
                //验证增量抽取时间类型是否正确
                buildInfoList = ValidateSquidProcess.validateTimeFormat(buildInfoList,map,squid,adapter);
                //校验trainfile和traindb是否有权限使用
                buildInfoList = ValidateSquidProcess.validateTrainFileorDBIsValid(buildInfoList,map,squid,adapter);
                //检查过程中只要出现错误，返回false，无需继续往下检查
                if (buildInfoList.size() != 0) {
                    for (BuildInfo buildInfo : buildInfoList) {
                        if (buildInfo.getInfoType() == SchedulerLogStatus.ERROR.getValue()) {
                            resultMap.put("Flag", false);
                            PushMessagePacket.pushMap(resultMap, DSObjectType.valueOf(squid.getSquid_type()),
                                    "1037", "0002",
                                    TokenUtil.getKey(), TokenUtil.getToken());
                            return;
                        }

                    }
                }
            }
            //如果没有所有的都没有错误，推送true
            resultMap.put("Flag", true);
            PushMessagePacket.pushMap(resultMap, DSObjectType.SQUID_FLOW,
                    "1037", "0002",
                    TokenUtil.getKey(), TokenUtil.getToken());

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            adapter.closeSession();
        }
    }

    /**
     * 测试
     *
     * @param args
     */
    public static void main(String[] args) {
        String info = "{RepositoryId:1,SquidflowId:1420}";
        CompileValidateService compileValidateService = new CompileValidateService();
        compileValidateService.compileValidate(info);

    }
}
