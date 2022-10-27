package com.eurlanda.datashire.modifyInterface;

import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.dao.*;
import com.eurlanda.datashire.dao.impl.*;
import com.eurlanda.datashire.entity.*;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.server.model.PivotSquid;
import com.eurlanda.datashire.server.model.SamplingSquid;
import com.eurlanda.datashire.sprint7.packet.PushMessagePacket;
import com.eurlanda.datashire.sprint7.service.squidflow.JobScheduleProcess;
import com.eurlanda.datashire.sprint7.service.squidflow.subservice.ExtractService;
import com.eurlanda.datashire.sprint7.service.squidflow.subservice.SquidFlowServicesub;
import com.eurlanda.datashire.sprint7.service.squidflow.subservice.StageSquidService;
import com.eurlanda.datashire.utility.ReturnValue;
import org.apache.log4j.Logger;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 新的接口具体方法实现类
 * Created by Administrator on 2016/12/13.
 */
public class NewSquidFlowServiceSub {
    private static Logger logger = Logger.getLogger(NewSquidFlowServiceSub.class);// 记录日志

    /**
     * 查询所有squid
     *
     * @param squidFlowId
     * @param repositoryId
     */
    public void newPushAllSquid(int squidFlowId,int repositoryId,String cmd1,String cmd2,String key,String token,IRelationalDataManager adapter) throws Exception{
        if (squidFlowId > 0) {
            ReturnValue out = new ReturnValue();
            ISquidFlowDao squidFlowDao = new SquidFlowDaoImpl(adapter);
            ISquidDao squidDao = new SquidDaoImpl(adapter);
            ISourceTableDao sourceTableDao = new SourceTableDaoImpl(adapter);
            IVariableDao variableDao = new VariableDaoImpl(adapter);
            long start = System.currentTimeMillis();
            //查询出squidType，根据squidType来推送squid信息
            try {
                //查询出该squidflow下面，squid的数量
                int count = squidFlowDao.getSquidCountBySquidFlow(squidFlowId);
                List<Map<String, Object>> typeObj = squidFlowDao.getSquidTypeBySquidFlow(squidFlowId);
                if (typeObj != null && typeObj.size() > 0) {
                    for (Map<String, Object> map : typeObj) {
                        final int squidTypeId = Integer.valueOf(map.get("SQUID_TYPE_ID") + "");
                        final Map<String, Object> outputMap = new HashMap<String, Object>();

                        if (squidTypeId == SquidTypeEnum.EXTRACT.value()
                                || squidTypeId == SquidTypeEnum.XML_EXTRACT.value()
                                || squidTypeId == SquidTypeEnum.WEBLOGEXTRACT.value()
                                || squidTypeId == SquidTypeEnum.WEBEXTRACT.value()
                                || squidTypeId == SquidTypeEnum.WEIBOEXTRACT.value()
                                || squidTypeId == SquidTypeEnum.HIVEEXTRACT.value()
                                || squidTypeId == SquidTypeEnum.CASSANDRA_EXTRACT.value()) {
                            DSObjectType obj = DSObjectType.parse(squidTypeId);
                            List<DataSquid> squidList = squidDao.getSquidListForParams(squidTypeId,
                                    squidFlowId,"",DataSquid.class);
                            List<DataSquid> dsList = new ArrayList<>();
                            for (DataSquid dataSquid : squidList) {
                                if (dsList.size() > 0 && dsList.size() % 20 == 0) {
                                    outputMap.clear();
                                    outputMap.put("Squids",dsList);
                                    outputMap.put("Count",count);
                                    PushMessagePacket.pushMap(outputMap,obj,cmd1,cmd2,key,token);
                                    logger.info("ExtractSquid 加载时间:" + (System.currentTimeMillis() - start));
                                    start = System.currentTimeMillis();
                                    dsList.clear();
                                }
                                ExtractService.setAllExtractSquidData(adapter,dataSquid);
                                dsList.add(dataSquid);

                            }
                            if (dsList.size() != 0) {
                                outputMap.clear();
                                outputMap.put("Squids",dsList);
                                outputMap.put("Count",count);
                                PushMessagePacket.pushMap(outputMap,obj,cmd1,cmd2,key,token);
                                logger.info("ExtractSquid 加载时间:" + (System.currentTimeMillis() - start));
                                start = System.currentTimeMillis();
                            }

                        } else if (squidTypeId == SquidTypeEnum.HBASEEXTRACT.value()) {
                            List<HBaseExtractSquid> squidList = squidDao.getSquidListForParams(squidTypeId,squidFlowId,"",HBaseExtractSquid.class);
                            List<HBaseExtractSquid> returnSquidList = new ArrayList<>();
                            //50推送一个
                            for (int i = 0; i < squidList.size(); i++) {
                                HBaseExtractSquid HbaseSquid = squidList.get(i);
                                ExtractService.setAllExtractSquidData(adapter,HbaseSquid);
                                if (i > 0 && i % 50 == 0) {
                                    outputMap.clear();
                                    outputMap.put("Squids",returnSquidList);
                                    outputMap.put("Count",count);
                                    PushMessagePacket.pushMap(outputMap,DSObjectType.HBASEEXTRACT,cmd1,cmd2,key,token);
                                    returnSquidList.clear();
                                }
                                returnSquidList.add(HbaseSquid);
                            }
                            if (returnSquidList.size() > 0) {
                                outputMap.clear();
                                outputMap.put("Squids",returnSquidList);
                                outputMap.put("Count",count);
                                PushMessagePacket.pushMap(outputMap,DSObjectType.HBASEEXTRACT,cmd1,cmd2,key,token);
                            }
                            returnSquidList.clear();
                            squidList.clear();
                            logger.info("DocExtractSquid 加载时间:" + (System.currentTimeMillis() - start));
                            start = System.currentTimeMillis();
                        } else if (squidTypeId == SquidTypeEnum.KAFKAEXTRACT.value()) {
                            List<KafkaExtractSquid> squidList = squidDao.getSquidListForParams(squidTypeId,squidFlowId,"",KafkaExtractSquid.class);
                            List<KafkaExtractSquid> returnSquidList = new ArrayList<>();
                            //50推送一个
                            for (int i = 0; i < squidList.size(); i++) {
                                KafkaExtractSquid kafkaSquid = squidList.get(i);
                                ExtractService.setAllExtractSquidData(adapter,kafkaSquid);
                                if (i > 0 && i % 50 == 0) {
                                    outputMap.clear();
                                    outputMap.put("Squids",returnSquidList);
                                    outputMap.put("Count",count);
                                    PushMessagePacket.pushMap(outputMap,DSObjectType.KAFKAEXTRACT,cmd1,cmd2,key,token);
                                    returnSquidList.clear();
                                }
                                returnSquidList.add(kafkaSquid);
                            }
                            if (returnSquidList.size() > 0) {
                                outputMap.clear();
                                outputMap.put("Squids",returnSquidList);
                                outputMap.put("Count",count);
                                PushMessagePacket.pushMap(outputMap,DSObjectType.KAFKAEXTRACT,cmd1,cmd2,key,token);
                            }
                            returnSquidList.clear();
                            squidList.clear();
                            logger.info("DocExtractSquid 加载时间:" + (System.currentTimeMillis() - start));
                            start = System.currentTimeMillis();
                        } else if (squidTypeId == SquidTypeEnum.DOC_EXTRACT.value()) {
                            List<DocExtractSquid> squidList = squidDao.getSquidListForParams(squidTypeId,squidFlowId,"",DocExtractSquid.class);
                            List<DocExtractSquid> returnSquidList = new ArrayList<>();
                            //50个推送一次
                            for (int i = 0; i < squidList.size(); i++) {
                                DocExtractSquid docExtractSquid = squidList.get(i);
                                ExtractService.setDocExtractSquidData(adapter,docExtractSquid);
                                if (i > 0 && i % 50 == 0) {
                                    outputMap.clear();
                                    outputMap.put("Squids",returnSquidList);
                                    outputMap.put("Count",count);
                                    PushMessagePacket.pushMap(outputMap,DSObjectType.DOC_EXTRACT,cmd1,cmd2,key,token);
                                    returnSquidList.clear();
                                }
                                returnSquidList.add(docExtractSquid);
                            }
                            if (returnSquidList.size() > 0) {
                                outputMap.clear();
                                outputMap.put("Squids",returnSquidList);
                                outputMap.put("Count",count);
                                PushMessagePacket.pushMap(outputMap,DSObjectType.DOC_EXTRACT,cmd1,cmd2,key,token);
                            }
                            returnSquidList.clear();
                            squidList.clear();
                            logger.info("DocExtractSquid 加载时间:" + (System.currentTimeMillis() - start));
                            start = System.currentTimeMillis();
                        } else if (squidTypeId == SquidTypeEnum.MONGODBEXTRACT.value()) {
                            List<MongodbExtractSquid> squidList = squidDao.getSquidListForParams(squidTypeId,squidFlowId,"",MongodbExtractSquid.class);
                            List<MongodbExtractSquid> returnSquidList = new ArrayList<>();
                            //50个推送一次
                            for (int i = 0; i < squidList.size(); i++) {
                                MongodbExtractSquid mongodb = squidList.get(i);
                                ExtractService.setMongodbExtractSquidData(adapter,mongodb);
                                if (i > 0 && i % 50 == 0) {
                                    outputMap.clear();
                                    outputMap.put("Squids",returnSquidList);
                                    outputMap.put("Count",count);
                                    PushMessagePacket.pushMap(outputMap,DSObjectType.MONGODBEXTRACT,cmd1,cmd2,key,token);
                                    returnSquidList.clear();
                                }
                                returnSquidList.add(mongodb);

                            }
                            if (returnSquidList.size() > 0) {
                                outputMap.clear();
                                outputMap.put("Squids",returnSquidList);
                                outputMap.put("Count",count);
                                PushMessagePacket.pushMap(outputMap,DSObjectType.MONGODBEXTRACT,cmd1,cmd2,key,token);
                            }
                            returnSquidList.clear();
                            squidList.clear();
                            logger.info("MONGODBEXTRACT 加载时间:" + (System.currentTimeMillis() - start));
                            start = System.currentTimeMillis();
                        } else if (squidTypeId == SquidTypeEnum.WEBSERVICEEXTRACT.value()) {
                            List<WebserviceExtractSquid> webserviceExtractSquid = squidDao.getSquidListForParams(squidTypeId,squidFlowId,"",WebserviceExtractSquid.class);
                            List<WebserviceExtractSquid> returnSquidList = new ArrayList<>();
                            if (webserviceExtractSquid != null && webserviceExtractSquid.size() > 0) {
                                //50推送一次
                                for (int i = 0; i < webserviceExtractSquid.size(); i++) {
                                    WebserviceExtractSquid wes = webserviceExtractSquid.get(i);
                                    ExtractService.setAllExtractSquidData(adapter,wes);
                                    ExtractService.setDSFCS(adapter,wes);
                                    if (i > 0 && i % 50 == 0) {
                                        outputMap.clear();
                                        outputMap.put("Squids",returnSquidList);
                                        outputMap.put("Count",count);
                                        PushMessagePacket.pushMap(outputMap,DSObjectType.valueOf(squidTypeId),cmd1,cmd2,key,token);
                                        returnSquidList.clear();
                                    }
                                    returnSquidList.add(wes);
                                }
                                if (returnSquidList.size() > 0) {
                                    outputMap.clear();
                                    outputMap.put("Squids",returnSquidList);
                                    outputMap.put("Count",count);
                                    PushMessagePacket.pushMap(outputMap,DSObjectType.valueOf(squidTypeId),cmd1,cmd2,key,token);
                                }
                                returnSquidList.clear();
                                webserviceExtractSquid.clear();
                            }
                            logger.info("dataMiningSquidList 加载时间:" + (System.currentTimeMillis() - start));
                            start = System.currentTimeMillis();
                        } else if (squidTypeId == SquidTypeEnum.HTTPEXTRACT.value()) {
                            List<HttpExtractSquid> listHttpExtractSquid = squidDao.getSquidListForParams(squidTypeId,squidFlowId,"",HttpExtractSquid.class);
                            List<HttpExtractSquid> returnSquidList = new ArrayList<>();
                            if (listHttpExtractSquid != null && listHttpExtractSquid.size() > 0) {
                                for (int i = 0; i < listHttpExtractSquid.size(); i++) {
                                    HttpExtractSquid wes = listHttpExtractSquid.get(i);
                                    ExtractService.setAllExtractSquidData(adapter,wes);
                                    ExtractService.setDSFCS(adapter,wes);
                                    if (i > 0 && i % 50 == 0) {
                                        outputMap.clear();
                                        outputMap.put("Squids",returnSquidList);
                                        outputMap.put("Count",count);
                                        PushMessagePacket.pushMap(outputMap,DSObjectType.valueOf(squidTypeId),cmd1,cmd2,key,token);
                                        returnSquidList.clear();
                                    }
                                    returnSquidList.add(wes);
                                }
                                if (returnSquidList.size() > 0) {
                                    outputMap.clear();
                                    outputMap.put("Squids",returnSquidList);
                                    outputMap.put("Count",count);
                                    PushMessagePacket.pushMap(outputMap,DSObjectType.valueOf(squidTypeId),cmd1,cmd2,key,token);
                                }
                                returnSquidList.clear();
                                listHttpExtractSquid.clear();
                            }
                            logger.info("dataMiningSquidList 加载时间:" + (System.currentTimeMillis() - start));
                            start = System.currentTimeMillis();
                        } else if (squidTypeId == SquidTypeEnum.DBSOURCE.value()
                                || squidTypeId==SquidTypeEnum.CLOUDDB.value()
                                || squidTypeId==SquidTypeEnum.TRAININGDBSQUID.value()) {
                            List<DBSourceSquid> squidList = squidDao.getSquidListForParams(squidTypeId,squidFlowId,"",DBSourceSquid.class);
                            List<DBSourceSquid> returnSquidList = new ArrayList<DBSourceSquid>();
                            //每50个推送一次
                            for (int i = 0; i < squidList.size(); i++) {
                                DBSourceSquid dbSourceSquid = squidList.get(i);
                                List<SourceTable> sourceTableList = sourceTableDao.getSourceTableBySquidId(dbSourceSquid.getId());
                                dbSourceSquid.setSourceTableList(sourceTableList);
                                if(squidTypeId==SquidTypeEnum.CLOUDDB.value()){
                                    dbSourceSquid.setType(DSObjectType.CLOUDDB.value());
                                } else if(squidTypeId == SquidTypeEnum.TRAININGDBSQUID.value()){
                                    dbSourceSquid.setType(DSObjectType.TRAININGDBSQUID.value());
                                }else {
                                    dbSourceSquid.setType(DSObjectType.DBSOURCE.value());
                                }
                                //查询变量集合
                                List<DSVariable> variables = variableDao.getDSVariableByScope(2,dbSourceSquid.getId());
                                if (variables == null) {
                                    variables = new ArrayList<DSVariable>();
                                }
                                dbSourceSquid.setVariables(variables);
                                if (i > 0 && i % 5 == 0) {
                                    outputMap.clear();
                                    outputMap.put("Squids",returnSquidList);
                                    outputMap.put("Count",count);
                                    if(squidTypeId==SquidTypeEnum.CLOUDDB.value()){
                                        PushMessagePacket.pushMap(outputMap,DSObjectType.CLOUDDB,cmd1,cmd2,key,token);
                                    } else if(squidTypeId==SquidTypeEnum.TRAININGDBSQUID.value()){
                                        PushMessagePacket.pushMap(outputMap,DSObjectType.TRAININGDBSQUID,cmd1,cmd2,key,token);
                                    } else {
                                        PushMessagePacket.pushMap(outputMap,DSObjectType.DBSOURCE,cmd1,cmd2,key,token);
                                    }
                                    returnSquidList.clear();
                                }
                                returnSquidList.add(dbSourceSquid);

                            }

                            if (returnSquidList.size() > 0) {
                                outputMap.clear();
                                outputMap.put("Squids",returnSquidList);
                                outputMap.put("Count",count);
                                if(squidTypeId==SquidTypeEnum.DBSOURCE.value()) {
                                    PushMessagePacket.pushMap(outputMap,DSObjectType.DBSOURCE,cmd1,cmd2,key,token);
                                } else if(squidTypeId==SquidTypeEnum.CLOUDDB.value()){
                                    PushMessagePacket.pushMap(outputMap,DSObjectType.CLOUDDB,cmd1,cmd2,key,token);
                                } else if(squidTypeId == SquidTypeEnum.TRAININGDBSQUID.value()){
                                    PushMessagePacket.pushMap(outputMap,DSObjectType.TRAININGDBSQUID,cmd1,cmd2,key,token);
                                }
                            }
                            squidList.clear();
                            returnSquidList.clear();
                            logger.info("dbSourceSquid 加载时间:" + (System.currentTimeMillis() - start));
                            start = System.currentTimeMillis();
                        } else if (squidTypeId == SquidTypeEnum.MONGODB.value()) {
                            List<NOSQLConnectionSquid> squidList = squidDao.getSquidListForParams(squidTypeId,squidFlowId,"",NOSQLConnectionSquid.class);
                            for (NOSQLConnectionSquid dbSourceSquid : squidList) {
                                List<SourceTable> sourceTableList = sourceTableDao.getSourceTableBySquidId(dbSourceSquid.getId());
                                dbSourceSquid.setSourceTableList(sourceTableList);
                                dbSourceSquid.setType(DSObjectType.MONGODB.value());
                                //查询变量集合
                                List<DSVariable> variables = variableDao.getDSVariableByScope(2,dbSourceSquid.getId());
                                if (variables == null) {
                                    variables = new ArrayList<DSVariable>();
                                }
                                dbSourceSquid.setVariables(variables);


                            }
                            outputMap.clear();
                            outputMap.put("Squids",squidList);
                            outputMap.put("Count",count);
                            PushMessagePacket.pushMap(outputMap,DSObjectType.MONGODB,cmd1,cmd2,key,token);
                            logger.info("NOSQLConnectionSquid 加载时间:" + (System.currentTimeMillis() - start));
                            start = System.currentTimeMillis();
                        } else if(squidTypeId == SquidTypeEnum.USERDEFINED.value()){
                            List<UserDefinedSquid> squidList = squidDao.getSquidListForParams(squidTypeId,squidFlowId,"",UserDefinedSquid.class);
                            ITransformationInputsDao transInputDao = new TransformationInputsDaoImpl(adapter);
                            ITransformationLinkDao transLinkDao = new TransformationLinkDaoImpl(adapter);
                            IReferenceColumnDao refColumnDao = new ReferenceColumnDaoImpl(adapter);
                            for (UserDefinedSquid definedSquid : squidList) {
                                //查找出所有的className
                                String sql="select alias_name from THIRD_JAR_DEFINITION";
                                List<Map<String,Object>> classNames = adapter.query2List(true,sql,null);
                                List<String> classNameList = new ArrayList<>();
                                for(Map<String,Object> className : classNames){
                                    classNameList.add(className.get("ALIAS_NAME")+"");
                                }
                                definedSquid.setClassNames(classNameList);
                                //查找出dataMapColumn
                                Map<String,String> params = new HashMap<>();
                                params.put("squid_id",definedSquid.getId()+"");
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
                                params.put("squid_id",definedSquid.getId()+"");
                                List<Transformation> transformations = adapter.query2List(params, Transformation.class);
                                if(transformations!=null){
                                    Map<Integer, List<TransformationInputs>> rsMap = transInputDao.getTransInputsBySquidId(definedSquid.getId());
                                    for(int i=0; i<transformations.size(); i++){
                                        int trans_id = transformations.get(i).getId();
                                        if(rsMap!=null&&rsMap.containsKey(trans_id)){
                                            transformations.get(i).setInputs(rsMap.get(trans_id));
                                        }
                                    }
                                }
                                definedSquid.setTransformations(transformations);

                                // 所有transformation link
                                List<TransformationLink> transformationLinks = transLinkDao.getTransLinkListBySquidId(definedSquid.getId());
                                definedSquid.setTransformationLinks(transformationLinks);
                                List<String> paramsList =new ArrayList<>();
                                paramsList.add(definedSquid.getId()+"");
                                sql ="select * from ds_column where squid_id=? ORDER BY RELATIVE_ORDER ASC";
                                //查询出column
                                List<Column> columns =adapter.query2List(sql,paramsList,Column.class);
                                definedSquid.setColumns(columns);
                                //查询出变量
                                List<DSVariable> variables = variableDao.getDSVariableByScope(2, definedSquid.getId());
                                if (variables==null){
                                    variables = new ArrayList<DSVariable>();
                                }
                                definedSquid.setVariables(variables);
                            }
                            outputMap.clear();
                            outputMap.put("Squids",squidList);
                            outputMap.put("Count",count);
                            PushMessagePacket.pushMap(outputMap,DSObjectType.USERDEFINED,cmd1,cmd2,key,token);
                            logger.info("UserDefined 加载时间:" + (System.currentTimeMillis() - start));
                            start = System.currentTimeMillis();
                        } else if(squidTypeId == SquidTypeEnum.STATISTICS.value()){
                            List<StatisticsSquid> squidList = squidDao.getSquidListForParams(squidTypeId,squidFlowId,"",StatisticsSquid.class);
                            ITransformationInputsDao transInputDao = new TransformationInputsDaoImpl(adapter);
                            ITransformationLinkDao transLinkDao = new TransformationLinkDaoImpl(adapter);
                            IReferenceColumnDao refColumnDao = new ReferenceColumnDaoImpl(adapter);
                            for (StatisticsSquid squid: squidList) {
                                //查找出所有的className
                                String sql="select statistics_name from ds_statistics_definition";
                                List<Map<String,Object>> classNames = adapter.query2List(true,sql,null);
                                List<String> classNameList = new ArrayList<>();
                                for(Map<String,Object> className : classNames){
                                    classNameList.add(className.get("STATISTICS_NAME")+"");
                                }
                                squid.setStatisticsNames(classNameList);
                                //查找出dataMapColumn
                                Map<String,Object> params = new HashMap<>();
                                params.put("squid_id",squid.getId());
                                List<StatisticsDataMapColumn> dataMapList = adapter.query2List2(true,params, StatisticsDataMapColumn.class);
                                squid.setStatisticsDataMapColumns(dataMapList);
                                //查找出parameterMap
                                List<StatisticsParameterColumn> parameterColumns = adapter.query2List2(true,params, StatisticsParameterColumn.class);
                                squid.setStatisticsParametersColumns(parameterColumns);
                                //查找referenceColumn
                                List<ReferenceColumn> referenceColumns = refColumnDao.getRefColumnListByRefSquidId(squid.getId());
                                squid.setSourceColumns(referenceColumns);
                                //查找出Transformation
                                params.clear();
                                params.put("squid_id",squid.getId()+"");
                                List<Transformation> transformations = adapter.query2List2(true,params, Transformation.class);
                                if(transformations!=null){
                                    Map<Integer, List<TransformationInputs>> rsMap = transInputDao.getTransInputsBySquidId(squid.getId());
                                    for(int i=0; i<transformations.size(); i++){
                                        int trans_id = transformations.get(i).getId();
                                        if(rsMap!=null&&rsMap.containsKey(trans_id)){
                                            transformations.get(i).setInputs(rsMap.get(trans_id));
                                        }
                                    }
                                }
                                squid.setTransformations(transformations);

                                // 所有transformation link
                                List<TransformationLink> transformationLinks = transLinkDao.getTransLinkListBySquidId(squid.getId());
                                squid.setTransformationLinks(transformationLinks);
                                List<String> paramsList =new ArrayList<>();
                                paramsList.add(squid.getId()+"");
                                sql ="select * from ds_column where squid_id=? ORDER BY RELATIVE_ORDER ASC";
                                //查询出column
                                List<Column> columns =adapter.query2List(sql,paramsList,Column.class);
                                squid.setColumns(columns);
                                //查询出变量
                                List<DSVariable> variables = variableDao.getDSVariableByScope(2, squid.getId());
                                if (variables==null){
                                    variables = new ArrayList<>();
                                }
                                squid.setVariables(variables);
                            }
                            outputMap.clear();
                            outputMap.put("Squids",squidList);
                            outputMap.put("Count",count);
                            PushMessagePacket.pushMap(outputMap,DSObjectType.STATISTICS,cmd1,cmd2,key,token);
                            logger.info("Statistics 加载时间:" + (System.currentTimeMillis() - start));
                            start = System.currentTimeMillis();
                        } else {
                            //查询出所有的调度任务
                            Map<Integer, JobSchedule> scheduleMap = new JobScheduleProcess().getJobSchedules(repositoryId,squidFlowId,null,true,out);
                            switch (SquidTypeEnum.valueOf(squidTypeId)) {
                                // FileFolderSquid added by lei.bin
                                case FILEFOLDER:
                                    List<FileFolderSquid> folderSquids = squidDao.getSquidListForParams(squidTypeId,squidFlowId,"",FileFolderSquid.class);
                                    if (null != folderSquids && folderSquids.size() > 0) {
                                        for (FileFolderSquid rSquid : folderSquids) {
                                            List<SourceTable> list = sourceTableDao.getSourceTableBySquidId(rSquid.getId());
                                            rSquid.setSourceTableList(list);

                                            //查询变量集合
                                            List<DSVariable> variables = variableDao.getDSVariableByScope(2,rSquid.getId());
                                            if (variables == null) {
                                                variables = new ArrayList<DSVariable>();
                                            }
                                            rSquid.setVariables(variables);
                                        }
                                        outputMap.clear();
                                        outputMap.put("Squids",folderSquids);
                                        outputMap.put("Count",count);
                                        PushMessagePacket.pushMap(outputMap,DSObjectType.FILEFOLDER,cmd1,cmd2,key,token);
                                    }
                                    break;
                                case FTP:
                                    // FtpSquid
                                    List<FtpSquid> ftpList = squidDao.getSquidListForParams(squidTypeId,squidFlowId,"",FtpSquid.class);
                                    if (null != ftpList && ftpList.size() > 0) {
                                        for (FtpSquid rSquid : ftpList) {
                                            List<SourceTable> list = sourceTableDao.getSourceTableBySquidId(rSquid.getId());
                                            rSquid.setSourceTableList(list);

                                            //查询变量集合
                                            List<DSVariable> variables = variableDao.getDSVariableByScope(2,rSquid.getId());
                                            if (variables == null) {
                                                variables = new ArrayList<DSVariable>();
                                            }
                                            rSquid.setVariables(variables);
                                        }
                                        outputMap.clear();
                                        outputMap.put("Squids",ftpList);
                                        outputMap.put("Count",count);
                                        PushMessagePacket.pushMap(outputMap,DSObjectType.FTP,cmd1,cmd2,key,token);
                                    }
                                    break;
                                case HDFS:
                                case SOURCECLOUDFILE:
                                case TRAINNINGFILESQUID:
                                    //HdfsSquid
                                    List<HdfsSquid> hdfs = squidDao.getSquidListForParams(squidTypeId,squidFlowId,"",HdfsSquid.class);
                                    if (null != hdfs && hdfs.size() > 0) {
                                        for (HdfsSquid rSquid : hdfs) {
                                            List<SourceTable> list = sourceTableDao.getSourceTableBySquidId(rSquid.getId());
                                            rSquid.setSourceTableList(list);

                                            //查询变量集合
                                            List<DSVariable> variables = variableDao.getDSVariableByScope(2,rSquid.getId());
                                            if (variables == null) {
                                                variables = new ArrayList<DSVariable>();
                                            }
                                            rSquid.setVariables(variables);
                                        }
                                        outputMap.clear();
                                        outputMap.put("Squids",hdfs);
                                        outputMap.put("Count",count);
                                        if(squidTypeId==SquidTypeEnum.HDFS.value()) {
                                            PushMessagePacket.pushMap(outputMap,DSObjectType.HDFS,cmd1,cmd2,key,token);
                                        } else if(squidTypeId == SquidTypeEnum.TRAINNINGFILESQUID.value()){
                                            PushMessagePacket.pushMap(outputMap,DSObjectType.TRAINNINGFILESQUID,cmd1,cmd2,key,token);
                                        } else {
                                            PushMessagePacket.pushMap(outputMap,DSObjectType.SOURCECLOUDFILE,cmd1,cmd2,key,token);
                                        }
                                    }
                                    break;
                                case WEIBO:
                                    //WeiboSquid
                                    try {
                                        List<WeiboSquid> weibo = squidDao.getSquidListForParams(squidTypeId,squidFlowId,"",WeiboSquid.class);
                                        if (null != weibo && weibo.size() > 0) {
                                            for (WeiboSquid rSquid : weibo) {
                                                if (null != scheduleMap.get(rSquid.getId())) {
                                                    rSquid.setJobSchedule(scheduleMap.get(rSquid.getId()));
                                                }
                                                List<SourceTable> list = sourceTableDao.getSourceTableBySquidId(rSquid.getId());
                                                rSquid.setSourceTableList(list);

                                                //查询变量集合
                                                List<DSVariable> variables = variableDao.getDSVariableByScope(2,rSquid.getId());
                                                if (variables == null) {
                                                    variables = new ArrayList<DSVariable>();
                                                }
                                                rSquid.setVariables(variables);
                                            }
                                            outputMap.clear();
                                            outputMap.put("Squids",weibo);
                                            outputMap.put("Count",count);
                                            PushMessagePacket.pushMap(outputMap,DSObjectType.WEIBO,cmd1,cmd2,key,token);
                                        }
                                        break;
                                    } catch (Exception e) {
                                        // TODO: handle exception
                                        e.printStackTrace();
                                    }
                                case WEB:
                                    //WebSquid
                                    try {
                                        List<WebSquid> web = squidDao.getSquidListForParams(squidTypeId,squidFlowId,"",WebSquid.class);
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
                                                List<DSVariable> variables = variableDao.getDSVariableByScope(2,rSquid.getId());
                                                if (variables == null) {
                                                    variables = new ArrayList<DSVariable>();
                                                }
                                                rSquid.setVariables(variables);
                                            }
                                            outputMap.clear();
                                            outputMap.put("Squids",web);
                                            outputMap.put("Count",count);
                                            PushMessagePacket.pushMap(outputMap,DSObjectType.WEB,cmd1,cmd2,key,token);
                                        }
                                        break;
                                    } catch (Exception e) {
                                        // TODO: handle exception
                                        e.printStackTrace();
                                    }
                                case DBDESTINATION:
                                    // DBDestinationSquid
                                    outputMap.clear();
                                    outputMap.put("Squids",squidDao.getSquidListForParams(squidTypeId,squidFlowId,"",DBDestinationSquid.class));
                                    outputMap.put("Count",count);
                                    PushMessagePacket.pushMap(outputMap,DSObjectType.DBDESTINATION,cmd1,cmd2,key,token);
                                    logger.info("DBDestinationSquid 加载时间:" + (System.currentTimeMillis() - start));
                                    start = System.currentTimeMillis();
                                    break;
                                case STAGE:
                                    // StageSquid
                                    List<StageSquid> squids = squidDao.getSquidListForParams(squidTypeId,squidFlowId,"",StageSquid.class);
                                    List<StageSquid> returnSquids = new ArrayList<>();
                                    if (squids != null && squids.size() > 0) {
                                        //每50个推送一次
                                        for (int i = 0; i < squids.size(); i++) { // 获得StagSquid
                                            StageSquid stageSquid = squids.get(i);
                                            StageSquidService.setStageSquidData(adapter,stageSquid);
                                            if (i > 0 && i % 50 == 0) {
                                                outputMap.clear();
                                                outputMap.put("Squids",returnSquids);
                                                outputMap.put("Count",count);
                                                PushMessagePacket.pushMap(outputMap,DSObjectType.STAGE,cmd1,cmd2,key,token);
                                                returnSquids.clear();
                                            }
                                            returnSquids.add(stageSquid);
                                        }
                                        if (returnSquids.size() > 0) {
                                            outputMap.clear();
                                            outputMap.put("Squids",returnSquids);
                                            outputMap.put("Count",count);
                                            PushMessagePacket.pushMap(outputMap,DSObjectType.STAGE,cmd1,cmd2,key,token);

                                        }
                                        returnSquids.clear();
                                        squids.clear();
                                    }
                                    logger.info("StageSquid 加载时间:" + (System.currentTimeMillis() - start));
                                    start = System.currentTimeMillis();
                                    break;
                                case GROUPTAGGING:
                                    List<GroupTaggingSquid> taggingSquids = squidDao.getSquidListForParams(squidTypeId,squidFlowId,"",GroupTaggingSquid.class);
                                    List<GroupTaggingSquid> returnTaggingSquids = new ArrayList<>();
                                    if (taggingSquids != null && taggingSquids.size() > 0) {
                                        //每50个推送一次
                                        for (int i = 0; i < taggingSquids.size(); i++) { // 获得StagSquid
                                            GroupTaggingSquid stageSquid = taggingSquids.get(i);
                                            StageSquidService.setGroupTaggingSquidData(adapter,stageSquid);
                                            if (i > 0 && i % 30 == 0) {
                                                outputMap.clear();
                                                outputMap.put("Squids",returnTaggingSquids);
                                                outputMap.put("Count",count);
                                                PushMessagePacket.pushMap(outputMap,DSObjectType.GROUPTAGGING,cmd1,cmd2,key,token);
                                                returnTaggingSquids.clear();
                                            }
                                            returnTaggingSquids.add(stageSquid);
                                        }
                                        if (returnTaggingSquids.size() > 0) {
                                            outputMap.clear();
                                            outputMap.put("Squids",returnTaggingSquids);
                                            outputMap.put("Count",count);
                                            PushMessagePacket.pushMap(outputMap,DSObjectType.GROUPTAGGING,cmd1,cmd2,key,token);

                                        }
                                        returnTaggingSquids.clear();
                                        taggingSquids.clear();
                                    }
                                    logger.info("StageSquid 加载时间:" + (System.currentTimeMillis() - start));
                                    start = System.currentTimeMillis();
                                    break;
                                case SAMPLINGSQUID:
                                    List<SamplingSquid> samplingSquids = squidDao.getSquidListForParams(squidTypeId,squidFlowId,"",SamplingSquid.class);
                                    List<SamplingSquid> returnSamplingSquids = new ArrayList<>();
                                    if (samplingSquids != null && samplingSquids.size() > 0) {
                                        //每50个推送一次
                                        for (int i = 0; i < samplingSquids.size(); i++) { // 获得StagSquid
                                            SamplingSquid stageSquid = samplingSquids.get(i);
                                            StageSquidService.setSamplingSquidData(adapter,stageSquid);
                                            if (i > 0 && i % 30 == 0) {
                                                outputMap.clear();
                                                outputMap.put("Squids",returnSamplingSquids);
                                                outputMap.put("Count",count);
                                                PushMessagePacket.pushMap(outputMap,DSObjectType.SAMPLINGSQUID,cmd1,cmd2,key,token);
                                                returnSamplingSquids.clear();
                                            }
                                            returnSamplingSquids.add(stageSquid);
                                        }
                                        if (returnSamplingSquids.size() > 0) {
                                            outputMap.clear();
                                            outputMap.put("Squids",returnSamplingSquids);
                                            outputMap.put("Count",count);
                                            PushMessagePacket.pushMap(outputMap,DSObjectType.SAMPLINGSQUID,cmd1,cmd2,key,token);

                                        }
                                        returnSamplingSquids.clear();
                                        samplingSquids.clear();
                                    }
                                    logger.info("StageSquid 加载时间:" + (System.currentTimeMillis() - start));
                                    start = System.currentTimeMillis();
                                    break;

                                case PIVOTSQUID:
                                    List<PivotSquid> pivotSquids = squidDao.getSquidListForParams(squidTypeId,squidFlowId,"",PivotSquid.class);
                                    List<PivotSquid> returnPivotSquids = new ArrayList<>();
                                    if (pivotSquids != null && pivotSquids.size() > 0) {
                                        //每50个推送一次
                                        for (int i = 0; i < pivotSquids.size(); i++) { // 获得StagSquid
                                            PivotSquid stageSquid = pivotSquids.get(i);
                                            StageSquidService.setPivotSquidData(adapter,stageSquid);
                                            if (i > 0 && i % 30 == 0) {
                                                outputMap.clear();
                                                outputMap.put("Squids",returnPivotSquids);
                                                outputMap.put("Count",count);
                                                PushMessagePacket.pushMap(outputMap,DSObjectType.PIVOTSQUID,cmd1,cmd2,key,token);
                                                returnPivotSquids.clear();
                                            }
                                            returnPivotSquids.add(stageSquid);
                                        }
                                        if (returnPivotSquids.size() > 0) {
                                            outputMap.clear();
                                            outputMap.put("Squids",returnPivotSquids);
                                            outputMap.put("Count",count);
                                            PushMessagePacket.pushMap(outputMap,DSObjectType.PIVOTSQUID,cmd1,cmd2,key,token);

                                        }
                                        returnPivotSquids.clear();
                                        pivotSquids.clear();
                                    }
                                    logger.info("StageSquid 加载时间:" + (System.currentTimeMillis() - start));
                                    start = System.currentTimeMillis();
                                    break;





                                case STREAM_STAGE:
                                    // StreamStageSquid
                                    List<StreamStageSquid> StreamSquid = squidDao.getSquidListForParams(squidTypeId,squidFlowId,"",StreamStageSquid.class);
                                    List<StreamStageSquid> returnSquidList = new ArrayList<>();
                                    if (StreamSquid != null && StreamSquid.size() > 0) {
                                        //50个推送一次
                                        for (int i = 0; i < StreamSquid.size(); i++) { // 获得StreamStageSquid
                                            StreamStageSquid StreamStageSquid = StreamSquid.get(i);
                                            StageSquidService.setStreamStageSquid(adapter,StreamStageSquid);
                                            if (i > 0 && i % 30 == 0) {
                                                outputMap.clear();
                                                outputMap.put("Squids",returnSquidList);
                                                outputMap.put("Count",count);
                                                PushMessagePacket.pushMap(outputMap,DSObjectType.STREAM_STAGE,cmd1,cmd2,key,token);
                                                returnSquidList.clear();
                                            }
                                            returnSquidList.add(StreamStageSquid);

                                        }
                                        if (returnSquidList.size() > 0) {
                                            outputMap.clear();
                                            outputMap.put("Squids",returnSquidList);
                                            outputMap.put("Count",count);
                                            PushMessagePacket.pushMap(outputMap,DSObjectType.STREAM_STAGE,cmd1,cmd2,key,token);
                                        }
                                        returnSquidList.clear();
                                        StreamSquid.clear();

                                    }
                                    logger.info("StageSquid 加载时间:" + (System.currentTimeMillis() - start));
                                    start = System.currentTimeMillis();
                                    break;

                                case HTTP:
                                    List<HttpSquid> httpSquid = squidDao.getSquidListForParams(squidTypeId,squidFlowId,"",HttpSquid.class);
                                    if (httpSquid != null && httpSquid.size() > 0) {
                                        for (HttpSquid httpSquidItem : httpSquid) { // 获得DataMining
                                            List<SourceTable> list = sourceTableDao.getSourceTableBySquidId(httpSquidItem.getId());
                                            httpSquidItem.setSourceTableList(list);

                                            //查询变量集合
                                            List<DSVariable> variables = variableDao.getDSVariableByScope(2,httpSquidItem.getId());
                                            if (variables == null) {
                                                variables = new ArrayList<DSVariable>();
                                            }
                                            httpSquidItem.setVariables(variables);
                                        }
                                        outputMap.clear();
                                        outputMap.put("Squids",httpSquid);
                                        outputMap.put("Count",count);
                                        PushMessagePacket.pushMap(outputMap,DSObjectType.valueOf(squidTypeId),cmd1,cmd2,key,token);
                                    }

                                    logger.info("dataMiningSquidList 加载时间:" + (System.currentTimeMillis() - start));
                                    start = System.currentTimeMillis();
                                    break;
                                case WEBSERVICE:
                                    List<WebserviceSquid> webServiceSquid = squidDao.getSquidListForParams(squidTypeId,squidFlowId,"",WebserviceSquid.class);
                                    if (webServiceSquid != null && webServiceSquid.size() > 0) {
                                        for (WebserviceSquid wsi : webServiceSquid) { // 获得DataMining
                                            List<SourceTable> list = sourceTableDao.getSourceTableBySquidId(wsi.getId());
                                            wsi.setSourceTableList(list);

                                            //查询变量集合
                                            List<DSVariable> variables = variableDao.getDSVariableByScope(2,wsi.getId());
                                            if (variables == null) {
                                                variables = new ArrayList<DSVariable>();
                                            }
                                            wsi.setVariables(variables);
                                        }
                                        outputMap.clear();
                                        outputMap.put("Squids",webServiceSquid);
                                        outputMap.put("Count",count);
                                        PushMessagePacket.pushMap(outputMap,DSObjectType.valueOf(squidTypeId),cmd1,cmd2,key,token);
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
                                    List<DataMiningSquid> dataMiningSquidList = squidDao.getSquidListForParams(squidTypeId,squidFlowId,"",DataMiningSquid.class);
                                    if (dataMiningSquidList != null && dataMiningSquidList.size() > 0) {
                                        for (DataMiningSquid dataMiningSquid : dataMiningSquidList) { // 获得DataMining
                                            StageSquidService.setDataMiningSquidData(adapter,dataMiningSquid);
                                            //double科学计数法转换
                                            NumberFormat nf = NumberFormat.getInstance();
                                            nf.setGroupingUsed(false);
                                            dataMiningSquid.setMax_value(Double.valueOf(nf.format(dataMiningSquid.getMax_value())));
                                            dataMiningSquid.setMin_value(Double.valueOf(nf.format(dataMiningSquid.getMin_value())));
                                        }
                                        outputMap.clear();
                                        outputMap.put("Squids",dataMiningSquidList);
                                        outputMap.put("Count",count);
                                        PushMessagePacket.pushMap(outputMap,DSObjectType.valueOf(squidTypeId),cmd1,cmd2,key,token);
                                    }
                                    logger.info("dataMiningSquidList 加载时间:" + (System.currentTimeMillis() - start));
                                    start = System.currentTimeMillis();
                                    break;
                                case REPORT:
                                    // ReportSquid added by bo.dang
                                    List<ReportSquid> reportSquidArray = squidDao.getSquidListForParams(squidTypeId,
                                            squidFlowId,"",ReportSquid.class);
                                    if (null != reportSquidArray && reportSquidArray.size() > 0) {
                                        for (ReportSquid reportSquid : reportSquidArray) {
                                            if (reportSquid.getFolder_id() > 0) {
                                                String path = SquidFlowServicesub.getReportNodeName(adapter,reportSquid.getFolder_id());
                                                reportSquid.setPublishing_path(path);
                                            }

                                            //查询变量集合
                                            List<DSVariable> variables = variableDao.getDSVariableByScope(2,reportSquid.getId());
                                            if (variables == null) {
                                                variables = new ArrayList<DSVariable>();
                                            }
                                            reportSquid.setVariables(variables);
                                        }
                                        outputMap.clear();
                                        outputMap.put("Squids",reportSquidArray);
                                        outputMap.put("Count",count);
                                        PushMessagePacket.pushMap(outputMap,DSObjectType.REPORT,cmd1,cmd2,key,token);
                                    }
                                    break;
                                case GISMAP:
                                    // ReportSquid added by bo.dang
                                    List<GISMapSquid> gisMapSquidArray = squidDao.getSquidListForParams(squidTypeId,
                                            squidFlowId,"",GISMapSquid.class);
                                    if (null != gisMapSquidArray && gisMapSquidArray.size() > 0) {
                                        for (GISMapSquid gisMapSquid : gisMapSquidArray) {
                                            if (gisMapSquid.getFolder_id() > 0) {
                                                String path = SquidFlowServicesub.getReportNodeName(adapter,gisMapSquid.getFolder_id());
                                                gisMapSquid.setPublishing_path(path);
                                            }

                                            //查询变量集合
                                            List<DSVariable> variables = variableDao.getDSVariableByScope(2,gisMapSquid.getId());
                                            if (variables == null) {
                                                variables = new ArrayList<DSVariable>();
                                            }
                                            gisMapSquid.setVariables(variables);
                                        }
                                        outputMap.clear();
                                        outputMap.put("Squids",gisMapSquidArray);
                                        outputMap.put("Count",count);
                                        PushMessagePacket.pushMap(outputMap,DSObjectType.GISMAP,cmd1,cmd2,key,token);
                                    }
                                    break;
                                case EXCEPTION:
                                    //当Exception数据量很大时，可以分页查询，一次查出50个
                                    List<ExceptionSquid> exceptionList = SquidFlowServicesub.getExceptionSquidListLimit(adapter,squidFlowId,"",out);
                                    List<ExceptionSquid> returnExceptionList = new ArrayList<>();
                                    if (exceptionList != null && exceptionList.size() > 0) {
                                        for (int i = 0; i < exceptionList.size(); i++) {
                                            ExceptionSquid exceptionSquid = exceptionList.get(i);
                                            StageSquidService.setExceptionSquidData(adapter,exceptionSquid);
                                            //查询变量集合
                                            List<DSVariable> variables = variableDao.getDSVariableByScope(2,exceptionSquid.getId());
                                            if (variables == null) {
                                                variables = new ArrayList<DSVariable>();
                                            }
                                            exceptionSquid.setVariables(variables);
                                            //50个推送一次
                                            if (i > 0 && i % 50 == 0) {
                                                outputMap.clear();
                                                outputMap.put("Squids",returnExceptionList);
                                                outputMap.put("Count",count);
                                                PushMessagePacket.pushMap(outputMap,DSObjectType.EXCEPTION,cmd1,cmd2,key,token);
                                                returnExceptionList.clear();
                                            }
                                            returnExceptionList.add(exceptionSquid);

                                        }
                                        if (returnExceptionList.size() > 0) {
                                            outputMap.clear();
                                            outputMap.put("Squids",returnExceptionList);
                                            outputMap.put("Count",count);
                                            PushMessagePacket.pushMap(outputMap,DSObjectType.EXCEPTION,cmd1,cmd2,key,token);
                                        }
                                        returnExceptionList.clear();
                                        exceptionList.clear();

                                    }
                                    logger.info("ExceptionSquid 加载时间:" + (System.currentTimeMillis() - start));
                                    start = System.currentTimeMillis();
                                    break;
                                case KAFKA:
                                    List<KafkaSquid> kafkaSquidList = squidDao.getSquidListForParams(squidTypeId,squidFlowId,"",KafkaSquid.class);
                                    if (kafkaSquidList != null && kafkaSquidList.size() > 0) {
                                        for (KafkaSquid kafkaSquid : kafkaSquidList) {
                                            //查询变量集合
                                            List<DSVariable> variables = variableDao.getDSVariableByScope(2,kafkaSquid.getId());
                                            List<SourceTable> tableList = sourceTableDao.getSourceTableBySquidId(kafkaSquid.getId());
                                            if (tableList == null)
                                                tableList = new ArrayList<SourceTable>();
                                            kafkaSquid.setSourceTableList(tableList);
                                            if(variables==null){
                                                variables = new ArrayList<>();
                                            }
                                            kafkaSquid.setVariables(variables);
                                        }
                                        outputMap.clear();
                                        outputMap.put("Squids",kafkaSquidList);
                                        outputMap.put("Count",count);
                                        PushMessagePacket.pushMap(outputMap,DSObjectType.KAFKA,cmd1,cmd2,key,token);
                                    }
                                    logger.info("Kafka Connection SquidModelBase 加载时间:" + (System.currentTimeMillis() - start));
                                    start = System.currentTimeMillis();
                                    break;
                                case HBASE:
                                    List<HBaseConnectionSquid>
                                            hbaseConnectionSquidList = squidDao.getSquidListForParams(squidTypeId,squidFlowId,"",HBaseConnectionSquid.class);
                                    if (hbaseConnectionSquidList != null && hbaseConnectionSquidList.size() > 0) {
                                        for (HBaseConnectionSquid hbaseConnectionSquid : hbaseConnectionSquidList) {
                                            //查询出变量信息
                                            List<DSVariable> variables = variableDao.getDSVariableByScope(2,hbaseConnectionSquid.getId());
                                            List<SourceTable> tableList = sourceTableDao.getSourceTableBySquidId(
                                                    hbaseConnectionSquid.getId());
                                            if (tableList == null)
                                                tableList = new ArrayList<SourceTable>();
                                            if(variables == null){
                                                variables = new ArrayList<>();
                                            }
                                            hbaseConnectionSquid.setVariables(variables);
                                            hbaseConnectionSquid.setSourceTableList(tableList);
                                        }
                                        outputMap.clear();
                                        outputMap.put("Squids",hbaseConnectionSquidList);
                                        outputMap.put("Count",count);
                                        PushMessagePacket.pushMap(outputMap,DSObjectType.HBASE,cmd1,cmd2,key,token);
                                    }
                                    logger.info("Hbase Connection SquidModelBase 加载时间:" + (System.currentTimeMillis() - start));
                                    start = System.currentTimeMillis();
                                    break;
                                case HIVE:
                                    List<SystemHiveConnectionSquid>
                                            hiveConnectionSquids = squidDao.getSquidListForParams(squidTypeId,squidFlowId,"",SystemHiveConnectionSquid.class);
                                    if (hiveConnectionSquids != null && hiveConnectionSquids.size() > 0) {
                                        for (SystemHiveConnectionSquid hbaseConnectionSquid : hiveConnectionSquids) {
                                            //查询出变量信息
                                            List<DSVariable> variables = variableDao.getDSVariableByScope(2,hbaseConnectionSquid.getId());
                                            List<SourceTable> tableList = sourceTableDao.getSourceTableBySquidId(
                                                    hbaseConnectionSquid.getId());
                                            if (tableList == null)
                                                tableList = new ArrayList<SourceTable>();
                                            if(variables == null){
                                                variables = new ArrayList<>();
                                            }
                                            hbaseConnectionSquid.setVariables(variables);
                                            hbaseConnectionSquid.setSourceTableList(tableList);
                                        }
                                        outputMap.clear();
                                        outputMap.put("Squids",hiveConnectionSquids);
                                        outputMap.put("Count",count);
                                        PushMessagePacket.pushMap(outputMap,DSObjectType.HIVE,cmd1,cmd2,key,token);
                                    }
                                    logger.info("Hive Connection SquidModelBase 加载时间:" + (System.currentTimeMillis() - start));
                                    start = System.currentTimeMillis();
                                    break;
                                case CASSANDRA:
                                    List<CassandraConnectionSquid>
                                            cassandraSquidList = squidDao.getSquidListForParams(squidTypeId,squidFlowId,"",CassandraConnectionSquid.class);
                                    if (cassandraSquidList != null && cassandraSquidList.size() > 0) {
                                        for (CassandraConnectionSquid cassandraConnectionSquid : cassandraSquidList) {
                                            //查询出变量信息
                                            List<DSVariable> variables = variableDao.getDSVariableByScope(2,cassandraConnectionSquid.getId());
                                            List<SourceTable> tableList = sourceTableDao.getSourceTableBySquidId(
                                                    cassandraConnectionSquid.getId());
                                            if (tableList == null)
                                                tableList = new ArrayList<SourceTable>();
                                            if(variables == null){
                                                variables = new ArrayList<>();
                                            }
                                            cassandraConnectionSquid.setVariables(variables);
                                            cassandraConnectionSquid.setSourceTableList(tableList);
                                        }
                                        outputMap.clear();
                                        outputMap.put("Squids",cassandraSquidList);
                                        outputMap.put("Count",count);
                                        PushMessagePacket.pushMap(outputMap,DSObjectType.CASSANDRA,cmd1,cmd2,key,token);
                                    }
                                    logger.info("Cassandra Connection SquidModelBase 加载时间:" + (System.currentTimeMillis() - start));
                                    start = System.currentTimeMillis();
                                    break;
                                default:
                                    //其他类型的
                                    @SuppressWarnings("unchecked")
                                    List<?> list = squidDao.getSquidListForParams(squidTypeId,
                                            squidFlowId,"",SquidTypeEnum.classOfValue(squidTypeId));
                                    if (list != null && list.size() > 0) {
                                        outputMap.clear();
                                        //添加Squid下面的variables
                                        for (int i = 0; i < list.size(); i++) {
                                            Squid squid = (Squid) list.get(i);
                                            List<DSVariable> dsVariables = variableDao.getDSVariableByScope(2,squid.getId());
                                            squid.setVariables(dsVariables);
                                        }
                                        outputMap.put("Squids",list);
                                        outputMap.put("Count",count);
                                        PushMessagePacket.pushMap(outputMap,
                                                DSObjectType.valueOf(squidTypeId),cmd1,cmd2,key,token);
                                    }
                                    logger.info(DSObjectType.parse(squidTypeId).name() + " 加载时间:" + (System.currentTimeMillis() - start));
                                    start = System.currentTimeMillis();
                                    break;
                            }
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                throw e;
            }

        } else {
            logger.error("加载squid flow反序列化失败！");
        }
    }
}