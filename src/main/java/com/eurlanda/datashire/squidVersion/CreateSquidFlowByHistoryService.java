package com.eurlanda.datashire.squidVersion;

import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.annotation.MultitableMapping;
import com.eurlanda.datashire.dao.*;
import com.eurlanda.datashire.dao.impl.*;
import com.eurlanda.datashire.entity.*;
import com.eurlanda.datashire.entity.dest.*;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.server.model.PivotSquid;
import com.eurlanda.datashire.server.model.SamplingSquid;
import com.eurlanda.datashire.sprint7.service.squidflow.ManagerSquidFlowProcess;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.StringUtils;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;

public class CreateSquidFlowByHistoryService {
    /**
     * 正在创建squid
     *
     * @param repository
     * @param newSquidFlowId
     * @param projectId
     * @param squidFlowHistory
     * @param adapter
     * @throws Exception
     */
    public static Map<Integer, Integer> createSquidFlowByHistoryInSquid(int repository, int newSquidFlowId,
                                                                        int projectId, SquidFlowHistory squidFlowHistory, IRelationalDataManager adapter) throws Exception {
        Map<Integer, Integer> squidIdMap = new HashMap<>();
        Map<Integer, Integer> sourceTableIdMap = new HashMap<>();
        try {
            // 正在生成squid
            IHistoryDataDao historyDataDao = new HistoryDataDaoImpl(adapter);
            IVariableDao variableDao = new VariableDaoImpl(adapter);
            // 查询出squidFlow变量信息，存入数据库
            List<HistoryData> historyDataList = historyDataDao.getVersionDataByProcessAndSquidType(squidFlowHistory, 1,
                    -1);
            if (historyDataList != null && historyDataList.size() > 0) {
                for (HistoryData historyData : historyDataList) {
                    List<DSVariable> dsVariableList = JsonUtil.toGsonList(historyData.getData(), DSVariable.class);
                    if (dsVariableList != null && dsVariableList.size() > 0) {
                        for (DSVariable dsVariable : dsVariableList) {
                            dsVariable.setProject_id(projectId);
                            dsVariable.setSquid_flow_id(newSquidFlowId);
                            variableDao.insert2(dsVariable);
                        }
                    }

                }
            }
            // 查询出squid信息，存入数据库,根据processType的类别来存入
            IDBSquidDao dbSquidDao = new DBSquidDaoImpl(adapter);
            ISourceTableDao sourceTableDao = new SourceTableDaoImpl(adapter);
            List<Integer> squidTypeList = historyDataDao.getVersionSquidType(squidFlowHistory);
            List<Integer> orderSquidTypeList = new ArrayList<>();
            List<Integer> extractSquidList = new ArrayList<>();
            Iterator<Integer> iterator = squidTypeList.iterator();
            while (iterator.hasNext()) {
                int squidType = iterator.next();
                if (!SquidTypeEnum.isDBSourceBySquidType(squidType)) {
                    orderSquidTypeList.add(squidType);
                    iterator.remove();
                }
            }
            List<Integer> destSquidType = new ArrayList<>();
            Iterator<Integer> iterator2 = orderSquidTypeList.iterator();
            while (iterator2.hasNext()) {
                int squidType = iterator2.next();
                if (SquidTypeEnum.isDestSquid(squidType)) {
                    destSquidType.add(squidType);
                    iterator2.remove();
                }
                if (SquidTypeEnum.isExtractBySquidType(squidType)) {
                    extractSquidList.add(squidType);
                    iterator2.remove();
                }
            }
            squidTypeList.addAll(extractSquidList);
            squidTypeList.addAll(orderSquidTypeList);
            squidTypeList.addAll(destSquidType);
            // 将按照squidType进行排序，保证执行的先后顺序是其他squid,落地squid,数据源squid
            int alreadyNum = 0;
            for (int squidType : squidTypeList) {
                historyDataList
                        .addAll(historyDataDao.getVersionDataByProcessAndSquidType(squidFlowHistory, 1, squidType));
            }
            //判断是否超过squid数量限制
            for(HistoryData data : historyDataList){
                List<Squid> squids = JsonUtil.toGsonList(data.getData(), Squid.class);
                if(squids!=null) {
                    Iterator<Squid> squidIterator = squids.iterator();
                    while (squidIterator.hasNext()) {
                        Squid squid = squidIterator.next();
                        if (squid.getSquidflow_id() <= 0) {
                            continue;
                        }
                        alreadyNum += 1;
                    }
                }
            }
            if(alreadyNum>0){
                ManagerSquidFlowProcess.validateNum(adapter,repository,alreadyNum);
            }
            //遍历信息(根据类型来)
            for (HistoryData historyData : historyDataList) {
                if (historyData.getSquidType() == -1) {
                    continue;
                }
                if (SquidTypeEnum.isDBSourceBySquidType(historyData.getSquidType())) {
                    if (historyData.getSquidType() == SquidTypeEnum.DBSOURCE.value()
                            || historyData.getSquidType() == SquidTypeEnum.CLOUDDB.value()
                            || historyData.getSquidType() == SquidTypeEnum.TRAININGDBSQUID.value()) {
                        List<DbSquid> dbSquids = JsonUtil.toGsonList(historyData.getData(), DbSquid.class);
                        for (DbSquid dbSquid : dbSquids) {
                            dbSquid.setSquidflow_id(newSquidFlowId);
                            int newSquidId = dbSquidDao.insert2(dbSquid);
                            squidIdMap.put(dbSquid.getId(), newSquidId);
                            List<SourceTable> sourceTableList = dbSquid.getSourceTableList();
                            if (sourceTableList != null) {
                                for (SourceTable sourceTable : sourceTableList) {
                                    // 插入sourceTable
                                    int newSourceTableId = sourceTableDao.InsertSourceTableAndSourceColumn(newSquidId,
                                            sourceTable);
                                    sourceTableIdMap.put(sourceTable.getId(), newSourceTableId);
                                    // 更新抽取squid的source_table_id,destination_squid_id
                                    //updateSourceTable(squidIdMap,sourceTable,newSourceTableId,adapter);
                                }
                            }
                            List<DSVariable> dsVariableList = dbSquid.getVariables();
                            if (dsVariableList != null) {
                                for (DSVariable dsVariable : dsVariableList) {
                                    dsVariable.setSquid_flow_id(newSquidFlowId);
                                    dsVariable.setSquid_id(newSquidId);
                                    dsVariable.setProject_id(projectId);
                                    dsVariable.setAdd_date(new Timestamp(new Date().getTime()).toString());
                                    dbSquidDao.insert2(dsVariable);
                                }
                            }

                        }
                    } else if (historyData.getSquidType() == SquidTypeEnum.HIVE.value()) {
                        List<SystemHiveConnectionSquid> dbSquids = JsonUtil.toGsonList(historyData.getData(), SystemHiveConnectionSquid.class);
                        for (SystemHiveConnectionSquid dbSquid : dbSquids) {
                            dbSquid.setSquidflow_id(newSquidFlowId);
                            int newSquidId = dbSquidDao.insert2(dbSquid);
                            squidIdMap.put(dbSquid.getId(), newSquidId);
                            List<SourceTable> sourceTableList = dbSquid.getSourceTableList();
                            if (sourceTableList != null) {
                                for (SourceTable sourceTable : sourceTableList) {
                                    // 插入sourceTable
                                    int newSourceTableId = sourceTableDao.InsertSourceTableAndSourceColumn(newSquidId,
                                            sourceTable);
                                    sourceTableIdMap.put(sourceTable.getId(), newSourceTableId);
                                    // 更新抽取squid的source_table_id,destination_squid_id
                                    //updateSourceTable(squidIdMap,sourceTable,newSourceTableId,adapter);
                                }
                            }
                            List<DSVariable> dsVariableList = dbSquid.getVariables();
                            if (dsVariableList != null) {
                                for (DSVariable dsVariable : dsVariableList) {
                                    dsVariable.setSquid_flow_id(newSquidFlowId);
                                    dsVariable.setSquid_id(newSquidId);
                                    dsVariable.setProject_id(projectId);
                                    dsVariable.setAdd_date(new Timestamp(new Date().getTime()).toString());
                                    dbSquidDao.insert2(dsVariable);
                                }
                            }

                        }
                    } else if (historyData.getSquidType() == SquidTypeEnum.CASSANDRA.value()) {
                        List<CassandraConnectionSquid> dbSquids = JsonUtil.toGsonList(historyData.getData(), CassandraConnectionSquid.class);
                        for (CassandraConnectionSquid dbSquid : dbSquids) {
                            dbSquid.setSquidflow_id(newSquidFlowId);
                            int newSquidId = dbSquidDao.insert2(dbSquid);
                            squidIdMap.put(dbSquid.getId(), newSquidId);
                            List<SourceTable> sourceTableList = dbSquid.getSourceTableList();
                            if (sourceTableList != null) {
                                for (SourceTable sourceTable : sourceTableList) {
                                    // 插入sourceTable
                                    int newSourceTableId = sourceTableDao.InsertSourceTableAndSourceColumn(newSquidId,
                                            sourceTable);
                                    sourceTableIdMap.put(sourceTable.getId(), newSourceTableId);
                                }
                            }
                            List<DSVariable> dsVariableList = dbSquid.getVariables();
                            if (dsVariableList != null) {
                                for (DSVariable dsVariable : dsVariableList) {
                                    dsVariable.setSquid_flow_id(newSquidFlowId);
                                    dsVariable.setSquid_id(newSquidId);
                                    dsVariable.setProject_id(projectId);
                                    dsVariable.setAdd_date(new Timestamp(new Date().getTime()).toString());
                                    dbSquidDao.insert2(dsVariable);
                                }
                            }

                        }
                    } else if (historyData.getSquidType() == SquidTypeEnum.HTTP.value()) {
                        List<HttpSquid> httpSquidList = JsonUtil.toGsonList(historyData.getData(), HttpSquid.class);
                        for (HttpSquid httpSquid : httpSquidList) {
                            httpSquid.setSquidflow_id(newSquidFlowId);
                            int newSquidId = dbSquidDao.insert2(httpSquid);
                            squidIdMap.put(httpSquid.getId(), newSquidId);
                            List<SourceTable> sourceTableList = httpSquid.getSourceTableList();
                            if (sourceTableList != null) {
                                for (SourceTable sourceTable : sourceTableList) {
                                    // 插入sourceTable
                                    int newSourceTableId = sourceTableDao.InsertSourceTableAndSourceColumn(newSquidId,
                                            sourceTable);
                                    sourceTableIdMap.put(sourceTable.getId(), newSourceTableId);
                                    //updateSourceTable(squidIdMap,sourceTable,newSourceTableId,adapter);
                                }
                            }

                            List<DSVariable> dsVariableList = httpSquid.getVariables();
                            if (dsVariableList != null) {
                                for (DSVariable dsVariable : dsVariableList) {
                                    dsVariable.setSquid_flow_id(newSquidFlowId);
                                    dsVariable.setSquid_id(newSquidId);
                                    dsVariable.setProject_id(projectId);
                                    dsVariable.setAdd_date(new Timestamp(new Date().getTime()).toString());
                                    dbSquidDao.insert2(dsVariable);
                                }
                            }
                        }
                    } else if (historyData.getSquidType() == SquidTypeEnum.WEBSERVICE.value()) {
                        List<WebserviceSquid> webserviceSquidList = JsonUtil.toGsonList(historyData.getData(),
                                WebserviceSquid.class);
                        for (WebserviceSquid serviceSquid : webserviceSquidList) {
                            serviceSquid.setSquidflow_id(newSquidFlowId);
                            int newSquidId = dbSquidDao.insert2(serviceSquid);
                            squidIdMap.put(serviceSquid.getId(), newSquidId);
                            List<SourceTable> sourceTableList = serviceSquid.getSourceTableList();
                            if (sourceTableList != null) {
                                for (SourceTable sourceTable : sourceTableList) {
                                    // 插入sourceTable
                                    int newSourceTableId = sourceTableDao.InsertSourceTableAndSourceColumn(newSquidId,
                                            sourceTable);
                                    sourceTableIdMap.put(sourceTable.getId(), newSourceTableId);
                                    //updateSourceTable(squidIdMap,sourceTable,newSourceTableId,adapter);
                                }
                            }

                            List<DSVariable> dsVariableList = serviceSquid.getVariables();
                            if (dsVariableList != null) {
                                for (DSVariable dsVariable : dsVariableList) {
                                    dsVariable.setSquid_flow_id(newSquidFlowId);
                                    dsVariable.setSquid_id(newSquidId);
                                    dsVariable.setProject_id(projectId);
                                    dsVariable.setAdd_date(new Timestamp(new Date().getTime()).toString());
                                    dbSquidDao.insert2(dsVariable);
                                }
                            }
                        }
                    } else if (historyData.getSquidType() == SquidTypeEnum.DBDESTINATION.value()) {
                        List<DBDestinationSquid> dbDestinationSquidList = JsonUtil.toGsonList(historyData.getData(),
                                DBDestinationSquid.class);
                        for (DBDestinationSquid dbDestinationSquid : dbDestinationSquidList) {
                            dbDestinationSquid.setSquidflow_id(newSquidFlowId);
                            int newSquidId = dbSquidDao.insert2((DbSquid) dbDestinationSquid);
                            squidIdMap.put(dbDestinationSquid.getId(), newSquidId);
                            List<SourceTable> sourceTableList = dbDestinationSquid.getSourceTableList();
                            if (sourceTableList != null) {
                                for (SourceTable sourceTable : sourceTableList) {
                                    // 插入sourceTable
                                    int newSourceTableId = sourceTableDao.InsertSourceTableAndSourceColumn(newSquidId,
                                            sourceTable);
                                    sourceTableIdMap.put(sourceTable.getId(), newSourceTableId);
                                    //updateSourceTable(squidIdMap,sourceTable,newSourceTableId,adapter);
                                }
                            }

                            List<DSVariable> dsVariableList = dbDestinationSquid.getVariables();
                            if (dsVariableList != null) {
                                for (DSVariable dsVariable : dsVariableList) {
                                    dsVariable.setSquid_flow_id(newSquidFlowId);
                                    dsVariable.setSquid_id(newSquidId);
                                    dsVariable.setProject_id(projectId);
                                    dsVariable.setAdd_date(new Timestamp(new Date().getTime()).toString());
                                    dbSquidDao.insert2(dsVariable);
                                }
                            }
                        }
                    } else if (historyData.getSquidType() == SquidTypeEnum.FILEFOLDER.value()) {
                        List<FileFolderSquid> fileFolderSquidList = JsonUtil.toGsonList(historyData.getData(),
                                FileFolderSquid.class);
                        for (FileFolderSquid fileFolderSquid : fileFolderSquidList) {
                            fileFolderSquid.setSquidflow_id(newSquidFlowId);
                            int newSquidId = dbSquidDao.insert2(fileFolderSquid);
                            squidIdMap.put(fileFolderSquid.getId(), newSquidId);
                            List<SourceTable> sourceTableList = fileFolderSquid.getSourceTableList();
                            if (sourceTableList != null) {
                                for (SourceTable sourceTable : sourceTableList) {
                                    // 插入sourceTable
                                    int newSourceTableId = sourceTableDao.InsertSourceTableAndSourceColumn(newSquidId,
                                            sourceTable);
                                    sourceTableIdMap.put(sourceTable.getId(), newSourceTableId);
                                    //updateSourceTable(squidIdMap,sourceTable,newSourceTableId,adapter);
                                }
                            }

                            List<DSVariable> dsVariableList = fileFolderSquid.getVariables();
                            if (dsVariableList != null) {
                                for (DSVariable dsVariable : dsVariableList) {
                                    dsVariable.setSquid_flow_id(newSquidFlowId);
                                    dsVariable.setSquid_id(newSquidId);
                                    dsVariable.setProject_id(projectId);
                                    dsVariable.setAdd_date(new Timestamp(new Date().getTime()).toString());
                                    dbSquidDao.insert2(dsVariable);
                                }
                            }
                        }
                    } else if (historyData.getSquidType() == SquidTypeEnum.FTP.value()) {
                        List<FtpSquid> ftpSquidList = JsonUtil.toGsonList(historyData.getData(), FtpSquid.class);
                        for (FtpSquid ftpSquid : ftpSquidList) {
                            ftpSquid.setSquidflow_id(newSquidFlowId);
                            int newSquidId = dbSquidDao.insert2(ftpSquid);
                            squidIdMap.put(ftpSquid.getId(), newSquidId);
                            List<SourceTable> sourceTableList = ftpSquid.getSourceTableList();
                            if (sourceTableList != null) {
                                for (SourceTable sourceTable : sourceTableList) {
                                    // 插入sourceTable
                                    int newSourceTableId = sourceTableDao.InsertSourceTableAndSourceColumn(newSquidId,
                                            sourceTable);
                                    sourceTableIdMap.put(sourceTable.getId(), newSourceTableId);
                                    //updateSourceTable(squidIdMap,sourceTable,newSourceTableId,adapter);
                                }
                            }
                            List<DSVariable> dsVariableList = ftpSquid.getVariables();
                            if (dsVariableList != null) {
                                for (DSVariable dsVariable : dsVariableList) {
                                    dsVariable.setSquid_flow_id(newSquidFlowId);
                                    dsVariable.setSquid_id(newSquidId);
                                    dsVariable.setProject_id(projectId);
                                    dsVariable.setAdd_date(new Timestamp(new Date().getTime()).toString());
                                    dbSquidDao.insert2(dsVariable);
                                }
                            }

                        }
                    } else if (historyData.getSquidType() == SquidTypeEnum.HDFS.value()
                            || historyData.getSquidType() == SquidTypeEnum.SOURCECLOUDFILE.value() || historyData.getSquidType()==SquidTypeEnum.TRAINNINGFILESQUID.value()) {
                        List<HdfsSquid> hdfsSquidList = JsonUtil.toGsonList(historyData.getData(), HdfsSquid.class);
                        for (HdfsSquid hdfsSquid : hdfsSquidList) {
                            hdfsSquid.setSquidflow_id(newSquidFlowId);
                            int newSquidId = dbSquidDao.insert2(hdfsSquid);
                            squidIdMap.put(hdfsSquid.getId(), newSquidId);
                            List<SourceTable> sourceTableList = hdfsSquid.getSourceTableList();
                            if (sourceTableList != null) {
                                for (SourceTable sourceTable : sourceTableList) {
                                    // 插入sourceTable
                                    int newSourceTableId = sourceTableDao.InsertSourceTableAndSourceColumn(newSquidId,
                                            sourceTable);
                                    sourceTableIdMap.put(sourceTable.getId(), newSourceTableId);
                                    //updateSourceTable(squidIdMap,sourceTable,newSourceTableId,adapter);
                                }
                            }

                            List<DSVariable> dsVariableList = hdfsSquid.getVariables();
                            if (dsVariableList != null) {
                                for (DSVariable dsVariable : dsVariableList) {
                                    dsVariable.setSquid_flow_id(newSquidFlowId);
                                    dsVariable.setSquid_id(newSquidId);
                                    dsVariable.setProject_id(projectId);
                                    dsVariable.setAdd_date(new Timestamp(new Date().getTime()).toString());
                                    dbSquidDao.insert2(dsVariable);
                                }
                            }
                        }
                    } else if (historyData.getSquidType() == SquidTypeEnum.WEB.value()) {
                        List<WebSquid> webSquidList = JsonUtil.toGsonList(historyData.getData(), WebSquid.class);
                        for (WebSquid webSquid : webSquidList) {
                            webSquid.setSquidflow_id(newSquidFlowId);
                            int newSquidId = dbSquidDao.insert2(webSquid);
                            squidIdMap.put(webSquid.getId(), newSquidId);
                            List<SourceTable> sourceTableList = webSquid.getSourceTableList();
                            if (sourceTableList != null) {
                                for (SourceTable sourceTable : sourceTableList) {
                                    // 插入sourceTable
                                    int newSourceTableId = sourceTableDao.InsertSourceTableAndSourceColumn(newSquidId,
                                            sourceTable);
                                    sourceTableIdMap.put(sourceTable.getId(), newSourceTableId);
                                    //updateSourceTable(squidIdMap,sourceTable,newSourceTableId,adapter);
                                }
                            }

                            List<DSVariable> dsVariableList = webSquid.getVariables();
                            if (dsVariableList != null) {
                                for (DSVariable dsVariable : dsVariableList) {
                                    dsVariable.setSquid_flow_id(newSquidFlowId);
                                    dsVariable.setSquid_id(newSquidId);
                                    dsVariable.setProject_id(projectId);
                                    dsVariable.setAdd_date(new Timestamp(new Date().getTime()).toString());
                                    dbSquidDao.insert2(dsVariable);
                                }
                            }
                        }
                    } else if (historyData.getSquidType() == SquidTypeEnum.WEIBO.value()) {
                        List<WeiboSquid> weiboSquidList = JsonUtil.toGsonList(historyData.getData(), WeiboSquid.class);
                        for (WeiboSquid weiboSquid : weiboSquidList) {
                            weiboSquid.setSquidflow_id(newSquidFlowId);
                            int newSquidId = dbSquidDao.insert2(weiboSquid);
                            squidIdMap.put(weiboSquid.getId(), newSquidId);
                            List<SourceTable> sourceTableList = weiboSquid.getSourceTableList();
                            if (sourceTableList != null) {
                                for (SourceTable sourceTable : sourceTableList) {
                                    // 插入sourceTable
                                    int newSourceTableId = sourceTableDao.InsertSourceTableAndSourceColumn(newSquidId,
                                            sourceTable);
                                    sourceTableIdMap.put(sourceTable.getId(), newSourceTableId);
                                    //updateSourceTable(squidIdMap,sourceTable,newSourceTableId,adapter);
                                }
                            }

                            List<DSVariable> dsVariableList = weiboSquid.getVariables();
                            if (dsVariableList != null) {
                                for (DSVariable dsVariable : dsVariableList) {
                                    dsVariable.setSquid_flow_id(newSquidFlowId);
                                    dsVariable.setSquid_id(newSquidId);
                                    dsVariable.setProject_id(projectId);
                                    dsVariable.setAdd_date(new Timestamp(new Date().getTime()).toString());
                                    dbSquidDao.insert2(dsVariable);
                                }
                            }
                        }
                    } else if (historyData.getSquidType() == SquidTypeEnum.MONGODB.value()) {
                        List<NOSQLConnectionSquid> mongoDBList = JsonUtil.toGsonList(historyData.getData(),
                                NOSQLConnectionSquid.class);
                        for (NOSQLConnectionSquid mongoDBSquid : mongoDBList) {
                            mongoDBSquid.setSquidflow_id(newSquidFlowId);
                            int newSquidId = dbSquidDao.insert2(mongoDBSquid);
                            squidIdMap.put(mongoDBSquid.getId(), newSquidId);
                            List<SourceTable> sourceTableList = mongoDBSquid.getSourceTableList();
                            if (sourceTableList != null) {
                                for (SourceTable sourceTable : sourceTableList) {
                                    // 插入sourceTable
                                    int newSourceTableId = sourceTableDao.InsertSourceTableAndSourceColumn(newSquidId,
                                            sourceTable);
                                    sourceTableIdMap.put(sourceTable.getId(), newSourceTableId);
                                    //updateSourceTable(squidIdMap,sourceTable,newSourceTableId,adapter);
                                }
                            }

                            List<DSVariable> dsVariableList = mongoDBSquid.getVariables();
                            if (dsVariableList != null) {
                                for (DSVariable dsVariable : dsVariableList) {
                                    dsVariable.setSquid_flow_id(newSquidFlowId);
                                    dsVariable.setSquid_id(newSquidId);
                                    dsVariable.setProject_id(projectId);
                                    dsVariable.setAdd_date(new Timestamp(new Date().getTime()).toString());
                                    dbSquidDao.insert2(dsVariable);
                                }
                            }
                        }
                    } else if (historyData.getSquidType() == SquidTypeEnum.KAFKA.value()) {
                        List<KafkaSquid> kafkaSquidList = JsonUtil.toGsonList(historyData.getData(), KafkaSquid.class);
                        for (KafkaSquid kafkaSquid : kafkaSquidList) {
                            kafkaSquid.setSquidflow_id(newSquidFlowId);
                            int newSquidId = dbSquidDao.insert2(kafkaSquid);
                            squidIdMap.put(kafkaSquid.getId(), newSquidId);
                            List<SourceTable> sourceTableList = kafkaSquid.getSourceTableList();
                            if (sourceTableList != null) {
                                for (SourceTable sourceTable : sourceTableList) {
                                    // 插入sourceTable
                                    int newSourceTableId = sourceTableDao.InsertSourceTableAndSourceColumn(newSquidId,
                                            sourceTable);
                                    sourceTableIdMap.put(sourceTable.getId(), newSourceTableId);
                                    //updateSourceTable(squidIdMap,sourceTable,newSourceTableId,adapter);
                                }
                            }

                            List<DSVariable> dsVariableList = kafkaSquid.getVariables();
                            if (dsVariableList != null) {
                                for (DSVariable dsVariable : dsVariableList) {
                                    dsVariable.setSquid_flow_id(newSquidFlowId);
                                    dsVariable.setSquid_id(newSquidId);
                                    dsVariable.setProject_id(projectId);
                                    dsVariable.setAdd_date(new Timestamp(new Date().getTime()).toString());
                                    dbSquidDao.insert2(dsVariable);
                                }
                            }
                        }
                    } else if (historyData.getSquidType() == SquidTypeEnum.HBASE.value()) {
                        List<HBaseConnectionSquid> hBaseConnectionSquidList = JsonUtil.toGsonList(historyData.getData(),
                                HBaseConnectionSquid.class);
                        for (HBaseConnectionSquid hBaseSquid : hBaseConnectionSquidList) {
                            hBaseSquid.setSquidflow_id(newSquidFlowId);
                            int newSquidId = dbSquidDao.insert2(hBaseSquid);
                            squidIdMap.put(hBaseSquid.getId(), newSquidId);
                            List<SourceTable> sourceTableList = hBaseSquid.getSourceTableList();
                            if (sourceTableList != null) {
                                for (SourceTable sourceTable : sourceTableList) {
                                    // 插入sourceTable
                                    int newSourceTableId = sourceTableDao.InsertSourceTableAndSourceColumn(newSquidId,
                                            sourceTable);
                                    sourceTableIdMap.put(sourceTable.getId(), newSourceTableId);
                                    //updateSourceTable(squidIdMap,sourceTable,newSourceTableId,adapter);
                                }
                            }

                            List<DSVariable> dsVariableList = hBaseSquid.getVariables();
                            if (dsVariableList != null) {
                                for (DSVariable dsVariable : dsVariableList) {
                                    dsVariable.setSquid_flow_id(newSquidFlowId);
                                    dsVariable.setSquid_id(newSquidId);
                                    dsVariable.setProject_id(projectId);
                                    dsVariable.setAdd_date(new Timestamp(new Date().getTime()).toString());
                                    dbSquidDao.insert2(dsVariable);
                                }
                            }
                        }
                    }
                } else if (historyData.getSquidType() == SquidTypeEnum.EXTRACT.value()
                        || historyData.getSquidType() == SquidTypeEnum.XML_EXTRACT.value()
                        || historyData.getSquidType() == SquidTypeEnum.WEBLOGEXTRACT.value()
                        || historyData.getSquidType() == SquidTypeEnum.WEBEXTRACT.value()
                        || historyData.getSquidType() == SquidTypeEnum.WEIBOEXTRACT.value()
                        || historyData.getSquidType() == SquidTypeEnum.HIVEEXTRACT.value()
                        || historyData.getSquidType() == SquidTypeEnum.CASSANDRA_EXTRACT.value()) {
                    List<DataSquid> dataSquidList = JsonUtil.toGsonList(historyData.getData(), DataSquid.class);
                    // 先插入，在生成数据源squid的时候进行更新sourceTableId
                    if (dataSquidList != null && dataSquidList.size() > 0) {
                        for (DataSquid dataSquid : dataSquidList) {
                            dataSquid.setSquidflow_id(newSquidFlowId);
                            if (dataSquid.getDestination_squid_id() != 0
                                    && squidIdMap.containsKey(dataSquid.getDestination_squid_id())) {
                                dataSquid.setDestination_squid_id(squidIdMap.get(dataSquid.getDestination_squid_id()));
                            }
                            if (sourceTableIdMap.containsKey(dataSquid.getSource_table_id())) {
                                dataSquid.setSource_table_id(sourceTableIdMap.get(dataSquid.getSource_table_id()));
                            }
                            if (dataSquid.getRef_squid_id() != 0
                                    && squidIdMap.containsKey(dataSquid.getRef_squid_id())) {
                                dataSquid.setRef_squid_id(squidIdMap.get(dataSquid.getRef_squid_id()));
                            }
                            int newSquidId = dbSquidDao.insert2(dataSquid);
                            squidIdMap.put(dataSquid.getId(), newSquidId);
                            List<DSVariable> dsVariableList = dataSquid.getVariables();
                            if (dsVariableList != null) {
                                for (DSVariable dsVariable : dsVariableList) {
                                    dsVariable.setProject_id(projectId);
                                    dsVariable.setSquid_flow_id(newSquidFlowId);
                                    dsVariable.setSquid_id(newSquidId);
                                    dsVariable.setAdd_date(new Timestamp(new Date().getTime()).toString());
                                    dbSquidDao.insert2(dsVariable);
                                }
                            }
                        }
                    }
                } else if (historyData.getSquidType() == SquidTypeEnum.HBASEEXTRACT.value()) {
                    List<HBaseExtractSquid> hBaseExtractSquidList = JsonUtil.toGsonList(historyData.getData(),
                            HBaseExtractSquid.class);
                    // 先插入，在生成数据源squid的时候进行更新sourceTableId
                    if (hBaseExtractSquidList != null && hBaseExtractSquidList.size() > 0) {
                        for (HBaseExtractSquid extractSquid : hBaseExtractSquidList) {
                            extractSquid.setSquidflow_id(newSquidFlowId);
                            if (extractSquid.getDestination_squid_id() != 0
                                    && squidIdMap.containsKey(extractSquid.getDestination_squid_id())) {
                                extractSquid.setDestination_squid_id(squidIdMap.get(extractSquid.getDestination_squid_id()));
                            }
                            if (sourceTableIdMap.containsKey(extractSquid.getSource_table_id())) {
                                extractSquid.setSource_table_id(sourceTableIdMap.get(extractSquid.getSource_table_id()));
                            }
                            if (extractSquid.getRef_squid_id() != 0
                                    && squidIdMap.containsKey(extractSquid.getRef_squid_id())) {
                                extractSquid.setRef_squid_id(squidIdMap.get(extractSquid.getRef_squid_id()));
                            }
                            int newSquidId = dbSquidDao.insert2(extractSquid);
                            squidIdMap.put(extractSquid.getId(), newSquidId);
                            List<DSVariable> dsVariables = extractSquid.getVariables();
                            if (dsVariables != null) {
                                for (DSVariable dsVariable : dsVariables) {
                                    dsVariable.setProject_id(projectId);
                                    dsVariable.setSquid_flow_id(newSquidFlowId);
                                    dsVariable.setSquid_id(newSquidId);
                                    dsVariable.setAdd_date(new Timestamp(new Date().getTime()).toString());
                                    dbSquidDao.insert2(dsVariable);
                                }
                            }
                        }
                    }
                } else if (historyData.getSquidType() == SquidTypeEnum.KAFKAEXTRACT.value()) {
                    List<KafkaExtractSquid> kafkaExtractSquids = JsonUtil.toGsonList(historyData.getData(),
                            KafkaExtractSquid.class);
                    // 先插入，在生成数据源squid的时候进行更新sourceTableId
                    if (kafkaExtractSquids != null && kafkaExtractSquids.size() > 0) {
                        for (KafkaExtractSquid extractSquid : kafkaExtractSquids) {
                            extractSquid.setSquidflow_id(newSquidFlowId);
                            if (extractSquid.getDestination_squid_id() != 0
                                    && squidIdMap.containsKey(extractSquid.getDestination_squid_id())) {
                                extractSquid.setDestination_squid_id(squidIdMap.get(extractSquid.getDestination_squid_id()));
                            }
                            if (sourceTableIdMap.containsKey(extractSquid.getSource_table_id())) {
                                extractSquid.setSource_table_id(sourceTableIdMap.get(extractSquid.getSource_table_id()));
                            }
                            if (extractSquid.getRef_squid_id() != 0
                                    && squidIdMap.containsKey(extractSquid.getRef_squid_id())) {
                                extractSquid.setRef_squid_id(squidIdMap.get(extractSquid.getRef_squid_id()));
                            }
                            int newSquidId = dbSquidDao.insert2(extractSquid);
                            squidIdMap.put(extractSquid.getId(), newSquidId);
                            List<DSVariable> dsVariables = extractSquid.getVariables();
                            if (dsVariables != null) {
                                for (DSVariable dsVariable : dsVariables) {
                                    dsVariable.setProject_id(projectId);
                                    dsVariable.setSquid_flow_id(newSquidFlowId);
                                    dsVariable.setSquid_id(newSquidId);
                                    dsVariable.setAdd_date(new Timestamp(new Date().getTime()).toString());
                                    dbSquidDao.insert2(dsVariable);
                                }
                            }
                        }
                    }
                } else if (historyData.getSquidType() == SquidTypeEnum.DOC_EXTRACT.value()) {
                    List<DocExtractSquid> docExtractSquids = JsonUtil.toGsonList(historyData.getData(),
                            DocExtractSquid.class);
                    // 先插入，在生成数据源squid的时候进行更新sourceTableId
                    if (docExtractSquids != null && docExtractSquids.size() > 0) {
                        for (DocExtractSquid extractSquid : docExtractSquids) {
                            extractSquid.setSquidflow_id(newSquidFlowId);
                            if (extractSquid.getDestination_squid_id() != 0
                                    && squidIdMap.containsKey(extractSquid.getDestination_squid_id())) {
                                extractSquid.setDestination_squid_id(squidIdMap.get(extractSquid.getDestination_squid_id()));
                            }
                            if (sourceTableIdMap.containsKey(extractSquid.getSource_table_id())) {
                                extractSquid.setSource_table_id(sourceTableIdMap.get(extractSquid.getSource_table_id()));
                            }
                            if (extractSquid.getRef_squid_id() != 0
                                    && squidIdMap.containsKey(extractSquid.getRef_squid_id())) {
                                extractSquid.setRef_squid_id(squidIdMap.get(extractSquid.getRef_squid_id()));
                            }
                            int newSquidId = dbSquidDao.insert2(extractSquid);
                            squidIdMap.put(extractSquid.getId(), newSquidId);
                            List<DSVariable> dsVariables = extractSquid.getVariables();
                            if (dsVariables != null) {
                                for (DSVariable dsVariable : dsVariables) {
                                    dsVariable.setProject_id(projectId);
                                    dsVariable.setSquid_flow_id(newSquidFlowId);
                                    dsVariable.setSquid_id(newSquidId);
                                    dsVariable.setAdd_date(new Timestamp(new Date().getTime()).toString());
                                    dbSquidDao.insert2(dsVariable);
                                }
                            }
                        }
                    }

                } else if (historyData.getSquidType() == SquidTypeEnum.MONGODBEXTRACT.value()) {
                    List<MongodbExtractSquid> mongodbExtractSquids = JsonUtil.toGsonList(historyData.getData(),
                            MongodbExtractSquid.class);
                    // 先插入，在生成数据源squid的时候进行更新sourceTableId
                    if (mongodbExtractSquids != null && mongodbExtractSquids.size() > 0) {
                        for (MongodbExtractSquid extractSquid : mongodbExtractSquids) {
                            extractSquid.setSquidflow_id(newSquidFlowId);
                            if (extractSquid.getDestination_squid_id() != 0
                                    && squidIdMap.containsKey(extractSquid.getDestination_squid_id())) {
                                extractSquid.setDestination_squid_id(squidIdMap.get(extractSquid.getDestination_squid_id()));
                            }
                            if (sourceTableIdMap.containsKey(extractSquid.getSource_table_id())) {
                                extractSquid.setSource_table_id(sourceTableIdMap.get(extractSquid.getSource_table_id()));
                            }
                            if (extractSquid.getRef_squid_id() != 0 && squidIdMap.containsKey(extractSquid.getRef_squid_id())) {
                                extractSquid.setRef_squid_id(squidIdMap.get(extractSquid.getRef_squid_id()));
                            }
                            int newSquidId = dbSquidDao.insert2(extractSquid);
                            squidIdMap.put(extractSquid.getId(), newSquidId);
                            List<DSVariable> dsVariables = extractSquid.getVariables();
                            if (dsVariables != null) {
                                for (DSVariable dsVariable : dsVariables) {
                                    dsVariable.setProject_id(projectId);
                                    dsVariable.setSquid_flow_id(newSquidFlowId);
                                    dsVariable.setSquid_id(newSquidId);
                                    dsVariable.setAdd_date(new Timestamp(new Date().getTime()).toString());
                                    dbSquidDao.insert2(dsVariable);
                                }
                            }
                        }
                    }
                } else if (historyData.getSquidType() == SquidTypeEnum.WEBSERVICEEXTRACT.value()) {
                    List<WebserviceExtractSquid> webserviceExtractSquids = JsonUtil.toGsonList(historyData.getData(),
                            WebserviceExtractSquid.class);
                    // 先插入，在生成数据源squid的时候进行更新sourceTableId
                    if (webserviceExtractSquids != null && webserviceExtractSquids.size() > 0) {
                        for (WebserviceExtractSquid extractSquid : webserviceExtractSquids) {
                            extractSquid.setSquidflow_id(newSquidFlowId);
                            if (extractSquid.getDestination_squid_id() != 0
                                    && squidIdMap.containsKey(extractSquid.getDestination_squid_id())) {
                                extractSquid.setDestination_squid_id(squidIdMap.get(extractSquid.getDestination_squid_id()));
                            }
                            if (sourceTableIdMap.containsKey(extractSquid.getSource_table_id())) {
                                extractSquid.setSource_table_id(sourceTableIdMap.get(extractSquid.getSource_table_id()));
                            }
                            if (extractSquid.getRef_squid_id() != 0
                                    && squidIdMap.containsKey(extractSquid.getRef_squid_id())) {
                                extractSquid.setRef_squid_id(squidIdMap.get(extractSquid.getRef_squid_id()));
                            }
                            int newSquidId = dbSquidDao.insert2(extractSquid);
                            squidIdMap.put(extractSquid.getId(), newSquidId);
                            List<DSVariable> dsVariables = extractSquid.getVariables();
                            if (dsVariables != null) {
                                for (DSVariable dsVariable : dsVariables) {
                                    dsVariable.setProject_id(projectId);
                                    dsVariable.setSquid_flow_id(newSquidFlowId);
                                    dsVariable.setSquid_id(newSquidId);
                                    dsVariable.setAdd_date(new Timestamp(new Date().getTime()).toString());
                                    dbSquidDao.insert2(dsVariable);
                                }
                            }
                        }
                    }
                } else if (historyData.getSquidType() == SquidTypeEnum.HTTPEXTRACT.value()) {
                    List<HttpExtractSquid> httpExtractSquids = JsonUtil.toGsonList(historyData.getData(),
                            HttpExtractSquid.class);
                    // 先插入，在生成数据源squid的时候进行更新sourceTableId
                    if (httpExtractSquids != null && httpExtractSquids.size() > 0) {
                        for (HttpExtractSquid extractSquid : httpExtractSquids) {
                            extractSquid.setSquidflow_id(newSquidFlowId);
                            if (extractSquid.getDestination_squid_id() != 0
                                    && squidIdMap.containsKey(extractSquid.getDestination_squid_id())) {
                                extractSquid.setDestination_squid_id(squidIdMap.get(extractSquid.getDestination_squid_id()));
                            }
                            if (sourceTableIdMap.containsKey(extractSquid.getSource_table_id())) {
                                extractSquid.setSource_table_id(sourceTableIdMap.get(extractSquid.getSource_table_id()));
                            }
                            if (extractSquid.getRef_squid_id() != 0
                                    && squidIdMap.containsKey(extractSquid.getRef_squid_id())) {
                                extractSquid.setRef_squid_id(squidIdMap.get(extractSquid.getRef_squid_id()));
                            }
                            int newSquidId = dbSquidDao.insert2(extractSquid);
                            squidIdMap.put(extractSquid.getId(), newSquidId);
                            List<DSVariable> dsVariables = extractSquid.getVariables();
                            if (dsVariables != null) {
                                for (DSVariable dsVariable : dsVariables) {
                                    dsVariable.setProject_id(projectId);
                                    dsVariable.setSquid_flow_id(newSquidFlowId);
                                    dsVariable.setSquid_id(newSquidId);
                                    dsVariable.setAdd_date(new Timestamp(new Date().getTime()).toString());
                                    dbSquidDao.insert2(dsVariable);
                                }
                            }
                        }
                    }
                } else if (historyData.getSquidType() == SquidTypeEnum.STAGE.value()) {
                    List<StageSquid> stageSquids = JsonUtil.toGsonList(historyData.getData(), StageSquid.class);
                    if (stageSquids != null && stageSquids.size() > 0) {
                        for (StageSquid stageSquid : stageSquids) {
                            stageSquid.setSquidflow_id(newSquidFlowId);
                            if (stageSquid.getDestination_squid_id() != 0
                                    && stageSquid.isIs_persisted()
                                    && squidIdMap.containsKey(stageSquid.getDestination_squid_id())) {
                                stageSquid.setDestination_squid_id(squidIdMap.get(stageSquid.getDestination_squid_id()));
                            }
                            int newSquidId = dbSquidDao.insert2(stageSquid);
                            squidIdMap.put(stageSquid.getId(), newSquidId);
                            List<DSVariable> dsVariables = stageSquid.getVariables();
                            if (dsVariables != null) {
                                for (DSVariable dsVariable : dsVariables) {
                                    dsVariable.setProject_id(projectId);
                                    dsVariable.setSquid_flow_id(newSquidFlowId);
                                    dsVariable.setSquid_id(newSquidId);
                                    dsVariable.setAdd_date(new Timestamp(new Date().getTime()).toString());
                                    dbSquidDao.insert2(dsVariable);
                                }
                            }
                        }
                    }
                } else if (historyData.getSquidType() == SquidTypeEnum.GROUPTAGGING.value()) {
                    List<GroupTaggingSquid> stageSquids = JsonUtil.toGsonList(historyData.getData(), GroupTaggingSquid.class);
                    if (stageSquids != null && stageSquids.size() > 0) {
                        for (GroupTaggingSquid stageSquid : stageSquids) {
                            stageSquid.setSquidflow_id(newSquidFlowId);
                            if (stageSquid.getDestination_squid_id() != 0
                                    && stageSquid.isIs_persisted()
                                    && squidIdMap.containsKey(stageSquid.getDestination_squid_id())) {
                                stageSquid.setDestination_squid_id(squidIdMap.get(stageSquid.getDestination_squid_id()));
                            }
                            int newSquidId = dbSquidDao.insert2(stageSquid);
                            squidIdMap.put(stageSquid.getId(), newSquidId);
                            List<DSVariable> dsVariables = stageSquid.getVariables();
                            if (dsVariables != null) {
                                for (DSVariable dsVariable : dsVariables) {
                                    dsVariable.setProject_id(projectId);
                                    dsVariable.setSquid_flow_id(newSquidFlowId);
                                    dsVariable.setSquid_id(newSquidId);
                                    dsVariable.setAdd_date(new Timestamp(new Date().getTime()).toString());
                                    dbSquidDao.insert2(dsVariable);
                                }
                            }
                        }
                    }
                }else if(historyData.getSquidType()==SquidTypeEnum.SAMPLINGSQUID.value()){
                    List<SamplingSquid> samplingSquids=JsonUtil.toGsonList(historyData.getData(), SamplingSquid.class);
                    if (samplingSquids != null && samplingSquids.size() > 0) {
                        for (SamplingSquid stageSquid : samplingSquids) {
                            stageSquid.setSquidflow_id(newSquidFlowId);
                            if (stageSquid.getDestination_squid_id() != 0
                                    && stageSquid.isIs_persisted()
                                    && squidIdMap.containsKey(stageSquid.getDestination_squid_id())) {
                                stageSquid.setDestination_squid_id(squidIdMap.get(stageSquid.getDestination_squid_id()));
                            }
                            int newSquidId = dbSquidDao.insert2(stageSquid);
                            squidIdMap.put(stageSquid.getId(), newSquidId);
                            List<DSVariable> dsVariables = stageSquid.getVariables();
                            if (dsVariables != null) {
                                for (DSVariable dsVariable : dsVariables) {
                                    dsVariable.setProject_id(projectId);
                                    dsVariable.setSquid_flow_id(newSquidFlowId);
                                    dsVariable.setSquid_id(newSquidId);
                                    dsVariable.setAdd_date(new Timestamp(new Date().getTime()).toString());
                                    dbSquidDao.insert2(dsVariable);
                                }
                            }
                        }
                    }
                }else if(historyData.getSquidType()==SquidTypeEnum.PIVOTSQUID.value()){
                    List<PivotSquid> pivotSquids = JsonUtil.toGsonList(historyData.getData(), PivotSquid.class);
                    if (pivotSquids != null && pivotSquids.size() > 0) {
                        for (PivotSquid stageSquid : pivotSquids) {
                            stageSquid.setSquidflow_id(newSquidFlowId);
                            if (stageSquid.getDestination_squid_id() != 0
                                    && stageSquid.isIs_persisted()
                                    && squidIdMap.containsKey(stageSquid.getDestination_squid_id())) {
                                stageSquid.setDestination_squid_id(squidIdMap.get(stageSquid.getDestination_squid_id()));
                            }
                            int newSquidId = dbSquidDao.insert2(stageSquid);
                            squidIdMap.put(stageSquid.getId(), newSquidId);
                            List<DSVariable> dsVariables = stageSquid.getVariables();
                            if (dsVariables != null) {
                                for (DSVariable dsVariable : dsVariables) {
                                    dsVariable.setProject_id(projectId);
                                    dsVariable.setSquid_flow_id(newSquidFlowId);
                                    dsVariable.setSquid_id(newSquidId);
                                    dsVariable.setAdd_date(new Timestamp(new Date().getTime()).toString());
                                    dbSquidDao.insert2(dsVariable);
                                }
                            }
                        }
                    }
                }


                else if (historyData.getSquidType() == SquidTypeEnum.STREAM_STAGE.value()) {
                    List<StreamStageSquid> stageSquids = JsonUtil.toGsonList(historyData.getData(),
                            StreamStageSquid.class);
                    if (stageSquids != null && stageSquids.size() > 0) {
                        for (StreamStageSquid stageSquid : stageSquids) {
                            stageSquid.setSquidflow_id(newSquidFlowId);
                            if (stageSquid.getDestination_squid_id() != 0
                                    && stageSquid.isIs_persisted()
                                    && squidIdMap.containsKey(stageSquid.getDestination_squid_id())) {
                                stageSquid.setDestination_squid_id(squidIdMap.get(stageSquid.getDestination_squid_id()));
                            }
                            int newSquidId = dbSquidDao.insert2(stageSquid);
                            squidIdMap.put(stageSquid.getId(), newSquidId);
                            List<DSVariable> dsVariables = stageSquid.getVariables();
                            if (dsVariables != null) {
                                for (DSVariable dsVariable : dsVariables) {
                                    dsVariable.setProject_id(projectId);
                                    dsVariable.setSquid_flow_id(newSquidFlowId);
                                    dsVariable.setSquid_id(newSquidId);
                                    dsVariable.setAdd_date(new Timestamp(new Date().getTime()).toString());
                                    dbSquidDao.insert2(dsVariable);
                                }
                            }
                        }
                    }
                } else if (historyData.getSquidType() == SquidTypeEnum.USERDEFINED.value()) {
                    List<UserDefinedSquid> definedSquids = JsonUtil.toGsonList(historyData.getData(),
                            UserDefinedSquid.class);
                    if (definedSquids != null && definedSquids.size() > 0) {
                        for (UserDefinedSquid definedSquid : definedSquids) {
                            definedSquid.setSquidflow_id(newSquidFlowId);
                            if (definedSquid.getDestination_squid_id() != 0
                                    && definedSquid.isIs_persisted()
                                    && squidIdMap.containsKey(definedSquid.getDestination_squid_id())) {
                                definedSquid.setDestination_squid_id(squidIdMap.get(definedSquid.getDestination_squid_id()));
                            }
                            int newSquidId = dbSquidDao.insert2(definedSquid);
                            squidIdMap.put(definedSquid.getId(), newSquidId);
                            List<DSVariable> dsVariables = definedSquid.getVariables();
                            if (dsVariables != null) {
                                for (DSVariable dsVariable : dsVariables) {
                                    dsVariable.setProject_id(projectId);
                                    dsVariable.setSquid_flow_id(newSquidFlowId);
                                    dsVariable.setSquid_id(newSquidId);
                                    dsVariable.setAdd_date(new Timestamp(new Date().getTime()).toString());
                                    dbSquidDao.insert2(dsVariable);
                                }
                            }
                        }
                    }
                } else if (historyData.getSquidType() == SquidTypeEnum.DEST_HIVE.value()) {
                    List<DestHiveSquid> definedSquids = JsonUtil.toGsonList(historyData.getData(),
                            DestHiveSquid.class);
                    if (definedSquids != null && definedSquids.size() > 0) {
                        for (DestHiveSquid definedSquid : definedSquids) {
                            definedSquid.setSquidflow_id(newSquidFlowId);
                            int newSquidId = dbSquidDao.insert2(definedSquid);
                            squidIdMap.put(definedSquid.getId(), newSquidId);
                            List<DSVariable> dsVariables = definedSquid.getVariables();
                            if (dsVariables != null) {
                                for (DSVariable dsVariable : dsVariables) {
                                    dsVariable.setProject_id(projectId);
                                    dsVariable.setSquid_flow_id(newSquidFlowId);
                                    dsVariable.setSquid_id(newSquidId);
                                    dsVariable.setAdd_date(new Timestamp(new Date().getTime()).toString());
                                    dbSquidDao.insert2(dsVariable);
                                }
                            }
                        }
                    }
                }else if (historyData.getSquidType() == SquidTypeEnum.DEST_CASSANDRA.value()) {
                    List<DestCassandraSquid> definedSquids = JsonUtil.toGsonList(historyData.getData(),
                            DestCassandraSquid.class);
                    if (definedSquids != null && definedSquids.size() > 0) {
                        for (DestCassandraSquid definedSquid : definedSquids) {
                            //获取版本时要把cassandra落地对应的数据源更新过来
                            if(squidIdMap.get(definedSquid.getDest_squid_id())!=null){
                                definedSquid.setDest_squid_id(squidIdMap.get(definedSquid.getDest_squid_id()));
                            }
                            definedSquid.setSquidflow_id(newSquidFlowId);
                            int newSquidId = dbSquidDao.insert2(definedSquid);
                            squidIdMap.put(definedSquid.getId(), newSquidId);
                            List<DSVariable> dsVariables = definedSquid.getVariables();
                            if (dsVariables != null) {
                                for (DSVariable dsVariable : dsVariables) {
                                    dsVariable.setProject_id(projectId);
                                    dsVariable.setSquid_flow_id(newSquidFlowId);
                                    dsVariable.setSquid_id(newSquidId);
                                    dsVariable.setAdd_date(new Timestamp(new Date().getTime()).toString());
                                    dbSquidDao.insert2(dsVariable);
                                }
                            }
                        }
                    }
                } else if(historyData.getSquidType() == SquidTypeEnum.STATISTICS.value()){
                    List<StatisticsSquid> definedSquids = JsonUtil.toGsonList(historyData.getData(),
                            StatisticsSquid.class);
                    if (definedSquids != null && definedSquids.size() > 0) {
                        for (StatisticsSquid definedSquid : definedSquids) {
                            definedSquid.setSquidflow_id(newSquidFlowId);
                            if (definedSquid.getDestination_squid_id() != 0
                                    && definedSquid.isIs_persisted()
                                    && squidIdMap.containsKey(definedSquid.getDestination_squid_id())) {
                                definedSquid.setDestination_squid_id(squidIdMap.get(definedSquid.getDestination_squid_id()));
                            }
                            int newSquidId = dbSquidDao.insert2(definedSquid);
                            squidIdMap.put(definedSquid.getId(), newSquidId);
                            List<DSVariable> dsVariables = definedSquid.getVariables();
                            if (dsVariables != null) {
                                for (DSVariable dsVariable : dsVariables) {
                                    dsVariable.setProject_id(projectId);
                                    dsVariable.setSquid_flow_id(newSquidFlowId);
                                    dsVariable.setSquid_id(newSquidId);
                                    dsVariable.setAdd_date(new Timestamp(new Date().getTime()).toString());
                                    dbSquidDao.insert2(dsVariable);
                                }
                            }
                        }
                    }
                } else if (SquidTypeEnum.isDataMingSquid(historyData.getSquidType())) {
                    List<DataMiningSquid> dataMiningSquids = JsonUtil.toGsonList(historyData.getData(),
                            DataMiningSquid.class);
                    if (dataMiningSquids != null && dataMiningSquids.size() > 0) {
                        for (DataMiningSquid dataMiningSquid : dataMiningSquids) {
                            dataMiningSquid.setSquidflow_id(newSquidFlowId);
                            if (dataMiningSquid.getDestination_squid_id() != 0
                                    && dataMiningSquid.isIs_persisted()
                                    && squidIdMap.containsKey(dataMiningSquid.getDestination_squid_id())) {
                                dataMiningSquid.setDestination_squid_id(squidIdMap.get(dataMiningSquid.getDestination_squid_id()));
                            }
                            int newSquidId = dbSquidDao.insert2(dataMiningSquid);
                            squidIdMap.put(dataMiningSquid.getId(), newSquidId);
                            List<DSVariable> dsVariables = dataMiningSquid.getVariables();
                            if (dsVariables != null) {
                                for (DSVariable dsVariable : dsVariables) {
                                    dsVariable.setProject_id(projectId);
                                    dsVariable.setSquid_flow_id(newSquidFlowId);
                                    dsVariable.setSquid_id(newSquidId);
                                    dsVariable.setAdd_date(new Timestamp(new Date().getTime()).toString());
                                    dbSquidDao.insert2(dsVariable);
                                }
                            }
                        }
                    }
                } else if (historyData.getSquidType() == SquidTypeEnum.REPORT.value()) {
                    List<ReportSquid> reportSquids = JsonUtil.toGsonList(historyData.getData(), ReportSquid.class);
                    if (reportSquids != null && reportSquids.size() > 0) {
                        for (ReportSquid reportSquid : reportSquids) {
                            reportSquid.setSquidflow_id(newSquidFlowId);
                            int newSquidId = dbSquidDao.insert2(reportSquid);
                            squidIdMap.put(reportSquid.getId(), newSquidId);
                            List<DSVariable> dsVariables = reportSquid.getVariables();
                            if (dsVariables != null) {
                                for (DSVariable dsVariable : dsVariables) {
                                    dsVariable.setProject_id(projectId);
                                    dsVariable.setSquid_flow_id(newSquidFlowId);
                                    dsVariable.setSquid_id(newSquidId);
                                    dsVariable.setAdd_date(new Timestamp(new Date().getTime()).toString());
                                    dbSquidDao.insert2(dsVariable);
                                }
                            }
                        }
                    }
                } else if (historyData.getSquidType() == SquidTypeEnum.GISMAP.value()) {
                    List<GISMapSquid> gisMapSquids = JsonUtil.toGsonList(historyData.getData(), GISMapSquid.class);
                    if (gisMapSquids != null) {
                        for (GISMapSquid gisMapSquid : gisMapSquids) {
                            gisMapSquid.setSquidflow_id(newSquidFlowId);
                            int newSquidId = dbSquidDao.insert2(gisMapSquid);
                            squidIdMap.put(gisMapSquid.getId(), newSquidId);
                            List<DSVariable> dsVariables = gisMapSquid.getVariables();
                            if (dsVariables != null) {
                                for (DSVariable dsVariable : dsVariables) {
                                    dsVariable.setProject_id(projectId);
                                    dsVariable.setSquid_flow_id(newSquidFlowId);
                                    dsVariable.setSquid_id(newSquidId);
                                    dsVariable.setAdd_date(new Timestamp(new Date().getTime()).toString());
                                    dbSquidDao.insert2(dsVariable);
                                }
                            }
                        }
                    }
                } else if (historyData.getSquidType() == SquidTypeEnum.EXCEPTION.value()) {
                    List<ExceptionSquid> exceptionSquids = JsonUtil.toGsonList(historyData.getData(),
                            ExceptionSquid.class);
                    if (exceptionSquids != null) {
                        for (ExceptionSquid exceptionSquid : exceptionSquids) {
                            exceptionSquid.setSquidflow_id(newSquidFlowId);
                            if (exceptionSquid.getDestination_squid_id() != 0
                                    && exceptionSquid.isIs_persisted()
                                    && squidIdMap.containsKey(exceptionSquid.getDestination_squid_id())) {
                                exceptionSquid.setDestination_squid_id(squidIdMap.get(exceptionSquid.getDestination_squid_id()));
                            }
                            int newSquidId = dbSquidDao.insert2(exceptionSquid);
                            squidIdMap.put(exceptionSquid.getId(), newSquidId);
                            List<DSVariable> dsVariables = exceptionSquid.getVariables();
                            if (dsVariables != null) {
                                for (DSVariable dsVariable : dsVariables) {
                                    dsVariable.setProject_id(projectId);
                                    dsVariable.setSquid_flow_id(newSquidFlowId);
                                    dsVariable.setSquid_id(newSquidId);
                                    dsVariable.setAdd_date(new Timestamp(new Date().getTime()).toString());
                                    dbSquidDao.insert2(dsVariable);
                                }
                            }
                        }
                    }
                } else if (historyData.getSquidType() == SquidTypeEnum.ANNOTATION.value()) {
                    List<AnnotationSquid> annotationSquids = JsonUtil.toGsonList(historyData.getData(),
                            AnnotationSquid.class);
                    if (annotationSquids != null) {
                        for (AnnotationSquid annotationSquid : annotationSquids) {
                            annotationSquid.setSquidflow_id(newSquidFlowId);
                            int newSquidId = dbSquidDao.insert2(annotationSquid);
                            squidIdMap.put(annotationSquid.getId(), newSquidId);
                            List<DSVariable> dsVariables = annotationSquid.getVariables();
                            if (dsVariables != null) {
                                for (DSVariable dsVariable : dsVariables) {
                                    dsVariable.setProject_id(projectId);
                                    dsVariable.setSquid_flow_id(newSquidFlowId);
                                    dsVariable.setSquid_id(newSquidId);
                                    dsVariable.setAdd_date(new Timestamp(new Date().getTime()).toString());
                                    dbSquidDao.insert2(dsVariable);
                                }
                            }
                        }
                    }
                } else if (SquidTypeEnum.isDestSquid(historyData.getSquidType())) {
                    //生成squid
                    if (SquidTypeEnum.DESTES.value() == historyData.getSquidType()) {
                        List<DestESSquid> destESSquidList = JsonUtil.toGsonList(historyData.getData(),
                                DestESSquid.class);
                        if (destESSquidList != null) {
                            for (DestESSquid esSquid : destESSquidList) {
                                esSquid.setSquidflow_id(newSquidFlowId);
                                int newSquidId = adapter.insert2(esSquid);
                                squidIdMap.put(esSquid.getId(), newSquidId);
                                //获取变量集合
                                List<DSVariable> dsVariables = esSquid.getVariables();
                                if (dsVariables != null) {
                                    for (DSVariable dsVariable : dsVariables) {
                                        dsVariable.setProject_id(projectId);
                                        dsVariable.setSquid_flow_id(newSquidFlowId);
                                        dsVariable.setSquid_id(newSquidId);
                                        dsVariable.setAdd_date(new Timestamp(new Date().getTime()).toString());
                                        adapter.insert2(dsVariable);
                                    }
                                }
                            }
                        }
                    }
                    if (SquidTypeEnum.DEST_IMPALA.value() == historyData.getSquidType()) {
                        List<DestImpalaSquid> destImpalaSquidList = JsonUtil.toGsonList(historyData.getData(),
                                DestImpalaSquid.class);
                        if (destImpalaSquidList != null) {
                            for (DestImpalaSquid impalaSquid : destImpalaSquidList) {
                                impalaSquid.setSquidflow_id(newSquidFlowId);
                                int newSquidId = adapter.insert2(impalaSquid);
                                squidIdMap.put(impalaSquid.getId(), newSquidId);
                                List<DSVariable> dsVariables = impalaSquid.getVariables();
                                if (dsVariables != null) {
                                    for (DSVariable dsVariable : dsVariables) {
                                        dsVariable.setProject_id(projectId);
                                        dsVariable.setSquid_flow_id(newSquidFlowId);
                                        dsVariable.setSquid_id(newSquidId);
                                        dsVariable.setAdd_date(new Timestamp(new Date().getTime()).toString());
                                        adapter.insert2(dsVariable);
                                    }
                                }
                            }
                        }
                    }
                    if (SquidTypeEnum.DEST_HDFS.value() == historyData.getSquidType() || historyData.getSquidType() == SquidTypeEnum.DESTCLOUDFILE.value()) {
                        List<DestHDFSSquid> destHDFSSquidList = JsonUtil.toGsonList(historyData.getData(),
                                DestHDFSSquid.class);
                        if (destHDFSSquidList != null) {
                            for (DestHDFSSquid destHDFSSquid : destHDFSSquidList) {
                                destHDFSSquid.setSquidflow_id(newSquidFlowId);
                                int newSquidId = adapter.insert2(destHDFSSquid);
                                squidIdMap.put(destHDFSSquid.getId(), newSquidId);
                                List<DSVariable> dsVariables = destHDFSSquid.getVariables();
                                if (dsVariables != null) {
                                    for (DSVariable dsVariable : dsVariables) {
                                        dsVariable.setProject_id(projectId);
                                        dsVariable.setSquid_flow_id(newSquidFlowId);
                                        dsVariable.setSquid_id(newSquidId);
                                        dsVariable.setAdd_date(new Timestamp(new Date().getTime()).toString());
                                        adapter.insert2(dsVariable);
                                    }
                                }
                            }
                        }
                    }
                } else {
                    List<Squid> squidList = JsonUtil.toGsonList(historyData.getData(), Squid.class);
                    for (Squid squid : squidList) {
                        squid.setSquidflow_id(newSquidFlowId);
                        int newSquidId = dbSquidDao.insert2(squid);
                        squidIdMap.put(squid.getId(), newSquidId);
                        List<DSVariable> dsVariables = squid.getVariables();
                        if (dsVariables != null) {
                            for (DSVariable dsVariable : dsVariables) {
                                dsVariable.setProject_id(projectId);
                                dsVariable.setSquid_flow_id(newSquidFlowId);
                                dsVariable.setSquid_id(newSquidId);
                                dsVariable.setAdd_date(new Timestamp(new Date().getTime()).toString());
                                dbSquidDao.insert2(dsVariable);
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            try {
                adapter.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
            throw e;

        }
        sourceTableIdMap.clear();
        sourceTableIdMap = null;
        return squidIdMap;
    }

    /**
     * 正在创建SquidLink
     *
     * @param squidFlowHistory
     * @param adapter
     */
    public static void createSquidLink(int squidFlowId, Map<Integer, Integer> squidIdMap,
                                       SquidFlowHistory squidFlowHistory, IRelationalDataManager adapter/*,IRelationalDataManager historyAdapter*/)
            throws Exception {
        //IHistoryDataDao historyDataDao = new HistoryDataDaoImpl(historyAdapter);
        IHistoryDataDao historyDataDao = new HistoryDataDaoImpl(adapter);
        ISquidLinkDao squidLinkDao = new SquidLinkDaoImpl(adapter);
        List<HistoryData> historyDatas = historyDataDao.getVersionDataByProcess(squidFlowHistory, 2);
        if (historyDatas != null) {
            for (HistoryData historyData : historyDatas) {
                List<SquidLink> squidLinks = JsonUtil.toGsonList(historyData.getData(), SquidLink.class);
                if (squidLinks != null) {
                    for (SquidLink squidLink : squidLinks) {
                        squidLink.setSquid_flow_id(squidFlowId);
                        squidLink.setFrom_squid_id(squidIdMap.get(squidLink.getFrom_squid_id()));
                        squidLink.setTo_squid_id(squidIdMap.get(squidLink.getTo_squid_id()));
                        squidLinkDao.insert2(squidLink);
                    }
                }
            }

        }
    }

    /**
     * 正在生成join @param squidFlowHistory @param squidIdMap @param adapter @param
     * historyAdapter @throws
     */
    public static void createSquidJoin(SquidFlowHistory squidFlowHistory, Map<Integer, Integer> squidIdMap,
                                       IRelationalDataManager adapter/*,IRelationalDataManager historyAdapter*/) throws Exception {
        //IHistoryDataDao historyDataDao = new HistoryDataDaoImpl(historyAdapter);
        IHistoryDataDao historyDataDao = new HistoryDataDaoImpl(adapter);
        List<HistoryData> historyDatas = historyDataDao.getVersionDataByProcess(squidFlowHistory, 3);
        if (historyDatas != null) {
            for (HistoryData historyData : historyDatas) {
                List<SquidJoin> squidJoins = JsonUtil.toGsonList(historyData.getData(), SquidJoin.class);
                if (squidJoins != null) {
                    for (SquidJoin squidJoin : squidJoins) {
                        squidJoin.setJoined_squid_id(squidIdMap.get(squidJoin.getJoined_squid_id()));
                        squidJoin.setTarget_squid_id(squidIdMap.get(squidJoin.getTarget_squid_id()));
                        adapter.insert2(squidJoin);
                    }
                }
            }
        }
    }

    /**
     * 正在创建Column
     *
     * @param squidFlowHistory
     * @param squidIdMap
     * @param adapter
     * @throws Exception
     */
    public static void createColumn(SquidFlowHistory squidFlowHistory, Map<Integer, Integer> squidIdMap,
                                    IRelationalDataManager adapter, int newSquidFlowId) throws Exception {
        //IHistoryDataDao historyDataDao = new HistoryDataDaoImpl(historyAdapter);
        IHistoryDataDao historyDataDao = new HistoryDataDaoImpl(adapter);
        ISourceColumnDao sourceColumnDao = new SourceColumnDaoImpl(adapter);
        ISquidDao squidDao = new SquidDaoImpl(adapter);
        ISquidLinkDao squidLinkDao = new SquidLinkDaoImpl(adapter);
        //根据squidType进行排序(先执行dbsource，然后extract，然后stage，exception,dataming)
        List<Integer> squidTypeList = historyDataDao.getVersionSquidType(squidFlowHistory);
        List<Integer> isDestSquidType = new ArrayList<>();
        //将dest放到最后
        Iterator<Integer> iterator = squidTypeList.iterator();
        while (iterator.hasNext()) {
            int type = iterator.next();
            if (SquidTypeEnum.isDestSquid(type)) {
                isDestSquidType.add(type);
                iterator.remove();
            }
        }
        squidTypeList.addAll(isDestSquidType);
        //进行排序首先执行extract，然后执行stage，然后执行exception
        Map<Integer, Integer> columnMap = new HashMap<>();
        Map<Integer, Integer> sourceMap = new HashMap<>();

        //按照类型遍历(出现性能问题的关键，因为这里的column会越来越多)
        for (Integer squidType : squidTypeList) {
            //查询出column
            List<HistoryData> historyDataList = historyDataDao.getVersionDataByProcessAndSquidType(squidFlowHistory, 4, squidType);
            //正在生成column
            if (historyDataList != null && historyDataList.size() > 0) {
                for (HistoryData historyData : historyDataList) {
                    if (historyData.getSquidType() == SquidTypeEnum.DESTES.value()) {
                        List<EsColumn> columnList = JsonUtil.toGsonList(historyData.getData(), EsColumn.class);
                        if (columnList != null) {
                            for (EsColumn esColumn : columnList) {
                                // 查找出新的squidId
                                if (squidIdMap.containsKey(esColumn.getSquid_id())) {
                                    int newSquidId = squidIdMap.get(esColumn.getSquid_id());
                                    esColumn.setColumn_id(columnMap.get(esColumn.getColumn_id()));
                                    esColumn.setSquid_id(newSquidId);
                                    adapter.insert2(esColumn);
                                }
                            }
                        }
                    } else if (historyData.getSquidType() == SquidTypeEnum.DEST_HDFS.value()
                            || historyData.getSquidType() == SquidTypeEnum.DESTCLOUDFILE.value()) {
                        List<DestHDFSColumn> columnList = JsonUtil.toGsonList(historyData.getData(), DestHDFSColumn.class);
                        if (columnList != null) {
                            for (DestHDFSColumn hdfsColumn : columnList) {
                                // 查找出新的squidId
                                if (squidIdMap.containsKey(hdfsColumn.getSquid_id())) {
                                    int newSquidId = squidIdMap.get(hdfsColumn.getSquid_id());
                                    hdfsColumn.setColumn_id(columnMap.get(hdfsColumn.getColumn_id()));
                                    hdfsColumn.setSquid_id(newSquidId);
                                    adapter.insert2(hdfsColumn);
                                }
                            }
                        }
                    } else if (historyData.getSquidType() == SquidTypeEnum.DEST_IMPALA.value()) {
                        List<DestImpalaColumn> columnList = JsonUtil.toGsonList(historyData.getData(),
                                DestImpalaColumn.class);
                        if (columnList != null) {
                            for (DestImpalaColumn impalaColumn : columnList) {
                                // 查找出新的squidId
                                if (squidIdMap.containsKey(impalaColumn.getSquid_id())) {
                                    int newSquidId = squidIdMap.get(impalaColumn.getSquid_id());
                                    impalaColumn.setColumn_id(columnMap.get(impalaColumn.getColumn_id()));
                                    impalaColumn.setSquid_id(newSquidId);
                                    adapter.insert2(impalaColumn);
                                }
                            }
                        }
                    } else if (historyData.getSquidType() == SquidTypeEnum.DEST_HIVE.value()) {
                        List<DestHiveColumn> columnList = JsonUtil.toGsonList(historyData.getData(),
                                DestHiveColumn.class);
                        if (columnList != null) {
                            for (DestHiveColumn hiveColumn : columnList) {
                                // 查找出新的squidId
                                if (squidIdMap.containsKey(hiveColumn.getSquid_id())) {
                                    int newSquidId = squidIdMap.get(hiveColumn.getSquid_id());
                                    if (columnMap.containsKey(hiveColumn.getColumn_id())) {
                                        hiveColumn.setColumn_id(columnMap.get(hiveColumn.getColumn_id()));
                                    } else {
                                        hiveColumn.setColumn_id(0);
                                    }
                                    hiveColumn.setSquid_id(newSquidId);
                                    adapter.insert2(hiveColumn);
                                }
                            }
                        }
                    }else if (historyData.getSquidType() == SquidTypeEnum.DEST_CASSANDRA.value()) {
                        List<DestCassandraColumn> columnList = JsonUtil.toGsonList(historyData.getData(),
                                DestCassandraColumn.class);
                        if (columnList != null) {
                            for (DestCassandraColumn cassandraColumn : columnList) {
                                // 查找出新的squidId
                                if (squidIdMap.containsKey(cassandraColumn.getSquid_id())) {
                                    int newSquidId = squidIdMap.get(cassandraColumn.getSquid_id());
                                    if (columnMap.containsKey(cassandraColumn.getColumn_id())) {
                                        cassandraColumn.setColumn_id(columnMap.get(cassandraColumn.getColumn_id()));
                                    } else {
                                        cassandraColumn.setColumn_id(0);
                                    }
                                    cassandraColumn.setSquid_id(newSquidId);
                                    adapter.insert2(cassandraColumn);
                                }
                            }
                        }
                    } else {
                        List<Column> columnList = JsonUtil.toGsonList(historyData.getData(), Column.class);
                        Map<String, Object> groupMap = new HashMap<>();
                        if (columnList != null) {
                            for (Column column : columnList) {
                                // 正在生成column
                                if (squidIdMap.containsKey(column.getSquid_id())) {
                                    int newSquidId = squidIdMap.get(column.getSquid_id());
                                    column.setSquid_id(squidIdMap.get(column.getSquid_id()));
                                    int newColumnId = adapter.insert2(column);
                                    columnMap.put(column.getId(), newColumnId);
                                }
                            }

                        }
                    }

                }
            }
            columnMap.putAll(sourceMap);
        }

        //正在生成referenceColumn
        for (Integer squidType : squidTypeList) {
            //查询出column
            // 根据squidType查询出referenceColumn
            List<HistoryData> referenceHistoryDatas = historyDataDao
                    .getVersionDataByProcessAndSquidType(squidFlowHistory, 5, squidType);
            List<ReferenceColumnGroup> referenceGroups = new ArrayList<ReferenceColumnGroup>();
            // referenceGroup
            if (referenceHistoryDatas != null) {
                for (HistoryData referenceHistoryData : referenceHistoryDatas) {
                    List<ReferenceColumnGroup> referenceColumns = JsonUtil
                            .toGsonList(referenceHistoryData.getData(), ReferenceColumnGroup.class);
                    if (referenceColumns != null && referenceColumns.size() > 0) {
                        referenceGroups.addAll(referenceColumns);
                    }
                }
            }
            // 正在生成referenceGroup
            for (ReferenceColumnGroup referenceColumnGroup : referenceGroups) {
                if (squidIdMap.containsKey(referenceColumnGroup.getReference_squid_id())) {
                    referenceColumnGroup
                            .setReference_squid_id(squidIdMap.get(referenceColumnGroup.getReference_squid_id()));
                    referenceColumnGroup.setId(adapter.insert2(referenceColumnGroup));
                }
            }
            // 正在生成referenceColumn
            for (ReferenceColumnGroup referenceColumnGroup : referenceGroups) {
                //根据referenceColumnGroup的ref_squid查找出对应的sourceColumn
                int squidTypeId = squidType;
                List<SourceColumn> sourceColumnList = new ArrayList<>();
                Map<String, Integer> sourceColumnMap = new HashMap<>();

                //如果是extract，此时需要从sourceColumn里面查找数据
                DataSquid dataSquid = squidDao.getSquidForCond(referenceColumnGroup.getReference_squid_id(), DataSquid.class);
                if (dataSquid.getSource_table_id() > 0){
                    List<SourceColumn> sourceColumns = sourceColumnDao.getSourceColumnByTableId(dataSquid.getSource_table_id());
                    if (sourceColumns != null) {
                        sourceColumnList.addAll(sourceColumns);
                    }
                }

                List<ReferenceColumn> referenceColumns = referenceColumnGroup.getReferenceColumnList();
                if (referenceColumns != null && referenceColumns.size() > 0) {
                    for (int i = 0; i < referenceColumns.size(); i++) {
                        ReferenceColumn referenceColumn = referenceColumns.get(i);
                        if (SquidTypeEnum.isExtractBySquidType(squidTypeId)) {
                            for (SourceColumn sourceColumn : sourceColumnList) {
                                if (sourceColumn.getName().equals(referenceColumn.getName())) {
                                    sourceMap.put(referenceColumn.getColumn_id(), sourceColumn.getId());
                                    referenceColumn.setColumn_id(sourceColumn.getId());
                                    referenceColumn.setGroup_id(referenceColumnGroup.getId());
                                    referenceColumn
                                            .setHost_squid_id(squidIdMap.get(referenceColumn.getHost_squid_id()));
                                    referenceColumn.setReference_squid_id(
                                            squidIdMap.get(referenceColumn.getReference_squid_id()));
                                    adapter.insert2(referenceColumn);
                                }
                            }
                        } else {
                            if (columnMap.containsKey(referenceColumn.getColumn_id())) {
                                referenceColumn.setColumn_id(columnMap.get(referenceColumn.getColumn_id()));
                                referenceColumn.setGroup_id(referenceColumnGroup.getId());
                                referenceColumn
                                        .setHost_squid_id(squidIdMap.get(referenceColumn.getHost_squid_id()));
                                referenceColumn.setReference_squid_id(
                                        squidIdMap.get(referenceColumn.getReference_squid_id()));
                                adapter.insert2(referenceColumn);
                            }
                        }
                    }
                }
            }
            columnMap.putAll(sourceMap);
        }


        /**
         * 修改GroupTaggingSquid的column问题
         */
        //修改所有的GroupTaggingSquid的column
        //查询出所有的GroupTaggingSquid
        String sql = "select ds.* from ds_squid ds where ds.squid_type_id = " + SquidTypeEnum.GROUPTAGGING.value() + " and ds.squid_flow_id=" + newSquidFlowId;
        List<DataSquid> groupSquids = adapter.query2List(true, sql, null, DataSquid.class);
        if (groupSquids != null) {
            for (DataSquid dataSquid : groupSquids) {
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
                sql = "UPDATE ds_squid SET GROUP_COLUMN=?,SORT_COLUMN=?,TAGGING_COLUMN=? WHERE ID=?";
                List<Object> params = new ArrayList<>();
                params.add(0, dataSquid.getGroupColumnIds());
                params.add(1, dataSquid.getSortingColumnIds());
                params.add(2, dataSquid.getTaggingColumnIds());
                params.add(3, dataSquid.getId());
                adapter.execute(sql, params);
            }
        }

        /**
         * 修改extractSquid的checkColumn属性
         */
        String ids = JsonUtil.toJSONString(SquidTypeEnum.extractTypeList());
        ids = ids.replaceAll("\\[", "(").replaceAll("\\]", ")");
        sql = "select * from ds_squid where squid_type_id in " + ids + " and squid_flow_id=" + newSquidFlowId;
        List<DataSquid> extractSquids = adapter.query2List(true, sql, null, DataSquid.class);
        if (extractSquids != null) {
            for (DataSquid dataSquid : extractSquids) {
                if (columnMap.containsKey(dataSquid.getCheck_column_id())) {
                    int newColumnId = columnMap.get(dataSquid.getCheck_column_id());
                    sql = "update ds_squid set check_column_id = " + newColumnId + " where id=" + dataSquid.getId();
                    adapter.execute(sql);
                }
            }
        }


        sql = "select ds.* from ds_squid ds where ds.squid_type_id = " + SquidTypeEnum.PIVOTSQUID.value() + " and ds.squid_flow_id=" + newSquidFlowId;
        List<PivotSquid> pivotSquids = adapter.query2List(true, sql, null, PivotSquid.class);
        if (pivotSquids != null) {
            for (PivotSquid pivotSquid : pivotSquids) {
                Integer oldPivotColumnId=pivotSquid.getPivotColumnId();
                Integer oldValueColumnId=pivotSquid.getValueColumnId();
                String oldGroupColumnId=pivotSquid.getGroupByColumnIds();
                String[] oldGroups=null;
                String newGroups="";
                if(oldGroupColumnId!=null && !oldGroupColumnId.equals("") && oldGroupColumnId.length()>0){
                    if(oldGroupColumnId.contains(",")){
                        oldGroups=oldGroupColumnId.split(",");
                    }else{
                        newGroups=columnMap.get(Integer.parseInt(oldGroupColumnId))+"";;
                    }
                }
                if(oldGroups!=null && oldGroups.length>0){
                    for(int i=0;i<oldGroups.length;i++){
                        Integer columnId=columnMap.get(Integer.parseInt(oldGroups[i]));
                        if(columnId!=null){
                            newGroups+=columnId+",";
                        }
                    }
                }
                Integer newPivot=columnMap.get(oldPivotColumnId);
                Integer newValue=columnMap.get(oldValueColumnId);
                pivotSquid.setPivotColumnId(newPivot);
                pivotSquid.setValueColumnId(newValue);

                if(newGroups.length()>0) {
                    if (newGroups.contains(",")) {
                        pivotSquid.setGroupByColumnIds(newGroups.substring(0, newGroups.length()-1));
                    } else {
                        pivotSquid.setGroupByColumnIds(newGroups);
                    }
                }
                adapter.update2(pivotSquid);
            }
        }
        //正在生成dataMapColumn,parameterColumn
        for (Integer squidType : squidTypeList) {
            if (squidType == SquidTypeEnum.USERDEFINED.value()) {
                List<HistoryData> historyDatas = historyDataDao.getVersionDataByProcessAndSquidType(squidFlowHistory, 1, squidType);
                if (historyDatas != null) {
                    for (HistoryData data : historyDatas) {
                        List<UserDefinedSquid> squids = JsonUtil.toGsonList(data.getData(), UserDefinedSquid.class);
                        if (squids != null) {
                            for (UserDefinedSquid userDefinedSquid : squids) {
                                List<UserDefinedMappingColumn> dataMaps = userDefinedSquid.getUserDefinedMappingColumns();
                                List<UserDefinedParameterColumn> parameterMaps = userDefinedSquid.getUserDefinedParameterColumns();
                                if (dataMaps != null) {
                                    for (UserDefinedMappingColumn mappingColumn : dataMaps) {
                                        if (squidIdMap.containsKey(mappingColumn.getSquid_id())) {
                                            mappingColumn.setSquid_id(squidIdMap.get(mappingColumn.getSquid_id()));
                                            if (mappingColumn.getColumn_id() == 0) {
                                                adapter.insert2(mappingColumn);
                                            } else if (columnMap.containsKey(mappingColumn.getColumn_id())) {
                                                mappingColumn.setColumn_id(columnMap.get(mappingColumn.getColumn_id()));
                                                adapter.insert2(mappingColumn);
                                            }
                                        }
                                    }
                                }
                                if (parameterMaps != null) {
                                    for (UserDefinedParameterColumn paramMap : parameterMaps) {
                                        if (squidIdMap.containsKey(paramMap.getSquid_id())) {
                                            paramMap.setSquid_id(squidIdMap.get(paramMap.getSquid_id()));
                                            adapter.insert2(paramMap);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            if (squidType == SquidTypeEnum.STATISTICS.value()) {
                List<HistoryData> historyDatas = historyDataDao.getVersionDataByProcessAndSquidType(squidFlowHistory, 1, squidType);
                if (historyDatas != null) {
                    for (HistoryData data : historyDatas) {
                        List<StatisticsSquid> squids = JsonUtil.toGsonList(data.getData(), StatisticsSquid.class);
                        if (squids != null) {
                            for (StatisticsSquid statisticsSquid : squids) {
                                List<StatisticsDataMapColumn> dataMaps = statisticsSquid.getStatisticsDataMapColumns();
                                List<StatisticsParameterColumn> parameterMaps = statisticsSquid.getStatisticsParametersColumns();
                                if (dataMaps != null) {
                                    for (StatisticsDataMapColumn mappingColumn : dataMaps) {
                                        if (squidIdMap.containsKey(mappingColumn.getSquid_id())) {
                                            mappingColumn.setSquid_id(squidIdMap.get(mappingColumn.getSquid_id()));
                                            if (mappingColumn.getColumn_id() == 0) {
                                                adapter.insert2(mappingColumn);
                                            } else if (columnMap.containsKey(mappingColumn.getColumn_id())) {
                                                mappingColumn.setColumn_id(columnMap.get(mappingColumn.getColumn_id()));
                                                adapter.insert2(mappingColumn);
                                            }
                                        }
                                    }
                                }
                                if (parameterMaps != null) {
                                    for (StatisticsParameterColumn paramMap : parameterMaps) {
                                        if (squidIdMap.containsKey(paramMap.getSquid_id())) {
                                            paramMap.setSquid_id(squidIdMap.get(paramMap.getSquid_id()));
                                            adapter.insert2(paramMap);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        //正在生成transformation
        for (Integer squidType : squidTypeList) {
            // 根据squidType查询出Transformation(transformationInputs,transformation)
            List<HistoryData> transformationHistoryData = historyDataDao
                    .getVersionDataByProcessAndSquidType(squidFlowHistory, 6, squidType);
            // 根据squidType查询出TransformationLink
            List<HistoryData> transformationLinkHistoryData = historyDataDao
                    .getVersionDataByProcessAndSquidType(squidFlowHistory, 7, squidType);
            List<Transformation> transformationList = new ArrayList<Transformation>();
            List<TransformationLink> transformationLinkList = new ArrayList<TransformationLink>();
            // Transformation
            if (transformationHistoryData != null) {
                for (HistoryData transformationData : transformationHistoryData) {
                    List<Transformation> transformations = JsonUtil.toGsonList(transformationData.getData(),
                            Transformation.class);
                    if (transformations != null) {
                        transformationList.addAll(transformations);
                    }
                }
            }
            // transformationLink
            if (transformationLinkHistoryData != null) {
                for (HistoryData transformationLink : transformationLinkHistoryData) {
                    List<TransformationLink> transformationLinks = JsonUtil
                            .toGsonList(transformationLink.getData(), TransformationLink.class);
                    if (transformationLinks != null) {
                        transformationLinkList.addAll(transformationLinks);
                    }
                }
            }
            //正在生成transformations
            Map<Integer, Integer> transMap = new HashMap<Integer, Integer>();
            List<TransformationInputs> transInputList = new ArrayList<>();
            for (int i = 0; i < transformationList.size(); i++) {
                Transformation transformation = transformationList.get(i);
                int newTransformationId = 0;
                // 生成Transformation
                if (transformation.getColumn_id() != 0
                        && transformation.getTranstype() == 0
                        && columnMap.containsKey(transformation.getColumn_id())) {
                    transformation.setColumn_id(columnMap.get(transformation.getColumn_id()));
                    transformation.setSquid_id(squidIdMap.get(transformation.getSquid_id()));
                    if (transformation.getModel_squid_id() != 0
                            && squidIdMap.containsKey(transformation.getModel_squid_id())) {
                        transformation.setModel_squid_id(squidIdMap.get(transformation.getModel_squid_id()));
                    }
                    if (transformation.getDictionary_squid_id() != 0
                            && squidIdMap.containsKey(transformation.getDictionary_squid_id())) {
                        transformation.setDictionary_squid_id(
                                squidIdMap.get(transformation.getDictionary_squid_id()));
                    }
                    newTransformationId = adapter.insert2(transformation);
                    transMap.put(transformation.getId(), newTransformationId);
                }
                if (transformation.getColumn_id() == 0
                        && transformation.getTranstype() != 0) {
                    if (transformation.getModel_squid_id() != 0
                            && squidIdMap.containsKey(transformation.getModel_squid_id())) {
                        transformation.setModel_squid_id(squidIdMap.get(transformation.getModel_squid_id()));
                    }
                    if (transformation.getDictionary_squid_id() != 0
                            && squidIdMap.containsKey(transformation.getDictionary_squid_id())) {
                        transformation.setDictionary_squid_id(
                                squidIdMap.get(transformation.getDictionary_squid_id()));
                    }
                    transformation.setSquid_id(squidIdMap.get(transformation.getSquid_id()));
                    newTransformationId = adapter.insert2(transformation);
                    transMap.put(transformation.getId(), newTransformationId);
                }
                // 生成TransformationInputs
                List<TransformationInputs> transformationInputList = transformation.getInputs();
                if (transformationInputList != null && transformationInputList.size() > 0) {
                    transInputList.addAll(transformationInputList);
                }
            }


            //正在生成TransformationInput
            for (TransformationInputs inputs : transInputList) {
                if (transMap.containsKey(inputs.getTransformationId())) {
                    inputs.setTransformationId(transMap.get(inputs.getTransformationId()));
                }
                if (inputs.getSource_transform_id() != 0 && transMap.containsKey(inputs.getSource_transform_id())) {
                    inputs.setSource_transform_id(transMap.get(inputs.getSource_transform_id()));
                }
                int newId = adapter.insert2(inputs);
            }


            //正在生成TransformationLink
            for (int i = 0; i < transformationLinkList.size(); i++) {
                TransformationLink transformationLink = transformationLinkList.get(i);
                if (transMap.containsKey(transformationLink.getFrom_transformation_id())
                        && transMap.containsKey(transformationLink.getTo_transformation_id())) {
                    transformationLink.setFrom_transformation_id(transMap.get(transformationLink.getFrom_transformation_id()));
                    transformationLink.setTo_transformation_id(transMap.get(transformationLink.getTo_transformation_id()));
                    adapter.insert2(transformationLink);
                }
            }
            transInputList.clear();
            transformationList.clear();
            transInputList = null;
            transformationList = null;
            transMap.clear();
            transMap = null;

        }
        columnMap.clear();
        columnMap = null;
        sourceMap.clear();
        sourceMap = null;
    }

    /**
     * 根据souce_table更新抽取squid的source_table_ID
     *
     * @param squidIdMap
     * @param sourceTable
     * @param newSourceTableId
     * @param adapter
     * @throws Exception
     */
    public static void updateSourceTable(Map<Integer, Integer> squidIdMap, SourceTable sourceTable,
                                         int newSourceTableId, IRelationalDataManager adapter) throws Exception {
        ISquidDao squidDao = new SquidDaoImpl(adapter);
        for (Integer idMap : squidIdMap.values()) {
            // 查询出squid的类型,根据类型获得对应的class,根据class判断是否需要更新source_table
            int squidTypeId = squidDao.getSquidTypeById(idMap);
            if (SquidTypeEnum.isExtractBySquidType(squidTypeId)) {
                MultitableMapping tm = (MultitableMapping) SquidTypeEnum.classOfValue(squidTypeId)
                        .getAnnotation(MultitableMapping.class);
                if (tm != null) {
                    String[] names = tm.name();
                    for (String tableName : names) {
                        if (tableName.equals("DS_SQUID")) {
                            continue;
                        }
                        String sql = "select * from " + tableName + " where id=" + idMap;
                        String updateSql = "update " + tableName + " set source_table_id =" + newSourceTableId;
                        StringBuffer buf = new StringBuffer(updateSql);
                        Map<String, Object> params = new HashMap<>();
                        params.put("id", idMap);

                        DataSquid dataSquid = adapter.query2Object(true, params, DataSquid.class);
                        //DataSquid dataSquid = adapter.query2Object(true, sql, null, DataSquid.class);
                        if (dataSquid != null) {
                            if (dataSquid.getSource_table_id() == sourceTable.getId()) {
                                if (dataSquid.getDestination_squid_id() != 0
                                        && squidIdMap.containsKey(dataSquid.getDestination_squid_id())) {
                                    buf.append(",destination_squid_id=" + squidIdMap.get(dataSquid.getDestination_squid_id()));
                                }
                                if (dataSquid.getRef_squid_id() != 0 && squidIdMap.containsKey(dataSquid.getRef_squid_id())) {
                                    buf.append(",ref_squid_id=" + squidIdMap.get(dataSquid.getRef_squid_id()));
                                }
                                buf.append(" where id=" + idMap);
                                adapter.execute(buf.toString());
                            }
                        }

                    }
                }

            } else {
                continue;
            }
        }
    }

}
