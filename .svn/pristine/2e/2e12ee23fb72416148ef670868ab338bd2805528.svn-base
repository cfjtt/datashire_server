package com.eurlanda.datashire.squidVersion;

import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.dao.IColumnDao;
import com.eurlanda.datashire.dao.IReferenceColumnDao;
import com.eurlanda.datashire.dao.ISourceTableDao;
import com.eurlanda.datashire.dao.ISquidDao;
import com.eurlanda.datashire.dao.ITransformationDao;
import com.eurlanda.datashire.dao.ITransformationInputsDao;
import com.eurlanda.datashire.dao.ITransformationLinkDao;
import com.eurlanda.datashire.dao.IUrlDao;
import com.eurlanda.datashire.dao.IVariableDao;
import com.eurlanda.datashire.dao.dest.IEsColumnDao;
import com.eurlanda.datashire.dao.dest.IHdfsColumnDao;
import com.eurlanda.datashire.dao.dest.ImpalaColumnDao;
import com.eurlanda.datashire.dao.dest.impl.EsColumnDaoImpl;
import com.eurlanda.datashire.dao.dest.impl.HdfsColumnDaoImpl;
import com.eurlanda.datashire.dao.dest.impl.ImpalaColumnDaoImpl;
import com.eurlanda.datashire.dao.impl.ColumnDaoImpl;
import com.eurlanda.datashire.dao.impl.ReferenceColumnDaoImpl;
import com.eurlanda.datashire.dao.impl.SourceTableDaoImpl;
import com.eurlanda.datashire.dao.impl.SquidDaoImpl;
import com.eurlanda.datashire.dao.impl.TransformationDaoImpl;
import com.eurlanda.datashire.dao.impl.TransformationInputsDaoImpl;
import com.eurlanda.datashire.dao.impl.TransformationLinkDaoImpl;
import com.eurlanda.datashire.dao.impl.UrlDaoImpl;
import com.eurlanda.datashire.dao.impl.VariableDaoImpl;
import com.eurlanda.datashire.entity.dest.DestCassandraColumn;
import com.eurlanda.datashire.entity.dest.DestHiveColumn;
import com.eurlanda.datashire.entity.*;
import com.eurlanda.datashire.entity.dest.DestHDFSColumn;
import com.eurlanda.datashire.entity.dest.DestImpalaColumn;
import com.eurlanda.datashire.entity.dest.EsColumn;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.server.model.SamplingSquid;
import com.eurlanda.datashire.sprint7.service.squidflow.JobScheduleProcess;
import com.eurlanda.datashire.sprint7.service.squidflow.subservice.SquidFlowServicesub;
import com.eurlanda.datashire.entity.StatisticsDataMapColumn;
import com.eurlanda.datashire.entity.StatisticsParameterColumn;
import com.eurlanda.datashire.entity.StatisticsSquid;
import com.eurlanda.datashire.entity.UserDefinedMappingColumn;
import com.eurlanda.datashire.entity.UserDefinedParameterColumn;
import com.eurlanda.datashire.entity.UserDefinedSquid;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 生成SquidFlow版本操作类
 *
 * @author Admin
 */
public class CreateSquidVersionService {
    private IRelationalDataManager adapter;

    public CreateSquidVersionService(IRelationalDataManager adapter) {
        this.adapter = adapter;
    }

    /**
     * 生成Squid信息(包括column，reference，Transformation)
     */
    // 生成版本信息squid
    public void createSquidVersionProcess(List<Map<String, Object>> squidTypeMaps, int repositoryId, int squidFlowId,
                                          int squidFlowType, String commit_time, int squidHistoryId, ReturnValue out) throws Exception {
        ISquidDao squidDao = new SquidDaoImpl(adapter);
        IVariableDao variableDao = new VariableDaoImpl(adapter);
        IReferenceColumnDao referenceColumnDao = new ReferenceColumnDaoImpl(adapter);
        try {
            //如果是空的squidflow，往数据库里面插入一个新的记录
            if (squidTypeMaps != null && squidTypeMaps.size() > 0) {
                for (Map<String, Object> squidTypeMap : squidTypeMaps) {
                    List returnSquidLists = new ArrayList();
                    List<Column> columnList = new ArrayList<>();
                    List<ReferenceColumnGroup> referenceColumnGroupList = new ArrayList<>();
                    List<Transformation> transformationList = new ArrayList<>();
                    List<TransformationLink> transformationLinkList = new ArrayList<>();
                    int squidTypeId = Integer.valueOf(squidTypeMap.get("SQUID_TYPE_ID") + "");
                    Class<?> className = SquidTypeEnum.classOfValue(squidTypeId);
                    List<?> squidLists = new ArrayList<>();
                    if (squidTypeId == SquidTypeEnum.EXCEPTION.value()) {
                        SquidFlowServicesub squidFlowServicesub = new SquidFlowServicesub(adapter);
                        squidLists = squidFlowServicesub.getExceptionSquidListLimit(adapter, squidFlowId, "", out);
                    } else {
                        squidLists = squidDao.getSquidListForParams(squidTypeId, squidFlowId, "", className);
                    }
                    for (int j = 0; j < squidLists.size(); j++) {
                        Squid squid = (Squid) squidLists.get(j);

                        // 查询squid下面的变量
                        List<DSVariable> squidVariable = variableDao.getDSVariableByScope(2, squid.getId());
                        if (squidVariable != null && squidVariable.size() > 0) {
                            squid.setVariables(squidVariable);
                        }

                        returnSquidLists = createSquidVersion(squidTypeId, repositoryId, squidFlowId, squidFlowType, squidHistoryId,
                                commit_time, returnSquidLists, squid, out);
                        // column
                        columnList = createColumnVersion(squidTypeId, repositoryId, squidFlowId, squidFlowType, squidHistoryId,
                                commit_time, columnList, squid);
                        // ReferenceColumn
                        referenceColumnGroupList = createReferenceColumn(squidTypeId, repositoryId, squidFlowId,
                                commit_time, referenceColumnGroupList, squid);
                        // Transformation
                        transformationList = createTransformation(squidTypeId, repositoryId, squidFlowId, squidFlowType, squidHistoryId,
                                commit_time, transformationList, squid);
                        // transformationLink
                        transformationLinkList = createTransformationLink(squidTypeId, repositoryId, squidFlowId,
                                squidFlowType, squidHistoryId, commit_time, transformationLinkList, squid);

                    }
                    if (returnSquidLists.size() > 0) {
                        HistoryData historyData = new HistoryData(repositoryId, squidHistoryId, squidFlowId, commit_time,
                                JsonUtil.toJSONString(returnSquidLists), squidTypeId, 1, squidFlowType);
                        adapter.insert2(historyData);
                        returnSquidLists.clear();
                    }
                    if (columnList.size() > 0) {
                        HistoryData historyData = new HistoryData(repositoryId, squidHistoryId, squidFlowId, commit_time,
                                JsonUtil.toJSONString(columnList), squidTypeId, 4, squidFlowType);
                        adapter.insert2(historyData);
                        columnList.clear();
                    }
                    if (referenceColumnGroupList.size() > 0) {
                        for (ReferenceColumnGroup referenceColumnGroup : referenceColumnGroupList) {
                            referenceColumnGroup.setReferenceColumnList(
                                    referenceColumnDao.getRefColumnListByGroupId(referenceColumnGroup.getId()));
                        }
                        HistoryData historyData = new HistoryData(repositoryId, squidHistoryId, squidFlowId, commit_time,
                                JsonUtil.toJSONString(referenceColumnGroupList), squidTypeId, 5, squidFlowType);
                        adapter.insert2(historyData);
                        referenceColumnGroupList.clear();
                    }
                    if (transformationList.size() > 0) {
                        HistoryData historyData = new HistoryData(repositoryId, squidHistoryId, squidFlowId, commit_time,
                                JsonUtil.toJSONString(transformationList), squidTypeId, 6, squidFlowType);
                        adapter.insert2(historyData);
                        transformationList.clear();
                    }
                    if (transformationLinkList.size() > 0) {
                        HistoryData historyData = new HistoryData(repositoryId, squidHistoryId, squidFlowId, commit_time,
                                JsonUtil.toJSONString(transformationLinkList), squidTypeId, 7, squidFlowType);
                        adapter.insert2(historyData);
                        transformationLinkList.clear();
                    }
                    // 置为null，让gc尽快回收
                    returnSquidLists.clear();
                    returnSquidLists = null;
                    columnList.clear();
                    columnList = null;
                    referenceColumnGroupList.clear();
                    referenceColumnGroupList = null;
                    transformationList.clear();
                    transformationList = null;
                    transformationLinkList.clear();
                    transformationLinkList = null;

                }
            } else {
                HistoryData historyData = new HistoryData(repositoryId, squidHistoryId, squidFlowId, commit_time, "[]", 1, 1, squidFlowType);
                adapter.insert2(historyData);
            }

        } catch (Exception e) {
            e.printStackTrace();
            try {
                adapter.rollback();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
            out.setMessageCode(MessageCode.ERR);
            throw e;
        }
    }

    /**
     * 生成squid版本信息
     *
     * @param squidTypeId
     * @param returnSquidLists
     * @param squid
     * @return
     */
    public List createSquidVersion(int squidTypeId, int repositoryId, int squidFlowId, int squidFlowType, int squidHistoryId,
                                   String commit_time, List returnSquidLists, Squid squid, ReturnValue out) throws Exception {
        ISourceTableDao sourceTableDao = new SourceTableDaoImpl(adapter);
        Map<Integer, JobSchedule> scheduleMap = new JobScheduleProcess().getJobSchedules(repositoryId, squidFlowId,
                null, true, out);

        if (SquidTypeEnum.isDBSourceBySquidType(squidTypeId)) {
            if (returnSquidLists.size() == 20) {
                HistoryData historyData = new HistoryData(repositoryId, squidHistoryId, squidFlowId, commit_time,
                        JsonUtil.toJSONString(returnSquidLists), squidTypeId, 1, squidFlowType);
                adapter.insert2(historyData);
                returnSquidLists.clear();
            }
        } else if (returnSquidLists.size() == 40) {
            HistoryData historyData = new HistoryData(repositoryId, squidHistoryId, squidFlowId, commit_time,
                    JsonUtil.toJSONString(returnSquidLists), squidTypeId, 1, squidFlowType);
            adapter.insert2(historyData);
            returnSquidLists.clear();
        }
        // 当squid为数据源时，查询出sourceTable
        if (SquidTypeEnum.isDBSourceBySquidType(squidTypeId)) {
            // 获取sourceTable
            List<SourceTable> sourceTableList = sourceTableDao.getSourceTableBySquidId(squid.getId());
            if (squidTypeId == SquidTypeEnum.DBSOURCE.value()) {
                DbSquid dbSquid = (DbSquid) squid;
                dbSquid.setType(DSObjectType.DBSOURCE.value());
                dbSquid.setSourceTableList(sourceTableList);
                returnSquidLists.add(dbSquid);
            } else if (squidTypeId == SquidTypeEnum.HIVE.value()) {
                SystemHiveConnectionSquid dbSquid = (SystemHiveConnectionSquid) squid;
                dbSquid.setType(DSObjectType.HIVE.value());
                dbSquid.setSourceTableList(sourceTableList);
                returnSquidLists.add(dbSquid);
            } else if (squidTypeId == SquidTypeEnum.CASSANDRA.value()) {
                CassandraConnectionSquid dbSquid = (CassandraConnectionSquid) squid;
                dbSquid.setType(DSObjectType.CASSANDRA.value());
                dbSquid.setSourceTableList(sourceTableList);
                returnSquidLists.add(dbSquid);
            } else if (squidTypeId == SquidTypeEnum.CLOUDDB.value()) {
                DbSquid dbSquid = (DbSquid) squid;
                dbSquid.setType(DSObjectType.CLOUDDB.value());
                dbSquid.setSourceTableList(sourceTableList);
                returnSquidLists.add(dbSquid);
            } else if(squidTypeId==SquidTypeEnum.TRAININGDBSQUID.value()){
                DbSquid dbSquid = (DbSquid) squid;
                dbSquid.setType(DSObjectType.TRAININGDBSQUID.value());
                dbSquid.setSourceTableList(sourceTableList);
                returnSquidLists.add(dbSquid);
            } else if (squidTypeId == SquidTypeEnum.HTTP.value()) {
                HttpSquid httpSquid = (HttpSquid) squid;
                httpSquid.setSourceTableList(sourceTableList);
                returnSquidLists.add(httpSquid);
            } else if (squidTypeId == SquidTypeEnum.WEBSERVICE.value()) {
                WebserviceSquid webserviceSquid = (WebserviceSquid) squid;
                webserviceSquid.setSourceTableList(sourceTableList);
                returnSquidLists.add(webserviceSquid);
            } else if (squidTypeId == SquidTypeEnum.DBDESTINATION.value()) {
                DBDestinationSquid dbDestinationSquid = (DBDestinationSquid) squid;
                dbDestinationSquid.setSourceTableList(sourceTableList);
                returnSquidLists.add(dbDestinationSquid);
            } else if (squidTypeId == SquidTypeEnum.FILEFOLDER.value()) {
                FileFolderSquid fileFolderSquid = (FileFolderSquid) squid;
                fileFolderSquid.setSourceTableList(sourceTableList);
                returnSquidLists.add(fileFolderSquid);
            } else if (squidTypeId == SquidTypeEnum.FTP.value()) {
                FtpSquid ftpSquid = (FtpSquid) squid;
                ftpSquid.setSourceTableList(sourceTableList);
                returnSquidLists.add(ftpSquid);
            } else if (squidTypeId == SquidTypeEnum.HDFS.value()
                    || SquidTypeEnum.SOURCECLOUDFILE.value() == squidTypeId || SquidTypeEnum.TRAINNINGFILESQUID.value()==squidTypeId) {
                HdfsSquid hdfsSquid = (HdfsSquid) squid;
                hdfsSquid.setSourceTableList(sourceTableList);
                returnSquidLists.add(hdfsSquid);
            } else if (squidTypeId == SquidTypeEnum.WEB.value()) {
                WebSquid webSquid = (WebSquid) squid;
                if (null != scheduleMap.get(webSquid.getId())) {
                    webSquid.setJobSchedule(scheduleMap.get(webSquid.getId()));
                }
                webSquid.setSourceTableList(sourceTableList);
                IUrlDao urlDao = new UrlDaoImpl(adapter);
                List<Url> webUrls = urlDao.getUrlsBySquidId(webSquid.getId());
                webSquid.setUrlList(webUrls);
                returnSquidLists.add(webSquid);
            } else if (squidTypeId == SquidTypeEnum.WEIBO.value()) {
                WeiboSquid weiboSquid = (WeiboSquid) squid;
                if (null != scheduleMap.get(weiboSquid.getId())) {
                    weiboSquid.setJobSchedule(scheduleMap.get(weiboSquid.getId()));
                }
                weiboSquid.setSourceTableList(sourceTableList);
                returnSquidLists.add(weiboSquid);
            } else if (squidTypeId == SquidTypeEnum.MONGODB.value()) {
                NOSQLConnectionSquid nosqlConnectionSquid = (NOSQLConnectionSquid) squid;
                nosqlConnectionSquid.setType(DSObjectType.MONGODB.value());
                nosqlConnectionSquid.setSourceTableList(sourceTableList);
                returnSquidLists.add(nosqlConnectionSquid);
            } else if (squidTypeId == SquidTypeEnum.KAFKA.value()) {
                KafkaSquid kafkaSquid = (KafkaSquid) squid;
                kafkaSquid.setSourceTableList(sourceTableList);
                returnSquidLists.add(kafkaSquid);
            } else if (squidTypeId == SquidTypeEnum.HBASE.value()) {
                HBaseConnectionSquid hBaseConnectionSquid = (HBaseConnectionSquid) squid;
                hBaseConnectionSquid.setSourceTableList(sourceTableList);
                returnSquidLists.add(hBaseConnectionSquid);
            }
        } else if (squidTypeId == SquidTypeEnum.EXTRACT.value() || squidTypeId == SquidTypeEnum.XML_EXTRACT.value()
                || squidTypeId == SquidTypeEnum.WEBLOGEXTRACT.value() || squidTypeId == SquidTypeEnum.WEBEXTRACT.value()
                || squidTypeId == SquidTypeEnum.WEIBOEXTRACT.value()) {
            DataSquid dataSquid = (DataSquid) squid;
            returnSquidLists.add(dataSquid);
        } else if (squidTypeId == SquidTypeEnum.HIVEEXTRACT.value()) {
            SystemHiveExtractSquid dataSquid = (SystemHiveExtractSquid) squid;
            returnSquidLists.add(dataSquid);
        } else if (squidTypeId == SquidTypeEnum.CASSANDRA_EXTRACT.value()) {
            CassandraExtractSquid dataSquid = (CassandraExtractSquid) squid;
            returnSquidLists.add(dataSquid);
        } else if (squidTypeId == SquidTypeEnum.HBASEEXTRACT.value()) {
            HBaseExtractSquid hBaseExtractSquid = (HBaseExtractSquid) squid;
            returnSquidLists.add(hBaseExtractSquid);
        } else if (squidTypeId == SquidTypeEnum.DOC_EXTRACT.value()) {
            DocExtractSquid docExtractSquid = (DocExtractSquid) squid;
            returnSquidLists.add(docExtractSquid);
        } else if (squidTypeId == SquidTypeEnum.MONGODBEXTRACT.value()) {
            DataSquid mongodbExtractSquid = (DataSquid) squid;
            mongodbExtractSquid.setType(DSObjectType.MONGODBEXTRACT.value());
            returnSquidLists.add(mongodbExtractSquid);
        } else if (squidTypeId == SquidTypeEnum.WEBSERVICEEXTRACT.value()) {
            WebserviceExtractSquid webserviceExtractSquid = (WebserviceExtractSquid) squid;
            returnSquidLists.add(webserviceExtractSquid);
        } else if (squidTypeId == SquidTypeEnum.HTTPEXTRACT.value()) {
            HttpExtractSquid httpExtractSquid = (HttpExtractSquid) squid;
            returnSquidLists.add(httpExtractSquid);
        } else if (squidTypeId == SquidTypeEnum.STAGE.value()) {
            StageSquid stageSquid = (StageSquid) squid;
            returnSquidLists.add(stageSquid);
        } else if (squidTypeId == SquidTypeEnum.GROUPTAGGING.value()) {
            GroupTaggingSquid tagSquid = (GroupTaggingSquid) squid;
            returnSquidLists.add(tagSquid);
        } else if (squidTypeId == SquidTypeEnum.SAMPLINGSQUID.value()) {
            SamplingSquid samplingSquidSquid = (SamplingSquid) squid;
            returnSquidLists.add(samplingSquidSquid);
        }else if (squidTypeId == SquidTypeEnum.STREAM_STAGE.value()) {
            StreamSquid streamSquid = (StreamSquid) squid;
            returnSquidLists.add(streamSquid);
        } else if (SquidTypeEnum.classOfValue(squidTypeId).getSimpleName().equals("DataMiningSquid")) {
            DataMiningSquid dataMiningSquid = (DataMiningSquid) squid;
            returnSquidLists.add(dataMiningSquid);
        } else if (squidTypeId == SquidTypeEnum.REPORT.value()) {
            ReportSquid reportSquid = (ReportSquid) squid;
            if (reportSquid.getFolder_id() > 0) {
                String path = SquidFlowServicesub.getReportNodeName(adapter, reportSquid.getFolder_id());
                reportSquid.setPublishing_path(path);
            }
            returnSquidLists.add(reportSquid);
        } else if (squidTypeId == SquidTypeEnum.GISMAP.value()) {
            GISMapSquid gisMapSquid = (GISMapSquid) squid;
            if (gisMapSquid.getFolder_id() > 0) {
                String path = SquidFlowServicesub.getReportNodeName(adapter, gisMapSquid.getFolder_id());
                gisMapSquid.setPublishing_path(path);
            }
            returnSquidLists.add(gisMapSquid);

        } else if (squidTypeId == SquidTypeEnum.EXCEPTION.value()) {
            ExceptionSquid exceptionSquid = (ExceptionSquid) squid;
            returnSquidLists.add(exceptionSquid);
        } else if (squidTypeId == SquidTypeEnum.USERDEFINED.value()) {
            UserDefinedSquid definedSquid = (UserDefinedSquid) squid;
            //查询出当前squid的下面的dataMapColumn和parameterColumn
            Map<String, Object> params = new HashMap<>();
            params.put("squid_id", squid.getId());
            List<UserDefinedMappingColumn> dataMapList = adapter.query2List2(true, params, UserDefinedMappingColumn.class);
            definedSquid.setUserDefinedMappingColumns(dataMapList);
            List<UserDefinedParameterColumn> parameterColumns = adapter.query2List2(true, params, UserDefinedParameterColumn.class);
            definedSquid.setUserDefinedParameterColumns(parameterColumns);
            returnSquidLists.add(definedSquid);
        } else if (squidTypeId == SquidTypeEnum.STATISTICS.value()) {
            StatisticsSquid statisSquid = (StatisticsSquid) squid;
            //查询出当前squid的下面的dataMapColumn和parameterColumn
            Map<String, Object> params = new HashMap<>();
            params.put("squid_id", squid.getId());
            List<StatisticsDataMapColumn> dataMapList = adapter.query2List2(true, params, StatisticsDataMapColumn.class);
            statisSquid.setStatisticsDataMapColumns(dataMapList);
            List<StatisticsParameterColumn> parameterColumns = adapter.query2List2(true, params, StatisticsParameterColumn.class);
            statisSquid.setStatisticsParametersColumns(parameterColumns);
            returnSquidLists.add(statisSquid);
        } else if (squidTypeId == SquidTypeEnum.ANNOTATION.value()) {
            AnnotationSquid annotationSquid = (AnnotationSquid) squid;
            returnSquidLists.add(squid);
        } else {
            returnSquidLists.add(squid);
        }
        return returnSquidLists;
    }

    /**
     * 生成column(以一个squid为单位)
     *
     * @param squid
     * @param columnList
     * @return
     */
    public List<Column> createColumnVersion(int squidTypeId, int repositoryId, int squidFlowId, int squidFlowType, int squidHistoryId,
                                            String commit_time, List<Column> columnList, Squid squid) throws Exception {
        IColumnDao columnDao = new ColumnDaoImpl(adapter);
        if (columnList.size() == 50) {
            HistoryData historyData = new HistoryData(repositoryId, squidHistoryId, squidFlowId, commit_time,
                    JsonUtil.toJSONString(columnList), squidTypeId, 4, squidFlowType);
            adapter.insert2(historyData);
            columnList.clear();
        }
        if (squidTypeId == SquidTypeEnum.DEST_IMPALA.value()) {
            ImpalaColumnDao impalaColumnDao = new ImpalaColumnDaoImpl(adapter);
            List<DestImpalaColumn> destImpalaColumnList = impalaColumnDao.getImpalaColumnBySquidId(true, squid.getId());
            if (destImpalaColumnList != null && destImpalaColumnList.size() > 0) {
                HistoryData historyData = new HistoryData(repositoryId, squidHistoryId, squidFlowId, commit_time,
                        JsonUtil.toJSONString(destImpalaColumnList), squidTypeId, 4, squidFlowType);
                adapter.insert2(historyData);
                destImpalaColumnList.clear();
                destImpalaColumnList = null;
            }

        } else if (squidTypeId == SquidTypeEnum.DEST_HDFS.value() || squid.getSquid_type() == SquidTypeEnum.DESTCLOUDFILE.value()) {
            IHdfsColumnDao hdfsColumnDao = new HdfsColumnDaoImpl(adapter);
            List<DestHDFSColumn> destHDFSColumnList = hdfsColumnDao.getHdfsColumnBySquidId(true, squid.getId());
            if (destHDFSColumnList != null && destHDFSColumnList.size() > 0) {
                HistoryData historyData = new HistoryData(repositoryId, squidHistoryId, squidFlowId, commit_time,
                        JsonUtil.toJSONString(destHDFSColumnList), squidTypeId, 4, squidFlowType);
                adapter.insert2(historyData);
                destHDFSColumnList.clear();
                destHDFSColumnList = null;
            }

        } else if (squidTypeId == SquidTypeEnum.DESTES.value()) {
            IEsColumnDao esColumnDao = new EsColumnDaoImpl(adapter);
            List<EsColumn> esColumnList = esColumnDao.getEsColumnsBySquidId(true, squid.getId());
            if (esColumnList != null && esColumnList.size() > 0) {
                HistoryData historyData = new HistoryData(repositoryId, squidHistoryId, squidFlowId, commit_time,
                        JsonUtil.toJSONString(esColumnList), squidTypeId, 4, squidFlowType);
                adapter.insert2(historyData);
                esColumnList.clear();
                esColumnList = null;
            }

        } else if (squidTypeId == SquidTypeEnum.DEST_HIVE.value()) {
            Map<String, Object> paramMap = new HashMap<>();
            paramMap.put("squid_id", squid.getId());
            List<DestHiveColumn> esColumnList = adapter.query2List2(true, paramMap, DestHiveColumn.class);
            if (esColumnList != null && esColumnList.size() > 0) {
                HistoryData historyData = new HistoryData(repositoryId, squidHistoryId, squidFlowId, commit_time,
                        JsonUtil.toJSONString(esColumnList), squidTypeId, 4, squidFlowType);
                adapter.insert2(historyData);
                esColumnList.clear();
                esColumnList = null;
            }
        }else if (squidTypeId == SquidTypeEnum.DEST_CASSANDRA.value()) {
            Map<String, Object> paramMap = new HashMap<>();
            paramMap.put("squid_id", squid.getId());
            List<DestCassandraColumn> esColumnList = adapter.query2List2(true, paramMap, DestCassandraColumn.class);
            if (esColumnList != null && esColumnList.size() > 0) {
                HistoryData historyData = new HistoryData(repositoryId, squidHistoryId, squidFlowId, commit_time,
                        JsonUtil.toJSONString(esColumnList), squidTypeId, 4, squidFlowType);
                adapter.insert2(historyData);
                esColumnList.clear();
                esColumnList = null;
            }
        } else {
            // column级别
            List<Column> columns = columnDao.getColumnListBySquidId(squid.getId());
            if (columns != null && columns.size() > 0) {
                columnList.addAll(columns);
            }
        }
        return columnList;
    }

    /**
     * 生成ReferenceGroup
     *
     * @param squidTypeId
     * @param repositoryId
     * @param squidFlowId
     * @param commit_time
     * @param squid
     * @return
     */
    public List<ReferenceColumnGroup> createReferenceColumn(int squidTypeId, int repositoryId, int squidFlowId,
                                                            String commit_time, List<ReferenceColumnGroup> referenceColumnGroupList, Squid squid) throws Exception {
        IReferenceColumnDao referenceColumnDao = new ReferenceColumnDaoImpl(adapter);
        List<ReferenceColumnGroup> referenceColumnGroups = referenceColumnDao
                .getRefColumnGroupListBySquidId(squid.getId());
        if (referenceColumnGroups != null && referenceColumnGroups.size() > 0) {
            referenceColumnGroupList.addAll(referenceColumnGroups);
        }
        return referenceColumnGroupList;
    }

    /**
     * 生成Transformation信息
     *
     * @param squidTypeId
     * @param repositoryId
     * @param squidFlowId
     * @param commit_time
     * @param transformationList
     * @param squid
     * @return
     */
    public List<Transformation> createTransformation(int squidTypeId, int repositoryId, int squidFlowId,
                                                     int squidFlowType, int squidHistoryId, String commit_time, List<Transformation> transformationList, Squid squid)
            throws Exception {
        ITransformationDao transformationDao = new TransformationDaoImpl(adapter);
        ITransformationInputsDao transformationInputsDao = new TransformationInputsDaoImpl(adapter);
        // 每50个插入一次
        if (transformationList.size() == 50) {
            HistoryData historyData = new HistoryData(repositoryId, squidHistoryId, squidFlowId, commit_time,
                    JsonUtil.toJSONString(transformationList), squidTypeId, 6, squidFlowType);
            adapter.insert2(historyData);
            transformationList.clear();
        }

        List<Transformation> transformations = transformationDao.getTransListBySquidId(squid.getId());
        if (transformations != null && transformations.size() > 0) {
            // 添加TransformationInput
            for (Transformation transformation : transformations) {
                //if (transformation.getTranstype() != 0 && transformation.getColumn_id() == 0) {
                List<TransformationInputs> inputs = transformationInputsDao.getTransInputListByTransId(transformation.getId());
                if (inputs != null && inputs.size() > 0) {
                    transformation.setInputs(inputs);
                }
                //}
            }
            transformationList.addAll(transformations);
        }
        return transformationList;
    }

    /**
     * 添加transformaitonLink信息
     */
    public List<TransformationLink> createTransformationLink(int squidTypeId, int repositoryId, int squidFlowId,
                                                             int squidFlowType, int squidHistoryId, String commit_time, List<TransformationLink> transformationLinkList, Squid squid)
            throws Exception {
        ITransformationLinkDao transformationLinkDao = new TransformationLinkDaoImpl(adapter);
        // 每50个插入数据库
        if (transformationLinkList.size() == 50) {
            HistoryData historyData = new HistoryData(repositoryId, squidHistoryId, squidFlowId, commit_time,
                    JsonUtil.toJSONString(transformationLinkList), squidTypeId, 7, squidFlowType);
            transformationLinkList.clear();
        }
        List<TransformationLink> transformationLinks = transformationLinkDao.getTransLinkListBySquidId(squid.getId());
        if (transformationLinks != null && transformationLinks.size() > 0) {
            transformationLinkList.addAll(transformationLinks);
        }
        return transformationLinkList;
    }
}
