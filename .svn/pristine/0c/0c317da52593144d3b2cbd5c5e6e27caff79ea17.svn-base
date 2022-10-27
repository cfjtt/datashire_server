package com.eurlanda.datashire.server.service;

import com.eurlanda.datashire.entity.SquidJoin;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.server.dao.*;
import com.eurlanda.datashire.server.exception.ErrorMessageException;
import com.eurlanda.datashire.server.model.Base.DataSquidBaseModel;
import com.eurlanda.datashire.server.model.Base.SquidModelBase;
import com.eurlanda.datashire.utility.MessageCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by eurlanda - new 2 on 2017/7/3.
 */
@Service
@Transactional
public class SquidService {
    private static final Logger logger = LoggerFactory.getLogger(SquidService.class);
    @Autowired
    private SquidDao squidDao;
    @Autowired
    private ReferenceColumnDao referenceColumnDao;
    @Autowired
    private TransformationDao transformationDao;
    @Autowired
    private TransformationInputsDao transformationInputsDao;
    @Autowired
    private ColumnDao columnDao;
    @Autowired
    private TransformationLinkDao transformationLinkDao;
    @Autowired
    private ReferenceColumnGroupDao referenceColumnGroupDao;
    @Autowired
    private SquidLinkDao squidLinkDao;
    @Autowired
    private SquidLinkService squidLinkService;
    @Autowired
    private SquidJoinDao squidJoinDao;

    @Autowired
    private SourceTableDao sourceTableDao;

    /**
     *
     * @param squidModelBaseList
     * @return
     * @throws Exception
     */
    @Transactional(rollbackFor = {Exception.class})
    public int deleteSquidBySquidObj(List<SquidModelBase> squidModelBaseList) throws Exception {
        List<Integer> squidIds = new ArrayList<>();

        if(squidModelBaseList ==null || squidModelBaseList.size() <= 0)
            throw new ErrorMessageException(MessageCode.DESERIALIZATION_FAILED.value());

        for (SquidModelBase squidModelBase : squidModelBaseList)
            squidIds.add(squidModelBase.getId());

        return deleteSquidBySquidId(squidIds);
    }
    /**
     * 删除Squid
     * @param  Squid对象集合
     * @return 删除的个数
     * @throws Exception
     */
    @Transactional(rollbackFor = {Exception.class})
    public int deleteSquidBySquidId(List<Integer> squidIds) throws Exception{
        int count =0;
        Map<String, String> paramMap = new HashMap<String, String>();
        try {
            if (squidIds == null || squidIds.size() <= 0)
                throw new ErrorMessageException(MessageCode.DESERIALIZATION_FAILED.value());

            //删除referenceColumn
            referenceColumnDao.deleteBySquidIdReferenceColumn(squidIds);

            //删除referenceColumnGroup
            referenceColumnGroupDao.deleteGroupBySquidId(squidIds);

            //删除Column
            columnDao.deleteColumnBySquidId(squidIds);

            //删除transformation
            transformationInputsDao.deleteTransInputBySquidId(squidIds);
            transformationLinkDao.deleteTransformationLinkBySquidId(squidIds);
            transformationDao.deleteTransformationBySquidId(squidIds);

            //删除SourceTable与SourceTable column
            squidDao.deleteSourceTableColumnBySquidId(squidIds);
            squidDao.deleteSourceTableBySquidId(squidIds);

            //删除ES column
            squidDao.deleteEsColumn(squidIds);

            //删除HDFS落地与Column
            squidDao.deleteHDFSColumn(squidIds);

            //删除Impala落地Column
            squidDao.deleteImpalaColumn(squidIds);

            //删除Join
            squidDao.deleteJoin(squidIds);

            //删除userdefinedDatamapColumn 和 userdefinedParametersColumn
            squidDao.deleteUserdefinedDatamapColumn(squidIds);
            squidDao.deleteUserdefinedParametersColumn(squidIds);

            //删除dest_hive
            squidDao.deleteDestHiveColumn(squidIds);

            //删除destCassandraColumn
            squidDao.deleteDestCassandraColumn(squidIds);

            //删除statistics_datamap_column 和 statistics_parameters_column
            squidDao.deleteStatisticsDatamapColumn(squidIds);
            squidDao.deleteStatisticsParametersColumn(squidIds);

            //删除squidLink
            squidLinkDao.deleteSquidLinkBysquidId(squidIds);

            //删除squid
            count= squidDao.deleteSquid(squidIds);
        }catch (Exception e){
            throw e;
        }
        return count;
    }


    /**
     * 删除Squid面板对象
     * @param squidIds
     * @param squidLinkIds
     * @return 封装好的map集合，里面装载了需要重置的属性，对应了各自的Squid Id
     * @throws Exception
     */
    @Transactional(rollbackFor = {Exception.class})
    public List<Map<String,Object>> deleteSquidPanelObj(List<Integer> squidIds, List<Integer> squidLinkIds) throws Exception {
        if (squidIds == null || squidIds.size() <= 0 || squidLinkIds == null || squidLinkIds.size() <= 0)
            throw new ErrorMessageException(MessageCode.DESERIALIZATION_FAILED.value());

        //返回对象集合元素
        List<Map<String,Object>> returnList=new ArrayList<Map<String,Object>>();

        //修改source Table中Is_extracted属性
        logger.info(String.format("selectSourceTableIdBySquids============================", squidIds));
        List<Integer> sourceTableIds=squidDao.selectSourceTableIdBySquids(squidIds);
        if(sourceTableIds!=null && sourceTableIds.size()>0){
            int updateExtract=sourceTableDao.updateIsExtractedByIds(sourceTableIds);
        }
        List<DataSquidBaseModel> squidModelBaseList = squidLinkService.deleteSquidLinkIds(squidLinkIds, squidIds);
        int count = this.deleteSquidBySquidId(squidIds); //通过SquidId删除Squid对象


        for (DataSquidBaseModel item : squidModelBaseList){
            Map<String, Object> map = new HashMap<>();

            List<SquidJoin> joins = squidDao.selectJoinBySquid(item.getId());
            map.put("squidId",item.getId());
            map.put("columnList",item.getColumns());
            map.put("referenceColumnList",item.getSourceColumns());
            map.put("transformationList",item.getTransformations());
            map.put("transLinkList",item.getTransformationLinks());
            map.put("squidJoinList",joins);

            if (item.getSquid_type() == SquidTypeEnum.EXCEPTION.value() || item.getSquid_type() == SquidTypeEnum.DATAVIEW.value() || item.getSquid_type() == SquidTypeEnum.COEFFICIENT.value()) {
                map.put("design_status",item.getDesign_status());
            }
            returnList.add(map); //添加到返回对象集合中
        }

        return returnList;
    }
}
