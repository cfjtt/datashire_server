package com.eurlanda.datashire.server.service;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.common.util.ConstantsUtil;
import com.eurlanda.datashire.entity.Squid;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.enumeration.JoinType;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.enumeration.TransformationTypeEnum;
import com.eurlanda.datashire.enumeration.datatype.SystemDatatype;
import com.eurlanda.datashire.server.dao.*;
import com.eurlanda.datashire.server.exception.ErrorMessageException;
import com.eurlanda.datashire.server.model.*;
import com.eurlanda.datashire.server.model.Base.DataSquidBaseModel;
import com.eurlanda.datashire.server.model.Base.SquidModelBase;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.StringUtils;
import com.sun.jersey.api.MessageException;
import jnr.ffi.annotations.In;
import net.sf.jsqlparser.expression.operators.relational.OldOracleJoinBinaryExpression;
import org.datanucleus.store.types.backed.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Eurlanda - Java、 on 2017/6/27.
 */
@Service
public class SquidLinkService {
    private static final Logger LOGGER= LoggerFactory.getLogger(SquidLinkService.class);

    @Autowired
    private SquidDao squidDao;
    @Autowired
    private SquidLinkDao squidLinkDao;
    @Autowired
    private SquidJoinDao squidJoinDao;
    @Autowired
    private ReferenceColumnGroupDao referenceColumnGroupDao;
    @Autowired
    private ReferenceColumnDao referenceColumnDao;
    @Autowired
    private TransformationDao transformationDao;
    @Autowired
    private  TransformationLinkDao transformationLinkDao;
    @Autowired
    private TransformationService transformationService;
    @Autowired
    private EsColumnDao esColumnDao;
    @Autowired
    private DestHDFSColumnDao destHDFSColumnDao;
    @Autowired
    private DestImpalaColumnDao destImpalaColumnDao;
    @Autowired
    private ColumnDao columnDao;
    @Autowired
    private DestHiveColumnDao destHiveColumnDao;
    @Autowired
    private DestCassandraColumnDao destCassandraColumnDao;
    @Autowired
    private TransformationInputsDao transformationInputsDao;
    @Autowired
    private GetMeatDataService getMeatDataService;
    @Autowired
    private SamplingSquidService samplingSquidService;
    @Autowired
    private PivotSquidService pivotSquidService;

    /**
     * 批量删除SquidLink
     * @param listSquidLinkIds SquidLinkIds
     * @param
     * @return
     * @throws Exception 如果是业务逻辑的错误，需要在Exception里面附带错误码
     */
    public List<DataSquidBaseModel> deleteSquidLinkIds(List<Integer> listSquidLinkIds)throws Exception{
        return deleteSquidLinkIds(listSquidLinkIds,null);
    }


    /**
     *批量删除SquidLink  参数是ListSquidLinkIds 和 ListSquids
     * @param listSquidLinkIds
     * @param listSquids
     * @return
     * @throws Exception
     */
    public List<DataSquidBaseModel> deleteSquidLinkIds(List<Integer> listSquidLinkIds,
                                                       List<Integer> listSquids)throws Exception{

        //作为返回收影响的squid集合
        List<DataSquidBaseModel> returnSquids=new ArrayList<DataSquidBaseModel>();

        int deleteSquLinkCount=0;
        int deleteDestesCount=0;
        int deleteHDFSCount=0;
        int updateHIVECount=0;
        int updateCASSCount=0;
        int deleteIMPALACount=0;
        int deleteColumnCount=0;
        int updateSquidCount=0;
        int deleteTransCount=0;
        int updateJoinCount=0;
        int deleteJoinCount=0;
        int updateGroupCount=0;
        int delTranInputCount=0;
        List<DataSquidBaseModel> squids=new ArrayList<DataSquidBaseModel>();
        boolean deleteTag=true;
        boolean deleteReColumn=true;
        //用来装下游是特殊类型的 squidLinkId.这些数据不用做循环删除referenceColumn,transformation,transInput
        List<Integer> squidLinkIds=new ArrayList<>();
        try{


            //查询下游squidIds
            LOGGER.debug(String.format("selectToSquidBySquLinkIds============================", listSquidLinkIds));
            List<Integer> toSquidIds=squidDao.selectToSquidBySquLinkIds(listSquidLinkIds);

            //与传过来得listSquids作比较。过滤掉不需要做同步的Squid
            if(listSquids!=null && listSquids.size()>0) {
                List<Integer> containSquid=new ArrayList<Integer>();
                for(Integer squId:listSquids){
                    if(toSquidIds.contains(squId)){
                        containSquid.add(squId);
                    }
                }
                if(containSquid!=null && containSquid.size()>0){
                    toSquidIds.removeAll(containSquid);
                }

                //根据传过来得参数ListSquids 查询它作为下游所关联的squidLinkIds  为了过滤不需要做同步的squLinkid
                //listSquidLinkIds  这个集合是过滤掉不用做同步剩下的squidLinkId 集合
                LOGGER.debug(String.format("selectSquLinkByToSquid============================", listSquids));
                List<Integer> linkIds=squidLinkDao.selectSquLinkByToSquid(listSquids);
                if(linkIds!=null && linkIds.size()>0){
                    List<Integer> containSquLinkId=new ArrayList<Integer>();
                    for(Integer squLinkId:linkIds){
                        if(listSquidLinkIds.contains(squLinkId)){
                            containSquLinkId.add(squLinkId);
                        }
                    }
                    if(containSquLinkId!=null && containSquLinkId.size()>0){
                        listSquidLinkIds.removeAll(containSquLinkId);
                    }
                }

            }
              //这是过滤后最终需要删除的LinkId 集合。不直接用listSquidLinkIds，是因为这个集合接下来会做一些排除工作。
            List<Integer> delSquidLinkIds=new ArrayList<>();
            delSquidLinkIds.addAll(listSquidLinkIds);

            Integer linkid=0;
            Integer sids=0;
            if(toSquidIds!=null &&toSquidIds.size()>0 ) {
                /**
                 * 开始批量处理ES落地Squid
                 */
                //判断下游squid 是否是特殊类型  SquidTypeEnum.DESTES.value()
                Map<String, Object> DESTESMap = new HashMap<String, Object>();
                DESTESMap.put("squidType", SquidTypeEnum.DESTES.value());
                DESTESMap.put("squidIds", toSquidIds);
                //返回结果包含squidIds和squidLinkIds
                LOGGER.debug(String.format("selectSquBySquTypeAndSquIds============================", DESTESMap));
                List<Map<String, Object>> DESTESSquids = squidDao.selectSquBySquTypeAndSquIds(DESTESMap);
                if (DESTESSquids != null && DESTESSquids.size() > 0) {
                    List<Integer> DESTESSIds = new ArrayList<>();
                    for (Map<String, Object> map : DESTESSquids) {
                        linkid = Integer.parseInt(map.get("squidLinkId").toString());
                        sids = Integer.parseInt(map.get("squid").toString());
                        //把包含的squidLinkId 放入不需要做循环删除referenceColumn...,squidLinkIds集合中。
                        squidLinkIds.add(linkid);
                        DESTESSIds.add(sids);
                    }
                    //批量删除DESTESColumn
                    LOGGER.debug(String.format("deleteEsColumnBySquId============================", DESTESSIds));
                    deleteDestesCount = esColumnDao.deleteEsColumnBySquId(DESTESSIds);
                }

                /**
                 * 开始批量处理HDFS落地Squid
                 */
                //判断下游squid 是否是特殊类型  SquidTypeEnum.DEST_HDFS.value()
                Map<String, Object> HDFSMap = new HashMap<String, Object>();
                HDFSMap.put("squidType", SquidTypeEnum.DEST_HDFS.value());
                HDFSMap.put("squidIds", toSquidIds);
                //返回结果包含squidIds和squidLinkIds
                LOGGER.debug(String.format("selectSquBySquTypeAndSquIds============================", HDFSMap));
                List<Map<String, Object>> HDFSSquids = squidDao.selectSquBySquTypeAndSquIds(HDFSMap);
                List<Integer> HDFSSquidIds = new ArrayList<>();
                if (HDFSSquids != null && HDFSSquids.size() > 0) {
                    for (Map<String, Object> map : HDFSSquids) {
                        linkid = Integer.parseInt(map.get("squidLinkId").toString());
                        sids = Integer.parseInt(map.get("squid").toString());
                        //把包含的squidLinkId 放入不需要做循环删除referenceColumn...,squidLinkIds集合中。
                        squidLinkIds.add(linkid);
                        HDFSSquidIds.add(sids);
                    }
                }
                /**
                 * 开始批量处理DESTCLOUDFILE落地Squid
                 */
                //判断下游squid 是否是特殊类型  SquidTypeEnum.DESTCLOUDFILE.value()
                Map<String, Object> destCloudFileMap = new HashMap<String, Object>();
                destCloudFileMap.put("squidType", SquidTypeEnum.DESTCLOUDFILE.value());
                destCloudFileMap.put("squidIds", toSquidIds);
                //返回结果包含squidIds和squidLinkIds
                LOGGER.debug(String.format("selectSquBySquTypeAndSquIds============================", destCloudFileMap));
                List<Map<String, Object>> destCloudFiles = squidDao.selectSquBySquTypeAndSquIds(destCloudFileMap);
                if (destCloudFiles != null && destCloudFiles.size() > 0) {
                    List<Integer> destCloudFile = new ArrayList<>();
                    for (Map<String, Object> map : destCloudFiles) {
                        linkid = Integer.parseInt(map.get("squidLinkId").toString());
                        sids = Integer.parseInt(map.get("squid").toString());
                        //把包含的squidLinkId 放入不需要做循环删除referenceColumn...,squidLinkIds集合中。
                        squidLinkIds.add(linkid);
                        HDFSSquidIds.add(sids);
                    }
                }
                //批量删除HDFSColumn
                if(HDFSSquidIds.size()>0) {
                    LOGGER.debug(String.format("deleteHDFSColumnBySquId============================", HDFSSquidIds));
                    deleteHDFSCount = destHDFSColumnDao.deleteHDFSColumnBySquId(HDFSSquidIds);
                }
                /**
                 * 开始批量处理impala落地Squid
                 */
                //判断下游squid 是否是特殊类型  SquidTypeEnum.DEST_IMPALA.value()
                Map<String, Object> IMPALAMap = new HashMap<String, Object>();
                IMPALAMap.put("squidType", SquidTypeEnum.DEST_IMPALA.value());
                IMPALAMap.put("squidIds", toSquidIds);
                LOGGER.debug(String.format("selectSquBySquTypeAndSquIds============================", IMPALAMap));
                List<Map<String, Object>> IMPALASquids = squidDao.selectSquBySquTypeAndSquIds(IMPALAMap);
                if (IMPALASquids != null && IMPALASquids.size() > 0) {
                    List<Integer> IMPALASquidIds = new ArrayList<>();
                    for (Map<String, Object> map : IMPALASquids) {
                        linkid = Integer.parseInt(map.get("squidLinkId").toString());
                        sids = Integer.parseInt(map.get("squid").toString());
                        //把包含的squidLinkId 放入不需要做循环删除referenceColumn...,squidLinkIds集合中。
                        squidLinkIds.add(linkid);
                        IMPALASquidIds.add(sids);
                    }
                    //批量删除ImpalaColumn
                    LOGGER.debug(String.format("deleteImpalaColumnByIds============================", IMPALASquidIds));
                    deleteIMPALACount = destImpalaColumnDao.deleteImpalaColumnByIds(IMPALASquidIds);
                }

                /**
                 * 开始批量处理Hive落地Squid
                 */
                //判断下游squid 是否是特殊类型  SquidTypeEnum.DEST_HIVE.value()
                Map<String, Object> HIVEAMap = new HashMap<String, Object>();
                HIVEAMap.put("squidType", SquidTypeEnum.DEST_HIVE.value());
                HIVEAMap.put("squidIds", toSquidIds);
                LOGGER.debug(String.format("selectSquBySquTypeAndSquIds============================", HIVEAMap));
                List<Map<String, Object>> HIVESquids = squidDao.selectSquBySquTypeAndSquIds(HIVEAMap);
                if (HIVESquids != null && HIVESquids.size() > 0) {
                    List<Integer> HIVESquidIds = new ArrayList<>();
                    for (Map<String, Object> map : HIVESquids) {
                        linkid = Integer.parseInt(map.get("squidLinkId").toString());
                        sids = Integer.parseInt(map.get("squid").toString());
                        //把包含的squidLinkId 放入不需要做循环删除referenceColumn...,squidLinkIds集合中。
                        squidLinkIds.add(linkid);
                        HIVESquidIds.add(sids);
                    }
                    //批量修改HIVEColumn
                    LOGGER.debug(String.format("updateColumnBySquIds============================", HIVESquidIds));
                    updateHIVECount = destHiveColumnDao.updateColumnBySquIds(HIVESquidIds);
                }


                /**
                 * 开始批量处理Cassandra落地Squid
                 */
                //判断下游squid 是否是特殊类型  SquidTypeEnum.DEST_CASSANDRA.value()
                Map<String, Object> CASSMap = new HashMap<String, Object>();
                CASSMap.put("squidType", SquidTypeEnum.DEST_CASSANDRA.value());
                CASSMap.put("squidIds", toSquidIds);
                LOGGER.debug(String.format("selectSquBySquTypeAndSquIds============================", CASSMap));
                List<Map<String, Object>> CASSSquids = squidDao.selectSquBySquTypeAndSquIds(CASSMap);
                if (CASSSquids != null && CASSSquids.size() > 0) {
                    List<Integer> CASSSquidIds = new ArrayList<>();
                    for (Map<String, Object> map : CASSSquids) {
                        linkid = Integer.parseInt(map.get("squidLinkId").toString());
                        sids = Integer.parseInt(map.get("squid").toString());
                        //把包含的squidLinkId 放入不需要做循环删除referenceColumn...,squidLinkIds集合中。
                        squidLinkIds.add(linkid);
                        CASSSquidIds.add(sids);
                    }
                    //批量修改CassandraColumn
                    LOGGER.debug(String.format("updateColumnBySquIds============================", CASSSquidIds));
                    updateCASSCount = destCassandraColumnDao.updateColumnBySquIds(CASSSquidIds);
                }

                /**
                 * 开始批量处理GROUPTAGGINGSquid
                 */
                //判断下游squid 是否是特殊类型  SquidTypeEnum.GROUPTAGGING.value()
                Map<String, Object> GROUPMap = new HashMap<String, Object>();
                GROUPMap.put("squidType", SquidTypeEnum.GROUPTAGGING.value());
                GROUPMap.put("squidIds", toSquidIds);
                LOGGER.debug(String.format("selectSquBySquTypeAndSquIds============================", GROUPMap));
                List<Map<String, Object>> GROUPSquids = squidDao.selectSquBySquTypeAndSquIds(GROUPMap);
                if (GROUPSquids != null && GROUPSquids.size() > 0) {
                    List<Integer> GroupsquidIds = new ArrayList<>();
                    List<Integer> LinkIds = new ArrayList<>();
                    for (Map<String, Object> map : GROUPSquids) {
                        linkid = Integer.parseInt(map.get("squidLinkId").toString());
                        sids = Integer.parseInt(map.get("squid").toString());
                        //把包含的squidLinkId 放入LinkIds中，
                        // 是因为squid类型是grouptagging和类型是stage 。分开处理删除referenceColumn。
                        LinkIds.add(linkid);
                        GroupsquidIds.add(sids);
                        //把包含的squidLinkId 放入不需要做循环删除referenceColumn集合中。
                        squidLinkIds.add(linkid);
                    }

                    //调用删除关联Transformation,TransLink,TransInput,ReferenceColumn,Column
                    deleteTag = deleteReColumnsAndTrans(LinkIds);
                    // squid 类型是groupTagging  则连column一起删除，
                    Map<String, Object> columnMap = new HashMap<String, Object>();
                    columnMap.put("squIds", GroupsquidIds);
                    columnMap.put("tag", ConstantsUtil.CN_GROUP_TAG);
                    //根据squidId查询column.排除column的name为tag的数据
                    LOGGER.debug(String.format("selectColumnBySquidIds============================", columnMap));
                    List<Integer> columnIds = columnDao.selectColumnBySquidIds(columnMap);
                    if (columnIds != null && columnIds.size() > 0) {
                        //根据ColumnId 和 目标的Groupsquid 查询column列上的虚拟transformation
                        Map<String, Object> transMap = new HashMap<String, Object>();
                        transMap.put("squidList", GroupsquidIds);
                        transMap.put("columnList", columnIds);
                        LOGGER.debug(String.format("selectByColumnIdAndSquId============================", transMap));
                        List<Integer> transIds = transformationDao.selectTransByCoIdAndSquIds(transMap);
                        //批量删除Transformation
                        if (transIds != null && transIds.size() > 0) {
                            //根据tranIds 查询虚拟Transformation上的input.
                            LOGGER.debug(String.format("selectTransInputByTransIds============================", transIds));
                            List<Integer> transInputIds = transformationInputsDao.selectTransInputByTransIds(transIds);
                            //批量删除Transinput
                            if (transInputIds != null && transInputIds.size() > 0) {
                                LOGGER.debug(String.format("deleteByInputIds============================", transInputIds));
                                delTranInputCount = transformationInputsDao.deleteByInputIds(transInputIds);
                            }
                            LOGGER.debug(String.format("deleteTransformations============================", transIds));
                            deleteTransCount = transformationDao.deleteTransformations(transIds);
                        }
                        //批量删除column
                        if (columnIds != null && columnIds.size() > 0) {
                            LOGGER.debug(String.format("deleteColumnByIds============================", columnIds));
                            deleteColumnCount = columnDao.deleteColumnByIds(columnIds);
                        }
                    }
                    //客户端已经把所有需要删除的squidLink都传了过来。所以这里就不用再做递归。


                }


                /**
                 * 开始批量处理SamplingSquid
                 */
                //判断下游squid 是否是特殊类型  SquidTypeEnum.SamplingSquid.value()
                Map<String, Object> samplingMap = new HashMap<String, Object>();
                samplingMap.put("squidType", SquidTypeEnum.SAMPLINGSQUID.value());
                samplingMap.put("squidIds", toSquidIds);
                LOGGER.debug(String.format("selectSquBySquTypeAndSquIds============================", samplingMap));
                List<Map<String, Object>> samplingSquids = squidDao.selectSquBySquTypeAndSquIds(samplingMap);
                if (samplingSquids != null && samplingSquids.size() > 0) {
                    List<Integer> samplingsquidIds = new ArrayList<>();
                    List<Integer> LinkIds = new ArrayList<>();
                    for (Map<String, Object> map : samplingSquids) {
                        linkid = Integer.parseInt(map.get("squidLinkId").toString());
                        sids = Integer.parseInt(map.get("squid").toString());
                        //把包含的squidLinkId 放入LinkIds中，
                        // 是因为squid类型是SamplingSquid和类型是stage 。分开处理删除referenceColumn。
                        LinkIds.add(linkid);
                        samplingsquidIds.add(sids);
                        //把包含的squidLinkId 放入不需要做循环删除referenceColumn集合中。
                        squidLinkIds.add(linkid);
                    }

                    //调用删除关联Transformation,TransLink,TransInput,ReferenceColumn,Column
                    deleteTag = deleteReColumnsAndTrans(LinkIds);
                    // squid 类型是SamplingSquid 则连column一起删除，
                    Map<String, Object> columnMap = new HashMap<String, Object>();
                    columnMap.put("squIds", samplingsquidIds);
                    columnMap.put("tag", "");
                    LOGGER.debug(String.format("selectColumnBySquidIds============================", columnMap));
                    List<Integer> columnIds = columnDao.selectColumnBySquidIds(columnMap);
                    if (columnIds != null && columnIds.size() > 0) {
                        //根据ColumnId 和 目标的Groupsquid 查询column列上的虚拟transformation
                        Map<String, Object> transMap = new HashMap<String, Object>();
                        transMap.put("squidList", samplingsquidIds);
                        transMap.put("columnList", columnIds);
                        LOGGER.debug(String.format("selectByColumnIdAndSquId============================", transMap));
                        List<Integer> transIds = transformationDao.selectTransByCoIdAndSquIds(transMap);
                        //批量删除Transformation
                        if (transIds != null && transIds.size() > 0) {
                            //根据tranIds 查询虚拟Transformation上的input.
                            LOGGER.debug(String.format("selectTransInputByTransIds============================", transIds));
                            List<Integer> transInputIds = transformationInputsDao.selectTransInputByTransIds(transIds);
                            //批量删除Transinput
                            if (transInputIds != null && transInputIds.size() > 0) {
                                LOGGER.debug(String.format("deleteByInputIds============================", transInputIds));
                                delTranInputCount = transformationInputsDao.deleteByInputIds(transInputIds);
                            }
                            LOGGER.debug(String.format("deleteTransformations============================", transIds));
                            deleteTransCount = transformationDao.deleteTransformations(transIds);
                        }
                        //批量删除column
                        if (columnIds != null && columnIds.size() > 0) {
                            LOGGER.debug(String.format("deleteColumnByIds============================", columnIds));
                            deleteColumnCount = columnDao.deleteColumnByIds(columnIds);
                        }
                    }
                    //客户端已经把所有需要删除的squidLink都传了过来。所以这里就不用再做递归。

                }



                /**
                 * 开始批量处理PivotSquid
                 */
                //判断下游squid 是否是特殊类型  SquidTypeEnum.GROUPTAGGING.value()
                Map<String, Object> pivotMap = new HashMap<String, Object>();
                pivotMap.put("squidType", SquidTypeEnum.PIVOTSQUID.value());
                pivotMap.put("squidIds", toSquidIds);
                LOGGER.debug(String.format("selectSquBySquTypeAndSquIds============================", pivotMap));
                List<Map<String, Object>> pivotSquids = squidDao.selectSquBySquTypeAndSquIds(pivotMap);
                if (pivotSquids != null && pivotSquids.size() > 0) {
                    List<Integer> pivotSquidIds = new ArrayList<>();
                    List<Integer> LinkIds = new ArrayList<>();
                    for (Map<String, Object> map : pivotSquids) {
                        linkid = Integer.parseInt(map.get("squidLinkId").toString());
                        sids = Integer.parseInt(map.get("squid").toString());
                        //把包含的squidLinkId 放入LinkIds中，
                        // 是因为squid类型是SamplingSquid和类型是stage 。分开处理删除referenceColumn。
                        LinkIds.add(linkid);
                        pivotSquidIds.add(sids);
                        //把包含的squidLinkId 放入不需要做循环删除referenceColumn集合中。
                        squidLinkIds.add(linkid);
                    }

                    //调用删除关联Transformation,TransLink,TransInput,ReferenceColumn,Column
                    deleteTag = deleteReColumnsAndTrans(LinkIds);
                    // squid 类型是groupTagging  则连column一起删除，
                    Map<String, Object> columnMap = new HashMap<String, Object>();
                    columnMap.put("squIds", pivotSquidIds);
                    columnMap.put("tag", "");
                    //根据squidId查询column.排除column的name为tag的数据
                    LOGGER.debug(String.format("selectColumnBySquidIds============================", columnMap));
                    List<Integer> columnIds = columnDao.selectColumnBySquidIds(columnMap);
                    if (columnIds != null && columnIds.size() > 0) {
                        //根据ColumnId 和 目标的Groupsquid 查询column列上的虚拟transformation
                        Map<String, Object> transMap = new HashMap<String, Object>();
                        transMap.put("squidList", pivotSquidIds);
                        transMap.put("columnList", columnIds);
                        LOGGER.debug(String.format("selectByColumnIdAndSquId============================", transMap));
                        List<Integer> transIds = transformationDao.selectTransByCoIdAndSquIds(transMap);
                        //批量删除Transformation
                        if (transIds != null && transIds.size() > 0) {
                            //根据tranIds 查询虚拟Transformation上的input.
                            LOGGER.debug(String.format("selectTransInputByTransIds============================", transIds));
                            List<Integer> transInputIds = transformationInputsDao.selectTransInputByTransIds(transIds);
                            //批量删除Transinput
                            if (transInputIds != null && transInputIds.size() > 0) {
                                LOGGER.debug(String.format("deleteByInputIds============================", transInputIds));
                                delTranInputCount = transformationInputsDao.deleteByInputIds(transInputIds);
                            }
                            LOGGER.debug(String.format("deleteTransformations============================", transIds));
                            deleteTransCount = transformationDao.deleteTransformations(transIds);
                        }
                        //批量删除column
                       /* if (columnIds != null && columnIds.size() > 0) {
                            LOGGER.debug(String.format("deleteColumnByIds============================", columnIds));
                            deleteColumnCount = columnDao.deleteColumnByIds(columnIds);
                        }*/
                    }
                    //客户端已经把所有需要删除的squidLink都传了过来。所以这里就不用再做递归。
                }



                /**
                 * 开始处理DataViewSquid 和 ExceptionSquid，COEFFICIENT
                 * DataViewSquid , ExceptionSquid ,COEFFICIENT，下下游如果是ExceptionSquid。都需要作废
                 * 把这些查到squidId 添加到一个集合中。一次性修改。
                 */
                //需要作废的squidId集合
                List<Integer>  invalidSquidIds=new ArrayList<Integer>();

                //根据squidLinkId 判断下游是否是DataViewSquid 或者是 ExceptionSquid
                Map<String, Object> dataViewMap = new HashMap<String, Object>();
                dataViewMap.put("squidIds", toSquidIds);
                dataViewMap.put("exception", SquidTypeEnum.EXCEPTION.value());
                dataViewMap.put("dataView", SquidTypeEnum.DATAVIEW.value());
                dataViewMap.put("coefficientSquid",SquidTypeEnum.COEFFICIENT.value());
                LOGGER.debug(String.format("selectSquBySquTypeAndSquIds============================", dataViewMap));
                List<Map<String, Object>> dataViewSquids = squidDao.selectExceViewSquByTypeAndSquIds(dataViewMap);
                if (dataViewSquids != null && dataViewSquids.size() > 0) {
                    for (Map<String, Object> map : dataViewSquids) {
                        linkid = Integer.parseInt(map.get("squidLinkId").toString());
                        sids = Integer.parseInt(map.get("squid").toString());
                        //把包含的squidLinkId 放入不需要做循环删除referenceColumn...,squidLinkIds集合中。
                        squidLinkIds.add(linkid);
                        //把查询到的squidId 放入需要作废总的集合中
                        invalidSquidIds.add(sids);
                    }
                    //这里先不做修改。因为下面还要查询下下游是否是ExceptionSquid
                }


                //排除之前下游是特殊类型的squidLinkIds。则剩下的需要批量删除referenceColumn和批量删除Join。
                if (squidLinkIds != null && squidLinkIds.size() > 0 && listSquidLinkIds != null && listSquidLinkIds.size() > 0) {
                    listSquidLinkIds.removeAll(squidLinkIds);
                }


                /**
                 * 根据squidLInkIds查询受影响的squidJoin,并且按照squid分组。
                 * 这样在同一个squid上面连接很多join时，只需要循环squid的个数。而非循环受影响join的个数
                 */
                if (listSquidLinkIds != null && listSquidLinkIds.size() > 0) {
                    LOGGER.debug(String.format("selectJoinByLinkIdAndGroupSquid============================", listSquidLinkIds));
                    List<Map<String, Object>> joinList = squidJoinDao.selectJoinByLinkIdAndGroupSquid(listSquidLinkIds);
                if (joinList != null && joinList.size() > 0) {
                    //这是最终要删除的joinId 集合
                    List<Integer> joinIdList = new ArrayList<>();
                    for (Map<String, Object> map : joinList) {
                        //取出来的可能是多个joinId组成的字符串
                        String joinId = map.get("joinIds").toString();
                        //取出来的可能是多个type组成的字符串
                        String typeId = map.get("joinTypes").toString();
                        //是根据squid分组的，所以它是单独的
                        Integer squid = Integer.parseInt(map.get("squid").toString());
                        List<Integer> joinIds = null;
                        //将Join字符串转换成List<Integer> 集合
                        if (joinId != null && !joinId.equals("")) {
                            //用来查询当前squid 除了选中的join而剩下的join。做修改
                            joinIds = new ArrayList<>();
                            List<String> joinStr = Arrays.asList(joinId.split(","));
                            for (String joId : joinStr) {
                                joinIds.add(Integer.parseInt(joId));
                            }
                            //添加所有需要删除的join
                            joinIdList.addAll(joinIds);
                        }
                        //将Type字符串转换成List<Integer> 集合
                        if (typeId != null && !typeId.equals("")) {
                            List<String> typeStr = Arrays.asList(typeId.split(","));
                            //用来判断该squid中选中的join中type是否包含 BaseTable
                            List<Integer> typeIds = new ArrayList<>();
                            for (String type : typeStr) {
                                typeIds.add(Integer.parseInt(type));
                            }
                            //判断删除的join中是否包含BaseTable
                            if (typeIds.contains(JoinType.BaseTable.value())) {
                                Map<String, Object> joinMap = new HashMap<String, Object>();
                                joinMap.put("joinedId", squid);
                                joinMap.put("ids", joinIds);
                                // 查询下游的squid是否还有别的join
                                LOGGER.debug(String.format("selectJoinsByjoinIdAndJoinedId============================", joinMap));
                                List<SquidJoin> squJoins = squidJoinDao.selectJoinsByjoinIdAndJoinedId(joinMap);
                                if (squJoins != null && squJoins.size() > 0) {
                                    //取第一个 修改type 为BaseTable
                                    SquidJoin join = squJoins.get(0);
                                    LOGGER.debug(String.format("updateTypeAndPrior============================", join.getId()));
                                    updateJoinCount = squidJoinDao.updateTypeAndPrior(join.getId());
                                    //查询referenceColumnGroup 修改它的setRelative_order
                                    Map<String, Object> groupMap = new HashMap<String, Object>();
                                    groupMap.put("refereId", join.getJoined_squid_id());
                                    groupMap.put("hostId", join.getTarget_squid_id());
                                    LOGGER.debug(String.format("selectByReSquIdAndHostSquId============================", groupMap));
                                    String groupId = referenceColumnGroupDao.selectByReSquIdAndHostSquId(groupMap);
                                    if (StringUtils.isNotNull(groupId)) {
                                        int id = Integer.valueOf(groupId);
                                        LOGGER.debug(String.format("updateRelativeById============================", groupId));
                                        updateGroupCount = referenceColumnGroupDao.updateRelativeById(id);
                                    }
                                }
                            }
                        }
                    }
                    //批量删除这些Squidjoin
                    LOGGER.debug(String.format("deleteSquidJoinByIds============================", joinIdList));
                    deleteJoinCount = squidJoinDao.deleteSquidJoinByIds(joinIdList);
                }

            }

                //剩下的squid LinkIds 都是需要调用删除SquidJoin,referenceColumn，Transformation，Transinputs
                if (listSquidLinkIds != null && listSquidLinkIds.size() > 0) {
                    deleteReColumn = deleteReColumnsAndTrans(listSquidLinkIds);
                }

                //根据squid 查询下下游是否是exceptionSquid
                if (toSquidIds != null && toSquidIds.size() > 0 && listSquidLinkIds != null && listSquidLinkIds.size() > 0) {
                    Map<String, Object> downExceptionMap = new HashMap<String, Object>();
                    downExceptionMap.put("squidIds", toSquidIds);
                    downExceptionMap.put("exceType", SquidTypeEnum.EXCEPTION.value());
                    LOGGER.debug(String.format("selectExceptionSquSquIds============================", downExceptionMap));
                    List<Integer> squidExceIds = squidDao.selectExceptionSquSquIds(downExceptionMap);
                    if (squidExceIds != null && squidExceIds.size() > 0) {
                        //需要作废的squidId,添加总的集合中
                        invalidSquidIds.addAll(squidExceIds);
                        //把受影响的下下游squid 添加到需要查询返回给客户的squidId集合中
                        toSquidIds.addAll(invalidSquidIds);
                    }
                }
                //批量修改需要作废的squid
                if(invalidSquidIds!=null && invalidSquidIds.size()>0){
                    LOGGER.debug(String.format("updateSquStatusBySquIds============================", invalidSquidIds));
                    updateSquidCount = squidDao.updateSquStatusBySquIds(invalidSquidIds);
                }


            }
            //批量删除SquidLink
            if(delSquidLinkIds!=null && delSquidLinkIds.size()>0){
                LOGGER.debug(String.format("deleteSquidLinkByIds============================", delSquidLinkIds));
                deleteSquLinkCount=squidLinkDao.deleteSquidLinkByIds(delSquidLinkIds);
            }

            //根据剩下的SquidIds  查询受影响的Squid
            if(toSquidIds!=null && toSquidIds.size()>0){
                LOGGER.debug(String.format("selectSquBySquIds============================", toSquidIds));
                squids=squidDao.selectSquBySquIds(toSquidIds);
            }
            //判断处理结果是否出现错误
        if(deleteSquLinkCount<0 ||deleteDestesCount<0 || deleteHDFSCount<0
                || !deleteReColumn || !deleteTag
                || updateHIVECount<0 || updateCASSCount<0 || deleteColumnCount<0
                || updateSquidCount<0 || deleteTransCount<0 ||delTranInputCount<0){
            LOGGER.debug("[deleteSquidLinkIds===================================exception]", listSquidLinkIds);
                throw new ErrorMessageException(MessageCode.ERR_DELETE.value());
          }

        }catch (Exception e){
            LOGGER.error("[deleteSquidLinkIds===================================exception]", e);
            throw new ErrorMessageException(MessageCode.ERR_DELETE.value(),e);
        }
        return squids;
    }


    /**
     *根据剩下的squidLinkIds删除 相关的referenceColumn,Transformation,TransInput,referenceColumnGroup
     * @param squidLinkIds
     * @return
     * @throws Exception
     */
    public boolean  deleteReColumnsAndTrans(List<Integer> squidLinkIds)throws Exception{
       List<Transformation> transList=new ArrayList<Transformation>();
       int deleteTransCount=0;
       int deleteReColumnCount=0;
       int deleteGroupCount=0;
       int delTranInputCount=0;
       boolean flag=true;
       List<DataSquidBaseModel> squids=new ArrayList<DataSquidBaseModel>();
       try {

           /** 根据squid LinkIds 查询referenceColumn。
            *  查询条件referenceColumn.HOST_SQUID_ID=ds_squid_link.FROM_SQUID_ID
            *  and referenceColumn.REFERENCE_SQUID_ID=ds_squid_link.TO_SQUID_ID
            * 这样不会查询到多余referenceColumn
            */
           LOGGER.debug(String.format("selectReColumnBySquLinkid============================", squidLinkIds));
           List<ReferenceColumn> referenceColumns=referenceColumnDao.selectReColumnBySquLinkid(squidLinkIds);
           if (referenceColumns != null && referenceColumns.size() > 0) {
               List<Integer> reColumnIds = new ArrayList<Integer>();
               List<Integer> reColumnGroupIds = new ArrayList<Integer>();
               for (ReferenceColumn reference : referenceColumns) {
                   //取columnId添加到reColumnIds中。为了后面查询transformation
                   reColumnIds.add(reference.getColumn_id());
                   if (!reColumnGroupIds.contains(reference.getGroup_id())) {
                       //reColumnGroupIds。为了后面删除referenceColumnGroup
                       reColumnGroupIds.add(reference.getGroup_id());
                   }
               }
               /**根据referenceColumnId 和squidLinkId 查询transformation。
                * 要让transformation中的squid_id和referenceColumn中的reference_squid_id
                * and transformation中的column_id和referenceColumn中column_id相关联
                * 这样不会查询到多余transformation
                */
               Map<String, Object> transMap = new HashMap<String, Object>();
               transMap.put("columnIdList", reColumnIds);
               transMap.put("squLinkIdList", squidLinkIds);
               LOGGER.debug(String.format("selectByColumnIdAndSquLinkId============================", transMap));
               List<Integer> transIds = transformationDao.selectByColumnIdAndSquLinkId(transMap);
               if (transIds != null && transIds.size() > 0) {
                   //查询是否含有TransLink 有则调用批量删除Tran面板的Service
                   LOGGER.debug(String.format("selectLinkByTranIds============================", transIds));
                   List<Integer> transLinkIds = transformationLinkDao.selectLinkByTranIds(transIds);
                   if (transLinkIds != null && transLinkIds.size() > 0) {
                       LOGGER.debug(String.format("deleteTransformationPanelObj============================", transLinkIds, transIds));
                       transList = transformationService.deleteTransformationPanelObj(transLinkIds, transIds,1);
                   } else {
                       //没有TransLink情况下。根据transIds查询虚拟Transformation上的input.
                       LOGGER.debug(String.format("selectTransInputByTransIds============================", transIds));
                       List<Integer> transInputIds=transformationInputsDao.selectTransInputByTransIds(transIds);
                       //批量删除Transinput
                       if(transInputIds!=null && transInputIds.size()>0){
                           LOGGER.debug(String.format("deleteByInputIds============================", transInputIds));
                           delTranInputCount=transformationInputsDao.deleteByInputIds(transInputIds);
                       }

                       //批量删除Transformation
                       LOGGER.debug(String.format("deleteTransformations============================", transIds));
                       deleteTransCount = transformationDao.deleteTransformations(transIds);
                   }
               }
               /**
                 * 不能根据查询到的referenceColumn中ColumnId直接批量删除referenceColumn,会删除多余的数据
                 * 只能根据squidLinkIds 查找，然后批量删除referenceColumn。和查询referenceColumn条件类似。
                 */
               LOGGER.info(String.format("deleteReColumnBySquidLinkIds============================", squidLinkIds));
               deleteReColumnCount = referenceColumnDao.deleteReColumnBySquidLinkIds(squidLinkIds);
               //批量删除referenceColumnGroup
               if (reColumnGroupIds != null && reColumnGroupIds.size() > 0) {
                   LOGGER.debug(String.format("deleteGroupByIds============================", reColumnGroupIds));
                   deleteGroupCount = referenceColumnGroupDao.deleteGroupByIds(reColumnGroupIds);
               }
           }
           if(deleteTransCount<0 || deleteGroupCount<0 ||deleteReColumnCount<0 ||
                   transList.size()<0 || squids.size()<0 || delTranInputCount<0){
               flag=false;
               throw new ErrorMessageException(MessageCode.ERR_DELETE.value());
           }
       }catch (Exception e){
           throw new ErrorMessageException(MessageCode.ERR_DELETE.value(),e);
       }


       return flag;
    }



    //用于存储Trans对应的inputs信息，key是TransTypeId,值是对应的tranInputs信息。有可能一个tranType对应多个tranInputs
    private static Map<String,List<Map<String,Object>>> Inputscache = null;

    /**
     * 用于存储Trans对应的inputs信息，key是TransTypeId,值是对应的tranInputs信息。有可能一个tranType对应多个tranInputs
     * @return
     */
    public static Map<String,List<Map<String,Object>>> getInputscache() {
        if(Inputscache == null) {
            synchronized (SquidLinkService.class) {
                if(Inputscache == null) {
                    IRelationalDataManager adapter = null;
                    try {
                        adapter = DataAdapterFactory.getDefaultDataManager();
                        adapter.openSession();
                        String sql = "select GROUP_CONCAT(d.INPUT_DATA_TYPE)," +
                                "GROUP_CONCAT(d.DESCRIPTION),GROUP_CONCAT(d.INPUT_ORDER)," +
                                "p.ID from ds_tran_input_definition d " +
                                "inner join ds_transformation_type p " +
                                "on d.`CODE`=p.`CODE` group by p.ID";
                        Map<String, Object> tranInputsMap = null;
                        List<Map<String, Object>> inputsMap = adapter.query2List(true, sql, null);
                        for (Map<String, Object> map : inputsMap) {
                            //用来存放一个TransType 对应的inputs集合信息
                            List<Map<String, Object>>inputsListMap=new ArrayList<Map<String,Object>>();
                            //是根据transtype分组的，所以它是单独的
                            String transTypeId = map.get("ID").toString();
                            //取出来的可能是多个inputDataType组成的字符串
                            String inputDataType = map.get("GROUP_CONCAT(D.INPUT_DATA_TYPE)").toString();
                            //取出来的可能是多个description组成的字符串
                            String description = map.get("GROUP_CONCAT(D.DESCRIPTION)") == null ? "" : map.get("GROUP_CONCAT(D.DESCRIPTION)").toString();
                            //取出来的可能是多个inputOrder组成的字符串
                            String inputOrder = map.get("GROUP_CONCAT(D.INPUT_ORDER)").toString();
                            //表示一个TranType 对应多个tranInputs
                            if(inputDataType.indexOf(",")!=-1){
                                //将数据按照,分割，
                                List<String> dataTypes= Arrays.asList(inputDataType.split(","));
                                List<String> descriptions= Arrays.asList(description.split(","));
                                List<String> inputOrders= Arrays.asList(inputOrder.split(","));
                                //组装数据，集合的size表示TransType对应多少个tranInputs.每个集合取得位置一样，可以保证inputs信息是对应的
                                for(int i=0;i<dataTypes.size();i++){
                                    tranInputsMap = new HashMap<String, Object>();
                                    tranInputsMap.put("inputDataType", dataTypes.get(i));
                                    if(descriptions.size()>0){
                                        tranInputsMap.put("description", descriptions.get(i));
                                    }else{
                                        tranInputsMap.put("description", "");
                                    }
                                    tranInputsMap.put("inputOrder", inputOrders.get(i));
                                    //添加到inputs集合中
                                    inputsListMap.add(tranInputsMap);
                                }
                            }else {
                                tranInputsMap = new HashMap<String, Object>();
                                tranInputsMap.put("inputDataType",inputDataType);
                                tranInputsMap.put("description", description);
                                tranInputsMap.put("inputOrder", inputOrder);
                                inputsListMap.add(tranInputsMap);
                            }
                            Inputscache.put(transTypeId, inputsListMap);
                        }

                    } catch (Exception e) {
                        e.printStackTrace();
                    }finally {
                        if(adapter!=null) adapter.closeSession();
                    }
                }
            }
        }

        return Inputscache;
    }




    public Map<String,Object> createSquidLink(SquidLink squidLink){
        Map<String, Object> squidLinkMap = new HashMap<String, Object>();
        //获取上游以及下游的Squid
        int sourceSquidId=squidLink.getFrom_squid_id();
        int toSquidId=squidLink.getTo_squid_id();
        squidLink.setType(DSObjectType.SQUIDLINK.value());
        //添加squid Link
        int insertSquidLink=squidLinkDao.insertSelective(squidLink);
        squidLinkMap.put("newSquidLinkId",squidLink.getId());
        //查询下游的squid类型
        Integer toSquidTypeId=squidDao.selectSquidTypeById(toSquidId);
        //判断类型是否要添加squid Join
        if(toSquidTypeId==DSObjectType.STAGE.value() || toSquidTypeId == DSObjectType.STREAM_STAGE.value() || toSquidTypeId == DSObjectType.GROUPTAGGING.value()){
            //根据下游Squid查询该Squid上是否还有别的SquidJoin
            List<SquidJoin> squidJoins=squidJoinDao.selectJoinByToSquid(toSquidId);
            SquidJoin squidJoin=new SquidJoin();
            squidJoin.setTarget_squid_id(sourceSquidId);
            squidJoin.setJoined_squid_id(toSquidId);
            if(squidJoins!=null && squidJoins.size()>0){
                squidJoin.setPrior_join_id(squidJoins.size() + 1);
                squidJoin.setJoinType(JoinType.InnerJoin.value());
                squidJoin.setJoin_Condition("");
            }else{
                squidJoin.setPrior_join_id(1);
                squidJoin.setJoinType(JoinType.BaseTable.value());
            }
            squidJoin.setKey(StringUtils.generateGUID());
           int insertJoin=squidJoinDao.insertSelective(squidJoin);
            squidLinkMap.put("newSquidJoin",squidJoin);
        }else if(toSquidTypeId==DSObjectType.DATAMINING.value()){
            squidLinkMap.put("newSquidJoin",null);
        }
        //查询上游的column
        List<Column> sourceColumns=columnDao.selectColumnBySquid_Id(sourceSquidId);
        if(sourceColumns!=null && sourceColumns.size()>0){
            //查询下游是否referenceColumnGroup
            List<ReferenceColumnGroup> rcgs=referenceColumnGroupDao.getRefColumnGroupListBySquidId(toSquidId);
            ReferenceColumnGroup columnGroup = new ReferenceColumnGroup();
            columnGroup.setReference_squid_id(toSquidId);
            if(rcgs!=null && rcgs.size()>0){
                columnGroup.setRelative_order(rcgs.size()+1);
            }else {
                columnGroup.setRelative_order(1);
            }
            int insertColumnGroup=referenceColumnGroupDao.insertReferenceColumnGroup(columnGroup);
            squidLinkMap.put("referenceColumnGroup",columnGroup);

            SquidModelBase fromSquid=squidDao.selectByPrimaryKey(sourceSquidId);
            List<ReferenceColumn> referenceColumnList=new ArrayList<ReferenceColumn>();
            List<Transformation> transformationList=new ArrayList<Transformation>();
            List<TransformationInputs> transformationInputsList=new ArrayList<TransformationInputs>();
            //组装插入数据
            for(Column column:sourceColumns){
                //开始组装referenceColumn数据
                ReferenceColumn referenceColumn=new ReferenceColumn();
                referenceColumn.setId(column.getId());
                referenceColumn.setColumn_id(column.getId());
                referenceColumn.setData_type(column.getData_type());
                referenceColumn.setName(column.getName());
                referenceColumn.setNullable(column.isNullable());
                referenceColumn.setLength(column.getLength());
                referenceColumn.setPrecision(column.getPrecision());
                referenceColumn.setScale(column.getScale());
                referenceColumn.setRelative_order(column.getRelative_order());
                referenceColumn.setSquid_id(sourceSquidId);
                referenceColumn.setReference_squid_id(toSquidId);
                referenceColumn.setHost_squid_id(sourceSquidId);
                referenceColumn.setIsPK(column.isPK());
                referenceColumn.setIsUnique(column.isUnique());
                referenceColumn.setIs_referenced(true);
                referenceColumn.setGroup_id(columnGroup.getId());
                referenceColumn.setGroup_name(fromSquid.getName());
                referenceColumn.setType(DSObjectType.COLUMN.value());
                referenceColumnList.add(referenceColumn);
                //开始组装虚拟的Transformation数据
                Transformation transformation=new Transformation();
                transformation.setSquid_id(toSquidId);
                transformation.setColumn_id(column.getId());
                transformation.setOutput_data_type(column.getData_type());
                transformation.setTranstype(TransformationTypeEnum.VIRTUAL.value());
                transformation.setOutput_number(1);
                transformationList.add(transformation);
            }
            int insertRefeColumn=referenceColumnDao.insert(referenceColumnList);
            int insertTrans=transformationDao.insert(transformationList);
            //添加完transformation之后组装transInputs信息
            for(Transformation trans:transformationList){
                //这个集合用于给transformation中的transInputs赋值
                List<TransformationInputs> transInputsList=new ArrayList<TransformationInputs>();
                //获取transType对应的inputs信息，有可能是多个。
                List<Map<String,Object>> inputsMapList=Inputscache.get(trans.getTranstype().toString());
                if(inputsMapList!=null && inputsMapList.size()>0){
                    for(Map<String,Object> inputsMap:inputsMapList){
                        //开始组装TransInputs
                        TransformationInputs transformationInputs=new TransformationInputs();
                        //虚拟的inputs，Input_Data_Type要与column的dataType一致。
                        transformationInputs.setInput_Data_Type(trans.getOutput_data_type());
                        transformationInputs.setTransformationId(trans.getId());
                        String description=inputsMap.get("description").toString();
                        String relative_order=inputsMap.get("inputOrder").toString();
                        transformationInputs.setDescription(description);
                        transformationInputs.setRelative_order(Integer.parseInt(relative_order));
                        //这个集合是最终要向数据库中插入的数据
                        transformationInputsList.add(transformationInputs);
                        //这个用于给trans赋值
                        transInputsList.add(transformationInputs);
                    }
                }
                //给trans赋值用于返回给客户端
                trans.setInputs(transInputsList);
            }
            int insertInputs=transformationInputsDao.insert(transformationInputsList);
            squidLinkMap.put("newSourceColumns", referenceColumnList);
            squidLinkMap.put("newVTransforms", transformationList);
        }
        return squidLinkMap;
    }

    public Map<String,Object> createSamplingSquidLink(SquidLink squidLink) throws Exception{
        Map<String,Object> map=new HashMap<String,Object>();
        try{
            //创建squidLink
            if(squidLink!=null){
                squidLink.setType(DSObjectType.SQUIDLINK.value());
                squidLinkDao.insertSelective(squidLink);
                map.put("SquidLinkId",squidLink.getId());
                //获取上游Squid
                int fromSquidId=squidLink.getFrom_squid_id();
                //获取下游Squid
                int toSquidId=squidLink.getTo_squid_id();
                SamplingSquid fromSquid=samplingSquidService.selectByPrimaryKey(fromSquidId);
                SamplingSquid toSquid=samplingSquidService.selectByPrimaryKey(toSquidId);
                //修改下游squid 的sourceSquidid  sourceSquidName
                toSquid.setSourceSquidId(fromSquidId);
                toSquid.setSourceSquidName(fromSquid.getName());
                samplingSquidService.updateSamplingSquid(toSquid);
                //根据上游Squid获取它的column
                List<Column> fromColumnList=columnDao.findByFromSquidId(fromSquidId);
                //添加referenceColumnGroup
                ReferenceColumnGroup referenceColumnGroup=new ReferenceColumnGroup();
                referenceColumnGroup.setReference_squid_id(toSquidId);
                referenceColumnGroup.setRelative_order(1);
                referenceColumnGroupDao.insertSelective(referenceColumnGroup);
                //用来批量添加的referenceColumn集合
                List<ReferenceColumn> referenceColumns=new ArrayList<>();
                //用来批量添加的column集合
                List<Column> columns=new ArrayList<>();
                //用来批量添加的右边的transformation集合
                List<Transformation> transformationRightList=new ArrayList<>();
                //用来批量添加的左边的transformation集合
                List<Transformation> transformationLeftList=new ArrayList<>();
                //用来需要的添加的transformation集合
                List<Transformation> transformations =new ArrayList<>();
                //用来批量添加的transformationLink集合
                List<TransformationLink> transformationLinkList=new ArrayList<>();
                int relativeOrder=0;
                //作为递归的referencxeColumns
                Map<Integer, Object> nameColumns = new HashMap<Integer, Object>();
                if(fromColumnList!=null && fromColumnList.size()>0){
                    Column sourceColumn=null;
                    for(int i = 0; i < fromColumnList.size(); i++){
                        sourceColumn=fromColumnList.get(i);
                        ReferenceColumn referenceColumn = getMeatDataService.initReference(sourceColumn, sourceColumn.getId(), fromSquidId, toSquidId, referenceColumnGroup);
                        referenceColumns.add(referenceColumn);
                        //左侧column
                        Column column=getMeatDataService.initColumn2(referenceColumn,referenceColumn.getRelative_order(),toSquidId,null);
                        columns.add(column);
                    }
                    //批量添加referenceCOlumn
                    referenceColumnDao.insert(referenceColumns);
                    //批量添加Column
                    columnDao.insert(columns);
                    map.put("ColumnList",columns);
                    map.put("ReferenceColumnList",referenceColumns);

                    //添加右侧虚拟transformation
                    for(ReferenceColumn referenceColumn:referenceColumns){
                        transformationRightList.add(getMeatDataService.initTransformation(toSquidId,referenceColumn.getColumn_id(),TransformationTypeEnum.VIRTUAL.value(),referenceColumn.getData_type(),1));
                    }
                    //添加左侧虚拟transformation
                    for(Column column:columns){
                        transformationLeftList.add(getMeatDataService.initTransformation(toSquidId,column.getId(),TransformationTypeEnum.VIRTUAL.value(),column.getData_type(),1));
                    }
                    transformations.addAll(transformationRightList);
                    transformations.addAll(transformationLeftList);
                    //批量添加transformation
                    transformationDao.insert(transformations);
                    List<com.eurlanda.datashire.server.model.TransformationInputs> transformationInputsLists=new ArrayList<>();
                    for(Transformation transformation:transformations){
                        List<Map<String,Object>> mapList=SquidLinkService.Inputscache.get(transformation.getTranstype()+"");
                        if(mapList!=null && mapList.size()>0){
                            List<com.eurlanda.datashire.server.model.TransformationInputs> transformationInputsList=new ArrayList<>();
                            for(Map<String,Object> inputsMap:mapList){
                                com.eurlanda.datashire.server.model.TransformationInputs transformationInputs=new com.eurlanda.datashire.server.model.TransformationInputs();
                                String description=inputsMap.get("description").toString();
                                String relative_order=inputsMap.get("inputOrder").toString();
                                transformationInputs.setRelative_order(Integer.parseInt(relative_order));
                                transformationInputs.setInput_Data_Type(transformation.getOutput_data_type());
                                transformationInputs.setDescription(description);
                                transformationInputs.setSource_transform_id(0);
                                transformationInputs.setTransformationId(transformation.getId());
                                transformationInputsList.add(transformationInputs);
                            }
                            transformation.setInputs(transformationInputsList);
                            transformationInputsLists.addAll(transformationInputsList);
                        }
                    }
                    //批量创建transformationInputs
                    transformationInputsDao.insert(transformationInputsLists);
                    map.put("TransformationList",transformations);
                    //初始化transformationLink
                    for(int i=0 ;i<transformationRightList.size();i++){
                        TransformationLink transformationLink=getMeatDataService.mergeTransformationLink(transformationRightList.get(i).getId(),transformationLeftList.get(i).getId(),i+1);
                        transformationLinkList.add(transformationLink);
                    }
                    //批量添加transformationLink
                    transformationLinkDao.insert(transformationLinkList);
                    map.put("TransformationLinkList",transformationLinkList);
                    //根据下游查询下下游  同步下游
                    List<SquidLink> squidLinks=squidLinkDao.getSquidLinkListByFromSquid(toSquidId);
                    if(squidLinks!=null && squidLinks.size()>0){
                        for(SquidLink squidLink1:squidLinks){
                            SquidModelBase squidModelBase=squidDao.selectByPrimaryKey(squidLink1.getTo_squid_id());
                            getMeatDataService.synchronousInsertColumn(toSquid.getId(),squidModelBase.getId(),squidModelBase.getSquid_type(),null,columns,referenceColumns);
                        }
                    }
                }
            }
        }catch (Exception e){
                e.printStackTrace();
            LOGGER.error("创建GroupTagging Link异常", e);
            throw new ErrorMessageException(MessageCode.INSERT_ERROR.value(),e);
        }
       return  map;
    }



    /**
     * 创建pivotSquidLink
     */
    public Map<String,Object> createPivotSquidLink(SquidLink squidLink) throws Exception{
        Map<String,Object> map=new HashMap<String,Object>();
        try{
            //创建squidLink
            if(squidLink!=null){
                squidLink.setType(DSObjectType.SQUIDLINK.value());
                squidLinkDao.insertSelective(squidLink);
                map.put("SquidLinkId",squidLink.getId());
                //获取上游Squid
                int fromSquidId=squidLink.getFrom_squid_id();
                //获取下游Squid
                int toSquidId=squidLink.getTo_squid_id();


                //根据上游Squid获取它的column
                List<Column> fromColumnList=columnDao.findByFromSquidId(fromSquidId);
                //添加referenceColumnGroup
                ReferenceColumnGroup referenceColumnGroup=new ReferenceColumnGroup();
                referenceColumnGroup.setReference_squid_id(toSquidId);
                referenceColumnGroup.setRelative_order(1);
                referenceColumnGroupDao.insertSelective(referenceColumnGroup);
                //用来批量添加的referenceColumn集合
                List<ReferenceColumn> referenceColumns=new ArrayList<>();

                if(fromColumnList!=null && fromColumnList.size()>0){
                    Column sourceColumn=null;
                    for(int i = 0; i < fromColumnList.size(); i++){
                        sourceColumn=fromColumnList.get(i);
                        ReferenceColumn referenceColumn = getMeatDataService.initReference(sourceColumn, sourceColumn.getId(), fromSquidId, toSquidId, referenceColumnGroup);
                        referenceColumns.add(referenceColumn);
                    }
                    //批量添加referenceCOlumn
                    referenceColumnDao.insert(referenceColumns);
                    //批量添加Column
                    map.put("ReferenceColumnList",referenceColumns);


                    map.put("TransformationList",null);

                }
            }
        }catch (Exception e){
            e.printStackTrace();
            LOGGER.error("创建pivot Link异常", e);
            throw new ErrorMessageException(MessageCode.INSERT_ERROR.value(),e);
        }
        return  map;
    }

}
