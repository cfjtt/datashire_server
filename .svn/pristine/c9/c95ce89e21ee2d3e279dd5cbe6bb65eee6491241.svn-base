package com.eurlanda.datashire.server.service;

import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.server.dao.*;
import com.eurlanda.datashire.server.exception.ErrorMessageException;
import com.eurlanda.datashire.server.model.Base.SquidModelBase;
import com.eurlanda.datashire.server.model.Column;
import com.eurlanda.datashire.server.model.ReferenceColumn;
import com.eurlanda.datashire.server.model.TransformationLink;
import com.eurlanda.datashire.utility.MessageCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import scala.Int;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by eurlanda - new 2 on 2017/6/19.
 */
@Service
public class ReferenceColumnService {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReferenceColumnService.class);
    @Autowired
    private ReferenceColumnDao referenceColumnDao;
    @Autowired
    private TransformationLinkDao transformationLinkDao;
    @Autowired
    private TransformationDao transformationDao;
    @Autowired
    private SourceColumnDao sourceColumnDao;
    @Autowired
    private TransformationInputsDao transformationInputsDao;
    @Autowired
    private ColumnDao columnDao;
    @Autowired
    private SquidDao squidDao;


    /**
     * 删除ReferenceColumn(给删除column调用)
     *
     * @param
     * @return
     */
    @Transactional(rollbackFor = {Exception.class})
    public int deleteReferenceColumn(List<Integer> ids) throws Exception {
        int count = 0;
        try {
            if (ids != null && ids.size() > 0) {
                //删除ReferenceColumn
                count = referenceColumnDao.deleteReferenceColumn(ids);
            } else {
                throw new ErrorMessageException(MessageCode.DESERIALIZATION_FAILED.value());
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
        return count;
    }

    /**
     * 删除ReferenceColumn 把对应的link 和虚拟的transformation 删掉 和删除SourceColumn
     *
     * @param ids
     * @return
     * @throws Exception
     */
    @Transactional(rollbackFor = {Exception.class})
    public int deleteReferenceColumnAndTransformation(List<Integer> ids, int fromId) throws Exception {
        int count = 0;
        List<Integer> listLink = new ArrayList<>();
        List<Integer> toTransIds = new ArrayList<>();
        Map<String, Object> paramMap = new HashMap<String, Object>();
        try {
            if (ids != null && ids.size() > 0) {
                int i = 0;
                //根据column_Id查对应的link
                List<TransformationLink> transformationLinks = transformationLinkDao.findTransformationLink(ids);
                if (transformationLinks != null && transformationLinks.size() > 0) {
                    for (TransformationLink transformationLink : transformationLinks) {
                        listLink.add(transformationLink.getId());
                        toTransIds.add(transformationLink.getTo_transformation_id());
                    }
                    //批量删除link
                    transformationLinkDao.deleteByPrimaryKey(listLink);
                    //同步下游EXCEPTIONsquid
                    List<Map<String, Object>> squidMaps = squidDao.findNextSquidByFromSquid(fromId);
                    List<Integer> columnIds = new ArrayList<>();
                    if (squidMaps != null && squidMaps.size()>0) {
                        for (Map<String, Object> squidMap : squidMaps) {
                            int toSquidId = Integer.parseInt(squidMap.get("id") + "");
                            int squidType = Integer.parseInt(squidMap.get("squid_type_id") + "");
                            if (SquidTypeEnum.EXCEPTION.value() == squidType) {
                                paramMap.put("columnIds", ids);
                                paramMap.put("squid", fromId);
                                List<String> names = referenceColumnDao.selectReferenceColumnBySquid(paramMap);
                                List<String> stringList = new ArrayList<>();
                                for (String name : names) {
                                    stringList.add("_" + name);
                                }
                                paramMap.clear();
                                paramMap.put("names", stringList);
                                paramMap.put("squid", toSquidId);
                                //查找要删除的columnId
                                columnIds = columnDao.selectColumnIdBySquidId(paramMap);
                                //查询该squid下面所有的column
                                List<Integer> columnList=columnDao.selectColumnBySquidId(toSquidId);
                                if(columnList!=null && columnList.size()>0){
                                    //移除掉要删除的column，剩下的column需要对relative_order排序。
                                    columnList.removeAll(columnIds);
                                    for(int j=0;j<columnList.size();j++){
                                        Column column=new Column();
                                        column.setId(columnList.get(j));
                                        column.setRelative_order(j+1);
                                        columnDao.updateByPrimaryKeySelective(column);
                                    }
                                }

                                break;
                            }
                        }
                        if (columnIds != null && columnIds.size() > 0) {
                            //批量删column
                            columnDao.deleteBatch(columnIds);
                            //批量删除下游左边的虚拟的transformation
                            transformationDao.deleteVirtualTransformations(columnIds);
                        }
                    } else {
                        //把对应的column的inputs的sourceTransid改为0
                        transformationInputsDao.updateTransInputsByTransId(toTransIds);
                    }
                }
                //删除虚拟的transformation
                transformationDao.deleteVirtualTransformations(ids);
                //删除SourceColumn
                sourceColumnDao.deleteSourceColumnByIds(ids);
                //删除ReferenceColumn
                count = referenceColumnDao.deleteReferenceColumn(ids);
            } else {
                throw new ErrorMessageException(MessageCode.DESERIALIZATION_FAILED.value());
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
        return count;
    }

    /**
     * 批量新增ReferenceColumn
     *
     * @param
     * @throws Exception
     */
    @Transactional(rollbackFor = {Exception.class})
    public int insertReferenceColumn(List<ReferenceColumn> referenceColumnList, Integer squid_id) throws Exception {
        int count = 0;
        try {
            if (squid_id == 0 || referenceColumnList.size() == 0) {
                throw new Exception("数据不完整");
            }
            if (null != referenceColumnList && referenceColumnList.size() > 0) {
                for (ReferenceColumn referenceColumn : referenceColumnList) {
                    referenceColumn.setReference_squid_id(squid_id);
                }
                count = referenceColumnDao.insert(referenceColumnList);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
        return count;
    }

    /**
     * 批量新增ReferenceColumn
     *
     * @param
     * @throws Exception
     */
    @Transactional(rollbackFor = {Exception.class})
    public int insertReferenceColumn(List<ReferenceColumn> referenceColumnList) throws Exception {
        int count = 0;
        try {
            if (referenceColumnList.size() == 0) {
                throw new Exception("数据不完整");
            }
            if (null != referenceColumnList && referenceColumnList.size() > 0) {
                count = referenceColumnDao.insert(referenceColumnList);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
        return count;
    }

    /**
     * 给transInputs中的 sourceTransformation 绑定名字
     *
     * @param transSquidId
     * @param transColumnId
     * @return
     * @throws SQLException
     */
    public String getRefColumnNameForTrans(int transSquidId, int transColumnId) throws SQLException {
        Map<String, Object> map = new HashMap<>();
        map.put("transSquidId", transSquidId);
        map.put("transColumnId", transColumnId);
        Map<String, Object> ref = referenceColumnDao.getRefColumnNameForTrans(map);
        String sourceName = ref == null || !ref.containsKey("NAME") ? "" : ref.get("NAME") + "";
        return sourceName;
    }
}
