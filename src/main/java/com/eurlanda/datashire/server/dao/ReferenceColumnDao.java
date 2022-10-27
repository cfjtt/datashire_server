package com.eurlanda.datashire.server.dao;

import com.eurlanda.datashire.server.model.ReferenceColumn;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

@Repository
public interface ReferenceColumnDao {

    //批量新增ReferenceColumn
    int insert(List<ReferenceColumn> list);
    //新增ReferenceColumn
    int insertSelective(ReferenceColumn record);

    //批量删除ReferenceColumn
    int deleteReferenceColumn(List<Integer> list);

    //根据squidId 批量删除ReferenceColumn
    int deleteBySquidIdReferenceColumn(List<Integer> list);

    List<ReferenceColumn> selectByReSquidAndHostSquid(Map<String,Object> map);


    int deleteReColumnByFromSquAndToSqu(Map<String,Object> map);

    Map<String, Object> getRefColumnNameForTrans(Map<String,Object> map);

    List<ReferenceColumn> selectReColumnBySquLinkid(List<Integer> list);

    int deleteReColumnBySquidLinkIds(List<Integer> list);

    List<ReferenceColumn> selectByReferenceSquidId(int referenceSquidId);

    Map<String, Object> selectReferenceColumnById(Map<String, Object> map);

    List<Map<String, Object>> selectSquidByColumnId(int id);

    //根据squidId和columnId查出reference的列名
    List<String> selectReferenceColumnBySquid(Map<String, Object> map);

}