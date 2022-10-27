package com.eurlanda.datashire.server.dao;

import com.eurlanda.datashire.server.model.ReferenceColumnGroup;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;
@Repository
public interface ReferenceColumnGroupDao {
    int deleteByPrimaryKey(Integer id);

    int insert(ReferenceColumnGroup record);

    int insertSelective(ReferenceColumnGroup record);

    ReferenceColumnGroup selectByPrimaryKey(Integer id);

    int updateByPrimaryKeySelective(ReferenceColumnGroup record);

    int updateByPrimaryKey(ReferenceColumnGroup record);

    String selectByReSquIdAndHostSquId(Map<String, Object> map);

    int updateRelativeById(Integer id);
    //添加ReferenceColumnGroup
    int insertReferenceColumnGroup(ReferenceColumnGroup record);

    //根据squidId查ReferenceColumnGroup
    List<ReferenceColumnGroup> findReferenceColumnGroup(int squidId);

    List<Integer> selectGroupByReferenceIds(List<Integer> refereIds);

    int deleteGroupByIds(List<Integer> ids);

    //根据squidId删除referenceColumnGroup
    int deleteGroupBySquidId(List<Integer> list);

    ReferenceColumnGroup selectGroupBySquidId(Map<String,Integer> map);

    int selectGroupCountBySquidId(int id);

    List<ReferenceColumnGroup> getRefColumnGroupListBySquidId(int id);
}