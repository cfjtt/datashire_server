package com.eurlanda.datashire.server.dao;

import com.eurlanda.datashire.server.model.SquidJoin;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

@Repository
public interface SquidJoinDao {
    int deleteByPrimaryKey(Integer id);

    int insert(SquidJoin record);

    int insertSelective(SquidJoin record);

    SquidJoin selectByPrimaryKey(Integer id);

    int updateByPrimaryKeySelective(SquidJoin record);

    int updateByPrimaryKey(SquidJoin record);

    SquidJoin selectJoinByFromsquidAndTosquid(Map<String,Object> map);

    int deleteSquidJoinByIds(List<Integer> joinIds);

    List<SquidJoin> selectJoinsByjoinIdAndJoinedId(Map<String,Object> map);

    int updateTypeAndPrior(Integer id);

    List<SquidJoin> selectJoinByFromsqusAndTosqus(Map<String,Object> map);

    List<SquidJoin> selectJoinBySquid(Integer squid);


    List<SquidJoin> selectJoinBysquidLinkIds(List<Integer> squids);


    List<Map<String,Object>> selectJoinByLinkIdAndGroupSquid(List<Integer> squidlink);

    List<SquidJoin> selectJoinByToSquid(Integer toSquidId);
}