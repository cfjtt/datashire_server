package com.eurlanda.datashire.server.dao;


import com.eurlanda.datashire.server.model.SquidLink;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface SquidLinkDao {
    int deleteByPrimaryKey(Integer id);

    int insert(SquidLink record);

    int insertSelective(SquidLink record);

    SquidLink selectByPrimaryKey(Integer id);

    int updateByPrimaryKeySelective(SquidLink record);

    int updateByPrimaryKey(SquidLink record);

    int deleteSquidLinkByIds(List<Integer> squidLinkIds);

    List<Integer> selectSquLinkByFromSquId(Integer squIds);

    SquidLink selectBySquidId(int toSquidId);

    //根据squidId 删除Squid link
    int deleteSquidLinkBysquidId(List<Integer> squidIds);


    List<SquidLink> selectSquidLinkByIds(List<Integer> squIdLinks);

    List<Integer> selectSquidLinkByFromSquIds(List<Integer> squIds);

    List<Integer> selectSquLinkByToSquid(List<Integer> squIds);

    List<SquidLink> getSquidLinkListByFromSquid(int fromId);

    List<Integer> selectSquidByFromSquId(Integer fromSquid);

    SquidLink getSquidLinkListByToSquidId(Integer toSquidId);


}