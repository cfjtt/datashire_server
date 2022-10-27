package com.eurlanda.datashire.server.dao;

import com.eurlanda.datashire.server.model.DestCassandraColumn;
import org.springframework.stereotype.Repository;

import java.util.List;
@Repository
public interface DestCassandraColumnDao {
    int deleteByPrimaryKey(Integer id);

    int insert(DestCassandraColumn record);

    int insertSelective(DestCassandraColumn record);

    DestCassandraColumn selectByPrimaryKey(Integer id);

    int updateByPrimaryKeySelective(DestCassandraColumn record);

    int updateByPrimaryKey(DestCassandraColumn record);

    int updateColumnBySquIds(List<Integer> squIds);
}