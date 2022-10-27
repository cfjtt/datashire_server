package com.eurlanda.datashire.server.dao;

import com.eurlanda.datashire.server.model.DestHiveColumn;
import org.springframework.stereotype.Repository;

import java.util.List;
@Repository
public interface DestHiveColumnDao {
    int deleteByPrimaryKey(Integer id);

    int insert(DestHiveColumn record);

    int insertSelective(DestHiveColumn record);

    DestHiveColumn selectByPrimaryKey(Integer id);

    int updateByPrimaryKeySelective(DestHiveColumn record);

    int updateByPrimaryKey(DestHiveColumn record);

    int updateColumnBySquIds(List<Integer> ids);
}