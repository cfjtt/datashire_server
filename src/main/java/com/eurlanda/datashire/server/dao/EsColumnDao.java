package com.eurlanda.datashire.server.dao;

import com.eurlanda.datashire.server.model.EsColumn;
import org.springframework.stereotype.Repository;

import java.util.List;
@Repository
public interface EsColumnDao {
    int deleteByPrimaryKey(Integer id);

    int insert(EsColumn record);

    int insertSelective(EsColumn record);

    EsColumn selectByPrimaryKey(Integer id);

    int updateByPrimaryKeySelective(EsColumn record);

    int updateByPrimaryKey(EsColumn record);

    int deleteEsColumnBySquId(List<Integer> squids);

    //批量添加EsColumn
    int insertBatch(List<EsColumn> esColumns);
}