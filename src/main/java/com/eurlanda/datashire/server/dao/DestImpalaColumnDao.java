package com.eurlanda.datashire.server.dao;

import com.eurlanda.datashire.server.model.DestImpalaColumn;
import org.springframework.stereotype.Repository;

import java.util.List;
@Repository
public interface DestImpalaColumnDao {
    int deleteByPrimaryKey(Integer id);

    int insert(DestImpalaColumn record);

    int insertSelective(DestImpalaColumn record);

    DestImpalaColumn selectByPrimaryKey(Integer id);

    int updateByPrimaryKeySelective(DestImpalaColumn record);

    int updateByPrimaryKey(DestImpalaColumn record);

    int deleteImpalaColumnByIds(List<Integer> ids);

    int insertBatch(List<DestImpalaColumn> destImpalaColumns);
}