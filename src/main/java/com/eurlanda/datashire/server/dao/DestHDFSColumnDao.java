package com.eurlanda.datashire.server.dao;

import com.eurlanda.datashire.server.model.DestHDFSColumn;
import org.springframework.stereotype.Repository;

import java.util.List;
@Repository
public interface DestHDFSColumnDao {
    int deleteByPrimaryKey(Integer id);

    int insert(DestHDFSColumn record);

    int insertSelective(DestHDFSColumn record);

    DestHDFSColumn selectByPrimaryKey(Integer id);

    int updateByPrimaryKeySelective(DestHDFSColumn record);

    int updateByPrimaryKey(DestHDFSColumn record);

    int deleteHDFSColumnBySquId(List<Integer> squids);

    int insertBatch(List<DestHDFSColumn> squids);
}