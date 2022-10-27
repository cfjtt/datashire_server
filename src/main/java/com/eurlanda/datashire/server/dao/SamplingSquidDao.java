package com.eurlanda.datashire.server.dao;

import com.eurlanda.datashire.server.model.SamplingSquid;
import org.springframework.stereotype.Repository;

@Repository
public interface SamplingSquidDao {
    int insertSamplingSquid(SamplingSquid samplingSquid);
    int updateSamplingSquid(SamplingSquid samplingSquid);
    SamplingSquid selectByPrimaryKey(Integer id);
}
