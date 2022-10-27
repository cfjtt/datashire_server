package com.eurlanda.datashire.server.dao;


import com.eurlanda.datashire.server.model.ScheduleJob;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ScheduleJobDao {
    int deleteByPrimaryKey(Integer id);

    int insert(ScheduleJob record);

    int insertSelective(ScheduleJob record);

    ScheduleJob selectByPrimaryKey(Integer id);

    int updateByPrimaryKeySelective(ScheduleJob record);

    int updateByPrimaryKey(ScheduleJob record);

    List<ScheduleJob> selectScheduleJobByRepositoryId(Integer repositoryId);
    List<ScheduleJob> selectBySelective(ScheduleJob job);
}