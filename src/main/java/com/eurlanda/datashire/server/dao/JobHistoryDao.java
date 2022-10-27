package com.eurlanda.datashire.server.dao;
import com.eurlanda.datashire.server.model.JobHistory;
import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Repository;

import java.util.List;


@Repository
public interface JobHistoryDao {
    int deleteByPrimaryKey(String taskId);

    int insert(JobHistory record);

    int insertSelective(JobHistory record);

    JobHistory selectByPrimaryKey(String taskId);

    int updateByPrimaryKeySelective(JobHistory record);

    int updateByPrimaryKey(JobHistory record);
    //分页查询某个job的所有日志
    List<JobHistory> selectJobHistoryPaged(@Param("jobId")Integer jobId, @Param("homePage") Integer homePage,@Param("pageSize") Integer pageSize);

    //查询某个job的日志的数量
    int selectJobHistoryCoutByJobId(Integer jobId);

}