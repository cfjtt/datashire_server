package com.eurlanda.datashire.dao;

import com.eurlanda.datashire.entity.JobSchedule;

import java.util.List;
import java.util.Map;

public interface IJobScheduleDao extends IBaseDao {
	/**
	 * 根据repositoryId获取所有的调度任务
	 * @param info
	 * @return
	 */
	public List<JobSchedule> getAllSchedules(int repositoryId) throws Exception;
	/**
	 * 根据repositoryId和squidflowId获取所有的调度任务
	 * @param info
	 * @return
	 */
	public List<JobSchedule> getAllSchedules(int repositoryId,int squidflowId) throws Exception;
	/**
	 * 获取部分字段的值
	 * @return
	 * @throws Exception
	 */
	public List<Map<String, Object>> getSomeColumn()throws Exception;
	/**
	 * 根据repositoryId和squidflowId更新状态
	 * @param repositoryId
	 * @param squidflowId
	 * @return
	 * @throws Exception
	 */
	public int updateScheduleStatus(int repositoryId,int squidflowId)throws Exception;
	/**
	 * 删除JobSchedule
	 * @param id
	 * @param out
	 * @return
	 */
	public boolean deleteJobSchedule(int id,int squidFlowId,int repositoryId) throws Exception;
	/**
	 * 获取repository-squidflow 的name
	 * @param out
	 * @return
	 */
	public Map<String, Object> getAllName() throws Exception;
	/**
	 * 暂停任务
	 * @param jonId
	 * @param out
	 */
	public int suspendJobSchedule(int jobId)throws Exception;
	/**
	 * 继续任务
	 * 
	 * @param jonId
	 * @param out
	 */
	public int resumeJobSchedule(int jobId) throws Exception;
}
