package com.eurlanda.datashire.dao.impl;

import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.dao.IJobScheduleDao;
import com.eurlanda.datashire.entity.JobSchedule;

import java.util.List;
import java.util.Map;

public class JobScheduleDaoImpl extends BaseDaoImpl implements IJobScheduleDao {
	public JobScheduleDaoImpl() {
	}

	public JobScheduleDaoImpl(IRelationalDataManager adapter) {
		this.adapter = adapter;
	}

	@Override
	public List<JobSchedule> getAllSchedules(int repositoryId) throws Exception {
		// TODO Auto-generated method stub
		String sql="select * from DS_SYS_JOB_SCHEDULE where repository_id="+repositoryId+" and object_type =1";
		List<JobSchedule> jobSchedules=adapter.query2List(true, sql, null, JobSchedule.class);
		return jobSchedules;
	}

	@Override
	public boolean deleteJobSchedule(int id, int squidFlowId, int repositoryId)
			throws Exception {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Map<String, Object> getAllName() throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int suspendJobSchedule(int jobId) throws Exception {
		// TODO Auto-generated method stub
		String sql="update DS_SYS_JOB_SCHEDULE set status='N' where id="+jobId;
		int executeId= adapter.execute(sql);
		return executeId;

	}

	@Override
	public int resumeJobSchedule(int jobId) throws Exception {
		// TODO Auto-generated method stub
		String sql = "update DS_SYS_JOB_SCHEDULE set status='Y' where id="+ jobId;
		int executeId= adapter.execute(sql);
		return executeId;
	}

	@Override
	public List<JobSchedule> getAllSchedules(int repositoryId, int squidflowId)
			throws Exception {
		// TODO Auto-generated method stub
		String sql="select * from DS_SYS_JOB_SCHEDULE where repository_id="+repositoryId+" and squid_flow_id="+squidflowId;
		List<JobSchedule> jobSchedules=adapter.query2List(true, sql, null, JobSchedule.class);
		return jobSchedules;
	}

	@Override
	public int updateScheduleStatus(int repositoryId, int squidflowId)
			throws Exception {
		// TODO Auto-generated method stub
		String sql="update DS_SYS_SQUID_FLOW_STATUS set status=0 where repository_id="+repositoryId+" and squid_flow_id="+squidflowId+"";
		int execute=adapter.execute(sql);
		return execute;
	}

	@Override
	public List<Map<String, Object>> getSomeColumn() throws Exception {
		// TODO Auto-generated method stub
		String flowNameSql = "select repository_id,squid_flow_id,squid_flow_name from DS_SYS_JOB_SCHEDULE order by id desc";
		List<Map<String, Object>> maps = adapter.query2List(true,
				flowNameSql, null);
		return maps;
	}

}
