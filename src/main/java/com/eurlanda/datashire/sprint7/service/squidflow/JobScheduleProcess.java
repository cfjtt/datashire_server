package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.dao.IJobScheduleDao;
import com.eurlanda.datashire.dao.IProjectDao;
import com.eurlanda.datashire.dao.IRepositoryDao;
import com.eurlanda.datashire.dao.ISquidFlowDao;
import com.eurlanda.datashire.dao.impl.JobScheduleDaoImpl;
import com.eurlanda.datashire.dao.impl.ProjectDaoImpl;
import com.eurlanda.datashire.dao.impl.RepositoryDaoImpl;
import com.eurlanda.datashire.dao.impl.SquidFlowDaoImpl;
import com.eurlanda.datashire.entity.JobSchedule;
import com.eurlanda.datashire.entity.Repository;
import com.eurlanda.datashire.server.utils.TokenUtil;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.RPCUtils;
import com.eurlanda.datashire.utility.ReturnValue;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JobScheduleProcess {
	static Logger logger = Logger.getLogger(JobScheduleProcess.class);// 记录日志
	private String token;
	public JobScheduleProcess()
	{
		
	}
	public JobScheduleProcess(String token)
	{
		this.token = token;
	}
	/**
	 * 获取所有的调度任务
	 * @param info
	 * @return
	 */
	public Map<String, Object> getAllSchedules(String info,ReturnValue out)
	{
		Map<String, Object> outputMap = new HashMap<String, Object>();
		IRelationalDataManager adapter = DataAdapterFactory.getDefaultDataManager();
		List<JobSchedule> jobSchedules=null;
		int repositoryId=0;
		try {
			adapter.openSession();
			Map<String, Object> map=JsonUtil.toHashMap(info);
			repositoryId=Integer.parseInt(map.get("RepositoryId").toString());
			IJobScheduleDao jobScheduleDao=new JobScheduleDaoImpl(adapter);
			jobSchedules=jobScheduleDao.getAllSchedules(repositoryId);
			outputMap.put("Schedules", jobSchedules);
		} catch (Exception e) {
			// TODO: handle exception
			out.setMessageCode(MessageCode.ERR_GETALLJOBSCHEDULES);
			try {
				adapter.rollback();
			} catch (SQLException e1) {
				logger.error("rollback err!", e1);
			}
		}finally {
			adapter.closeSession();
		}
		return outputMap;
	}

	/**
	 * 更新调度任务
	 * 
	 * @return
	 */
	public void updateSchedules(String info, ReturnValue out) {
		IRelationalDataManager adapter = DataAdapterFactory.getDefaultDataManager();
		List<JobSchedule> jobSchedules=null;
		boolean updateFlag = false;
		boolean opeartFlag=true;
		try {
			Map<String, Object> map = JsonUtil.toHashMap(info);
			 jobSchedules = JsonUtil.toGsonList(
					map.get("JobSchedules").toString(), JobSchedule.class);
			adapter.openSession();
			IJobScheduleDao jobScheduleDao=new JobScheduleDaoImpl(adapter);
			for (JobSchedule jobSchedule : jobSchedules) {
				updateFlag = jobScheduleDao.update(jobSchedule);
				if (!updateFlag) {
					out.setMessageCode(MessageCode.ERR_JOBSCHEDULES);
					break;
				}else{
					if(0!=jobSchedule.getObject_type())
					{
						//对squidflow进行加锁
						LockSquidFlowProcess lockSquidFlowProcess=new LockSquidFlowProcess(TokenUtil.getToken());
						lockSquidFlowProcess.getLockOnSquidFlow(jobSchedule.getSquid_flow_id(), jobSchedule.getProject_id(), jobSchedule.getRepository_id(), 2, out, adapter);
					}
				}
			}
		} catch (Exception e) {
			// TODO: handle exception
			opeartFlag=false;
			e.printStackTrace();
			out.setMessageCode(MessageCode.ERR_JOBSCHEDULES);
			try {
				adapter.rollback();
			} catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
				logger.fatal("rollback err!", e1);
			}
		} finally {
			adapter.closeSession();
			/*if(opeartFlag)
			{
				//调用引擎先停止后启动
				RPCUtils rpcUtils=new RPCUtils();
				rpcUtils.stopSchedules(this.getAllJobId(jobSchedules));
				rpcUtils.startSchedules(this.getAllJobId(jobSchedules));
			}*/
			
		}
	}
	/**
	 * 增加调度任务
	 * @return
	 */
	public Map<String, Object> addSchedules(String info,ReturnValue out)
	{
		Map<String, Object> outputMap = new HashMap<String, Object>();
		IRelationalDataManager adapter = DataAdapterFactory.getDefaultDataManager();
		List<JobSchedule> jobSchedules=null;
		List<Map<String, Object>> proName=null;
		List<Map<String, Object>> flowName=null;
		int executeId=0;
		List<Integer> integers=new ArrayList<Integer>();
		boolean opeartFlag=true;
		try {
			Map<String, Object> map = JsonUtil.toHashMap(info);
		    jobSchedules = JsonUtil.toGsonList(
					map.get("JobSchedules").toString(), JobSchedule.class);
			//拼装where条件
			Map<String, Object> condition=this.getCondition(jobSchedules);
			adapter.openSession();
			IJobScheduleDao jobScheduleDao=new JobScheduleDaoImpl(adapter);
			IProjectDao projectDao=new ProjectDaoImpl(adapter);
			ISquidFlowDao squidFlowDao=new SquidFlowDaoImpl(adapter);
			if (0 != jobSchedules.get(0).getObject_type()) 
			{
				proName = projectDao.getSomeProject(condition.get("project")
						.toString());
				flowName = squidFlowDao.getSomeSquidFlow(condition.get(
						"squidflow").toString());
			}
			for (JobSchedule jobSchedule : jobSchedules) {
				//jobSchedule=this.timeFormat(jobSchedule);
				if(0!=jobSchedule.getObject_type())
				{
					this.setProjectName(proName, jobSchedule);
					this.setSquidFlowName(flowName, jobSchedule);
				}
				executeId= jobScheduleDao.insert2(jobSchedule);
				if (executeId<=0) {
					out.setMessageCode(MessageCode.ERR_JOBSCHEDULES);
					break;
				}else
				{
					integers.add(executeId);
					if(0!=jobSchedule.getObject_type()) {
						//对squidflow进行加锁
						LockSquidFlowProcess lockSquidFlowProcess=new LockSquidFlowProcess(TokenUtil.getToken());
						lockSquidFlowProcess.getLockOnSquidFlow(jobSchedule.getSquid_flow_id(), jobSchedule.getProject_id(), jobSchedule.getRepository_id(), 2, out, adapter);
					}
					
				}
			}
		} catch (Exception e) {
			// TODO: handle exception
			opeartFlag=false;
			e.printStackTrace();
			out.setMessageCode(MessageCode.ERR_JOBSCHEDULES);
			try {
				adapter.rollback();
			} catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
				logger.fatal("rollback err!", e1);
			}
		}finally {
			adapter.closeSession();
			if(opeartFlag)
			{
				//调用引擎启动
				//RPCUtils rpcUtils=new RPCUtils();
				//rpcUtils.startSchedules(this.getAllJobId2(integers));
			}
		}
		outputMap.put("Ids", integers);
		return outputMap;
	}
	private void setProjectName(List<Map<String, Object>> proName,
			JobSchedule jobSchedule) {
		for(Map<String, Object> proMap:proName)
		{
			if(Integer.parseInt(proMap.get("ID").toString())==jobSchedule.getProject_id())
			{
				jobSchedule.setProject_name(proMap.get("NAME").toString());
				break;
			}
		}
	}
	private void setSquidFlowName(List<Map<String, Object>> flowName,JobSchedule jobSchedule) {
		for(Map<String, Object> flowMap:flowName)
		{
			if(Integer.parseInt(flowMap.get("ID").toString())==jobSchedule.getSquid_flow_id())
			{
				jobSchedule.setSquid_flow_name(flowMap.get("NAME").toString());
				break;
			}
		}
	}
	/**
	 * 删除JobSchedule
	 * @param id
	 * @param out
	 * @return
	 */
	public boolean deleteJobSchedule(int id,int squidFlowId,int repositoryId,ReturnValue out)
	{
		IRelationalDataManager adapter = DataAdapterFactory.getDefaultDataManager();
		Map<String, String> params = new HashMap<String, String>(); // 查询参数
		int executId=0;
		try {
			adapter.openSession();
			IJobScheduleDao jobScheduleDao=new JobScheduleDaoImpl(adapter);
			//根据id去查询该JOB_SCHEDULE的状态
			params.put("ID", String.valueOf(id));
			JobSchedule jobSchedule=jobScheduleDao.getObjectById(id, JobSchedule.class);
			if(null==jobSchedule)
			{
				out.setMessageCode(MessageCode.WARN_NOT_EXIST_JOBSCHEDULE);
				return false;
			}
			//暂停状态
			if(!jobSchedule.isJob_status())
			{
				executId=jobScheduleDao.delete(id, JobSchedule.class);
				//根据repositoryId和squidFlowId查询是否还有记录，没有记录则更新 squidflowstatus表
				List<JobSchedule> Schedules=jobScheduleDao.getAllSchedules(repositoryId, squidFlowId);
				if(Schedules.size()==0) {
					//更新squidflowstatus表的状态
					int execute=jobScheduleDao.updateScheduleStatus(repositoryId, squidFlowId);
					if(execute<=0) {
						logger.error("删除job时,更新squidflowstatus状态异常");
						return false;
					}
					LockSquidFlowProcess process = new LockSquidFlowProcess();
					process.sendAllClient(repositoryId,squidFlowId,0);
				}
			}else
			{
				out.setMessageCode(MessageCode.WARN_DELETEJOBSCHEDULE);
				return false;
			}
		} catch (Exception e) {
			// TODO: handle exception
			out.setMessageCode(MessageCode.ERR_JOBSCHEDULES);
			try {
				adapter.rollback();
			} catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
				logger.fatal("rollback err!", e1);
			}
			return false;
		}finally {
			adapter.closeSession();
		}
		return executId>=0?true :false;
	}

	/**
	 * 查询JobSchedule集合
	 * 
	 * @param repository_id
	 * @param squid_flow_id
	 * @param squid
	 * @param adapter
	 * @return
	 */
	public Map<Integer, JobSchedule> getJobSchedules(int repository_id,
			int squid_flow_id, IRelationalDataManager newAdapter,
			boolean inSession, ReturnValue out) {
		Map<Integer, JobSchedule> scheduleMap = new HashMap<Integer, JobSchedule>();
		List<JobSchedule> jobSchedules = null;
		IRelationalDataManager adapter = null;
		try {
			if (null == newAdapter) {
				adapter = DataAdapterFactory.getDefaultDataManager();
				adapter.openSession();
			} else {
				adapter = newAdapter;
			}
			IJobScheduleDao jobScheduleDao=new JobScheduleDaoImpl(adapter);
			jobSchedules = jobScheduleDao.getAllSchedules(repository_id, squid_flow_id);
			for (JobSchedule jobSchedule : jobSchedules) {
				scheduleMap.put(jobSchedule.getSquid_id(), jobSchedule);
			}
		} catch (Exception e) {
			// TODO: handle exception
			out.setMessageCode(MessageCode.ERR_JOBSCHEDULES);
			try {
				adapter.rollback();
			} catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
				logger.fatal("rollback err!", e1);
			}
		} finally {
			if(adapter != null)
			{
				adapter.closeSession();
			}
		}
		return scheduleMap;

	}
	/**
	 * 时间格式化
	 * @param jobSchedule
	 * @return
	 */
	public JobSchedule timeFormat(JobSchedule jobSchedule)
	{
		if(StringUtils.isNotBlank(jobSchedule.getDay_begin_date()))//time
		{
			jobSchedule.setDay_begin_date(jobSchedule.getDay_begin_date()+":00");
		}
		if(StringUtils.isNotBlank(jobSchedule.getDay_end_date()))//time
		{
			jobSchedule.setDay_end_date(jobSchedule.getDay_end_date()+":00");
		}
		if(StringUtils.isNotBlank(jobSchedule.getWeek_begin_date()))//time
		{
			jobSchedule.setWeek_begin_date(jobSchedule.getWeek_begin_date()+":00");
		}
		if(StringUtils.isNotBlank(jobSchedule.getMonth_begin_date()))//time
		{
			jobSchedule.setMonth_begin_date(jobSchedule.getMonth_begin_date()+":00");
		}
		return jobSchedule;
	}

	/**
	 * 拼接任务id
	 * 
	 * @param jobSchedules
	 * @return
	 */
	private String getAllJobId(List<JobSchedule> jobSchedules) {
		String allJobId = "";
		for (int i = 0; i < jobSchedules.size(); i++) {
			allJobId += jobSchedules.get(i).getId();
			if (i != jobSchedules.size() - 1) {
				allJobId += ",";
			}
		}
		return allJobId;
	}
	/**
	 * 拼接任务id
	 * 
	 * @param jobSchedules
	 * @return
	 */
	private String getAllJobId2(List<Integer> integers) {
		String allJobId = "";
		for (int i = 0; i < integers.size(); i++) {
			allJobId += integers.get(i).toString();
			if (i != integers.size() - 1) {
				allJobId += ",";
			}
		}
		return allJobId;
	}
	/**
	 * 拼接sql条件
	 * 
	 * @param jobSchedules
	 * @return
	 */
	private Map<String, Object> getCondition(List<JobSchedule> jobSchedules) {
		Map<String, Object> map = new HashMap<String, Object>();
		String project = "(";
		String squidflow = "(";
		for (int i = 0; i < jobSchedules.size(); i++) {
			project += jobSchedules.get(i).getProject_id();
			squidflow += jobSchedules.get(i).getSquid_flow_id();
			if (i != jobSchedules.size() - 1) {
				project += ",";
				squidflow += ",";
			}
		}
		project += ")";
		squidflow += ")";
		map.put("project", project);
		map.put("squidflow", squidflow);
		return map;
	}

	/**
	 * 获取repository-squidflow 的name
	 * 
	 * @param out
	 * @return
	 */
	public Map<String, Object> getAllName(ReturnValue out) {
		Map<String, Object> map = new HashMap<String, Object>();
		Map<String, Object> repNameMap = new HashMap<String, Object>();// repositoryName
		IRelationalDataManager adapter = DataAdapterFactory.getDefaultDataManager();
		try {
			adapter.openSession();
			IRepositoryDao repositoryDao=new RepositoryDaoImpl(adapter);
			IJobScheduleDao jobScheduleDao=new JobScheduleDaoImpl(adapter);
			List<Map<String, Object>> maps = jobScheduleDao.getSomeColumn();
			List<Repository> repositories = repositoryDao.getAllRepository();
			for (Repository repository : repositories) {
				repNameMap.put(String.valueOf(repository.getId()),
						repository.getName());
			}
			for (Map<String, Object> map2 : maps) {
				if (!map.containsKey(map2.get("REPOSITORY_ID") + "&"
						+ map2.get("SQUID_FLOW_ID"))) {
					map.put(map2.get("REPOSITORY_ID") + "&" + map2.get("SQUID_FLOW_ID"),
					repNameMap.get(map2.get("REPOSITORY_ID").toString()) + "&" + map2.get("SQUID_FLOW_NAME"));
				}
			}
		} catch (Exception e) {
			// TODO: handle exception
			out.setMessageCode(MessageCode.ERR_JOBLOG);
			try {
				adapter.rollback();
			} catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
				logger.error("rollback err!", e1);
			}
		} finally {
			adapter.closeSession();
		}
		return map;
	}
	/**
	 * 暂停任务
	 * @param jonId
	 * @param out
	 */
	public void suspendJobSchedule(int jobId,ReturnValue out)
	{
		IRelationalDataManager adapter = DataAdapterFactory.getDefaultDataManager();
		boolean flag=false;
		try {
			adapter.openSession();
			IJobScheduleDao jobScheduleDao=new JobScheduleDaoImpl(adapter);
			if(jobScheduleDao.suspendJobSchedule(jobId)<0)
			{
				out.setMessageCode(MessageCode.ERR_JOBSCHEDULES);
			}else
			{
				flag=true;
			}
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
			try {
				adapter.rollback();
			} catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
				logger.error("rollback err!", e1);
			}
			out.setMessageCode(MessageCode.ERR_JOBSCHEDULES);
		}finally {
			adapter.closeSession();
			if(flag)
			{
				//调用引擎启动
				RPCUtils rpcUtils=new RPCUtils();
				rpcUtils.stopSchedule(jobId);
			}
		}
	}

	/**
	 * 继续任务
	 * 
	 * @param jonId
	 * @param out
	 */
	public void resumeJobSchedule(int jobId, ReturnValue out) {
		IRelationalDataManager adapter = DataAdapterFactory.getDefaultDataManager();
		boolean flag=false;
		try {
			adapter.openSession();
			IJobScheduleDao jobScheduleDao=new JobScheduleDaoImpl(adapter);
			if (jobScheduleDao.resumeJobSchedule(jobId) < 0) {
				out.setMessageCode(MessageCode.ERR_JOBSCHEDULES);
			} else {
				flag=true;
			}
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
			try {
				adapter.rollback();
			} catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
				logger.error("rollback err!", e1);
			}
			out.setMessageCode(MessageCode.ERR_JOBSCHEDULES);
		} finally {
			adapter.closeSession();
			/*if(flag)
			{
				// 调用引擎启动
				RPCUtils rpcUtils = new RPCUtils();
				rpcUtils.startSchedule(jobId);
			}*/
		}
	}
	
	public static void main(String[] args) {
		
		try {
			ReturnValue out=new ReturnValue();
			Map<String, Object> map= new JobScheduleProcess().getAllName(out);
			/*List<JobSchedule> jobSchedules=new ArrayList<JobSchedule>();
			JobSchedule jobSchedule1=new JobSchedule();
			jobSchedule1.setProject_id(1);
			jobSchedule1.setSquid_flow_id(1);
			JobSchedule jobSchedule2=new JobSchedule();
			jobSchedule2.setProject_id(2);
			jobSchedule2.setSquid_flow_id(2);
			jobSchedules.add(jobSchedule1);
			jobSchedules.add(jobSchedule2);*/
			/* SimpleDateFormat formatter = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
			 String a=formatter.format(new Date());
			 String aa="06:00"+":00";
			 System.out.println(aa); */
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
		 
	}
}
