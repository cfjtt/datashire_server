package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.server.utils.TokenUtil;
import com.eurlanda.datashire.utility.CommonConsts;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.ReturnValue;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

import java.util.Map;
@Service
public class JobScheduleService {
	static Logger logger = Logger.getLogger(JobScheduleService.class);// 记录日志
	private String token;//令牌根据令牌得到相应的连接信息
	private String key;//key值
	
	public String getToken() {
		return token;
	}
	public void setToken(String token) {
		this.token = token;
	}
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	//异常处理机制
    ReturnValue out = null;
	/**
	 * 获取所有的调度任务
	 * @param info
	 * @return
	 */
	public String getAllSchedules(String info){
		out = new ReturnValue();
		JobScheduleProcess jobScheduleProcess=new JobScheduleProcess(TokenUtil.getToken());
		Map<String, Object> map=jobScheduleProcess.getAllSchedules(info, out);
		return infoNewMessagePacket(map, DSObjectType.JOBSCHEDULE, out);
	}
	/**
	 * 更新调度任务
	 * @return
	 */
	public String updateSchedules(String info)
	{
		out = new ReturnValue();
		JobScheduleProcess jobScheduleProcess=new JobScheduleProcess(TokenUtil.getToken());
		jobScheduleProcess.updateSchedules(info, out);
		return JsonUtil.toString(null, DSObjectType.JOBSCHEDULE, out);
	}
	/**
	 * 增加调度任务
	 * @return
	 */
	public String addSchedules(String info)
	{
		out = new ReturnValue();
		JobScheduleProcess jobScheduleProcess=new JobScheduleProcess(TokenUtil.getToken());
		Map<String, Object> map=jobScheduleProcess.addSchedules(info, out);
		return infoNewMessagePacket(map, DSObjectType.JOBSCHEDULE, out);
	}
	/**
	 * 删除JobSchedule
	 * @param id
	 * @param out
	 * @return
	 */
	public boolean deleteJobSchedule(int id,int squidFlowId,int repositoryId,ReturnValue out)
	{
		JobScheduleProcess jobScheduleProcess=new JobScheduleProcess(TokenUtil.getToken());
		return jobScheduleProcess.deleteJobSchedule(id,squidFlowId,repositoryId, out);
	} 
   /**
    * 启动运行日志推送
    * @param info
    * @return
    */
	public String startRunningLogs(String info) {
		out = new ReturnValue();
		Map<String, Object> map = JsonUtil.toHashMap(info);
		int timeSpan = Integer.parseInt(map.get("TimeSpan").toString());
		int job_id=Integer.parseInt(map.get("JobId").toString());
		CommonConsts.scheduleMap.put(job_id+"", TokenUtil.getKey()+"&"+TokenUtil.getToken());
		CommonConsts.scheduleToKenMap.put(TokenUtil.getToken(), job_id+"");
		/*JobLogProcess jobLogProcess=new JobLogProcess();
		JobScheduleProcess jobScheduleProcess=new JobScheduleProcess();
		//获取所有的repository-squidflow的name组合
		Map<String, Object> nameMap=jobScheduleProcess.getAllName(out);
		//启动定时任务的时候需要绑定token
		TaskUtils task = null;
		if (!CommonConsts.squidflowTimer.containsKey(token)){
			CommonConsts.squidflowTimer.put(token, new Timer(token));
			CommonConsts.stopTimer.put(token, false);
			task = new TaskUtils(out, token,jobLogProcess,key,nameMap,1,null,job_id);
			CommonConsts.timerTaskMap.put(token, task);
		}else{
			task = CommonConsts.timerTaskMap.get(token);
		}
		CommonConsts.squidflowTimer.get(token).schedule(task, 0, timeSpan * 1000);*/
		return null;
	}
	/**
	 * 暂停任务
	 * @param info
	 * @return
	 */
	public String suspendJobSchedule(String info)
	{
		out = new ReturnValue();
		JobScheduleProcess jobScheduleProcess=new JobScheduleProcess(TokenUtil.getToken());
		Map<String, Object> map = JsonUtil.toHashMap(info);
		int jobId=Integer.parseInt(map.get("JobScheduleId").toString());
		jobScheduleProcess.suspendJobSchedule(jobId, out);
		return JsonUtil.toString(null, DSObjectType.JOBSCHEDULE, out);
	}
	/**
	 * 继续任务
	 * @param info
	 * @return
	 */
	public String resumeJobSchedule(String info)
	{
		out = new ReturnValue();
	    JobScheduleProcess jobScheduleProcess=new JobScheduleProcess(TokenUtil.getToken());
	    Map<String, Object> map = JsonUtil.toHashMap(info);
		int jobId=Integer.parseInt(map.get("JobScheduleId").toString());
		jobScheduleProcess.resumeJobSchedule(jobId, out);
	    return JsonUtil.toString(null, DSObjectType.JOBSCHEDULE, out);
	}
	/**
	 * 单对象转换成Json格式
	 * 作用描述：
	 * 修改说明：
	 * @param <T>
	 * @return
	 *	 *@deprecated 请参考 JsonUtil.toString(...)
	 */
	private <T> String infoNewMessagePacket(T object,DSObjectType type,ReturnValue out) {
		return JsonUtil.toJsonString(object, type, out.getMessageCode());
	}
}
