package com.eurlanda.datashire.utility;

import com.eurlanda.datashire.server.utils.TokenUtil;
import com.eurlanda.datashire.sprint7.service.squidflow.JobLogProcess;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.TimerTask;

public class TaskUtils extends TimerTask {
	static Logger logger = Logger.getLogger(TaskUtils.class);// 记录日志
	private ReturnValue out;
	private String token;
	private String key;
	private JobLogProcess jobLogProcess;
	//private Map<String,Timer> map;
	private Map<String, Object> nameMap;
	private int type;//传输日志的类型 0:指定的squidflow, 1:所有的squidflow
	private String task_id;//任务id
	private int job_id;//job_id

	public TaskUtils() {
	}

	public TaskUtils(ReturnValue out, String token,
			JobLogProcess jobLogProcess, String key,Map<String, Object> nameMap,int type,String task_id,int job_id) {
		this.out = out;
		this.token = token;
		this.jobLogProcess = jobLogProcess;
		this.key = key;
		//this.map = map;
		this.nameMap=nameMap;
		this.type=type;
		this.task_id=task_id;
		this.job_id=job_id;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		try {
			if(0==type)
			{
				if(null!=CommonConsts.stopTimer.get(TokenUtil.getToken()+"&"+task_id)&&true==CommonConsts.stopTimer.get(token+"&"+task_id))
				{
					CommonConsts.squidTimer.get(TokenUtil.getToken()+"&"+task_id).cancel();
					CommonConsts.squidTimer.remove(TokenUtil.getToken()+"&"+task_id);
					CommonConsts.stopTimer.remove(TokenUtil.getToken()+"&"+task_id);
					CommonConsts.timerTaskMap.remove(TokenUtil.getToken()+"&"+task_id);
					logger.info("===================定时任务推送终止,发起停止命令的客户端： "+token+"==任务id"+task_id+"===============");
				}else{
					jobLogProcess.startRunningLogs(out, TokenUtil.getToken(), TokenUtil.getKey(),nameMap,type,task_id,-1);
				}
			}else if(1==type)
			{
				if(null!=CommonConsts.squidflowTimer.get(TokenUtil.getToken())&&true==CommonConsts.stopTimer.get(TokenUtil.getToken()))
				{
					CommonConsts.squidflowTimer.get(TokenUtil.getToken()).cancel();
					CommonConsts.squidflowTimer.remove(TokenUtil.getToken());
					CommonConsts.stopTimer.remove(TokenUtil.getToken());
					CommonConsts.timerTaskMap.remove(TokenUtil.getToken());
					logger.info("===================定时任务推送任务终止,发起停止命令的客户端： "+token+"=================");
				}else{
					jobLogProcess.startRunningLogs(out, TokenUtil.getToken(), TokenUtil.getKey(),nameMap,type,task_id,job_id);
				}
			}
			
		} catch (Exception e) {
			// TODO: handle exception
			out.setMessageCode(MessageCode.ERR_JOBLOG);
		}
		

	}

}
