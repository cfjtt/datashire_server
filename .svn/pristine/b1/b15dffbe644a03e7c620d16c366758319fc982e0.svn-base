package com.eurlanda.datashire.utility;

import com.eurlanda.datashire.sprint7.service.squidflow.JobLogService;
import com.eurlanda.report.global.Global;
import org.apache.log4j.Logger;

public class StopSendLog {
	
	static Logger logger = Logger.getLogger(JobLogService.class);// 记录日志
    /**
     * 停止日志推送
     * @param strToken
     */
	public void stopSendLog(String strToken) {
		if (null!=Global.getTask2Map(strToken) &&StringUtils.isNotBlank(Global.getTask2Map(strToken).toString())){
			String taskId = String.valueOf(Global.getTask2Map(strToken));
			if(null!=CommonConsts.stopTimer.get(strToken+"&"+taskId)&&StringUtils.isNotBlank(CommonConsts.stopTimer.get(strToken+"&"+taskId).toString()))
			{
				if(true!=CommonConsts.stopTimer.get(strToken+"&"+taskId))
				{
					//调用取消停止squidflow任务
					new JobLogService().stopRunningLogs(strToken, taskId);
				}
				//CommonConsts.stopTimer.remove(strToken+"&"+taskId);
			}
		}
		//调用所有取消调度任务的任务
		logger.info("调用所有取消调度任务的任务 tokey:"+strToken);
		if(null!=CommonConsts.stopTimer.get(strToken)&&StringUtils.isNotBlank(CommonConsts.stopTimer.get(strToken).toString()))
		{
			if(true!=CommonConsts.stopTimer.get(strToken))
			{
				new JobLogService(strToken).stopRunningLogs();
			}
			//CommonConsts.stopTimer.remove(strToken);
		}
	}
}
