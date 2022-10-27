package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.server.utils.TokenUtil;
import com.eurlanda.datashire.utility.CommonConsts;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.ReturnValue;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;
/**
 * 终止日志推送
 * @author 
 *
 */
@Service
public class JobLogService {
	static Logger logger = Logger.getLogger(JobLogService.class);// 记录日志
	private String token;//令牌根据令牌得到相应的连接信息
	private String key;//key值
	public JobLogService() {
	}
	public JobLogService(String token) {
		this.token=token;
	}
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
	/**
	 * 停止日志推送
	 * @return 
	 */
	public String stopRunningLogs() {
		ReturnValue out = new ReturnValue();
		//CommonConsts.stopTimer.put(token, true);
		String jobId = CommonConsts.scheduleToKenMap.get(TokenUtil.getToken());
		CommonConsts.scheduleMap.remove(jobId);
		CommonConsts.scheduleToKenMap.remove(TokenUtil.getToken());
		logger.info("============================客户端："+token+"发起停止日志命令=======================================");
        return JsonUtil.toJsonString(null, DSObjectType.JOBHISTORY, out.getMessageCode());
	}
	/**
	 * 停止日志推送
	 * @param token
	 * @param task_id
	 * @return
	 */
	public void stopRunningLogs(String token,String task_id) {
		CommonConsts.stopTimer.put(token+"&"+task_id, true);
		logger.info("============================客户端："+token+",任务ID："+task_id+"发起停止日志的命令=======================================");
	}
}
