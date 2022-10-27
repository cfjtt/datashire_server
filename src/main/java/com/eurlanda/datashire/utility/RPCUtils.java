package com.eurlanda.datashire.utility;

import com.alibaba.fastjson.JSON;
import com.eurlanda.datashire.common.rpc.IEngineService;
import com.eurlanda.datashire.server.DatashireServer;
import com.eurlanda.datashire.server.model.User;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * RMI接口处理类
 * @author yi.zhou
 *
 */
public class RPCUtils {
	
	private static Logger logger = Logger.getLogger(RPCUtils.class);// 记录日志
	/*private static IEngineService iEngineService;
	private static IServerService iServerService;
	private static NettyTransceiver client;*/


    /**
	 * 提交流式计算SquidFlow引擎工作计划
	 * @param squidFlowId squidFlow Id
	 * @param repositoryId repository Id
	 * @param jsonParam 启动工作任务的参数，Json字符串格式
	 * @return
     */
	public String postLauchEngineFlowCalculationJob(int squidFlowId, int repositoryId, String jsonParam, ReturnValue out, User user) {
		int cnt = 0;
		try{
			Map<String, Object> jsonConfig =  JsonUtil.toHashMap(jsonParam);
			jsonConfig.put("userid", user.getId());
			//至少运行2次
			while (cnt < 2){
				try {
//					CharSequence appId = ServerEndPoint.getEngineService().launchStreamJob(squidFlowId, repositoryId, JsonUtil.toGsonString(jsonConfig));
					CharSequence appId = DatashireServer
                            .getEngineService().launchStreamJob(squidFlowId, repositoryId, JsonUtil.toGsonString(jsonConfig));
					if (appId != null){
						System.out.println(cnt);
						return appId.toString();
					}
				} catch (java.io.IOException ioe) {
					cnt += 1;
					logger.error(ioe.getMessage());
				}
			}
			out.setMessageCode(MessageCode.ERR_RPCDISCONNECTED);
			return "";
		}catch (Exception ex){
			logger.error("启动流式计算SquidFlow工作计划失败", ex);
			out.setMessageCode(MessageCode.ERR_LAUNCHENGINEJOB);
			return "";
		}
	}

	/**
	 * 提交停止流式计算SquidFlow的引擎工作计算
	 * @param applicationId 用于区分停止哪个App的Id
	 * @return 是否成功
     */
	public boolean postStopEngineFlowCalculationJob(String applicationId, ReturnValue out, User user){
		int cnt = 0;
		try {
			while (cnt < 2){
				try {
					Map<String, String> config = new HashMap<>();
					config.put("userid", user.getId() + "");
				 	return DatashireServer.getEngineService().shutdownStreamJob(applicationId, JsonUtil.toGsonString(config));
//				 	return ServerEndPoint.getEngineService().shutdownStreamJob(applicationId, JsonUtil.toGsonString(config));
				}catch (IOException ioe){
					cnt += 1;
					logger.error(ioe.getMessage());
				}
			}
			out.setMessageCode(MessageCode.ERR_RPCDISCONNECTED);
			return false;
		} catch (Exception e){
			logger.error("启动流式计算SquidFlow工作计划失败");
			out.setMessageCode(MessageCode.ERR_LAUNCHENGINEJOB);
			return false;
		}
	}

	/**
	 * 获取引擎的版本信息
	 * @return
     */
	public String getEngineVersionInfo(){
		try {
			return DatashireServer.getEngineService().test("t001").toString();
//			return ServerEndPoint.getEngineService().test("t001").toString();
		}catch (IOException ioe){
			logger.error(ioe.getMessage());
		}
		return "error";
	}

	/**
	 * 
	 * @date 2014-5-19
	 * @author yi.zhou
	 * @param reportSquidId
	 *            报表squidId
	 * @param squidFlowId
	 *            SFID
	 * @param repositoryId
	 *            仓库ID
	 * @param params
	 * 			    Debug标志参数(breakPoints:断点、dataViewers、数据查看器、destinations:目的地)
	 */
	public String postLaunchEngineJob(int reportSquidId, int squidFlowId, int repositoryId, String params, ReturnValue out) {
		String taskId = "";
		int cnt = 0;
		try{
			while(cnt<2){
				try {
//					CharSequence task = ServerEndPoint.getEngineService().launchEngineJob(squidFlowId, repositoryId, params,"");
					CharSequence task = DatashireServer.getEngineService().launchEngineJob(squidFlowId, repositoryId, params,"");
					if (task!=null){
						taskId = task.toString();
						System.out.println(cnt);
						return taskId;
					}
				} catch (java.io.IOException e){
					cnt += 1;
					logger.error(e.getMessage());
				}
			}
			out.setMessageCode(MessageCode.ERR_RPCDISCONNECTED);
			return "";
		}
		catch (Exception e) {
			logger.error("avro 异常....." + e.getMessage());
			String code = e.getMessage().substring(e.getMessage().lastIndexOf(":")+1).trim();
			out.setMessageCode(MessageCode.ERR_LAUNCHENGINEJOB);
			if(StringUtils.isNotEmpty(code) && code.matches("[0-9]+")){
				if(MessageCode.UNION_COLUMN_SIZE_IS_NOT_EQUALS.value()==Integer.parseInt(code.trim())){
					out.setMessageCode(MessageCode.UNION_COLUMN_SIZE_IS_NOT_EQUALS);
				}
			}
			return "";
		} 
	}
	
	public boolean resumeEngine(String taskId, int debugSquidId, ReturnValue out){
		int cnt = 0;
		try{
			while(cnt<2){
				try {
//					IEngineService engineService = ServerEndPoint.getEngineService();
					IEngineService engineService = DatashireServer.getEngineService();
					return engineService.resumeEngine(taskId, debugSquidId,"");
				} catch (java.io.IOException e){
					cnt += 1;
					logger.error(e.getMessage());
				}
			}
			out.setMessageCode(MessageCode.ERR_RPCDISCONNECTED);
			return false;
		}
		catch (Exception e) {
			logger.error("avro 异常....." + e.getMessage());
			out.setMessageCode(MessageCode.ERR_LAUNCHENGINEJOB);
			return false;
		}
	}
	
	public boolean stopEngine(String taskId, ReturnValue out){
		int cnt = 0;
		try{
			while(cnt<2){
				try {
//					IEngineService engineService = ServerEndPoint.getEngineService();
					IEngineService engineService = DatashireServer.getEngineService();
					return engineService.shutdownSFTask(taskId,"");
				} catch (java.io.IOException e){
					cnt += 1;
					logger.error(e.getMessage());
				}
			}
			out.setMessageCode(MessageCode.ERR_RPCDISCONNECTED);
			return false;
		}
		catch (Exception e) {
			logger.error("avro 异常....." + e.getMessage());
			out.setMessageCode(MessageCode.ERR_LAUNCHENGINEJOB);
			return false;
		}
	}

	/**
	 *元数据检查
	 *
	 * @param squidFlowId out
	 */
	public CharSequence metaDataCheck(int squidFlowId,ReturnValue out) {
		int cnt = 0;
		try{
			while(cnt<2) {
				try {
//					return ServerEndPoint.getEngineService().validateSquidFlow(squidFlowId, "");
					return DatashireServer.getEngineService().validateSquidFlow(squidFlowId, "");
				} catch (Exception e) {
					cnt += 1;
					logger.error(e.getMessage());
				}
			}
			out.setMessageCode(MessageCode.ERR_RPCDISCONNECTED);
			return "";
		}catch (Exception e) {
			logger.error("avro 异常....." + e.getMessage());
			out.setMessageCode(MessageCode.ERR_LAUNCHENGINEJOB);
			return "";
		}
	}








	/**
	 * 开始任务(多个)
	 * 
	 * @param allJobId
	 */
	public void startSchedules(String allJobId,String operationType) {
		try {
//			IEngineService engineService = ServerEndPoint.getEngineService();
			IEngineService engineService = DatashireServer.getEngineService();
			engineService.startSchedules(allJobId,operationType);
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}
	/**
	 * 开始任务(单个)
	 * @param jobId
	 */
    public void startSchedule(int jobId)
    {
    	try {
//			IEngineService engineService = ServerEndPoint.getEngineService();
			IEngineService engineService = DatashireServer.getEngineService();
			engineService.startSchedule(jobId,"");
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
    }
	/**
	 * 暂停任务(多个)
	 * 
	 * @param allJobId
	 */
	public void stopSchedules(String allJobId) {
		try {
//			IEngineService engineService = ServerEndPoint.getEngineService();
			IEngineService engineService = DatashireServer.getEngineService();
			engineService.stopSchedules(allJobId,"");
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}
	/**
	 * 暂停任务(单个)
	 * 
	 * @param JobId
	 */
	public void stopSchedule(int JobId) {
		try {
//			IEngineService engineService = ServerEndPoint.getEngineService();
			IEngineService engineService = DatashireServer.getEngineService();
			engineService.stopSchedule(JobId,"");
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}
	
	/*synchronized public static IEngineService getEngineService() {
		if (iEngineService == null) {
			try {
				if (client == null) {
					client = new NettyTransceiver(new InetSocketAddress(SysConf.getValue("rpc.engine.addr"), Integer.parseInt(SysConf.getValue("rpc.engine.port"))));
				}
				iEngineService = SpecificRequestor.getClient(IEngineService.class, client);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return iEngineService;
	}
	
	synchronized public static IServerService getServerService() {
		if (iServerService == null) {
			try {
				if (client == null) {
					client = new NettyTransceiver(new InetSocketAddress("127.0.0.1", 9002));
				}
				iServerService = SpecificRequestor.getClient(IServerService.class, client);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return iServerService;
	}*/
	
	public static void main(String[] args) {
		startSquidFlow();
	}
	
	public static void startSquidFlow(){
		//IEngineService engine =getEngineService();
		Map<String, Object> params = new HashMap<String, Object>();
		List<Integer> breakPoints = new ArrayList<Integer>();
		List<Integer> dataViewers = new ArrayList<Integer>();
		dataViewers.add(233);
		List<Integer> destinations = new ArrayList<Integer>();
		if (breakPoints!=null&&breakPoints.size()>0){
			params.put("breakPoints", breakPoints);
		}
		if (dataViewers!=null&&dataViewers.size()>0){
			params.put("dataViewers", dataViewers);
		}
		if (destinations!=null&&destinations.size()>0){
			params.put("destinations", destinations);
		}
		String temp = JSON.toJSONString(params);
		//java.lang.CharSequence params = "{\"breakPoints\":[53],\"dataViewers\":[53]}";
		String rs = new RPCUtils().postLaunchEngineJob(0, 99, 1, temp, new ReturnValue());
		System.out.println(rs);
	}
	
	public static void resumeSquid(){
		boolean flag = new RPCUtils().resumeEngine("d873af26-144d-49e2-a10e-14ccf17fb6c9", 53, new ReturnValue());
		System.out.println(flag);
	}
	
	public static void stopSquid(){
		boolean flag = new RPCUtils().stopEngine("b004d9f1-a2fb-4331-bcc8-36009a41b903", new ReturnValue());
		System.out.println(flag);
	}
	
}
