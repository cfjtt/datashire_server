package com.eurlanda.datashire.utility;

import com.alibaba.fastjson.JSON;
import com.eurlanda.datashire.entity.JobHistory;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.sprint7.packet.PushMessagePacket;
import com.eurlanda.datashire.sprint7.service.squidflow.JobScheduleProcess;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

public class LogConsumerUtils {
	static Logger logger = Logger.getLogger(LogConsumerUtils.class);// 记录日志
    public  static List<Map<String,Object>> listMap = new ArrayList<Map<String,Object>>();
	private static ConsumerConnector consumer;
	// 获取topic
	private static String topic = "ds_log";

	private static long firstTime;

	private static ConsumerConnector getConsumer() throws Exception{
		if (consumer == null) {
			synchronized (LogConsumerUtils.class) {
				if (consumer == null) {
					consumer = Consumer.createJavaConsumerConnector(createConsumerConfig());
				}
			}
		}
		return consumer;
	}

	private static KafkaStream<byte[], byte[]> getKafkaLogStream() throws Exception {
		Map<String, Integer> topickMap = new HashMap<String, Integer>();
		topickMap.put(topic, 1);
		Map<String, List<KafkaStream<byte[], byte[]>>> streamMap = getConsumer()
				.createMessageStreams(topickMap);
		return streamMap.get(topic).get(0);
	}

	private static ConsumerConfig createConsumerConfig() throws Exception {
		Properties props = new Properties();
		props.put("zookeeper.connect", SysConf.getValue("ZOOKEEPER_ADDRESS"));
		props.put("group.id", SysConf.getValue("ZOOKEEPER_GROUP") + (1000 + new Random().nextInt(2000)));
		props.put("consumer.id", SysConf.getValue("ZOOKEEPER_CONSUMER"));
		props.put("zookeeper.session.timeout.ms", "10000");
		props.put("auto.commit.interval.ms", "1000");
		return new ConsumerConfig(props);
	}

	// private static LinkedBlockingQueue<SFLog> queue = new
	// LinkedBlockingQueue<>();
	/*
	 * private static final int PAGE_SIZE = 1000; private static final
	 * AtomicBoolean lock = new AtomicBoolean(false);
	 */

	public static void consumSFLog() throws Exception{
		ConsumerIterator<byte[], byte[]> iter = getKafkaLogStream().iterator();
		Map<String, Object> nameMap = new HashMap<String, Object>();
		Map<String, Object> outputMap = new HashMap<String, Object>();
		int i=1;
		while (iter.hasNext()) {
			byte[] bytes = iter.next().message();
			Map<String,Object> map= (Map<String, Object>) JSON.parse(bytes);
			String taskId = "";
			if(null!=map.get("taskId")){
				taskId = map.get("taskId").toString();
			}
			String jobId = map.get("jobId").toString();
			//System.out.println("taskId:"+taskId+"======="+jobId);
			if (CommonConsts.scheduleMap.containsKey(taskId)
					||CommonConsts.scheduleMap.containsKey(jobId)){
				listMap.add(map);
			}
		}
	}

	public static void pushSchedule() throws Exception{
		firstTime = new Date().getTime();
		while (true) {  // taskId 客户端点击启动, jobId: 调度运行的日志
			long tempTime = new Date().getTime();
			if ((tempTime-firstTime)>500||listMap.size()>1000){
				List<Map<String,Object>> tempMap = new ArrayList<Map<String,Object>>();
				synchronized (listMap) {
					tempMap.addAll(listMap);
					listMap = new ArrayList<Map<String,Object>>();
				}
				Map<String, Object> nameMap = new HashMap<String, Object>();
				Map<String, Object> outputMap = new HashMap<String, Object>();
				Map<String, List<JobHistory>> pushMap = new HashMap<String, List<JobHistory>>();

				for (Map<String, Object> map : tempMap) {
					String taskId = "";
					int type = 0;
					if(null!=map.get("taskId")){
						taskId = map.get("taskId").toString();
					}
					String jobId = map.get("jobId").toString();
					//System.out.println("taskId:"+taskId+"======="+jobId);
					if (CommonConsts.scheduleMap.containsKey(taskId)
							||CommonConsts.scheduleMap.containsKey(jobId)){
						String keyVal = "";
						if (CommonConsts.scheduleMap.containsKey(jobId)){
							type = 1;
							JobScheduleProcess jobScheduleProcess=new JobScheduleProcess();
							nameMap= jobScheduleProcess.getAllName(new ReturnValue());
							keyVal = CommonConsts.scheduleMap.get(jobId);
						}else{
							keyVal = CommonConsts.scheduleMap.get(taskId);
						}
						Date nowTime = new Date(System.currentTimeMillis());
						SimpleDateFormat sdFormatter = new SimpleDateFormat(
								"yyyy-MM-dd HH:mm:ss.SSS");
						String retStrFormatNowDate = sdFormatter.format(nowTime);
						JobHistory history = getConsumerLog(retStrFormatNowDate, map, type, nameMap);
						if (pushMap.containsKey(keyVal)){
							pushMap.get(keyVal).add(history);
						}else{
                            List<JobHistory> tempLog = new ArrayList<>();
							tempLog.add(history);
							pushMap.put(keyVal, tempLog);
						}
					}
				}
				for (String keyVal : pushMap.keySet()) {
					String key = keyVal.split("&")[0];
					String token = keyVal.split("&")[1];
					int type = 0;
					List<JobHistory> jobHistorys =  pushMap.get(keyVal);
					if (jobHistorys!=null&&jobHistorys.size()>0){
						outputMap.put("JobHistorys", jobHistorys);
						if (CommonConsts.scheduleMap.containsKey(jobHistorys.get(0).getJob_id())){
							type = 1;
						}
						if (type==0){
							PushMessagePacket.pushMap(outputMap, DSObjectType.JOBHISTORY, "1011", "0400", key, token);
						}else{
							PushMessagePacket.pushMap(outputMap, DSObjectType.JOBHISTORY, "1012", "0004",key, token);
						}
					}
				}
				firstTime = new Date().getTime();
			} else {
				Thread.sleep(500);
			}
		}
	}
	
	public static void getConsumerLog(String retStrFormatNowDate,
			List<Map<String, Object>> tempMap, List<JobHistory> tempLog,int type,Map<String, Object> nameMap) {
		for (Map<String, Object> map : tempMap) {
			JobHistory jobHistory = new JobHistory();
			jobHistory.setJob_id(Integer.parseInt(map.get("jobId").toString()));
			//jobHistory.setNum(Integer.parseInt(map.get("num").toString()));
			jobHistory.setCreate_time(retStrFormatNowDate);// 时间
			if (null != map.get("taskId")) {
				jobHistory.setTask_id(map.get("taskId").toString());// 任务编号
			}
			if (null != map.get("squidName")) {
				jobHistory.setSquid_name(map.get("squidName").toString());
			}
			if (null != map.get("logLevel")) {
				jobHistory.setStatus(Integer.parseInt(map.get("logLevel")
						.toString()));// 状态
			}
			if(0!=type)//不为指定squidflow时
			{
				jobHistory.setRepository_name(nameMap
						.get(map.get("repositoryId") + "&"
								+ map.get("squidFlowId")).toString()
						.split("&")[0]);// 仓库名称
				jobHistory.setSquid_flow_name(nameMap
						.get(map.get("repositoryId") + "&"
								+ map.get("squidFlowId")).toString()
						.split("&")[1]);// squidflow名称
			}
			jobHistory.setMessage(map.get("message").toString());// 信息
			tempLog.add(jobHistory);
		}
	}
	
	public static JobHistory getConsumerLog(String retStrFormatNowDate,
			Map<String, Object> map,int type,Map<String, Object> nameMap) {
		JobHistory jobHistory = new JobHistory();
		jobHistory.setJob_id(Integer.parseInt(map.get("jobId").toString()));
		//jobHistory.setNum(Integer.parseInt(map.get("num").toString()));
		jobHistory.setCreate_time(retStrFormatNowDate);// 时间
		if (null != map.get("taskId")) {
			jobHistory.setTask_id(map.get("taskId").toString());// 任务编号
		}
		if (null != map.get("squidName")) {
			jobHistory.setSquid_name(map.get("squidName").toString());
		}
		if (null != map.get("logLevel")) {
			jobHistory.setStatus(Integer.parseInt(map.get("logLevel")
					.toString()));// 状态
		}
		if(0!=type)//不为指定squidflow时
		{
			jobHistory.setRepository_name(nameMap
					.get(map.get("repositoryId") + "&"
							+ map.get("squidFlowId")).toString()
					.split("&")[0]);// 仓库名称
			jobHistory.setSquid_flow_name(nameMap
					.get(map.get("repositoryId") + "&"
							+ map.get("squidFlowId")).toString()
					.split("&")[1]);// squidflow名称
		}
		jobHistory.setMessage(map.get("message").toString());// 信息
		return jobHistory;
	}

	// 大于5秒
	/*
	 * public static class LogTimerTask extends TimerTask {
	 * 
	 * @Override public void run() { if (queue.size() > 0) { insertLog(); } } }
	 */
	public static void main2(String[] args) throws Exception{
		ConsumerIterator<byte[], byte[]> iter = getKafkaLogStream().iterator();
		List<JobHistory> histories=new ArrayList<JobHistory>();
		Date nowTime = new Date(System.currentTimeMillis());
		SimpleDateFormat sdFormatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		String retStrFormatNowDate = sdFormatter.format(nowTime);
		while (iter.hasNext()) {
			byte[] bytes = iter.next().message();
			System.out.println(new String(bytes));
            /**
			Map<String,Object> map= (Map<String, Object>) JSON.parse(bytes); 
			JobHistory jobHistory=new JobHistory();
			jobHistory.setJob_id(Integer.parseInt(map.get("jobId").toString()));
			jobHistory.setCreate_time(retStrFormatNowDate);// 时间
			if(null!=map.get("taskId"))
			{
				jobHistory.setTask_id(map.get("taskId").toString());// 任务编号
			}
			if(null!=map.get("squidName"))
			{
				jobHistory.setSquid_name(map.get("squidName").toString());
			}
			if (null !=map.get("logLevel")) {
				jobHistory.setStatus(Integer.parseInt(map.get("logLevel").toString()));// 状态
			}
			jobHistory.setMessage(map.get("message").toString());// 信息
			histories.add(jobHistory);
			// queue.add(sfLog);
			// 1000条
			/*
			 * if (queue.size() > PAGE_SIZE) { insertLog(); }

			// break;
			if(histories.size()>100)
			{
				break;
			}
			System.out.println("原始日志");
			/* SFLog sfLog = JSON.parseObject(bytes, SFLog.class); */
			// queue.add(sfLog);
			// 1000条
			/*
			 * if (queue.size() > PAGE_SIZE) { insertLog(); }
			 */
			/* break; */
		}
	}
}
