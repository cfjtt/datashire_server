package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.adapter.db.HbaseAdapter;
import com.eurlanda.datashire.entity.DBConnectionInfo;
import com.eurlanda.datashire.entity.JobHistory;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.sprint7.packet.PushMessagePacket;
import com.eurlanda.datashire.utility.LogConsumerUtils;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JobLogProcess {
	static Logger logger = Logger.getLogger(JobLogProcess.class);// 记录日志
	private String token;
	private int currentNum=0;
	public JobLogProcess() {
		
	}
	public JobLogProcess(String token) {
		this.token = token;
	}
	
	public void  startRunningLogs(ReturnValue out,String token,String key,Map<String, Object> nameMap,int type,String task_id,int job_id)
	{
		try {
			Map<String, Object> outputMap = new HashMap<String, Object>();
			//Map<String, Object> map = JsonUtil.toHashMap(info);
			/*int repository_id=Integer.parseInt(map.get("RepositoryId").toString());
			int squid_flow_id=Integer.parseInt(map.get("SquidFlowId").toString());
			int timeSpan=Integer.parseInt(map.get("TimeSpan").toString());*/
			//boolean stopFlag=false;//停止状态
			//根据repository_id和squid_flowId查询所有集合
			List<JobHistory> histories= new ArrayList<JobHistory>();
			//调用push推送方法
			if(0==type){
				histories = this.getJobHistory11(nameMap,type,task_id,job_id);
				if(null!=histories&&histories.size()>0){
					outputMap.put("JobHistorys", histories);
					PushMessagePacket.pushMap(outputMap, DSObjectType.JOBHISTORY, "1011", "0400",key, token);
					logger.info("=================正在推送中： "+token+"==任务id "+task_id+"====================");
				}else{
					logger.info("=========暂无新数据,稍后推送： "+token+"==任务id "+task_id+"====================");	
				}
			}else if(1==type){
				histories = this.getJobHistory22(nameMap,type,task_id,job_id);
				if(null!=histories&&histories.size()>0){
					outputMap.put("JobHistorys", histories);
					PushMessagePacket.pushMap(outputMap, DSObjectType.JOBHISTORY, "1012", "0004",key, token);
					logger.info("=================正在推送中 ："+token+"====================");
				}else{
					logger.info("=========暂无新数据,稍后推送： "+token+"====================");
				}
			}
		/*	//定时任务推送日志给前端
			 Timer timer = new Timer();  
			 timer.schedule(new TaskUtils(), 0, timeSpan*1000); */ 
		} catch (Exception e) {
			// TODO: handle exception
			out.setMessageCode(MessageCode.ERR_JOBLOG);
		}
		
	} 
  /**
   * 获取hbase连接，并且查询JobHistory集合
   * @param nameMap
   * @param type
   * @return
   */
	private synchronized List<JobHistory> getJobHistory11(Map<String, Object> nameMap,int type,String task_id,int job_id) {
		//获取当前时间
		String retStrFormatNowDate = getCurrentTime();
		System.out.println(retStrFormatNowDate);
		List<JobHistory> histories = new ArrayList<JobHistory>();
		List<JobHistory> tempLog=new ArrayList<JobHistory>();
		//kafka获取对象
		List<Map<String,Object>> tempMap=LogConsumerUtils.listMap;
		//将map转为list(由于引擎和后台使用的日志对象属性名不一致,才回多出此部),后期强烈建议统一
		this.getConsumerLog(retStrFormatNowDate, tempMap, tempLog,type,nameMap);
		//ResultSet rs = null;
		//Connection conn = null;
		//String sql=null;
		try {
			//conn = CommonConsts.application_Context.getBean("dataSource_sys_hbase", DataSource.class).getConnection();
			// 结果集
			if(0==type)//指定的squidflow
			{
				for(JobHistory temp : tempLog)
				{
					if (task_id.equals(temp.getTask_id())
							&& temp.getNum() > currentNum) {
						histories.add(temp);
					}
				}
			//sql="select id,create_time,task_id,log_level,repository_id,squid_flow_id,message,job_id,squid_name from DS_SYS.SF_LOG  where task_id='"+task_id+"' and  id >"+lastId+" order by id desc limit 30";
			}else if(1==type)//所有的squidflow
			{
				for(JobHistory temp : tempLog)
				{
					if (job_id == temp.getJob_id()
							&& temp.getNum() > currentNum) {
						histories.add(temp);
					}
				}
			// sql="select id,create_time,task_id,log_level,repository_id,squid_flow_id,message,job_id,squid_name from DS_SYS.SF_LOG where job_id = "+job_id+"  and  id >"+lastId+" order by id desc limit 30";
			}
			
			//rs = conn.createStatement().executeQuery(sql);//where repository_id="+repository_id+" and squid_flow_id="+squid_flow_id+"
			/*if (histories.size()>0) {
				JobHistory jobHistory = new JobHistory();
				jobHistory.setLog_id(Integer.parseInt(rs.getString("ID")));
				jobHistory.setCreate_time(rs.getTimestamp("CREATE_TIME").toString());// 时间
				//logger.info("仓库名=="+rs.getString("REPOSITORY_ID")+"      squidflow名="+rs.getString("SQUID_FLOW_ID"));
				if(null!=rs.getString("TASK_ID"))
				{
					jobHistory.setTask_id(rs.getString("TASK_ID"));// 任务编号
				}
				if(0!=type)//不为指定squidflow时
				{
					jobHistory.setRepository_name(nameMap
							.get(rs.getString("REPOSITORY_ID") + "&"
									+ rs.getString("SQUID_FLOW_ID")).toString()
							.split("&")[0]);// 仓库名称
					jobHistory.setSquid_flow_name(nameMap
							.get(rs.getString("REPOSITORY_ID") + "&"
									+ rs.getString("SQUID_FLOW_ID")).toString()
							.split("&")[1]);// squidflow名称
				}
				if(null!=rs.getString("SQUID_NAME"))
				{
					jobHistory.setSquid_name(rs.getString("SQUID_NAME"));
				}
				if (null != rs.getString("LOG_LEVEL")) {
					jobHistory.setStatus(Integer.parseInt(rs.getString("LOG_LEVEL")));// 状态
				}
				jobHistory.setMessage(rs.getString("MESSAGE"));// 信息
				histories.add(jobHistory);
			}*/
			//lastTime=histories.get(0).getCreate_time();//获取最后一条记录的时间
			if(histories.size()>0)
			{
				currentNum=histories.get(histories.size()-1).getNum();//获取最后一条日志的id
			}
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		} /*finally {
			try {
				rs.close();
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}*/
		return histories;
	}
	
	/**
	   * 获取hbase连接，并且查询JobHistory集合
	   * @param nameMap
	   * @param type
	   * @return
	   */
	private synchronized List<JobHistory> getJobHistory22(Map<String, Object> nameMap,int type,String task_id,int job_id) {
		//获取当前时间
		String retStrFormatNowDate = getCurrentTime();
		System.out.println(retStrFormatNowDate);
		List<JobHistory> histories = new ArrayList<JobHistory>();
		ResultSet rs = null;
		Connection conn = null;
		String sql=null;
		try {
			DBConnectionInfo info = HbaseAdapter.getHbaseConnection();
			conn = new HbaseAdapter(info).getConnection();
			// 结果集
			sql="select id,create_time,task_id,log_level,repository_id,squid_flow_id,message,job_id,squid_name from DS_SYS.SF_LOG where job_id = "+job_id+"  and  id >"+currentNum+" order by id desc limit 30";
			rs = conn.createStatement().executeQuery(sql);//where repository_id="+repository_id+" and squid_flow_id="+squid_flow_id+"
			if (rs.next()) {
				JobHistory jobHistory = new JobHistory();
				jobHistory.setLog_id(Integer.parseInt(rs.getString("ID")));
				jobHistory.setCreate_time(rs.getTimestamp("CREATE_TIME").toString());// 时间
				//logger.info("仓库名=="+rs.getString("REPOSITORY_ID")+"      squidflow名="+rs.getString("SQUID_FLOW_ID"));
				if(null!=rs.getString("TASK_ID"))
				{
					jobHistory.setTask_id(rs.getString("TASK_ID"));// 任务编号
				}
				if(0!=type)//不为指定squidflow时
				{
					jobHistory.setRepository_name(nameMap
							.get(rs.getString("REPOSITORY_ID") + "&"
									+ rs.getString("SQUID_FLOW_ID")).toString()
							.split("&")[0]);// 仓库名称
					jobHistory.setSquid_flow_name(nameMap
							.get(rs.getString("REPOSITORY_ID") + "&"
									+ rs.getString("SQUID_FLOW_ID")).toString()
							.split("&")[1]);// squidflow名称
				}
				if(null!=rs.getString("SQUID_NAME"))
				{
					jobHistory.setSquid_name(rs.getString("SQUID_NAME"));
				}
				if (null != rs.getString("LOG_LEVEL")) {
					jobHistory.setStatus(Integer.parseInt(rs.getString("LOG_LEVEL")));// 状态
				}
				jobHistory.setMessage(rs.getString("MESSAGE"));// 信息
				histories.add(jobHistory);
			}
			//lastTime=histories.get(0).getCreate_time();//获取最后一条记录的时间
			if(histories.size()>0)
			{
				currentNum=histories.get(0).getLog_id();//获取最后一条日志的id
			}
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		} finally {
			try {
				rs.close();
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return histories;
	}

	public void getConsumerLog(String retStrFormatNowDate,
			List<Map<String, Object>> tempMap, List<JobHistory> tempLog,int type,Map<String, Object> nameMap) {
		for (Map<String, Object> map : tempMap) {
			JobHistory jobHistory = new JobHistory();
			jobHistory.setJob_id(Integer.parseInt(map.get("jobId").toString()));
			jobHistory.setNum(Integer.parseInt(map.get("num").toString()));
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
    /**
     * 获取当前时间
     * @return
     */
	public String getCurrentTime() {
		Date nowTime = new Date(System.currentTimeMillis());
		SimpleDateFormat sdFormatter = new SimpleDateFormat(
				"yyyy-MM-dd HH:mm:ss");
		String retStrFormatNowDate = sdFormatter.format(nowTime);
		return retStrFormatNowDate;
	}
	private List<JobHistory> getJobHistory(int repository_id,int squid_flow_id) {
		List<JobHistory> histories = new ArrayList<JobHistory>();
		try {
				JobHistory jobHistory = new JobHistory();
				jobHistory.setLog_id(1);
				jobHistory.setCreate_time("2013-05-26");// 时间
				jobHistory.setTask_id("T0001");// 任务编号
				jobHistory.setRepository_name("TEST");// 仓库名称
				jobHistory.setSquid_flow_name("SQUIDFLOW");// squidflow名称
				jobHistory.setStatus(1);// 状态
				jobHistory.setMessage("该squidflow正在运行中");// 信息
				histories.add(jobHistory);
			//lastTime=histories.get(0).getCreate_time();//获取最后一条记录的时间
			//lastId=histories.get(0).getLog_id();//获取最后一条日志的id
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		} 
		return histories;
	}

	// string类型转换为date类型
	// strTime要转换的string类型的时间，formatType要转换的格式yyyy-MM-dd HH:mm:ss//yyyy年MM月dd日
	// HH时mm分ss秒，
	// strTime的时间格式必须要与formatType的时间格式相同
	public static Date stringToDate(String strTime, String formatType)
			throws Exception {
		SimpleDateFormat formatter = new SimpleDateFormat(formatType);
		Date date = null;
		date = formatter.parse(strTime);
		return date;
	}

	// string类型转换为long类型
	// strTime要转换的String类型的时间
	// formatType时间格式
	// strTime的时间格式和formatType的时间格式必须相同
	public static long stringToLong(String strTime, String formatType)
			throws Exception {
		Date date = stringToDate(strTime, formatType); // String类型转成date类型
		if (date == null) {
			return 0;
		} else {
			long currentTime = dateToLong(date); // date类型转成long类型
			return currentTime;
		}
	}

	// date类型转换为long类型
	// date要转换的date类型的时间
	public static long dateToLong(Date date) {
		return date.getTime();
	}
	
	public static void main(String[] args) throws Exception {
	long a=	JobLogProcess.stringToLong("2014-10-14 18:07:00", "yyyy-MM-dd hh:mm:ss");
	long b=	JobLogProcess.stringToLong("2014-10-14 18:10:00", "yyyy-MM-dd hh:mm:ss");
	long c=System.currentTimeMillis();
	System.out.println(a);
	System.out.println(b);
	System.out.println("当前时间    "+c);
	if(b>a)
	{
		System.out.println("后面的时间比前面的大");
	}
	if(c>b)
	{
		System.out.println("当前时间大于把b");
	}
		//List<JobHistory> histories=	new JobLogProcess().getJobHistory(1,1);
	}
}
