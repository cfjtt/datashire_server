package com.eurlanda.datashire.server.api;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.dao.IJobScheduleDao;
import com.eurlanda.datashire.dao.impl.JobScheduleDaoImpl;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.server.annotation.SocketApi;
import com.eurlanda.datashire.server.annotation.SocketApiMethod;
import com.eurlanda.datashire.server.model.JobHistory;
import com.eurlanda.datashire.server.model.ScheduleJob;
import com.eurlanda.datashire.server.service.JobHistoryService;
import com.eurlanda.datashire.server.service.ScheduleJobService;
import com.eurlanda.datashire.server.utils.TokenUtil;
import com.eurlanda.datashire.sprint7.service.squidflow.LockSquidFlowProcess;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.RPCUtils;
import com.eurlanda.datashire.utility.ReturnValue;
import org.quartz.CronExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@Service
@SocketApi("2016")
public class ScheduleJobApi {
    private static final Logger logger = LoggerFactory.getLogger(ScheduleJobApi.class);
    @Autowired
    private ScheduleJobService jobService;

    @Autowired
    private JobHistoryService jobHistoryService;

    /**
     * 开始调度，调用rpc和引擎通信
     * @param info
     */
    @SocketApiMethod(commandId = "0001")
    public String startJobSchedule(String info){
        //对squidflow进行加锁
        ReturnValue out = new ReturnValue();
        IRelationalDataManager adapter = DataAdapterFactory.getDefaultDataManager();
        try {
            adapter.openSession();
            Map<String,Object> paramMap = JsonUtil.toHashMap(info);
            int jobId = Integer.parseInt(paramMap.get("JobScheduleId")+"");
            ScheduleJob job = jobService.selectByPrimaryKey(jobId);
            int jobStatus = job.getJob_status();
            LockSquidFlowProcess lockSquidFlowProcess=new LockSquidFlowProcess(TokenUtil.getToken());
            lockSquidFlowProcess.getLockOnSquidFlow(job.getSquid_flow_id(), job.getProject_id(), job.getRepository_id(), 2, out, adapter);
            //更新状态
            job.setJob_status(1);
            jobService.updateByPrimaryKeySelective(job);
            //调用引擎接口
            RPCUtils rpcUtils=new RPCUtils();
            rpcUtils.startSchedules(jobId+"",jobStatus+"");
        } catch (Exception e) {
            e.printStackTrace();
            out.setMessageCode(MessageCode.ERR_JOBSCHEDULES);
        } finally {
            if(adapter!=null){
                adapter.closeSession();
            }
        }
        return JsonUtil.toJsonString(null,DSObjectType.JOBSCHEDULE,out.getMessageCode());

    }

    /**
     * 停止任务
     * @param info
     */
    @SocketApiMethod(commandId = "0003")
    public String stopJobSchedule(String info){
        ReturnValue out = new ReturnValue();
        Map<String,Object> paramMap = JsonUtil.toHashMap(info);
        int jobId = Integer.parseInt(paramMap.get("JobScheduleId")+"");
        //更新状态
        ScheduleJob job = new ScheduleJob();
        job.setId(jobId);
        job.setJob_status(0);
        try{
            jobService.updateByPrimaryKeySelective(job);
            //调用引擎接口
            RPCUtils rpcUtils=new RPCUtils();
            rpcUtils.stopSchedules(jobId+"");
        }catch (Exception e){
            e.printStackTrace();
            out.setMessageCode(MessageCode.ERR_JOBSCHEDULES);
        }
        return JsonUtil.toJsonString(null,DSObjectType.JOBSCHEDULE,out.getMessageCode());
    }

    /**
     * 暂停任务
     * @param info
     */
    @SocketApiMethod(commandId = "0002")
    public String suspendJobSchedule(String info){
        ReturnValue out = new ReturnValue();
        Map<String,Object> paramMap = JsonUtil.toHashMap(info);
        int jobId = Integer.parseInt(paramMap.get("JobScheduleId")+"");
        ScheduleJob job = new ScheduleJob();
        job.setId(jobId);
        job.setJob_status(2);
        try{
            jobService.updateByPrimaryKeySelective(job);
            //调用引擎接口
            RPCUtils rpcUtils=new RPCUtils();
            rpcUtils.stopSchedules(jobId+"");
        }catch (Exception e){
            e.printStackTrace();
            out.setMessageCode(MessageCode.ERR_JOBSCHEDULES);
        }

        return JsonUtil.toJsonString(null,DSObjectType.JOBSCHEDULE,out.getMessageCode());
    }

    @SocketApiMethod(commandId = "0004")
    public String deleteJobSchedule(String info){
        ReturnValue out = new ReturnValue();
        Map<String,Object> paramMap = JsonUtil.toHashMap(info);
        int jobId = Integer.parseInt(paramMap.get("JobScheduleId")+"");
        IRelationalDataManager adapter = DataAdapterFactory.getDefaultDataManager();
        //删除调度
        try {
            ScheduleJob job = jobService.selectByPrimaryKey(jobId);
            jobService.deleteByPrimaryKey(jobId);
            //判断此时是否还有其他的调度，如果没有，则更新调度信息
            ScheduleJob job2 = new ScheduleJob();
            job2.setSquid_flow_id(job.getSquid_flow_id());
            List<ScheduleJob> existJobs = jobService.selectBySelective(job2);
            if(existJobs.size()==0) {
                adapter.openSession();
                IJobScheduleDao jobScheduleDao = new JobScheduleDaoImpl(adapter);
                jobScheduleDao.updateScheduleStatus(job.getRepository_id(), job.getSquid_flow_id());
                //推送解锁状态(包括自己本身)
                LockSquidFlowProcess process = new LockSquidFlowProcess();
                process.sendAllClientWithSelf(job.getRepository_id(),job.getSquid_flow_id(),0);
            }
            //调用引擎接口
            RPCUtils rpcUtils=new RPCUtils();
            rpcUtils.stopSchedules(jobId+"");
        }catch (Exception e){
            e.printStackTrace();
            out.setMessageCode(MessageCode.ERR_JOBSCHEDULES);
        } finally {
            if(adapter!=null){
                adapter.closeSession();
            }
        }
        return JsonUtil.toJsonString(null,DSObjectType.JOBSCHEDULE,out.getMessageCode());

    }

    /**
     * 解析cron表达式，并返回指定次数的结果
     */
        @SocketApiMethod(commandId="0005")
    public String getCronDemos(String info){

        ReturnValue  out = new ReturnValue();
        HashMap<String,Object> dataMap =  JsonUtil.toHashMap(info);
        HashMap<String,Object> resultMap = new HashMap<>();
        String cronData = dataMap.get("CronExpression")+"";
        Integer times = Integer.parseInt(dataMap.get("Times")+"");
        //限定次数
        if (times < 1 || times > 1000){
            out.setMessageCode(MessageCode.EXPRESSION_ERROR);
            return JsonUtil.toJsonString(null, DSObjectType.JOBSCHEDULE,out.getMessageCode());
        }
        CronExpression expression = null;
        List<String> cronDemos = new ArrayList<>();
        try{
            expression = new CronExpression(cronData);
        }catch (ParseException e){
            e.printStackTrace();
            logger.debug("---------cron表达式错误!-----------------");
            out.setMessageCode(MessageCode.EXPRESSION_ERROR);
            return JsonUtil.toJsonString(null,DSObjectType.JOBSCHEDULE,out.getMessageCode());
        }
        Date date = new Date();
        for(int i=0; i<times; i++) {
            date = expression.getNextValidTimeAfter(date);
            if(date == null){
                logger.debug("---------cron表达式的时间为之前的时间-----------------");
                out.setMessageCode(MessageCode.ERR_DATA_CONVERSION);
                return JsonUtil.toJsonString(null,DSObjectType.JOBSCHEDULE,out.getMessageCode());
            }
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            cronDemos.add(sdf.format(date));
        }
        resultMap.put("CronDemos",cronDemos);
        return JsonUtil.toJsonString(resultMap, DSObjectType.JOBSCHEDULE,out.getMessageCode());
    }


    @SocketApiMethod(commandId = "0006")
    public String loadScheduleRunLog(String info){
        ReturnValue  out = new ReturnValue();
        Map<String,Object> resultMap = new HashMap<>();
        Map<String,Object> dataMap = JsonUtil.toHashMap(info);
        logger.debug("---------loadScheduleRunLog接收到的参数-------------"+info);
        Integer jobId = Integer.parseInt(dataMap.get("JobId")+"");
        Integer pageIndex = Integer.parseInt(dataMap.get("PageIndex")+"");
        Integer pageSize = Integer.parseInt(dataMap.get("PageSize")+"");
        List<JobHistory> jobHistoryList = jobHistoryService.selectJobHistoryPaged(jobId,pageIndex,pageSize);
        Integer totleCount = jobHistoryService.selectJobHistoryCoutByJobId(jobId);
        Integer totlePageCount = totleCount % pageSize == 0 ? totleCount / pageSize
                : totleCount / pageSize + 1;
        logger.debug("-----totlePageCount:"+totlePageCount+"-----jobHistoryList:---------"+jobHistoryList.toString());
        resultMap.put("JobHistorys",jobHistoryList);
        resultMap.put("TotlePageCount",totlePageCount);
        return  JsonUtil.toJsonString(resultMap,DSObjectType.JOBSCHEDULE,out.getMessageCode());
    }

    @SocketApiMethod(commandId = "0007")
    public String updateSysJobSchedule(String info){
        ReturnValue  out = new ReturnValue();
        logger.debug("---------updateSysJobSchedule接收到的参数-------------"+info);
        Map<String,Object> dataMap = JsonUtil.toHashMap(info);
        List<ScheduleJob> scheduleJobs = JsonUtil.toGsonList(dataMap.get("JobSchedules")+"",ScheduleJob.class);
        for (ScheduleJob scheduleJob:scheduleJobs) {
            jobService.updateByPrimaryKeySelective(scheduleJob);
        }
        return JsonUtil.toJsonString(null,DSObjectType.JOBSCHEDULE,out.getMessageCode());
    }

    @SocketApiMethod(commandId = "0008")
    public String createSysJobSchedule(String info){
        ReturnValue out = new ReturnValue();
        logger.debug("------------createSysJobSchedule收到的信息:---------------------"+info);
        Map<String,Object> resultMap = new HashMap<>();
        Map<String,Object> dataMap = JsonUtil.toHashMap(info);
        List<Integer> idLists = new ArrayList<>();
        List<ScheduleJob> scheduleJobs = JsonUtil.toGsonList(dataMap.get("JobSchedules")+"",ScheduleJob.class);
        for (ScheduleJob scheduleJob:scheduleJobs) {
            jobService.insertSelective(scheduleJob);
            idLists.add(scheduleJob.getId());
        }
        resultMap.put("Ids",idLists);
        return JsonUtil.toJsonString(resultMap,DSObjectType.JOBSCHEDULE,out.getMessageCode());
    }


    @SocketApiMethod(commandId = "0009")
    public String getAllSysJobSchedule(String info){
        ReturnValue out = new ReturnValue();
        Map<String,Object> resultMap = new HashMap<>();
        Map<String,Object> dataMap = JsonUtil.toHashMap(info);
        Integer repositoryId = Integer.parseInt(dataMap.get("RepositoryId")+"");
        List<ScheduleJob> scheduleJobList = jobService.selectScheduleJobByRepositoryId(repositoryId);
        resultMap.put("Schedules",scheduleJobList);
        return JsonUtil.toJsonString(resultMap,DSObjectType.JOBSCHEDULE,out.getMessageCode());
    }
}
