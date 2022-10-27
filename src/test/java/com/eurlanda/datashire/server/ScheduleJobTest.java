package com.eurlanda.datashire.server;

import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.server.api.ScheduleJobApi;
import com.eurlanda.datashire.server.model.JobHistory;
import com.eurlanda.datashire.server.model.ScheduleJob;
import com.eurlanda.datashire.server.service.JobHistoryService;
import com.eurlanda.datashire.server.service.ScheduleJobService;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by test on 2017/10/26.
 */

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({"classpath:config/applicationContext.xml"})
public class ScheduleJobTest {

    @Autowired
    ScheduleJobApi scheduleJobApi;

    @Autowired
    private ScheduleJobService jobService;

    @Autowired
    private JobHistoryService jobHistoryService;

    @Test
    public void test01(){

        ScheduleJob schdule = new ScheduleJob();
       // schdule.setJobStatus(99);
        schdule.setId(4);

        jobService.updateByPrimaryKeySelective(schdule);



    }

    @Test
    public void test02(){

        List<JobHistory> list = jobHistoryService.selectJobHistoryPaged(2,2,2);
        System.out.printf("11");

    }

    @Test
    public void createSysJobSchedule(){
            String info = "{\"JobSchedules\":\n" +
                    "\t[{\"Comment\":\"11\",\"Repository_id\":1,\"Project_id\":2004842,\"Squid_flow_id\":55207,\"Schedule_Type\":1,\"Cron_expression\":\"22222\",\"Job_status\":0,\"Enable_email\":0,\"Email_address\":\"\",\"Id\":0,\"Key\":\"498d526c-e080-4a04-9497-6ab8f93b4dfa\",\"Name\":\"hwjBug_Quantify_schedule\"}]}";
            String msg = scheduleJobApi.createSysJobSchedule(info);
            System.out.println(msg);

    }

    @Test
    public void updateSysJobSchedule(){
        String info = "{\"JobSchedules\":\n" +
                "\t[{\"Comment\":\"11\",\"Repository_id\":1,\"Project_id\":2004842,\"Squid_flow_id\":55207,\"Schedule_Type\":1,\"Cron_expression\":\"22222\",\"Job_status\":0,\"Enable_email\":0,\"Email_address\":\"\",\"Id\":6,\"Key\":\"498d526c-e080-4a04-9497-6ab8f93b4dfa\",\"Name\":\"update\"}]}";
        scheduleJobApi.updateSysJobSchedule(info);

    }

    @Test
    public void loadScheduleRunLog(){
        String info = "{\"JobId\":1,\"PageIndex\":1,\"PageSize\":\"2\"}";
        String msg = scheduleJobApi.loadScheduleRunLog(info);
        System.out.println(msg);

    }

}
