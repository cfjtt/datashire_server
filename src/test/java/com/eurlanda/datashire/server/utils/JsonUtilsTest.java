package com.eurlanda.datashire.server.utils;

import com.eurlanda.datashire.server.model.ScheduleJob;
import com.eurlanda.datashire.utility.JsonUtil;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by test on 2017/10/26.
 */
public class JsonUtilsTest {

    @Test
    public void test() {
        String str = "{\"JobSchedules\":[{\"Comment\":\"11\",\"Repository_id\":1,\"Project_id\":2004842,\"Squid_flow_id\":55207,\"Schedule_Type\":1,\"Cron_expression\":null,\"Job_status\":0,\"Enable_email\":0,\"Email_address\":\"\",\"Id\":0,\"Key\":\"498d526c-e080-4a04-9497-6ab8f93b4dfa\",\"Name\":\"hwjBug_Quantify_schedule\"}]}";
        Map<String,Object> dataMap = JsonUtil.toHashMap(str);
        List<Integer> idLists = new ArrayList<>();
        List<ScheduleJob> scheduleJobs = JsonUtil.toGsonList(dataMap.get("JobSchedules")+"",ScheduleJob.class);
        System.out.println(scheduleJobs);
    }

}
