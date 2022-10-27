package com.eurlanda.test;

import com.eurlanda.datashire.utility.StringUtils;
import org.junit.Assert;
import org.junit.Test;
import util.JsonUtil;

import java.util.HashMap;

public class TestTest {
	@Test
	public void testEQ(){
		int a=1;
		int b=1; 
		Assert.assertEquals(a, b);
	}

	@Test
	public void test(){
		boolean flag = StringUtils.isStartWithNumber("wwww");
		System.out.println("x");
	}

	@Test
	public void test2() {
		String str = "{\"UpdateMangoDBExtractSquid\":{\"Columns\":[],\"SourceColumns\":[],\"Is_indexed\":true,\"Is_persisted\":true,\"destination_squid_id\":94,\"top_n\":0,\"truncate_existing_data_flag\":1,\"process_mode\":0,\"cdc\":0,\"encoding\":0,\"Transformations\":[],\"TransformationLinks\":[],\"Table_name\":\"test2_cc\",\"Source_table_id\":567,\"Description\":\"12\",\"Location_x\":340,\"Location_y\":64,\"Squid_height\":70.0,\"Squid_width\":70.0,\"Squid_type\":39,\"Squidflow_id\":3,\"Filter\":\"{\\\"event_ts\\\":{$ne:null},\\\"causedby\\\":\\\"user\\\",\\\"mode\\\":\\\"wakeup\\\",\\\"status.0\\\":\\\"on\\\"}\",\"Design_status\":0,\"Variables\":[],\"Id\":92,\"Key\":\"A2AC8FF4-324F-BEB3-ED9A-CC0C718886EB\",\"Name\":\"e_test2\"}}";
		HashMap<String, Object> data = JsonUtil.toHashMap(str);
		System.out.println(data);

	}
}
