package com.eurlanda.test;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.entity.DBSourceSquid;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.utility.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SquidDaoTest {
	
	public static void main(String[] args) {
		IRelationalDataManager adapter3 = null;
		long start = System.currentTimeMillis();
		Map<String, Object> outputMap = new HashMap<String, Object>(); 
		int squidTypeId = 0;
        int squid_flow_id = 1;
		try {
			// dbSourceSquid
            adapter3 = DataAdapterFactory.getDefaultDataManager();
            adapter3.openSession();
            
            String sql = "select * from ds_squid t1 left join ds_sql_connection t2 on t1.id=t2.id "
            		+ " where squid_flow_id="+squid_flow_id+" and squid_type_id="+squidTypeId;
            List<DBSourceSquid> squidList = adapter3.query2List(true, 
    				sql, null, DBSourceSquid.class);
            if(StringUtils.isNotNull(squidList) && !squidList.isEmpty()){
            	DBSourceSquid newDbSourceSquid = null;
                List<DBSourceSquid> dbSourceList = new ArrayList<DBSourceSquid>(5);
                for(int i = 0; i<squidList.size(); i++){
                	//DataPlug dataPlug = new DataPlug(token, tempAdapter);
                    if(squidTypeId == SquidTypeEnum.DBSOURCE.value()){
                    	newDbSourceSquid = squidList.get(i);
                    	//newDbSourceSquid.setSourceTableList(dataPlug.getSourceTable(newDbSourceSquid.getId(), new ReturnValue(), adapter3));
                    	newDbSourceSquid.setType(DSObjectType.DBSOURCE.value());
                    	dbSourceList.add(newDbSourceSquid);
                        if(dbSourceList.size()==2){
                        	outputMap.clear();
                        	outputMap.put("Squids", dbSourceList);
                            //PushMessagePacket.pushMap(outputMap, DSObjectType.DBSOURCE, cmd1, cmd2, key, token);
                            //dbSourceList.clear();
                            //try {sleep(100);} catch (InterruptedException e1) {}
                        }
                        System.out.println(dbSourceList.size());
                        continue;
                    }
                }
                if(!dbSourceList.isEmpty()){
                	outputMap.clear();
                	outputMap.put("Squids", dbSourceList);
                    //PushMessagePacket.pushMap(outputMap, DSObjectType.DBSOURCE, cmd1, cmd2, key, token);
                }
            }
		} catch (Exception e) {
			System.out.println("load squid flow error, squid_flow_id = "+squid_flow_id);
		} finally{
			if (adapter3!=null)
			adapter3.closeSession();
			System.out.println("dbSourceSquid 加载时间:"+(System.currentTimeMillis()-start));
			start = System.currentTimeMillis();
		}
	}
	
}
