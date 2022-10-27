package com.eurlanda.datashire.server.api;

import com.eurlanda.datashire.entity.DbSquid;
import com.eurlanda.datashire.server.annotation.SocketApi;
import com.eurlanda.datashire.server.annotation.SocketApiMethod;
import com.eurlanda.datashire.server.service.SourceTableService;
import com.eurlanda.datashire.utility.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Created by eurlanda - new 2 on 2017/7/11.
 */
@Service
@SocketApi("2010")
public class SourceTableApi {
    private static final Logger logger = LoggerFactory.getLogger(SourceTableApi.class);
    @Autowired
    SourceTableService sourceTableService;


    /**
     *
     * @param info
     * @return
     */
    @SocketApiMethod(commandId = "0001")
    public String getSourceTable(String info){
        DbSquid dbSquid= JsonUtil.toGsonBean(JsonUtil.toHashMap(info).get("Squid") + "", DbSquid.class);
        try {
            return sourceTableService.getSourceTable(dbSquid,1);
        }catch (Exception e){
            e.printStackTrace();
        }
        return "";
    }
}
