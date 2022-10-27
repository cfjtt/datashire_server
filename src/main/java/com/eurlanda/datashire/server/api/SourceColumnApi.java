package com.eurlanda.datashire.server.api;

import com.eurlanda.datashire.entity.DbSquid;
import com.eurlanda.datashire.entity.SourceTable;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.server.annotation.SocketApi;
import com.eurlanda.datashire.server.annotation.SocketApiMethod;
import com.eurlanda.datashire.server.service.SourceColumnService;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.SysConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Created by eurlanda - new 2 on 2017/7/12.
 */
@Service
@SocketApi("2011")
public class SourceColumnApi {

    private static final Logger logger = LoggerFactory.getLogger(SourceColumnApi.class);
    @Autowired
    SourceColumnService columnService;
    @Autowired
    com.eurlanda.datashire.sprint7.service.squidflow.RepositoryService repService;
    /**
     *
     * @param info
     * @return
     */
    @SocketApiMethod(commandId = "0001")
    public String getSourceColumn(String info){
        DbSquid dbSquid= JsonUtil.toGsonBean(JsonUtil.toHashMap(info).get("DbSquid") + "", DbSquid.class);
        if(dbSquid.getSquid_type()== SquidTypeEnum.CLOUDDB.value()
                && (SysConf.getValue("cloud_host")+":3306").equals(dbSquid.getHost())){
            int squidFlowId = dbSquid.getSquidflow_id();
            //确定链接信息
            String dbUrl = repService.getDBUrlBySquidFlowId(squidFlowId);
            dbSquid.setHost(dbUrl);
            /*dbSquid.setHost(SysConf.getValue("cloud_db_ip_port"));*/
        }
        if(dbSquid.getSquid_type()==SquidTypeEnum.TRAININGDBSQUID.value()
                && (dbSquid.getHost().equals(SysConf.getValue("train_db_host")))){
            //dbSquid.setHost(SysConf.getValue("train_db_real_host"));
            int squidFlowId = dbSquid.getSquidflow_id();
            //确定链接信息
            String dbUrl = repService.getDBUrlBySquidFlowId(squidFlowId);
            dbSquid.setHost(dbUrl);
        }
        SourceTable  sourceTable=JsonUtil.toGsonBean(JsonUtil.toHashMap(info).get("SourceTable")+ "", SourceTable.class);
        try {
            return columnService.getSourceColumn(dbSquid, sourceTable,1);
        }catch (Exception e){
            e.printStackTrace();
        }
        return "";
    }
}
