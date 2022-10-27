package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.entity.SystemHiveConnectionSquid;
import com.eurlanda.datashire.entity.SystemHiveExtractSquid;
import com.eurlanda.datashire.entity.operation.BeyondSquidException;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.enumeration.DataBaseType;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.server.utils.TokenUtil;
import com.eurlanda.datashire.sprint7.packet.PushMessagePacket;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import com.eurlanda.datashire.utility.SysConf;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

/**
 * HIVE
 * Created by Eurlanda on 2017/3/22.
 */
@Service
public class SystemHiveService {
    private String key;
    private String token;
    private ReturnValue out;
    private Logger logger = Logger.getLogger(SystemHiveService.class);
    /**
     * 创建squid hive
     * @param info
     * @return
     */
    public String createSystemHiveSquid(String info){
        out=new ReturnValue();
        Map<String, Object> map=new HashMap<String, Object>();
        IRelationalDataManager adapter= null;
        try{
            Map<String, Object> data= JsonUtil.toHashMap(info);
            SystemHiveConnectionSquid hiveSquid =  JsonUtil.toGsonBean(data.get("SystemHiveConnectionSquid")+"", SystemHiveConnectionSquid.class);
            hiveSquid.setDb_type(DataBaseType.HIVE.value());
            hiveSquid.setHost(SysConf.getValue("hive.host")+":"+SysConf.getValue("hive.port"));
            hiveSquid.setUser_name(SysConf.getValue("hive.username"));
            hiveSquid.setSquid_type(SquidTypeEnum.HIVE.value());
            if(hiveSquid!=null){
                adapter = DataAdapterFactory.getDefaultDataManager();
                adapter.openSession();
                int squidId = adapter.insert2(hiveSquid);
                map.put("SystemHiveConnectionSquidId",squidId);
            } else {
                out.setMessageCode(MessageCode.DESERIALIZATION_FAILED);
            }
        } catch (BeyondSquidException e){
            try {
                if (adapter!=null) adapter.rollback();
            } catch (SQLException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            out.setMessageCode(MessageCode.ERR_SQUID_COUNT_MAX);
            logger.error("创建SystemHiveConnectSquid异常",e );
        } catch (Exception e) {
            // TODO: handle exception
            try {
                if (adapter!=null) adapter.rollback();
            } catch (SQLException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            out.setMessageCode(MessageCode.INSERT_ERROR);
            logger.error("创建SystemHiveConnectSquid异常",e );
        } finally {
            if (adapter!=null) adapter.closeSession();
        }
        return JsonUtil.toJsonString(map, DSObjectType.HIVE, out.getMessageCode());
    }

    /**
     * 修改hive squid
     * @param info
     * @return
     */
    public String updateSystemHiveSquid(String info){
        out = new ReturnValue();
        IRelationalDataManager adapter= null;
        Map<String, Object> paramsMap = JsonUtil.toHashMap(info);
        try {
            SystemHiveConnectionSquid hiveSquid = JsonUtil.toGsonBean(paramsMap.get("SystemHiveConnectionSquid") + "", SystemHiveConnectionSquid.class);
            hiveSquid.setDb_type(DataBaseType.HIVE.value());
            hiveSquid.setHost(SysConf.getValue("hive.host")+":"+SysConf.getValue("hive.port"));
            hiveSquid.setUser_name(SysConf.getValue("hive.username"));
            if (null != hiveSquid) {
                adapter = DataAdapterFactory.getDefaultDataManager();
                adapter.openSession();
                boolean flag = adapter.update2(hiveSquid);
                if (!flag) {
                    out.setMessageCode(MessageCode.UPDATE_ERROR);
                }
            } else {
                out.setMessageCode(MessageCode.ERR_SQUID_NULL);
            }
        } catch (Exception e){
            try {
                adapter.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
            out.setMessageCode(MessageCode.UPDATE_ERROR);
            logger.error("更新SystemHiveSquid异常", e);
        } finally {
            if(adapter!=null) {
                adapter.closeSession();
            }
        }
        return JsonUtil.toJsonString(new HashMap<String, Object>(),DSObjectType.HIVE,out.getMessageCode());
    }

    /**
     * 创建Hive Extract(批量创建)
     * @param info
     * @return
     */
    public String createSystemHiveExtractSquids(String info){
        out = new ReturnValue();
        IRelationalDataManager adapter = null;
        Timer timer = new Timer();
        try {
            final Map<String, Object> returnMaps = new HashMap<>();
            returnMaps.put("1",1);
            key = TokenUtil.getKey();
            token = TokenUtil.getToken();
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    PushMessagePacket.pushMap(returnMaps,DSObjectType.HIVE,"1051","0003",key,
                            token,MessageCode.BATCH_CODE.value());
                }
            },25 * 1000,25 * 1000);
            Map<String,Object> infoMap = JsonUtil.toHashMap(info);
            List<Integer> sourceTableIds=JsonUtil.toGsonList(infoMap.get("SourceTableIds")+"", Integer.class);
            Integer squidFlowId = Integer.parseInt(infoMap.get("SquidFlowId").toString());
            Integer connectionSquidId = Integer.parseInt(infoMap.get("ConnectionSquidId").toString());
            Integer x = Integer.parseInt(infoMap.get("X").toString());
            Integer y = Integer.parseInt(infoMap.get("Y").toString());
            HiveExtractSquidService extractSquidService = new HiveExtractSquidService();
            extractSquidService.createExtractBatch(sourceTableIds,connectionSquidId,squidFlowId,out,"1051","0003",x,y);
        } catch(Exception e){
            try {
                adapter.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
        } finally {
            timer.purge();
            timer.cancel();
            if(adapter!=null){
                adapter.closeSession();
            }
        }
        return null;
    }

    /**
     * 单独创建HiveExtractSquid
     * @param info
     * @return
     */
    public String createSystemHiveExtractSquid(String info){
        out = new ReturnValue();
        Timer timer = new Timer();
        try {
            final Map<String, Object> returnMaps = new HashMap<>();
            returnMaps.put("1",1);
            key = TokenUtil.getKey();
            token = TokenUtil.getToken();
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    PushMessagePacket.pushMap(returnMaps,DSObjectType.HIVE,"1051","0004",key,
                            token,MessageCode.BATCH_CODE.value());
                }
            },25 * 1000,25 * 1000);
            HiveExtractSquidService hiveExtractService = new HiveExtractSquidService();
            return hiveExtractService.createHiveExtract(info, out);
        } catch(Exception e){
            e.printStackTrace();
        } finally {
            timer.purge();
            timer.cancel();
        }
        return null;
    }

    /**
     * 创建extract，抽取个别列
     * @param info
     * @return
     */
    public String createSystemHiveExtractSquidByColumn(String info){
        out = new ReturnValue();
        Timer timer = new Timer();
        try {
            final Map<String, Object> returnMaps = new HashMap<>();
            returnMaps.put("1",1);
            key = TokenUtil.getKey();
            token = TokenUtil.getToken();
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    PushMessagePacket.pushMap(returnMaps,DSObjectType.HIVE,"1051","0007",key,
                            token,MessageCode.BATCH_CODE.value());
                }
            },25 * 1000,25 * 1000);
            HiveExtractSquidService hiveExtractService = new HiveExtractSquidService();
            return hiveExtractService.createHiveExtractByColumn(info, out);
        } catch(Exception e){
            e.printStackTrace();
        } finally {
            timer.cancel();
            timer.purge();
        }
        return null;
    }

    /**
     * 更新hiveExtractSquid
     * @param info
     * @return
     */
    public String updateSystemHiveExtractSquid(String info){
        out = new ReturnValue();
        IRelationalDataManager adapter= null;
        Map<String,Object> infoMap = JsonUtil.toHashMap(info);
        try {
            SystemHiveExtractSquid hiveSquid = JsonUtil.toGsonBean(infoMap.get("SystemHiveExtractSquid") + "", SystemHiveExtractSquid.class);
            if (null != hiveSquid) {
                adapter = DataAdapterFactory.getDefaultDataManager();
                adapter.openSession();
                boolean flag = adapter.update2(hiveSquid);
                if (!flag) {
                    out.setMessageCode(MessageCode.UPDATE_ERROR);
                }
            } else {
                out.setMessageCode(MessageCode.ERR_SQUID_NULL);
            }
        } catch (Exception e){
            try {
                adapter.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
            out.setMessageCode(MessageCode.UPDATE_ERROR);
            logger.error("更新HiveExtract异常", e);
        } finally {
            if(adapter!=null) {
                adapter.closeSession();
            }
        }
        return JsonUtil.toJsonString(new HashMap<String, Object>(),DSObjectType.HIVEEXTRACT,out.getMessageCode());
    }

    /**
     * 连接hive
     * @param info
     * @return
     */
    public String connectSystemHiveConnectionSquid(String info){
        Timer timer  = new Timer();
        try {
            final Map<String, Object> returnMaps = new HashMap<>();
            returnMaps.put("1",1);
            key = TokenUtil.getKey();
            token = TokenUtil.getToken();
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    PushMessagePacket.pushMap(returnMaps,DSObjectType.HIVE,"1051","0006",key,
                            token,MessageCode.BATCH_CODE.value());
                }
            },25 * 1000,25 * 1000);
            out = new ReturnValue();
            HiveExtractSquidService hiveService  = new HiveExtractSquidService();
            return hiveService.connectHiveSquid(info,out);
        } catch (Exception e) {
            logger.equals(e);
            e.printStackTrace();
        } finally {
            timer.purge();
            timer.cancel();
        }
        return null;
    }

    /**
     * 预览hive表数据
     * @param info
     * @return
     */
    public String previewHiveData(String info){
        Timer timer = new Timer();
        final Map<String, Object> returnMaps = new HashMap<>();
        returnMaps.put("1",1);
        key = TokenUtil.getKey();
        token = TokenUtil.getToken();
        try {
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    PushMessagePacket.pushMap(returnMaps,DSObjectType.SQUID,"1051","0008",key,
                            token,MessageCode.BATCH_CODE.value());
                }
            },25*1000,25*1000);
            out = new ReturnValue();
            DataShirProcess repositoryProcess = new DataShirProcess(TokenUtil.getToken());
            Map<String, Object> map = repositoryProcess.queryHiveData(info, out);
            return JsonUtil.toJsonString(map, DSObjectType.HIVE, out.getMessageCode());
        } catch (Exception e){
            timer.purge();
            timer.cancel();
        } finally {
            timer.purge();
            timer.cancel();
        }
        return null;
    }
}
