package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.entity.CassandraConnectionSquid;
import com.eurlanda.datashire.entity.CassandraExtractSquid;
import com.eurlanda.datashire.entity.operation.BeyondSquidException;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.enumeration.DataBaseType;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.server.utils.TokenUtil;
import com.eurlanda.datashire.sprint7.packet.PushMessagePacket;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Created by Eurlanda on 2017/4/10.
 */
@Service
public class CassandraService {
    private String key;
    private String token;
    private Logger logger = Logger.getLogger(CassandraService.class);
    /**
     * 创建Cassandra squid
     * @param info
     * @return
     */
    public String createCassandraConnectionSquid(String info){
        ReturnValue out=new ReturnValue();
        Map<String, Object> map=new HashMap<String, Object>();
        IRelationalDataManager adapter= null;
        try{
            Map<String, Object> data= JsonUtil.toHashMap(info);
            CassandraConnectionSquid connectionSquid =  JsonUtil.toGsonBean(data.get("CassandraConnectionSquid")+"", CassandraConnectionSquid.class);
            if(connectionSquid!=null){
                connectionSquid.setDb_type(DataBaseType.CASSANDRA.value());
                connectionSquid.setSquid_type(SquidTypeEnum.CASSANDRA.value());
                adapter = DataAdapterFactory.getDefaultDataManager();
                adapter.openSession();
                int squidId = adapter.insert2(connectionSquid);
                map.put("CassandraConnectionSquidId",squidId);
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
            logger.error("创建CassandraConnectionSquid异常",e );
        } catch (Exception e) {
            // TODO: handle exception
            try {
                if (adapter!=null) adapter.rollback();
            } catch (SQLException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            out.setMessageCode(MessageCode.INSERT_ERROR);
            logger.error("创建CassandraConnectionSquid异常",e );
        } finally {
            if (adapter!=null) adapter.closeSession();
        }
        return JsonUtil.toJsonString(map, DSObjectType.CASSANDRA, out.getMessageCode());
    }

    /**
     * 更新CassandraSquid
     * @param info
     * @return
     */
    public String updateCassandraConnectionSquid(String info){
        ReturnValue out = new ReturnValue();
        IRelationalDataManager adapter= null;
        Map<String, Object> paramsMap = JsonUtil.toHashMap(info);
        try {
            CassandraConnectionSquid connectionSquid = JsonUtil.toGsonBean(paramsMap.get("CassandraConnectionSquid") + "", CassandraConnectionSquid.class);
            if (null != connectionSquid) {
                connectionSquid.setDb_type(DataBaseType.CASSANDRA.value());
                adapter = DataAdapterFactory.getDefaultDataManager();
                adapter.openSession();
                boolean flag = adapter.update2(connectionSquid);
                if (!flag) {
                    out.setMessageCode(MessageCode.UPDATE_ERROR);
                }
            } else {
                out.setMessageCode(MessageCode.ERR_SQUID_NULL);
            }
        } catch (Exception e){
            try {
                if(adapter!=null) {
                    adapter.rollback();
                }
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
            out.setMessageCode(MessageCode.UPDATE_ERROR);
            logger.error("更新cassandraConnectionSquid异常", e);
        } finally {
            if(adapter!=null) {
                adapter.closeSession();
            }
        }
        return JsonUtil.toJsonString(new HashMap<String, Object>(),DSObjectType.CASSANDRA,out.getMessageCode());
    }

    /**
     * 单独创建cassandra extract
     * @param info
     * @return
     */
    public String createCassandraExtractSquid(String info){
        ReturnValue out = new ReturnValue();
        Timer timer = new Timer();
        try {
            final Map<String, Object> returnMaps = new HashMap<>();
            returnMaps.put("1",1);
            key = TokenUtil.getKey();
            token = TokenUtil.getToken();
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    PushMessagePacket.pushMap(returnMaps,DSObjectType.CASSANDRA,"1061","0004",key,
                            token,MessageCode.BATCH_CODE.value());
                }
            },25 * 1000,25 * 1000);
            CassandraExtractSquidService extractSquidService = new CassandraExtractSquidService();
            return extractSquidService.createExtractSquid(info, out);
        } catch(Exception e){
            e.printStackTrace();
        } finally {
            timer.purge();
            timer.cancel();
        }
        return null;
    }

    /**
     * 批量创建Cassandra Extract
     * @param info
     */
    public void createCassandraExtractSquids(String info){
        ReturnValue out = new ReturnValue();
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
                    PushMessagePacket.pushMap(returnMaps,DSObjectType.CASSANDRA,"1061","0003",key,
                            token,MessageCode.BATCH_CODE.value());
                }
            },25 * 1000,25 * 1000);
            Map<String,Object> infoMap = JsonUtil.toHashMap(info);
            List<Integer> sourceTableIds=JsonUtil.toGsonList(infoMap.get("SourceTableIds")+"", Integer.class);
            Integer squidFlowId = Integer.parseInt(infoMap.get("SquidFlowId").toString());
            Integer connectionSquidId = Integer.parseInt(infoMap.get("ConnectionSquidId").toString());
            Integer x = Integer.parseInt(infoMap.get("X").toString());
            Integer y = Integer.parseInt(infoMap.get("Y").toString());
            CassandraExtractSquidService extractSquidService = new CassandraExtractSquidService();
            extractSquidService.createExtractBatch(sourceTableIds,connectionSquidId,squidFlowId,out,"1061","0003",x,y);
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
    }

    /**
     * 拖拽某些列创建Extract
     * @param info
     * @return
     */
    public String createCassandraExtractSquidByColumn(String info){
        ReturnValue out = new ReturnValue();
        Timer timer = new Timer();
        try {
            final Map<String, Object> returnMaps = new HashMap<>();
            returnMaps.put("1",1);
            key = TokenUtil.getKey();
            token = TokenUtil.getToken();
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    PushMessagePacket.pushMap(returnMaps,DSObjectType.CASSANDRA,"1061","0007",key,
                            token,MessageCode.BATCH_CODE.value());
                }
            },25 * 1000,25 * 1000);
            CassandraExtractSquidService extractSquidService = new CassandraExtractSquidService();
            return extractSquidService.createExtractByColumn(info, out);
        } catch(Exception e){
            e.printStackTrace();
        } finally {
            timer.cancel();
            timer.purge();
        }
        return null;
    }

    /**
     * 更新ExtractSquid
     * @param info
     * @return
     */
    public String updateCassandraExtractSquid(String info){
        ReturnValue out = new ReturnValue();
        IRelationalDataManager adapter= null;
        Map<String,Object> infoMap = JsonUtil.toHashMap(info);
        try {
            CassandraExtractSquid extractSquid = JsonUtil.toGsonBean(infoMap.get("CassandraExtractSquid") + "", CassandraExtractSquid.class);
            if (null != extractSquid) {
                adapter = DataAdapterFactory.getDefaultDataManager();
                adapter.openSession();
                boolean flag = adapter.update2(extractSquid);
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
            logger.error("更新CassExtracrSquid异常", e);
        } finally {
            if(adapter!=null) {
                adapter.closeSession();
            }
        }
        return JsonUtil.toJsonString(new HashMap<String, Object>(),DSObjectType.CASSANDRA_EXTRACT,out.getMessageCode());
    }

    /**
     * 连接squid
     * @param info
     * @return
     */
    public String connectCassandraConnectionSquid(String info){
        Timer timer  = new Timer();
        try {
            ReturnValue out =  new ReturnValue();
            final Map<String, Object> returnMaps = new HashMap<>();
            returnMaps.put("1",1);
            key = TokenUtil.getKey();
            token = TokenUtil.getToken();
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    PushMessagePacket.pushMap(returnMaps,DSObjectType.CASSANDRA,"1061","0006",key,
                            token,MessageCode.BATCH_CODE.value());
                }
            },25 * 1000,25 * 1000);
            out = new ReturnValue();
            CassandraExtractSquidService cassandraSquid  = new CassandraExtractSquidService();
            return cassandraSquid.connectCassandraSquid(info,out);
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
     * 预览数据
     * @param info
     * @return
     */
    public String previewCassandraData(String info){
        ReturnValue out =  new ReturnValue();
        Timer timer = new Timer();
        final Map<String, Object> returnMaps = new HashMap<>();
        returnMaps.put("1",1);
        key = TokenUtil.getKey();
        token = TokenUtil.getToken();
        try {
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    PushMessagePacket.pushMap(returnMaps,DSObjectType.SQUID,"1061","0008",key,
                            token,MessageCode.BATCH_CODE.value());
                }
            },25 * 1000,25 * 1000);
            DataShirProcess repositoryProcess = new DataShirProcess(TokenUtil.getToken());
            Map<String, Object> map = repositoryProcess.queryCassandraData(info,out);
            return JsonUtil.toJsonString(map, DSObjectType.CASSANDRA, out.getMessageCode());
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
