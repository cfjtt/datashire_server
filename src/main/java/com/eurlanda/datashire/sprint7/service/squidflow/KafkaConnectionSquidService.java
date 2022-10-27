package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.entity.KafkaSquid;
import com.eurlanda.datashire.sprint7.packet.InfoNewMessagePacket;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by yhc on 1/20/2016.
 */
@Service
public class KafkaConnectionSquidService {

    private static Log log = LogFactory.getLog(KafkaConnectionSquidService.class);

    private String token;//令牌根据令牌得到相应的连接信息
    public String getKey() {
        return key;
    }
    public void setKey(String key) {
        this.key = key;
    }

    private String key;// key值
    public String getToken() {
        return token;
    }
    public void setToken(String token) {
        this.token = token;
    }
    /**
     * 创建Kafka连接Squid
     * @param info
     * @return
     */
    public String createKafkaConnectionSquid(String info) {
        ReturnValue out = new ReturnValue();
        Map<String, Object> outputMap = new HashMap<>();
        IRelationalDataManager adapter = null;
        try {
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            //获取数据Map
            Map<String, Object> paramsMap = JsonUtil.toHashMap(info);
            KafkaSquid newKafkaSquid = JsonUtil.toGsonBean(String.valueOf(paramsMap.get("KafkaConnectionSquid")), KafkaSquid.class);
            if (newKafkaSquid == null){
                out.setMessageCode(MessageCode.NODATA);
            } else {
                int newSquidId = adapter.insert2(newKafkaSquid);
                outputMap.put("newSquidId", newSquidId);
            }
        } catch (Exception e){
            log.error("Create KafkaConnectionSquid error", e);
            out.setMessageCode(MessageCode.INSERT_ERROR);
            try {
                adapter.rollback();
            } catch (SQLException sqlE){
                log.fatal("rollback error", sqlE);
            }
        } finally {
            adapter.closeSession();
        }

        InfoNewMessagePacket infoPacket = new InfoNewMessagePacket(outputMap, out.getMessageCode().value());
        return infoPacket.toJsonString();
    }

    /**
     * 更新 Kafka Connection SquidModelBase
     * @param info
     * @return
     */
    public String updateKafkaConnectionSquid(String info){
        ReturnValue out = new ReturnValue();
        DataAdapterFactory adapterFactory = null;
        IRelationalDataManager adapter = null;

        try {
            adapterFactory = DataAdapterFactory.newInstance();
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            Map<String, Object> paramsMap = JsonUtil.toHashMap(info);
            KafkaSquid currentSquid = JsonUtil.toGsonBean(String.valueOf(paramsMap.get("KafkaConnectionSquid")), KafkaSquid.class);
            if (currentSquid != null){
                adapter.update2(currentSquid);
            } else {
              out.setMessageCode(MessageCode.NODATA);
            }
        } catch (Exception e){
            log.error("Updated Kafka Connection SquidModelBase error.", e);
            out.setMessageCode(MessageCode.UPDATE_ERROR);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                log.fatal("rollback err!", e1);
            }
        }finally {
            adapter.closeSession();
        }

        InfoNewMessagePacket infoNewMessagePacket = new InfoNewMessagePacket<>(new HashMap<>(), out.getMessageCode().value());
        return  infoNewMessagePacket.toJsonString();
    }


}
