package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.dao.ISquidDao;
import com.eurlanda.datashire.dao.impl.SquidDaoImpl;
import com.eurlanda.datashire.entity.StreamStageSquid;
import com.eurlanda.datashire.entity.operation.BeyondSquidException;
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
 * Created by yhc on 3/11/2016.
 */
@Service
public class StreamStageSquidService {
    private static Log log = LogFactory.getLog(StreamStageSquidService.class);

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
     * 创建 stream stage squid
     * @param info
     * @return
     */
    public String createStreamStageSquid(String info){
        ReturnValue out = new ReturnValue();
        Map<String, Object> map=new HashMap<String, Object>();
        IRelationalDataManager adapter= null;
        try {
            HashMap<String, Object> data=JsonUtil.toHashMap(info);
            StreamStageSquid currentSquid =  JsonUtil.toGsonBean(data.get("StreamStageSquid").toString(), StreamStageSquid.class);
            if(null!=currentSquid ){
                adapter = DataAdapterFactory.getDefaultDataManager();
                adapter.openSession();
                ISquidDao squidDao=new SquidDaoImpl(adapter);
                int squidId=squidDao.insert2(currentSquid);
                currentSquid.setId(squidId);
                map.put("StreamStageSquid", currentSquid);
            }else{
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
            log.error("创建StageSquids异常",e );
        } catch (Exception e) {
            // TODO: handle exception
            try {
                if (adapter!=null) adapter.rollback();
            } catch (SQLException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            out.setMessageCode(MessageCode.INSERT_ERROR);
            log.error("创建StageSquids异常",e );
        }finally{
            if (adapter!=null) adapter.closeSession();
        }
        InfoNewMessagePacket infoNewMessagePacket = new InfoNewMessagePacket<>(map, out.getMessageCode().value());
        return  infoNewMessagePacket.toJsonString();
    }

    /**
     * 更新Stream stage squid
     * @param info
     * @return
     */
    public String updateStreamStageSquid(String info){
        ReturnValue out = new ReturnValue();
        DataAdapterFactory adapterFactory = null;
        IRelationalDataManager adapter = null;

        try {
            adapterFactory = DataAdapterFactory.newInstance();
            adapter = DataAdapterFactory.getDefaultDataManager();
            Map<String, Object> paramsMap = JsonUtil.toHashMap(info);
            StreamStageSquid currentSquid = JsonUtil.toGsonBean(String.valueOf(paramsMap.get("StreamStageSquid")), StreamStageSquid.class);
            adapter.openSession();
            if (currentSquid != null){
                adapter.update2(currentSquid);
            } else {
                out.setMessageCode(MessageCode.NODATA);
            }
        } catch (Exception e){
            log.error("Updated Steam stage squid error.", e);
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
