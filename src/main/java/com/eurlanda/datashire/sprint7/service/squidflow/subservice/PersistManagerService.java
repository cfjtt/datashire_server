package com.eurlanda.datashire.sprint7.service.squidflow.subservice;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.entity.DataSquid;
import com.eurlanda.datashire.server.utils.TokenUtil;
import com.eurlanda.datashire.sprint7.packet.InfoNewMessagePacket;
import com.eurlanda.datashire.sprint7.service.squidflow.PersistManagerProcess;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Eurlanda-dev on 2016/4/13.
 *
 */
@Service
public class PersistManagerService {

    private static Logger log = Logger.getLogger(PersistManagerService.class);// 记录日志
    private String token;//令牌根据令牌得到相应的连接信息
    private String key;//key值

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    //异常处理机制
    ReturnValue out = new ReturnValue();

    /**
     * 作用描述：落地数据表  返回一个sql语句给客户端
     * @param info
     * @return
     * @date 2016_04_18
     * @author dzp
     */
    public String getSQLForCreateTable (String info){
        out  = new ReturnValue();
        Map<String, Object> outputMap = new HashMap<>();
        IRelationalDataManager adapter = null;
        DataAdapterFactory adaFactory = null;
        try {
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            //获取数据Map
            Map<String, Object> paramsMap = JsonUtil.toHashMap(info);
            DataSquid currentSquid = JsonUtil.toGsonBean(paramsMap.get("DataSquid").toString(), DataSquid.class);

            if (currentSquid == null){
                out.setMessageCode(MessageCode.NODATA);
            } else {
                PersistManagerProcess process = new PersistManagerProcess(TokenUtil.getToken(),TokenUtil.getKey());
                String sql = process.getSQLForCreateTable(currentSquid,out);
                outputMap.put("SQL", sql);
            }
        } catch (Exception e){
            log.error("GetSQLForCreateTable error", e);
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
     * 作用描述：创建落地表通过SQL语句
     * @param info
     * @return
     * @date 2016_04_15
     * @author dzp
     */

    public String createTableBySQL (String info){
        Map<String, Object> outputMap = new HashMap<>();
        IRelationalDataManager adapter = null;
        DataAdapterFactory adaFactory = null;
        String SQLError = "";
        List<Integer> list = new ArrayList<Integer>();
        String newTableName = "";
        try {
            adapter = DataAdapterFactory.getDefaultDataManager();
            //获取数据Map
            Map<String, Object> paramsMap = JsonUtil.toHashMap(info);
            DataSquid currentSquid = JsonUtil.toGsonBean(paramsMap.get("DataSquid").toString(), DataSquid.class);
            String persist_sql = paramsMap.get("SQL").toString();
            adapter.openSession();
            if (currentSquid == null || persist_sql==null){
                out.setMessageCode(MessageCode.NODATA);
            } else {
                PersistManagerProcess process = new PersistManagerProcess(TokenUtil.getToken(),TokenUtil.getKey());
                newTableName = process.createTableBySQL(currentSquid,persist_sql,out,list);
            }
        } catch (Exception e){
            log.error("createTableBySQL error", e);
            outputMap.put("SQLError",e.getMessage());
            out.setMessageCode(MessageCode.INSERT_ERROR);
            try {
                adapter.rollback();
            } catch (SQLException sqlE){
                log.fatal("rollback error", sqlE);
            }
            outputMap.put("SQLError", e.getMessage());
        } finally {
            adapter.closeSession();
            outputMap.put("NewTableName",newTableName);
        }
        InfoNewMessagePacket infoPacket = new InfoNewMessagePacket(outputMap, out.getMessageCode().value());
        return infoPacket.toJsonString();
    }

}
