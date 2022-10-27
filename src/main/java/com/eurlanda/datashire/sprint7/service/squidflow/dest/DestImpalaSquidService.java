package com.eurlanda.datashire.sprint7.service.squidflow.dest;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.entity.Column;
import com.eurlanda.datashire.entity.SquidLink;
import com.eurlanda.datashire.entity.dest.DestImpalaColumn;
import com.eurlanda.datashire.entity.dest.DestImpalaSquid;
import com.eurlanda.datashire.sprint7.packet.InfoNewMessagePacket;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Created by dzp on 2016/5/13.
 */
@Service
public class DestImpalaSquidService {
    private static Log log = LogFactory.getLog(DestEsSquidService.class);
    private String token;//令牌根据令牌得到相应的连接信息
    private String key;// key值

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

    /**
     * 创建impala squid
     * @param info  "RepositoryId:int SquidFlowId:int DestImpalaSquid:DestImpalaSquid"
     * @return  "newSquidId:int"
     */
    public String createDestImpalaSquid(String info){
        ReturnValue out = new ReturnValue();
        Map<String, Object> output = new HashMap<>();
        DataAdapterFactory adapterFactory = null;
        IRelationalDataManager adapter =null;
        try {
            //adapterFactory = DataAdapterFactory.newInstance();
            adapter = DataAdapterFactory.getDefaultDataManager();
            Map<String, Object> paramsMap = JsonUtil.toHashMap(info);
            adapter.openSession();
            DestImpalaSquid destImpalaSquid = JsonUtil.toGsonBean(paramsMap.get("DestImpalaSquid")
                    .toString(), DestImpalaSquid.class);
            if (destImpalaSquid!=null){
                int newSquidId = adapter.insert2(destImpalaSquid);
                output.put("newSquidId",newSquidId);
            }else{
                out.setMessageCode(MessageCode.NODATA);
            }
        }catch (Exception e){
            log.error("CreatDestImpalaSquid is error",e);
            out.setMessageCode(MessageCode.INSERT_ERROR);
            try{
                adapter.rollback();
            }catch (SQLException e1){  // 数据库回滚失败（程序不能处理该异常）！
                log.fatal("rollback err",e1);
            }
        }finally {
            adapter.closeSession();
        }
        InfoNewMessagePacket infoNewMessagePacket = new InfoNewMessagePacket<>(output,out.getMessageCode().value());
        return infoNewMessagePacket.toJsonString();
    }

    /**
     * 更新impala squid
     * @param info "DestImpalaSquid:DestImpalaSquid"
     */
    public String updateDestImpalaSquid (String info){
        IRelationalDataManager adapter = null;
        ReturnValue out = new ReturnValue();
        Map<String, Object> paramsMap = JsonUtil.toHashMap(info);
        try{
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            DestImpalaSquid destImpalaSquid = JsonUtil.toGsonBean(paramsMap.get("DestImpalaSquid")
                    .toString(),DestImpalaSquid.class);
            if (destImpalaSquid!=null){
                adapter.update2(destImpalaSquid);
            }else{
                out.setMessageCode(MessageCode.NODATA);
            }
        }catch (Exception e){
            log.error("UpdateDestImpalaSquid is error",e);
            out.setMessageCode(MessageCode.UPDATE_ERROR);
            try{
                adapter.rollback();
            }catch (SQLException e1){  // 数据库回滚失败（程序不能处理该异常）！
                log.fatal("rollback err !",e1);
            }
        }
        finally {
            adapter.closeSession();
        }
        InfoNewMessagePacket infoNewMessagePacket = new InfoNewMessagePacket<>(new HashMap<>(),out.getMessageCode().value());
        return infoNewMessagePacket.toJsonString();
    }

    /**
     * 创建impala link, 创建link的时候,需要得到上游squid 的column
     * @param info "squidLink"
     * @return SquidLink
     */
    public String creatDestImpalaSquidLink(String info){
        ReturnValue out = new ReturnValue();
        IRelationalDataManager adapter = null;
        Map<String, Object> outPut = new HashMap<>();
        Map<String, Object> params = JsonUtil.toHashMap(info);
        SquidLink squidLink = JsonUtil.toGsonBean(params.get("SquidLink").toString(),SquidLink.class);
        squidLink.setKey(UUID.randomUUID().toString());
        try {
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            squidLink.setId(adapter.insert2(squidLink));
            //1.获取上游的column
            Map<String, Object> maps = new HashMap<>();
            maps.put("squid_id", squidLink.getFrom_squid_id() + "");
            List<Column> columns = adapter.query2List2(true,maps,Column.class);
            if (columns!=null){
                //2.根据上游squid 的column和impalaSquidId生成impalaColumn
                for (Column column : columns){
                    DestImpalaColumn impalaColumn = ImpalaColumnService.getImpalaColumnByColumn(column,squidLink.getTo_squid_id());
                    // 3.插入到数据库
                    adapter.insert2(impalaColumn);
                }
            }
            outPut.put("SquidLink", squidLink);
        }catch (Exception e) {
            log.error("creatDestImpalaSquidLink is error", e);
            out.setMessageCode(MessageCode.SQL_ERROR);
            try {  // 数据库回滚失败（程序不能处理该异常）！
                adapter.rollback();
             } catch (SQLException e1) {
                log.fatal("rollback err!", e1);
                }
             } finally {
                if(adapter != null) {
                    adapter.closeSession();
                }
            }
        InfoNewMessagePacket infoNewMessagePacket = new InfoNewMessagePacket<>(outPut,out.getMessageCode().value());
        return infoNewMessagePacket.toJsonString();
    }
}
