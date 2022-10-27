package com.eurlanda.datashire.sprint7.service.squidflow.dest;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.entity.Column;
import com.eurlanda.datashire.entity.SquidLink;
import com.eurlanda.datashire.entity.dest.DestESSquid;
import com.eurlanda.datashire.entity.dest.EsColumn;
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
 * Created by zhudebin on 15-9-17.
 */
@Service
public class DestEsSquidService {

    private static Log log = LogFactory.getLog(DestEsSquidService.class);

    private String token;//令牌根据令牌得到相应的连接信息
    private String key;// key值

    /**
     * 创建ES落地squid
     * @param info  "RepositoryId:int DestESSquid:DestESSquid"
     * @return  "newSquidId:int"
     */
    public String createDestESSquid(String info) {
        ReturnValue out = new ReturnValue();

        Map<String, Object> outputMap = new HashMap<>();
        DataAdapterFactory adapterFactory = null;
        IRelationalDataManager adapter = null;
        try {
            //adapterFactory = DataAdapterFactory.newInstance();
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            Map<String, Object> paramsMap = JsonUtil.toHashMap(info);
            DestESSquid squid  = JsonUtil.toGsonBean(String.valueOf(paramsMap.get("DestESSquid")), DestESSquid.class);
            if (null != squid) {
                int newId = adapter.insert2(squid);
                outputMap.put("newSquidId", newId);
            }else{
                out.setMessageCode(MessageCode.NODATA);
            }
        } catch (Exception e) {
            log.error("createDestEsSquid is error", e);
            out.setMessageCode(MessageCode.INSERT_ERROR);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                log.fatal("rollback err!", e1);
            }
        } finally {
            adapter.closeSession();
        }
        InfoNewMessagePacket infoNewMessagePacket = new InfoNewMessagePacket<>(outputMap, out.getMessageCode().value());
        return  infoNewMessagePacket.toJsonString();

    }

    /**
     * 更新ES落地squid
     * @param info "DestESSquid:DestESSquid"
     */
    public String updateDestESSquid(String info) {
        ReturnValue out = new ReturnValue();
        DataAdapterFactory adapterFactory = null;
        IRelationalDataManager adapter = null;
        try {
            adapterFactory = DataAdapterFactory.newInstance();
            adapter =DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            Map<String, Object> paramsMap = JsonUtil.toHashMap(info);
            DestESSquid squid  = JsonUtil.toGsonBean(String.valueOf(paramsMap.get("DestESSquid")),
                    DestESSquid.class);
            if (null != squid) {
                adapter.update2(squid);
            }else{
                out.setMessageCode(MessageCode.NODATA);
            }
        } catch (Exception e) {
            log.error("updateDestWsSquid is error", e);
            out.setMessageCode(MessageCode.UPDATE_ERROR);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                log.fatal("rollback err!", e1);
            }
        } finally {
            if(adapter != null) {
                adapter.closeSession();
            }
        }
        InfoNewMessagePacket infoNewMessagePacket = new InfoNewMessagePacket<>(new HashMap<>(), out.getMessageCode().value());
        return  infoNewMessagePacket.toJsonString();
    }

    /**
     * 创建es 落地squid link, 创建link的时候，ESSquid需要添加EScolumn
     * CreateDestESSquidLink 99999999 "SquidLink:SquidLink" "SquidLinkId:int"
     * @param info SquidLink
     * @return SquidLinkId
     */
    public String createDestESSquidLink(String info) {
        ReturnValue out = new ReturnValue();
        Map<String, Object> paramsMap = JsonUtil.toHashMap(info);
        SquidLink squidLink = JsonUtil.toGsonBean(String.valueOf(paramsMap.get("SquidLink")), SquidLink.class);
        Map<String, Object> outputMap = new HashMap<>();
        squidLink.setKey(UUID.randomUUID().toString());
        IRelationalDataManager adapter = null;
        try {
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            // 插入 squidLink
            squidLink.setId(adapter.insert2(squidLink));
            // 添加 ESColumn
            // 1. 获取上游squid column
            Map<String, String> params = new HashMap<>();
            params.put("squid_id", squidLink.getFrom_squid_id() + "");
            List<Column> columns = adapter.query2List(true, params, Column.class);
            if(columns != null) {
                // 2. 生成escolumn前，先删除掉所有的column
                // 2. 生成ESColumn
                for(Column column : columns) {
                    EsColumn esColumn = EsColumnService.genEsColumnByColumn(column, squidLink.getTo_squid_id());
                    // 3. 插入数据库
                    adapter.insert2(esColumn);
                }
            }

            outputMap.put("SquidLinkId", squidLink.getId());
        } catch (Exception e) {
            log.error("createDestESSquidLink error", e);
            out.setMessageCode(MessageCode.SQL_ERROR);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                log.fatal("rollback err!", e1);
            }
        } finally {
            if(adapter != null) {
                adapter.closeSession();
            }
        }

        InfoNewMessagePacket infoNewMessagePacket = new InfoNewMessagePacket<>(outputMap, out.getMessageCode().value());
        return  infoNewMessagePacket.toJsonString();
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }
}
