package com.eurlanda.datashire.sprint7.service.squidflow.dest;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.dao.dest.IEsColumnDao;
import com.eurlanda.datashire.dao.dest.impl.EsColumnDaoImpl;
import com.eurlanda.datashire.entity.Column;
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

/**
 * Created by zhudebin on 15-9-17.
 */
@Service
public class EsColumnService {

    private static Log log = LogFactory.getLog(EsColumnService.class);

    private String token;
    private String key;// key值

    /**
     * "RepositoryId:int DestESSquidId:int" "ESColumns:List<ESColumn>"
     * @param info
     * @return
     */
    public String getESColumns(String info) {
        ReturnValue out = new ReturnValue();

        Map<String, Object> outputMap = new HashMap<>();
        DataAdapterFactory adapterFactory = null;
        IRelationalDataManager adapter = null;
        try {
            adapterFactory = DataAdapterFactory.newInstance();
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            Map<String, Object> paramsMap = JsonUtil.toHashMap(info);

            Integer destESSquidId = (Integer)paramsMap.get("DestESSquidId");
            if (null != destESSquidId) {
                IEsColumnDao esColumnDao = new EsColumnDaoImpl(adapter);
                List<EsColumn> esColumns = esColumnDao.getEsColumnsBySquidId(destESSquidId);
                outputMap.put("ESColumns", esColumns);
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
        InfoNewMessagePacket
                infoNewMessagePacket = new InfoNewMessagePacket<>(outputMap, out.getMessageCode().value());
        return  infoNewMessagePacket.toJsonString();
    }

    /**
     * "UpdateESColumns:List<ESColumn>"
     * @param info
     */
    public String updateESColumns(String info) {
        ReturnValue out = new ReturnValue();
        DataAdapterFactory adapterFactory = null;
        IRelationalDataManager adapter = null;
        try {
            //adapterFactory = DataAdapterFactory.newInstance();
            adapter =DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            Map<String, Object> paramsMap = JsonUtil.toHashMap(info);
            List<EsColumn>
                    esColumns  = JsonUtil.toGsonList(String.valueOf(paramsMap.get("UpdateESColumns")),
                    EsColumn.class);
            if (null != esColumns) {
                for(EsColumn ec : esColumns) {
                    adapter.update2(ec);
                }
            }else{
                out.setMessageCode(MessageCode.NODATA);
            }
        } catch (Exception e) {
            log.error("update esColumns is error", e);
            out.setMessageCode(MessageCode.UPDATE_ERROR);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                log.fatal("rollback err!", e1);
            }
        } finally {
            adapter.closeSession();
        }
        InfoNewMessagePacket infoNewMessagePacket = new InfoNewMessagePacket<>(new HashMap<>(), out.getMessageCode().value());
        return  infoNewMessagePacket.toJsonString();
    }

    /**
     * 通过column 生成EsColumn
     * @param column es squid上游squid的column
     * @param squidId es squid id
     * @return
     */
    public static EsColumn genEsColumnByColumn(Column column, int squidId) {
        EsColumn esColumn = new EsColumn();
        esColumn.setColumn(column);
        esColumn.setColumn_id(column.getId());
        esColumn.setField_name(column.getName());
        esColumn.setIs_mapping_id(0);
        esColumn.setIs_persist(1);
        esColumn.setSquid_id(squidId);
        return esColumn;
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
