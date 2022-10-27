package com.eurlanda.datashire.sprint7.service.squidflow.dest;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.dao.dest.ImpalaColumnDao;
import com.eurlanda.datashire.dao.dest.impl.ImpalaColumnDaoImpl;
import com.eurlanda.datashire.entity.Column;
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

/**
 * Created by dzp on 2016/5/13.
 */
@Service
public class ImpalaColumnService {
    private static Log log = LogFactory.getLog(ImpalaColumnService.class);
    private String token; //令牌根据令牌得到相应的连接信息
    private String key; // key值

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
     *获取impalaColumn
     * @param info  "RepositoryId:int SquidFlowId:int DestImpalaSquid:DestImpalaSquid"
     * @return ImpalaColumns:List<DestImpalaColumn>
     */
    public String getDestImpalaColumn(String info){
        ReturnValue out = new ReturnValue();
        Map<String, Object> params = JsonUtil.toHashMap(info);
        Map<String, Object> outPut = new HashMap<>();
        IRelationalDataManager adapter = null;
        try{
            adapter = DataAdapterFactory.getDefaultDataManager();
            DestImpalaSquid impalaSquid = JsonUtil.toGsonBean(params.get("DestImpalaSquid").toString(),DestImpalaSquid.class);
            int impalaSquidId = impalaSquid.getId();
            if (impalaSquidId!=0){
                adapter.openSession();
                ImpalaColumnDao impalaColumnDao = new ImpalaColumnDaoImpl(adapter);
                List<DestImpalaColumn> impalaColumns = impalaColumnDao.getImpalaColumnBySquidId(false,impalaSquidId);
                outPut.put("ImpalaColumns",impalaColumns);
            }else {
                out.setMessageCode(MessageCode.NODATA);
            }
        }catch(Exception e){
            log.error("GetDestImpalaColumn is error",e);
            out.setMessageCode(MessageCode.INSERT_ERROR);
            try {
                adapter.rollback();
            }catch(SQLException e1){
                log.fatal("rollback is error",e1);
            }
        }finally {
            adapter.closeSession();
        }
        InfoNewMessagePacket infoNewMessagePacket = new InfoNewMessagePacket<>(outPut,out.getMessageCode().value());
        return infoNewMessagePacket.toJsonString();
    }

    /**
     *更新impalaColumn
     * @param info  "UpdateDestImpalaColumns:List<DestImpalaColumn>"
     */
    public String updateDestImpalaColumns(String info){
        ReturnValue out = new ReturnValue();
        Map<String, Object> params = JsonUtil.toHashMap(info);
        IRelationalDataManager adapter = null;
        try{
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            List<DestImpalaColumn> impalaColumns = JsonUtil.toGsonList(String.valueOf(params.
                    get("UpdateDestImpalaColumns")), DestImpalaColumn.class);
            if (impalaColumns!=null){
                for (DestImpalaColumn dc : impalaColumns){
                    adapter.update2(dc);
                }
            }else {
                out.setMessageCode(MessageCode.NODATA);
            }
        }catch(Exception e){
            log.error("UpdateDestImpalaColumns error!!!" , e);
            out.setMessageCode(MessageCode.UPDATE_ERROR);
            try{
                adapter.rollback();
            }catch (SQLException e1){
                log.fatal("rollback error!!!",e1);
            }
        }finally {
            adapter.closeSession();
        }
        InfoNewMessagePacket infoNewMessagePacket = new InfoNewMessagePacket<>(new HashMap<>(),out.getMessageCode().value());
        return infoNewMessagePacket.toJsonString();
    }

    /**
     * 通过column 生成impalaColumn
     * @param column impala squid上游squid的column
     * @param squidId impala_squid_id
     * @return impalaColumn
     */
    public static DestImpalaColumn getImpalaColumnByColumn(Column column,int squidId){
        DestImpalaColumn impalaColumn = new DestImpalaColumn();
        impalaColumn.setColumn(column);
        impalaColumn.setColumn_id(column.getId());
        impalaColumn.setSquid_id(squidId);
        impalaColumn.setIs_dest_column(1);
        impalaColumn.setField_name(column.getName());
        impalaColumn.setColumn_order(column.getRelative_order());
        return impalaColumn;
    }
}
