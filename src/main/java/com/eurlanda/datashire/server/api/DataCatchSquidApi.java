package com.eurlanda.datashire.server.api;

import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.server.annotation.SocketApi;
import com.eurlanda.datashire.server.annotation.SocketApiMethod;
import com.eurlanda.datashire.server.exception.ErrorMessageException;
import com.eurlanda.datashire.server.model.DataCatchSquid;
import com.eurlanda.datashire.server.service.DataCatchSquidService;
import com.eurlanda.datashire.sprint7.packet.InfoNewMessagePacket;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by eurlanda - new 2 on 2017/6/28.
 */
@Service
@SocketApi("2013")
public class DataCatchSquidApi {
    private static final Logger logger = LoggerFactory.getLogger(DataCatchSquidApi.class);
    @Autowired
    DataCatchSquidService dataCatchSquidService;

    /**
     * 创建DataViewSquid
     * @param info
     * @return
     */
    @SocketApiMethod(commandId = "0001")
    public String createCatchSquidSquid(String info){
        ReturnValue out = new ReturnValue();
        Map<String, Object> resultMap = new HashMap<String, Object>();
        try {
            resultMap= dataCatchSquidService.createDataCatchSquid(info);
        }catch (ErrorMessageException e){
            out.setMessageCode(MessageCode.parse(e.getErrorCode()));
        }catch (Exception e){
            e.printStackTrace();
            out.setMessageCode(MessageCode.INSERT_ERROR);
            if(e.getMessage().equals(MessageCode.ERR_SQUID_OVER_LIMIT.value()+"")){
                out.setMessageCode(MessageCode.ERR_SQUID_OVER_LIMIT);
            }
            logger.error("创建DataViewSquid异常", e);
        }
        // 返回新增的FolderID
        InfoNewMessagePacket<Map<String, Object>> infoMessage = new InfoNewMessagePacket<Map<String, Object>>();
        infoMessage.setInfo(resultMap);
        infoMessage.setDesc("创建DataViewSquid");
        infoMessage.setCode(out.getMessageCode().value());
        return JsonUtil.object2Json(infoMessage);
    }

    /**
     *
     * @param info
     * @return
     */
    @SocketApiMethod(commandId = "0002")
    public String updateCatchSquidSquid(String info){
        ReturnValue out = new ReturnValue();
        Map<String, Object> resultMap = new HashMap<String, Object>();
        DataCatchSquid dataCatchSquid = JsonUtil.toGsonBean(JsonUtil.toHashMap(info).get("Squid") + "", DataCatchSquid.class);
        try {
            dataCatchSquidService.updateCatchSquidSquid(dataCatchSquid);
        }catch (ErrorMessageException e){
            out.setMessageCode(MessageCode.parse(e.getErrorCode()));
        }catch (Exception e){
            e.printStackTrace();
            out.setMessageCode(MessageCode.UPDATE_ERROR);
            logger.error("更新DataViewSquid异常", e);
        }
        return JsonUtil.toJsonString(resultMap, DSObjectType.DATAVIEW, out.getMessageCode());
    }

}
