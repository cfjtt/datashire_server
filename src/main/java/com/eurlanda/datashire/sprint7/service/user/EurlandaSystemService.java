package com.eurlanda.datashire.sprint7.service.user;

import com.eurlanda.datashire.sprint7.packet.InfoNewMessagePacket;
import com.eurlanda.datashire.utility.CommonConsts;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.RPCUtils;
import com.eurlanda.datashire.utility.ReturnValue;
import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yhc on 2/19/2016.
 * 关于服务端系统级别的功能
 */
@Service
public class EurlandaSystemService {
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
    private static Log log = LogFactory.getLog(EurlandaSystemService.class);

    /**
     * 获取服务器的版本信息
     * @param info
     * @return
     */
    public String getServiceVersionInfo(String info) {
        String serviceVersion = CommonConsts.VERSION;
        ReturnValue returnValue = new ReturnValue();

        RPCUtils rpc = new RPCUtils();
        String engineVersion = "Nan";
        try{
            engineVersion = rpc.getEngineVersionInfo();
        }catch (Exception ex){
            returnValue.setMessageCode(MessageCode.ERR_RPCDISCONNECTED);
        }

        Map<String, Object> outputMap = new HashMap<>();
        outputMap.put("ServiceVersion", serviceVersion + "." + engineVersion);

        InfoNewMessagePacket infoPacket = new InfoNewMessagePacket(outputMap, returnValue.getMessageCode().value());
        return infoPacket.toJsonString();
    }
}
