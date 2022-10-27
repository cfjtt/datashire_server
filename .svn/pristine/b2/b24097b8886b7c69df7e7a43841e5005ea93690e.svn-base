package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.entity.DataMiningSquid;
import com.eurlanda.datashire.enumeration.DMLType;
import com.eurlanda.datashire.server.utils.TokenUtil;
import com.eurlanda.datashire.sprint7.packet.InfoNewMessagePacket;
import com.eurlanda.datashire.sprint7.service.squidflow.subservice.DataMiningSquidServiceSub;
import com.eurlanda.datashire.sprint7.service.squidflow.subservice.ExtractServicesub;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
public class DataMiningSquidService {
    public static final Logger logger = Logger
            .getLogger(ExtractDBSquidService.class);
    private String token;
    private String key;

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
     * 创建DataMiningSquid
     *
     * @param info
     * @return
     * @author bo.dang
     * @date 2014年5月8日
     */
    public String createDataMiningSquid(String info) {
        ReturnValue out = new ReturnValue();
        Map<String, Object> infoMap = JsonUtil.toHashMap(info);
        DataMiningSquid dataMiningSquid = JsonUtil.toGsonBean(infoMap.get("DataMiningSquid").toString(), DataMiningSquid.class);
        int repositoryId = Integer.parseInt(infoMap.get("RepositoryId").toString(), 10);
        Map<String, Object> resultMap = new DataMiningSquidServiceSub(TokenUtil.getToken()).createDataMiningSquid(dataMiningSquid, repositoryId, out);

        // 返回新增的FolderID
        InfoNewMessagePacket<Map<String, Object>> infoMessage = new InfoNewMessagePacket<Map<String, Object>>();
        infoMessage.setInfo(resultMap);
        infoMessage.setDesc("创建DataMiningSquid");
        infoMessage.setCode(out.getMessageCode().value());
        return JsonUtil.object2Json(infoMessage);
    }

    /**
     * 更新DataMiningSquid信息
     *
     * @param info
     * @return
     * @author bo.dang
     * @date 2014年5月7日
     */
    public String updateDataMiningSquid(String info) {
        ReturnValue out = new ReturnValue();
        Map<String, Class<?>> paramsMap = new HashMap<String, Class<?>>();
        paramsMap.put("DataMiningSquid", DataMiningSquid.class);
        // 更新XmlExtractSquid
        Map<String, Object> resultMap = new ExtractServicesub(TokenUtil.getToken()).execute(info, paramsMap, DMLType.UPDATE.value(), out);
        InfoNewMessagePacket<Map<String, Object>> infoMessage = new InfoNewMessagePacket<Map<String, Object>>();
        infoMessage.setCode(out.getMessageCode().value());
        if (out.getMessageCode() == MessageCode.SUCCESS) {
            infoMessage.setInfo(resultMap);
        } else {
            infoMessage.setInfo(new HashMap<String, Object>());
        }
        infoMessage.setDesc("更新XmlExtarctSquid信息");
        return JsonUtil.object2Json(infoMessage);
    }
}
