package com.eurlanda.datashire.sprint7.service.metadata;

import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.server.utils.TokenUtil;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.ReturnValue;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class RepositoryExportService {
    /**
     * 对RepositoryExportService的日志进行记录
     */
    static Logger logger = Logger.getLogger(RepositoryExportService.class);

    private String token;

    private String key;

    /**
     * 导出squidFlows
     *
     * @param info
     * @return
     */
    public String exportSquidFlows(String info) {
        logger.info("开始导出squidFlows");
        ReturnValue out = new ReturnValue();
        RepositoryExportProcess process = new RepositoryExportProcess(TokenUtil.getToken(), key);
        Map<String, Object> map = process.exportSquidFlows(info, out);
        logger.info("导出squidflows结束");
        return JsonUtil.toJsonString(map, DSObjectType.SQUID_FLOW, out.getMessageCode());
    }


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
}
