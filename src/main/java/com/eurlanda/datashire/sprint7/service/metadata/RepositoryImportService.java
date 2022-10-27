package com.eurlanda.datashire.sprint7.service.metadata;

import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.ReturnValue;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * Repository下的对象导入
 *
 * @author yi.zhou
 */
@Service
public class RepositoryImportService {
    /**
     * 对RepositoryExportService的日志进行记录
     */
    static Logger logger = Logger.getLogger(RepositoryImportService.class);

    private String token;

    private String key;

    /**
     * 导入squidFlows
     *
     * @param info
     * @return
     */
    public String importSquidFlows(String info) {
        ReturnValue out = new ReturnValue();
        RepositoryImportProcess process = new RepositoryImportProcess(token, key);
        Map<String, Object> map = process.importSquidFlows(info, out);
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
