package com.eurlanda.datashire.server.service;

import com.eurlanda.datashire.server.dao.ServerParameterDao;
import org.apache.commons.collections.map.HashedMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * 系统参数
 * Created by zhudebin on 2017/6/8.
 */
@Service
public class ServerParameterService {

    @Autowired
    private ServerParameterDao serverParameterDao;

    public Map<String, Object> getServerParameter() {
        Map<String, Object> map = new HashedMap();
        List<Map<String, Object>> params = serverParameterDao.findList();
        for(Map<String, Object> param : params) {
            map.put(param.get("NAME").toString(), param.get("VALUE"));
        }
        return map;
    }
}
