package com.eurlanda.datashire.server.api;

import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.server.annotation.SocketApi;
import com.eurlanda.datashire.server.annotation.SocketApiMethod;
import com.eurlanda.datashire.server.service.ColumnService;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Eurlanda on 2017/6/19.
 */
@Service
@SocketApi("2002")
public class ColumnApi {
    private static final Logger logger = LoggerFactory.getLogger(ColumnApi.class);

    @Autowired
    private ColumnService service;

    @SocketApiMethod(commandId = "0001")
    public String deleteColumn(String info){
        Map<String,Object> paramMap = JsonUtil.toHashMap(info);
        List<Integer> idsList = JsonUtil.toList(paramMap.get("ListColumnId"),Integer.class);
        ReturnValue out = new ReturnValue();
        //批量删除column
        Map<String,Object> returnMap = new HashMap<>();
        returnMap.put("DeleteCount",idsList.size());
        try {
            service.deleteColumn(idsList, 0);
        } catch (Exception e) {
            e.printStackTrace();
            out.setMessageCode(MessageCode.ERR_DELETE_COLUM);
            returnMap.put("DeleteCount",0);
        }
        return JsonUtil.toJsonString(returnMap, DSObjectType.COLUMN,out.getMessageCode());
    }


    public static void main(String[] args){
        List<Integer> ids = new ArrayList<>();
        ids.add(1856371);
        ids.add(1856372);
        ids.add(1856373);
        ids.add(1856374);
        ColumnApi api = new ColumnApi();
        api.deleteColumn(JsonUtil.toJSONString(ids));
    }

}
