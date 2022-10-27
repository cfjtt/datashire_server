package com.eurlanda.datashire.server.api;

import com.eurlanda.datashire.server.annotation.SocketApi;
import com.eurlanda.datashire.server.annotation.SocketApiMethod;
import com.eurlanda.datashire.server.exception.ErrorMessageException;
import com.eurlanda.datashire.server.service.ReferenceColumnService;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by eurlanda - new 2 on 2017/6/19.
 */
@Service
@SocketApi("2003")
public class ReferenceColumnApi {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReferenceColumnApi.class);

    @Autowired
    private ReferenceColumnService referenceColumnService;

    //删除ReferenceColumn
    @SocketApiMethod(commandId = "0001")
    public String deleteReferenceColumn(String info){
        ReturnValue out = new ReturnValue();
        Map<String, Object> map = new HashMap<String, Object>();
        List<Integer> list = JsonUtil.toGsonList(JsonUtil.toHashMap(info).get("ListReferenceColumnId") + "", Integer.class);
        Integer squidId = Integer.valueOf(JsonUtil.toHashMap(info).get("SquidId") + "");
        try {
            //返回删除的数量
            map.put("DeleteCount", referenceColumnService.deleteReferenceColumnAndTransformation(list, squidId));
        }catch (ErrorMessageException e){
            out.setMessageCode(MessageCode.parse(e.getErrorCode()));
        }catch (Exception e){
            e.printStackTrace();
            out.setMessageCode(MessageCode.ERR_DELETE_COLUM);
            LOGGER.error("删除ReferenceColumn异常", e);
        }
        return JsonUtil.toJsonString(map,null, out.getMessageCode());
    }
}
