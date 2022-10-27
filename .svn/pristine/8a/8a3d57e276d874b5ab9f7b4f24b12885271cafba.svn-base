package com.eurlanda.datashire.server.api;

import com.eurlanda.datashire.server.annotation.SocketApi;
import com.eurlanda.datashire.server.annotation.SocketApiMethod;
import com.eurlanda.datashire.server.dao.TransformationInputsDao;
import com.eurlanda.datashire.server.exception.ErrorMessageException;
import com.eurlanda.datashire.server.model.TransformationInputs;
import com.eurlanda.datashire.server.model.TransformationLink;
import com.eurlanda.datashire.server.service.TransformationLinkService;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Eurlanda - Java、 on 2017/11/6.
 */
@Service
@SocketApi("2017")
public class TransformationLinkApi {
    @Autowired
    private TransformationLinkService transformationLinkService;
    @Autowired
    private TransformationInputsDao transformationInputsDao;
    /**
     * 批量创建TransformationLink
     * 及添加transInputs
     *只适用于几个特殊的trans类型 CHOICE CONCATENATE NUMASSEMBLE CSVASSEMBLE
     */
    @SocketApiMethod(commandId = "0001")
    public String BatchCreateTransformationLink(String info){
        ReturnValue out=new ReturnValue();
        Map<String,Object> map=new HashMap<String,Object>();
        try {
            List<TransformationLink> transformationLinkList=JsonUtil.toGsonList(JsonUtil.toHashMap(info).get("ListTransLink")+"", TransformationLink.class);
            if(transformationLinkList!=null && transformationLinkList.size()>0){
                map=transformationLinkService.insertTransformationLink(transformationLinkList);
            }
        }catch (ErrorMessageException e){
            e.printStackTrace();
            out.setMessageCode(MessageCode.parse(e.getErrorCode()));
        }catch (Exception e){
            e.printStackTrace();
            out.setMessageCode(MessageCode.INSERT_ERROR);
        }

        return JsonUtil.toJsonString(map,null,out.getMessageCode());
    }
}

