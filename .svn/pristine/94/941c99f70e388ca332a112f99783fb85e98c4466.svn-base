package com.eurlanda.datashire.server.api;

import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.server.annotation.SocketApi;
import com.eurlanda.datashire.server.annotation.SocketApiMethod;
import com.eurlanda.datashire.server.model.SamplingSquid;
import com.eurlanda.datashire.server.model.SquidLink;
import com.eurlanda.datashire.server.service.SquidLinkService;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
@SocketApi("2009")
public class SquidLinkApi {
    @Autowired
    private SquidLinkService squidLinkService;

    @SocketApiMethod(commandId = "0001")
    public String CreateSamplingSquidLink(String info){
         ReturnValue out = new ReturnValue();
         Map<String,Object> map=new HashMap<String,Object>();
        try {
            Map<String, Object> data= JsonUtil.toHashMap(info);
            SquidLink squidLink = JsonUtil.toGsonBean(data.get("SquidLink").toString(),SquidLink.class);
            if(squidLink!=null){
                map=squidLinkService.createSamplingSquidLink(squidLink);
            }else{
                out.setMessageCode(MessageCode.DESERIALIZATION_FAILED);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return JsonUtil.toJsonString(map, DSObjectType.SQUIDLINK, out.getMessageCode());

    }

    @SocketApiMethod(commandId = "0002")
    public String createPivotSquidLink(String info){
        ReturnValue out = new ReturnValue();
        Map<String,Object> map=new HashMap<String,Object>();
        try {
            Map<String, Object> data= JsonUtil.toHashMap(info);
            SquidLink squidLink = JsonUtil.toGsonBean(data.get("SquidLink").toString(),SquidLink.class);
            if(squidLink!=null){
                map=squidLinkService.createPivotSquidLink(squidLink);
            }else{
                out.setMessageCode(MessageCode.DESERIALIZATION_FAILED);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return JsonUtil.toJsonString(map, DSObjectType.SQUIDLINK, out.getMessageCode());

    }



}
