package com.eurlanda.datashire.server.api;

import com.eurlanda.datashire.entity.DBSourceSquid;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.server.annotation.SocketApi;
import com.eurlanda.datashire.server.annotation.SocketApiMethod;
import com.eurlanda.datashire.server.dao.SquidDao;
import com.eurlanda.datashire.server.exception.ErrorMessageException;
import com.eurlanda.datashire.server.service.ExtractSquidService;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@SocketApi("2018")
public class ExtractSquidApi {

    @Autowired
    private ExtractSquidService extractSquidService;
    @Autowired
    private SquidDao squidDao;

    @SocketApiMethod(commandId = "0001")
    public String previewUnionTableNamesBySql(String info){
        ReturnValue out = new ReturnValue();
        Map<String,Object> returnMap=new HashMap<String,Object>();
        DBSourceSquid dbSourceSquid = JsonUtil.toGsonBean(JsonUtil.toHashMap(info).get("DbSquid") + "", DBSourceSquid.class);
        String sql=JsonUtil.toHashMap(info).get("Sql") + "";
        List<String> tableNames=new ArrayList<>();
        try{
               if(dbSourceSquid!=null && sql!=null && !"".equals(sql)){
                   tableNames=extractSquidService.getUnionTableNamesBySql(dbSourceSquid,sql);
                   returnMap.put("UnionTableNames",JsonUtil.toJSONString(tableNames));
               }
            }catch (ErrorMessageException e){
                e.printStackTrace();
                out.setMessageCode(MessageCode.parse(e.getErrorCode()));
         } catch (Exception e){
                e.printStackTrace();
               out.setMessageCode(MessageCode.ERR);
            }
        return JsonUtil.toJsonString(returnMap, DSObjectType.EXTRACT,out.getMessageCode());
    }
    /**
     * 修改抽取squid 最后增量值
     */
    @SocketApiMethod(commandId = "0002")
    public String updateIncreaseLastValue(String info){
        ReturnValue out = new ReturnValue();
        Map<String,Object> returnMap=new HashMap<>();
        try{
            Map<String,Object> paramMap = JsonUtil.toHashMap(info);
            int squidId = Integer.parseInt(paramMap.get("SquidId")+"");
            Map<String,Object> map=new HashMap<>();
            map.put("lastValue",paramMap.get("LastValue"));
            map.put("squidId",squidId);
            int a=squidDao.updateSquidByLastValue(map);
            if(a<0){
                out.setMessageCode(MessageCode.UPDATE_ERROR);
            }
        }catch (Exception e){
            e.printStackTrace();
            out.setMessageCode(MessageCode.UPDATE_ERROR);
        }
        return JsonUtil.toJsonString(returnMap, DSObjectType.EXTRACT,out.getMessageCode());

    }

}
