package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.entity.SquidIndexes;
import com.eurlanda.datashire.enumeration.DMLType;
import com.eurlanda.datashire.server.utils.TokenUtil;
import com.eurlanda.datashire.sprint7.packet.InfoNewMessagePacket;
import com.eurlanda.datashire.sprint7.service.squidflow.subservice.SquidIndexsServiceSub;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * SquidIndexs处理类
 * @author bo.dang
 * @date 2014年5月15日
 */
@Service
public class SquidIndexsService {
    public static final Logger logger = Logger.getLogger(DocExtractSquidService.class);
    private String token;
    private String key;

    // 异常处理机制
    ReturnValue out = new ReturnValue();
    
/**
     * 根据squid获取squidIndexsList
     * @param info
     * @return
     * @author bo.dang
     * @date 2014年5月15日
     */
    public String getSquidIndexsListBySquidId(String info){
        @SuppressWarnings("unchecked")
        Map<String, Object> infoMap = JsonUtil.toHashMap(info);
        int squidId = Integer.parseInt(infoMap.get("SquidId").toString(), 10);
        List<SquidIndexes> squidIndexsList = new SquidIndexsServiceSub(TokenUtil.getToken()).getSquidIndexsListBySquidId(squidId);
        
        Map<String, Object> resultMap = new HashMap<String, Object>();
        resultMap.put("SquidIndexesList", squidIndexsList);
        InfoNewMessagePacket<Map<String, Object>> infoMessage = new InfoNewMessagePacket<Map<String, Object>>();
        infoMessage.setInfo(resultMap);
        infoMessage.setDesc("根据squid获取squidIndexsList");
        
        return JsonUtil.object2Json(infoMessage);
    }
    
    /**
     * 创建squidIndexs
     * @param info
     * @return
     * @author bo.dang
     * @date 2014年5月15日
     */
    public String createSquidIndexs(String info){
    	out = new ReturnValue();
        Map<String, Class<?>> paramsMap = new HashMap<String, Class<?>>();
        paramsMap.put("SquidIndexes", SquidIndexes.class);
        Map<String, Object> resultMap = new RepositoryServiceHelper(TokenUtil.getToken()).execute(info, paramsMap, DMLType.INSERT.value(), out);
        InfoNewMessagePacket<Map<String, Object>> infoMessage = new InfoNewMessagePacket<Map<String, Object>>();
        infoMessage.setCode(out.getMessageCode().value());
        if (out.getMessageCode()==MessageCode.SUCCESS){
        	infoMessage.setInfo(resultMap);
        }else{
        	infoMessage.setInfo(new HashMap<String, Object>());
        }
        infoMessage.setDesc("创建squidIndexs");
        return JsonUtil.object2Json(infoMessage);
    }

    /**
     * 更新SquidIndexs
     * @param info
     * @return
     * @author bo.dang
     * @date 2014年5月16日
     */
    public String updateSquidIndexs(String info){
        Map<String, Object> infoMap = JsonUtil.toHashMap(info);
        List<SquidIndexes> squidIndexsList = JsonUtil.toGsonList(infoMap.get("SquidIndexesList").toString(), SquidIndexes.class);
        List<Integer> squidIndexsIdList = new SquidIndexsServiceSub(TokenUtil.getToken()).updateSquidIndexs(squidIndexsList);
        Map<String, Object> resultMap = new HashMap<String, Object>();
        // 返回更新SquidIndexs的ID
        resultMap.put("SquidIndexesIdList", squidIndexsIdList);
        InfoNewMessagePacket<Map<String, Object>> infoMessage = new InfoNewMessagePacket<Map<String,Object>>();
        infoMessage.setInfo(resultMap);
        infoMessage.setDesc("更新SquidIndexs");
        return JsonUtil.object2Json(infoMessage);
    }
    
/*    *//**
     * 删除index对象
     * @param info
     * @return
     * @author bo.dang
     * @date 2014年5月17日
     *//*
    public String deleteSquidIndexs(String info){
        Map<String, Object> infoMap = JsonUtil.toHashMap(info);
        SquidIndexs squidIndexs = JsonUtil.toGsonBean(infoMap.get("SquidIndexs").toString(), SquidIndexs.class);
        SquidIndexs squidIndexsResult = new SquidIndexsServiceSub(token).deleteSquidIndexs(squidIndexs);
        Map<String, Object> resultMap = new HashMap<String, Object>();
        // 返回更新SquidIndexs的ID
        resultMap.put("SquidIndexs", squidIndexsResult);
        InfoNewMessagePacket<Map<String, Object>> infoMessage = new InfoNewMessagePacket<Map<String,Object>>();
        infoMessage.setInfo(resultMap);
        infoMessage.setDesc("删除SquidIndexs");
        return JsonUtil.object2Json(infoMessage);
    }*/
    
    
    /**
     * 删除index对象
     * @param info
     * @return
     * @author bo.dang
     * @date 2014年5月17日
     */
    public String createSquidIndexsToDB(String info){
    	out = new ReturnValue();
        Map<String, Object> infoMap = JsonUtil.toHashMap(info);
        List<SquidIndexes> squidIndexsList = JsonUtil.toGsonList(infoMap.get("SquidIndexesList").toString(), SquidIndexes.class);
        List<Integer> squidIndexsIdList = new SquidIndexsServiceSub(TokenUtil.getToken()).createSquidIndexsToDB(squidIndexsList, out);
        Map<String, Object> resultMap = new HashMap<String, Object>();
        // 返回更新SquidIndexs的ID
        resultMap.put("SquidIndexesIdList", squidIndexsIdList);
        InfoNewMessagePacket<Map<String, Object>> infoMessage = new InfoNewMessagePacket<Map<String,Object>>();
        infoMessage.setInfo(resultMap);
        infoMessage.setCode(out.getMessageCode().value());
        infoMessage.setDesc("删除SquidIndexs");
        return JsonUtil.object2Json(infoMessage);
    }
    
    
    /**
     * 删除index对象
     * @param info
     * @return
     * @author bo.dang
     * @date 2014年5月17日
     */
    public String dropSquidIndexsFromDB(String info){
    	out = new ReturnValue();
        Map<String, Object> infoMap = JsonUtil.toHashMap(info);
        SquidIndexes squidIndexs = JsonUtil.toGsonBean(infoMap.get("SquidIndexes").toString(), SquidIndexes.class);
    	Boolean dropFlag = new SquidIndexsServiceSub(TokenUtil.getToken()).dropSquidIndexsFromDB(squidIndexs, out);
        Map<String, Object> resultMap = new HashMap<String, Object>();
        // 返回更新SquidIndexs的ID
        resultMap.put("Result", dropFlag);
        InfoNewMessagePacket<Map<String, Object>> infoMessage = new InfoNewMessagePacket<Map<String,Object>>();
        infoMessage.setInfo(resultMap);
        infoMessage.setCode(out.getMessageCode().value());
        infoMessage.setDesc("删除索引");
        return JsonUtil.object2Json(infoMessage);
    }
}
