package com.eurlanda.datashire.server.api;

import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.server.annotation.SocketApi;
import com.eurlanda.datashire.server.annotation.SocketApiMethod;
import com.eurlanda.datashire.server.exception.ErrorMessageException;
import com.eurlanda.datashire.server.model.Transformation;
import com.eurlanda.datashire.server.service.TransformationLinkService;
import com.eurlanda.datashire.server.service.TransformationService;
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
 * Created by eurlanda - new 2 on 2017/6/20.
 */
@Service
@SocketApi("2004")
public class TransformationApi {
    private static final Logger LOGGER = LoggerFactory.getLogger(TransformationApi.class);
    @Autowired
    private TransformationService transformationService;

    @Autowired
    private TransformationLinkService transformationLinkService;

    /**
     * 删除Transformation要把对应的inputs删掉
     */
    @SocketApiMethod(commandId = "0001")
    public String deleteTransformation(String info) {
        ReturnValue out = new ReturnValue();
        Map<String, Object> map = new HashMap<String, Object>();
        List<Integer> list = new ArrayList<>();
        List<Transformation> transformationList = JsonUtil.toGsonList(JsonUtil.toHashMap(info).get("ListTransformation") + "", Transformation.class);
        if (transformationList != null && transformationList.size() > 0) {
            for (Transformation transformation : transformationList) {
                list.add(transformation.getId());
            }
        }
        try {
            //返回删除的数量
            map.put("DeleteCount",transformationService.deleteTransformation(list));
        }catch (ErrorMessageException e){
            out.setMessageCode(MessageCode.parse(e.getErrorCode()));
        }catch (Exception e){
            e.printStackTrace();
            out.setMessageCode(MessageCode.ERR_DELETE);
            LOGGER.error("删除Transformation异常", e);
        }
        return JsonUtil.toJsonString(map, DSObjectType.TRANSFORMATION, out.getMessageCode());
    }

    /**
     * 删除TransformationLink 修改收到影响的Trans的input中的source trans id
     * @param info
     * @return
     */
    @SocketApiMethod(commandId = "0002")
    public String deleteTransformationLinkIds(String info){
        List<Transformation> transList=null;
        ReturnValue out=new ReturnValue();
        Map<String,Object> returnMap= new HashMap<String,Object>();
        List<Integer> transformationLinkIds = JsonUtil.toGsonList(JsonUtil.toHashMap(info).get("ListTransformationLinkId") + "", Integer.class);
            if(transformationLinkIds!=null && transformationLinkIds.size()>0){
                try{
                    transList=transformationLinkService.deleteTransformationLinkIds(transformationLinkIds);
                } catch(ErrorMessageException e){
                    out.setMessageCode(MessageCode.parse(e.getErrorCode()));
                } catch (Exception e){
                    e.printStackTrace();
                    out.setMessageCode(MessageCode.ERR_DELETE);
                    LOGGER.error("删除Transformation异常", e);
                }
            }
                returnMap.put("ListTransformation",transList);
        return JsonUtil.toJsonString(returnMap, DSObjectType.TRANSFORMATIONLINK,out.getMessageCode());
    }

    /**
     *
     * 删除Trans面板上的对象
     * @param info
     * @return
     */
    @SocketApiMethod(commandId = "0003")
    public String deleteTransformationPanelObj(String info){
        HashMap<String, String> infoMap = JsonUtil.toHashMap(info);
        ReturnValue out = new ReturnValue();
        Map<String,Object> resultMap= new HashMap<String,Object>();

        List<Integer> transIds = JsonUtil.toList(infoMap.get("ListTransformationId"), Integer.class);
        List<Integer> transLinkIds = JsonUtil.toList(infoMap.get("ListTransformationLinkId"), Integer.class);

        //必须两种都调用才会使用这个接口，否则就不执行
        if (transIds != null && transIds.size() > 0 && transLinkIds != null && transLinkIds.size() > 0){
            try{
                List<Transformation> transList = transformationService.deleteTransformationPanelObj(transLinkIds, transIds,0);
                if (transList != null && transList.size() > 0)
                    resultMap.put("ListTransformation", transList);
            } catch (ErrorMessageException e){
                out.setMessageCode(MessageCode.parse(e.getErrorCode()));
            } catch (Exception e){
                e.printStackTrace();
                out.setMessageCode(MessageCode.ERR_DELETE);
            }
        }
        return JsonUtil.toJsonString(resultMap, DSObjectType.TRANSFORMATION, out.getMessageCode());
    }
}
