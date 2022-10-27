package com.eurlanda.datashire.server.api;

import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.server.annotation.SocketApi;
import com.eurlanda.datashire.server.annotation.SocketApiMethod;
import com.eurlanda.datashire.server.exception.ErrorMessageException;
import com.eurlanda.datashire.server.model.NormalizerSquid;
import com.eurlanda.datashire.server.model.Transformation;
import com.eurlanda.datashire.server.service.NormalizerSquidService;
import com.eurlanda.datashire.server.service.TransformationInputsService;
import com.eurlanda.datashire.sprint7.packet.InfoNewMessagePacket;
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
 * Created by eurlanda - new 2 on 2017/6/27.
 */
@Service
@SocketApi("2012")
public class NormalizerSquidApi {
    private static final Logger logger = LoggerFactory.getLogger(NormalizerSquidApi.class);

    @Autowired
    NormalizerSquidService normalizerSquidService;
    @Autowired
    TransformationInputsService transformationInputsService;


    /**
     * 创建NormalizerSquid
     * @param info
     * @return
     */
    @SocketApiMethod(commandId = "0001")
    public String createNormalizerSquid(String info){
        ReturnValue out = new ReturnValue();
        Map<String, Object> resultMap = new HashMap<String, Object>();
        Transformation transformationTrain=null;
        List<Transformation> transformationList=null;
        NormalizerSquid normalizerSquid = JsonUtil.toGsonBean(JsonUtil.toHashMap(info).get("NormalizerSquid") + "", NormalizerSquid.class);
        try {
            resultMap= normalizerSquidService.insertNormalizerSquid(normalizerSquid);
            //根据 ds_tran_inputs_definiton 记录生成 input
            transformationList=(List<Transformation>)resultMap.get("TransformationList");
            for (Transformation newTrans: transformationList){
                newTrans.setInputs(transformationInputsService.initTransInputs(newTrans,newTrans.getOutput_data_type(),-1, "", -1, 0));
            }
            transformationTrain=(Transformation)resultMap.get("TransformationTrain");
            transformationTrain.setInputs(transformationInputsService.initTransInputs(transformationTrain, 0, -1, "", -1, 0));
        }catch (ErrorMessageException e){
            out.setMessageCode(MessageCode.parse(e.getErrorCode()));
            logger.error("创建NormalizerSquid异常", e);
        }catch (Exception e){
            e.printStackTrace();
            out.setMessageCode(MessageCode.INSERT_ERROR);
            if(e.getMessage().equals(MessageCode.ERR_SQUID_OVER_LIMIT.value()+"")){
                out.setMessageCode(MessageCode.ERR_SQUID_OVER_LIMIT);
            }
            logger.error("创建NormalizerSquid异常", e);
        }
        resultMap.put("TransformationList",transformationList);
        resultMap.put("TransformationTrain",transformationTrain);
        // 返回新增的FolderID
        InfoNewMessagePacket<Map<String, Object>> infoMessage = new InfoNewMessagePacket<Map<String, Object>>();
        infoMessage.setInfo(resultMap);
        infoMessage.setDesc("创建NormalizerSquid");
        infoMessage.setCode(out.getMessageCode().value());
        return JsonUtil.object2Json(infoMessage);
    }

    /**
     * 修改NormalizerSquid
     * @param info
     * @return
     */
    @SocketApiMethod(commandId = "0002")
    public String updateNormalizerSquid(String info){
        ReturnValue out = new ReturnValue();
        Map<String, Object> map = new HashMap<String, Object>();
        NormalizerSquid normalizerSquid = JsonUtil.toGsonBean(JsonUtil.toHashMap(info).get("NormalizerSquid") + "", NormalizerSquid.class);
        try {
            normalizerSquidService.updateNormalizerSquid(normalizerSquid);
        }catch (ErrorMessageException e){
            out.setMessageCode(MessageCode.parse(e.getErrorCode()));
        }catch (Exception e){
            e.printStackTrace();
            out.setMessageCode(MessageCode.UPDATE_ERROR);
            logger.error("修改NormalizerSquid异常", e);
        }
        return JsonUtil.toJsonString(map, DSObjectType.NORMALIZER, out.getMessageCode());
    }

    /**
     * 创建NormalizerSquidLink
     * @param info
     * @return
     */
    @SocketApiMethod(commandId = "0003")
    public String createNormalizerSquidLink(String info){
        ReturnValue out = new ReturnValue();
        Map<String, Object> resultMap = new HashMap<String, Object>();
        Transformation transformationTrain=null;
        List<Transformation> transformationList=null;
        try {
            resultMap=normalizerSquidService.createNormalizerSquidLink(info);
            //根据 ds_tran_inputs_definiton 记录生成 input
            transformationList=(List<Transformation>)resultMap.get("TransformationList");
            for (Transformation newTrans: transformationList){
                newTrans.setInputs(transformationInputsService.initTransInputs(newTrans,newTrans.getOutput_data_type(),-1, "", -1, 0));
            }
        }catch (ErrorMessageException e){
            out.setMessageCode(MessageCode.parse(e.getErrorCode()));
        }catch (Exception e){
            e.printStackTrace();
            out.setMessageCode(MessageCode.INSERT_ERROR);
            logger.error("创建NormalizerSquidLink异常", e);
        }
        resultMap.put("TransformationList",transformationList);
        // 返回新增的FolderID
        InfoNewMessagePacket<Map<String, Object>> infoMessage = new InfoNewMessagePacket<Map<String, Object>>();
        infoMessage.setInfo(resultMap);
        infoMessage.setDesc("创建NormalizerSquid");
        infoMessage.setCode(out.getMessageCode().value());
        return JsonUtil.object2Json(infoMessage);
    }

    /**
     * 获取NormalizerSquid模型
     * @param info
     * @return
     */
    @SocketApiMethod(commandId = "0004")
    public String getNormalizerSquidIds(String info){
        ReturnValue out = new ReturnValue();
        Map<String, Object> resultMap = new HashMap<String, Object>();
        Map<String, Object> parmsMap = JsonUtil.toHashMap(info);
        int repositoryIds=Integer.valueOf(parmsMap.get("RepositoryId")+"");
        int squidId = Integer.valueOf(parmsMap.get("SquidId")+"");
        boolean isFilter=Boolean.valueOf(parmsMap.get("IsFilter")+"");
        try {
            if (repositoryIds>0){
                Map<String,Object> map = normalizerSquidService.getProjectBySquidId(squidId);
                int projectId = Integer.parseInt(map.get("ID")+"");
                resultMap= normalizerSquidService.getNormalizerSquidIds(repositoryIds,squidId,isFilter,projectId);
            }
        }catch (ErrorMessageException e){
            out.setMessageCode(MessageCode.parse(e.getErrorCode()));
        }catch (Exception e){
            e.printStackTrace();
            out.setMessageCode(MessageCode.INSERT_ERROR);
            logger.error("获取NormalizerSquid模型异常", e);
        }
        InfoNewMessagePacket<Map<String, Object>> infoMessage = new InfoNewMessagePacket<Map<String, Object>>();
        infoMessage.setInfo(resultMap);
        infoMessage.setDesc("获取NormalizerSquid模型");
        infoMessage.setCode(out.getMessageCode().value());
        return JsonUtil.object2Json(infoMessage);
    }
}
