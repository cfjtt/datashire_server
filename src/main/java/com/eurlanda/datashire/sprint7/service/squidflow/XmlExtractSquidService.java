package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.entity.SquidLink;
import com.eurlanda.datashire.entity.XmlExtractSquid;
import com.eurlanda.datashire.enumeration.DMLType;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.server.utils.TokenUtil;
import com.eurlanda.datashire.sprint7.packet.InfoNewMessagePacket;
import com.eurlanda.datashire.sprint7.packet.PushMessagePacket;
import com.eurlanda.datashire.sprint7.service.squidflow.subservice.ExtractServicesub;
import com.eurlanda.datashire.sprint7.service.squidflow.subservice.XmlExtractSquidServiceSub;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import com.eurlanda.datashire.utility.StringUtils;
import org.apache.log4j.Logger;
import org.apache.poi.poifs.filesystem.NotOLE2FileException;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

@Service
public class XmlExtractSquidService {
    public static final Logger logger = Logger
            .getLogger(ExtractDBSquidService.class);
    private String token;
    private String key;

    // 异常处理机制
    ReturnValue out = new ReturnValue();

    /**
     * 创建XmlExtractSquid
     * 
     * @param info
     * @return
     * @author bo.dang
     * @date 2014年5月8日
     */
    public String createXmlExtractSquid(String info) {
    	out = new ReturnValue();
        Map<String, Class<?>> paramsMap = new HashMap<String, Class<?>>();
        paramsMap.put("XmlExtractSquid", XmlExtractSquid.class);
        paramsMap.put("SquidLink", SquidLink.class);
        Map<String, Object> resultMap = new ExtractServicesub(TokenUtil.getToken()).execute(info, paramsMap, DMLType.INSERT.value(), out);
        
        // 返回新增的FolderID
        InfoNewMessagePacket<Map<String, Object>> infoMessage = new InfoNewMessagePacket<Map<String,Object>>();
        infoMessage.setCode(out.getMessageCode().value());
        if (out.getMessageCode()==MessageCode.SUCCESS){
        	infoMessage.setInfo(resultMap);
        }else{
        	infoMessage.setInfo(new HashMap<String, Object>());
        }
        infoMessage.setDesc("创建XmlExtractSquid");
        return JsonUtil.object2Json(infoMessage);
    }

    /**
     * 获取XmlExtractSquid元数据
     * 
     * @param info
     * @return
     * @author bo.dang
     * @date 2014年5月8日
     */
    @SuppressWarnings("unchecked")
    public String getSourceDataByXmlExtractSquid(String info) {
    	out = new ReturnValue();
        Map<String, Object> infoMap = JsonUtil.toHashMap(info);
        InfoNewMessagePacket<Map<String, Object>> infoMessage = new InfoNewMessagePacket<Map<String, Object>>();
        Timer timer = new Timer();
        final Map<String,Object> returnMap = new HashMap<>();
        returnMap.put("1",1);
        key = TokenUtil.getKey();
        token = TokenUtil.getToken();
        try {
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    PushMessagePacket.pushMap(returnMap,DSObjectType.SQUID,"1005","0001",key,token,MessageCode.BATCH_CODE.value());
                }

            },25*1000,25*1000);
            Map<String, Object> resultMap = new XmlExtractSquidServiceSub(TokenUtil.getToken()).getSourceDataByExtractSquid(infoMap, out);
            infoMessage.setInfo(resultMap);
            if(StringUtils.isNotNull(resultMap)){
                infoMessage.setCode(out.getMessageCode().value());
            } else {
                infoMessage.setCode(MessageCode.ERR_EXTRACT_SQUID_NO_DATA.value());
            }
        } catch (NotOLE2FileException e){
            timer.cancel();
            timer.purge();
            logger.error(e);
            infoMessage.setCode(MessageCode.ERR_FILEOLE2.value());
        } catch (Exception e){
            timer.cancel();
            timer.purge();
            logger.error(e);
            logger.error("抽取file foler connection中的文件失败");
            infoMessage.setCode(MessageCode.ERR_EXTRACT_SQUID_NO_DATA.value());
        } finally {
            timer.cancel();
            timer.purge();
        }

        infoMessage.setDesc("获取XmlExtractSquid元数据");
        return JsonUtil.object2Json(infoMessage);
    }
    
    /**
     * 更新XmlExtractSquid信息
     * @param info
     * @return
     * @author bo.dang
     * @date 2014年5月7日
     */
    public String updateXmlExtractSquid(String info){
        out = new ReturnValue();
        Map<String, Class<?>> paramsMap = new HashMap<String, Class<?>>();
        paramsMap.put("XmlExtractSquid", XmlExtractSquid.class);
        // 更新XmlExtractSquid
        Map<String, Object> resultMap = new ExtractServicesub(TokenUtil.getToken()).execute(info, paramsMap, DMLType.UPDATE.value(), out);
        InfoNewMessagePacket<Map<String, Object>> infoMessage = new InfoNewMessagePacket<Map<String, Object>>();
        infoMessage.setCode(out.getMessageCode().value());
        if (out.getMessageCode()==MessageCode.SUCCESS){
        	infoMessage.setInfo(resultMap);
        }else{
        	infoMessage.setInfo(new HashMap<String, Object>());
        }
        infoMessage.setDesc("更新XmlExtarctSquid信息");
        return JsonUtil.object2Json(infoMessage);
    }

}
