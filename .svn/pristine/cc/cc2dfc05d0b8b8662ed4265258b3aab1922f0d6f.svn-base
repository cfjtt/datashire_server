package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.entity.WeiBoExtractSquid;
import com.eurlanda.datashire.enumeration.DMLType;
import com.eurlanda.datashire.server.utils.TokenUtil;
import com.eurlanda.datashire.sprint7.packet.InfoNewMessagePacket;
import com.eurlanda.datashire.sprint7.service.squidflow.subservice.ExtractServicesub;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import com.eurlanda.datashire.utility.StringUtils;
import org.apache.log4j.Logger;
import org.apache.poi.poifs.filesystem.NotOLE2FileException;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
public class WeiBoExtractSquidService {
    public static final Logger logger = Logger.getLogger(ExtractDBSquidService.class);
    private String token;
    private String key;
    
    
    //异常处理机制
    ReturnValue out = new ReturnValue();
    
    /**
     * 创建WeiBoExtractSquid
     * @param info
     * @return
     * @author bo.dang
     * @date 2014年5月6日
     */
    public String createWeiBoExtractSquid(String info){
    	out = new ReturnValue();
        Map<String, Object> infoMap = JsonUtil.toHashMap(info);
        InfoNewMessagePacket<Map<String, Object>> infoMessage = new InfoNewMessagePacket<Map<String,Object>>();

        try{
            Map<String, Object> resultMap = new ExtractServicesub(TokenUtil.getToken()).getSourceDataByExtractSquid(infoMap, out);
            infoMessage.setInfo(resultMap);
            // 有源数据或者解析成功
            if(StringUtils.isNotNull(resultMap)){
                infoMessage.setCode(out.getMessageCode().value());
            } else {
                infoMessage.setCode(MessageCode.ERR_EXTRACT_SQUID_NO_DATA.value());
            }
        } catch (NotOLE2FileException e){
            logger.error(e);
            infoMessage.setCode(MessageCode.ERR_FILEOLE2.value());
        } catch (Exception e){
            logger.error(e);
            logger.error("抽取file foler connection中的文件失败");
            infoMessage.setCode(MessageCode.ERR_EXTRACT_SQUID_NO_DATA.value());
        }

        infoMessage.setDesc("创建WeiBoExtractSquidService");
        return JsonUtil.object2Json(infoMessage);
    }
    
    /**
     * 更新WeiBoExtractSquid信息
     * @param info
     * @return
     * @author bo.dang
     * @date 2014年5月7日
     */
    public String updateWeiBoExtractSquid(String info){
    	out = new ReturnValue();
        Map<String, Class<?>> paramsMap = new HashMap<String, Class<?>>();
        paramsMap.put("WeiBoExtractSquid", WeiBoExtractSquid.class);
        // 更新DocExtractSquid
        Map<String, Object> resultMap = new ExtractServicesub(TokenUtil.getToken()).execute(info, paramsMap, DMLType.UPDATE.value(), out);
        InfoNewMessagePacket<Map<String, Object>> infoMessage = new InfoNewMessagePacket<Map<String, Object>>();
        infoMessage.setCode(out.getMessageCode().value());
        if (out.getMessageCode()==MessageCode.SUCCESS){
        	infoMessage.setInfo(resultMap);
        }else{
        	infoMessage.setInfo(new HashMap<String, Object>());
        }
        infoMessage.setDesc("更新WeiBoExtarctSquid信息");
        return JsonUtil.object2Json(infoMessage);
    }
}
