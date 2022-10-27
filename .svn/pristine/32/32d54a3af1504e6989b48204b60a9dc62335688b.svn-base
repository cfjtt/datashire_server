package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.entity.SourceTable;
import com.eurlanda.datashire.entity.operation.ExtractSquidAndSquidLink;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.enumeration.DataBaseType;
import com.eurlanda.datashire.server.utils.TokenUtil;
import com.eurlanda.datashire.sprint7.packet.InfoMessagePacket;
import com.eurlanda.datashire.sprint7.packet.ListMessagePacket;
import com.eurlanda.datashire.sprint7.service.squidflow.subservice.ExtractServicesub;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Eurlanda on 2017/3/22.
 */
public class HiveExtractSquidService {

    private Logger logger = Logger.getLogger(HiveExtractSquidService.class);
    /**
     * 批量创建hiveExtract
     * @param sourceTableIds
     * @param connectionSquidId
     * @param squidFlowId
     * @param out
     * @param cmd1
     * @param cmd2
     * @param x
     * @param y
     * @return
     */
    public void createExtractBatch(List<Integer> sourceTableIds, Integer connectionSquidId, Integer squidFlowId, ReturnValue out, String cmd1, String cmd2, Integer x, Integer y){
        SquidService server = new SquidService(TokenUtil.getToken());
        server.createHiveExtractTable(sourceTableIds,connectionSquidId, squidFlowId,out,cmd1,cmd2,TokenUtil.getKey(),x,y);
    }
    /**
     * 创建落地目标(单个创建)
     * @param info
     * @param out
     * @return
     */
    public String createHiveExtract(String info,ReturnValue out){
        info=info.replaceAll("SystemHiveExtractSquid","ExtractSquid");
        List<ExtractSquidAndSquidLink> vo = JsonUtil.toGsonList(info,ExtractSquidAndSquidLink.class);
        ListMessagePacket<ExtractSquidAndSquidLink> packet = new ListMessagePacket<ExtractSquidAndSquidLink>();
        packet.setType(DSObjectType.HIVEEXTRACT);
        ExtractServicesub subService = new ExtractServicesub(TokenUtil.getToken());
        if(vo!=null){
            subService.drag2ExtractSquid(vo, out);
            packet.setDataList(vo);
            packet.setCode(out.getMessageCode().value());
        }else{
            packet.setCode(MessageCode.ERR_EXTRACT_SQUID_NO_DATA.value());
        }
        return JsonUtil.object2Json(packet);
    }

    /**
     * 支持个别列抽取
     * @param info
     * @param out
     * @return
     */
    public String createHiveExtractByColumn(String info,ReturnValue out){
        info=info.replaceAll("SystemHiveExtractSquid","ExtractSquid");
        ExtractSquidAndSquidLink vo = JsonUtil.toGsonList(info, ExtractSquidAndSquidLink.class) == null? null:JsonUtil.toGsonList(info, ExtractSquidAndSquidLink.class).get(0);
        InfoMessagePacket<ExtractSquidAndSquidLink> packet = new InfoMessagePacket<ExtractSquidAndSquidLink>();
        packet.setType(DSObjectType.HIVEEXTRACT);
        ExtractServicesub extractServicesub = new ExtractServicesub(TokenUtil.getToken());
        if(vo!=null){
            packet.setInfo(extractServicesub.drag2ExtractSquid(vo, out));
            packet.setCode(out.getMessageCode().value());
        }else{
            packet.setCode(MessageCode.DESERIALIZATION_FAILED.value());
        }
        return JsonUtil.object2Json(packet);
    }

    /**
     * 连接hive
     * @param info
     * @param out
     * @return
     */
    public String connectHiveSquid(String info,ReturnValue out) throws Exception {
        ConnectProcess connectProcess = new ConnectProcess(TokenUtil.getToken(), TokenUtil.getKey());
        List<SourceTable> sourceTables = connectProcess.getConnect(info, DataBaseType.HIVE.value(), out);
        // 对返回值对象进行封装
        ListMessagePacket<SourceTable> listMessage = new ListMessagePacket<SourceTable>();
        listMessage.setCode(out.getMessageCode().value());
        if(sourceTables == null) {
            sourceTables = new ArrayList<>();
        }
        listMessage.setDataList(sourceTables);
        return JsonUtil.object2Json(listMessage);
    }
}
