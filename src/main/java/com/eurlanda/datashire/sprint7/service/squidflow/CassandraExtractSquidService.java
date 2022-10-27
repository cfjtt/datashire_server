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

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Eurlanda on 2017/4/10.
 */
public class CassandraExtractSquidService {

    /**
     * 单独创建CassandraExtract
     *
     * @param info
     * @param out
     * @return
     */
    public String createExtractSquid(String info, ReturnValue out) {
        info = info.replaceAll("CassandraExtractSquid", "ExtractSquid");
        List<ExtractSquidAndSquidLink> vo = JsonUtil.toGsonList(info, ExtractSquidAndSquidLink.class);
        ListMessagePacket<ExtractSquidAndSquidLink> packet = new ListMessagePacket<ExtractSquidAndSquidLink>();
        packet.setType(DSObjectType.CASSANDRA_EXTRACT);
        ExtractServicesub subService = new ExtractServicesub(TokenUtil.getToken());
        if (vo != null) {
            subService.drag2ExtractSquid(vo, out);
            packet.setDataList(vo);
            packet.setCode(out.getMessageCode().value());
        } else {
            packet.setCode(MessageCode.ERR_EXTRACT_SQUID_NO_DATA.value());
        }
        return JsonUtil.object2Json(packet);
    }

    /**
     * 批量创建Cassandra Extract
     *
     * @param sourceTableIds
     * @param connectionSquidId
     * @param squidFlowId
     * @param out
     * @param cmd1
     * @param cmd2
     * @param x
     * @param y
     */
    public void createExtractBatch(List<Integer> sourceTableIds, Integer connectionSquidId, Integer squidFlowId, ReturnValue out, String cmd1, String cmd2, Integer x, Integer y) {
        SquidService server = new SquidService(TokenUtil.getToken());
        server.createCassandraExtractTable(sourceTableIds, connectionSquidId, squidFlowId, out, cmd1, cmd2, TokenUtil.getKey(), x, y);
    }

    /**
     * 拖拽某些列创建Extract
     *
     * @param info
     * @param out
     * @return
     */
    public String createExtractByColumn(String info, ReturnValue out) {
        info = info.replaceAll("CassandraExtractSquid", "ExtractSquid");
        ExtractSquidAndSquidLink vo = JsonUtil.toGsonList(info, ExtractSquidAndSquidLink.class) == null ? null : JsonUtil.toGsonList(info, ExtractSquidAndSquidLink.class).get(0);
        InfoMessagePacket<ExtractSquidAndSquidLink> packet = new InfoMessagePacket<ExtractSquidAndSquidLink>();
        packet.setType(DSObjectType.CASSANDRA_EXTRACT);
        ExtractServicesub extractServicesub = new ExtractServicesub(TokenUtil.getToken());
        if (vo != null) {
            packet.setInfo(extractServicesub.drag2ExtractSquid(vo, out));
            packet.setCode(out.getMessageCode().value());
        } else {
            packet.setCode(MessageCode.DESERIALIZATION_FAILED.value());
        }
        return JsonUtil.object2Json(packet);
    }

    /**
     * 连接数据库
     *
     * @param info
     * @return
     */
    public String connectCassandraSquid(String info, ReturnValue out) throws Exception {
        ConnectProcess connectProcess = new ConnectProcess(TokenUtil.getToken(), TokenUtil.getKey());
        List<SourceTable> sourceTables = connectProcess.getCassandraConnect(info, DataBaseType.CASSANDRA.value(), out);
        // 对返回值对象进行封装
        ListMessagePacket<SourceTable> listMessage = new ListMessagePacket<SourceTable>();
        listMessage.setCode(out.getMessageCode().value());
        if (sourceTables == null) {
            sourceTables = new ArrayList<>();
        }
        listMessage.setDataList(sourceTables);
        return JsonUtil.object2Json(listMessage);
    }
}
