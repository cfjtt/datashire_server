package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.entity.*;
import com.eurlanda.datashire.enumeration.DMLType;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.enumeration.TransformationTypeEnum;
import com.eurlanda.datashire.enumeration.datatype.SystemDatatype;
import com.eurlanda.datashire.server.utils.TokenUtil;
import com.eurlanda.datashire.sprint7.packet.InfoNewMessagePacket;
import com.eurlanda.datashire.sprint7.service.squidflow.subservice.ReferenceGroupService;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import com.eurlanda.datashire.utility.StringUtils;
import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * Created by yhc on 1/21/2016.
 */
@Service
public class KafkaExtractSquidService {

    private static Log log = LogFactory.getLog(KafkaConnectionSquidService.class);

    private String token;//令牌根据令牌得到相应的连接信息
    private String key;// key值

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getToken() {
        return token;
    }
    public void setToken(String token) {
        this.token = token;
    }

    /**
     * 创建Kafka 抽取 SquidModelBase
     * @param info
     * @return
     */
    public String createKafkaExtractSquid(String info) {
        ReturnValue out = new ReturnValue();
        Map<String, Object> outputMap = new HashMap<>();
        IRelationalDataManager adapter = null;
        DataAdapterFactory adaFactory = null;

        try {
            //adaFactory = DataAdapterFactory.newInstance();
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            //获取数据Map
            Map<String, Object> paramsMap = JsonUtil.toHashMap(info);

            //解析前台传递的参数
            int SquidFlowId = (int) paramsMap.get("SquidFlowId");
            int ConnectionSquidId = (int) paramsMap.get("ConnectionSquidId");
            int SourceTableId = (int) paramsMap.get("SourceTableId");
            int positionX = (int)paramsMap.get("PositionX");
            int positionY = (int)paramsMap.get("PositionY");

            //查询SourceTable表
            Map<String, String> sourceTableParam = new HashMap<>();
            sourceTableParam.put("id", SourceTableId + "");
            DBSourceTable sourceTable = adapter.query2Object2(true, sourceTableParam, DBSourceTable.class);

            //创建实体对象
            KafkaExtractSquid kafkaExtractSquid = new KafkaExtractSquid();
            kafkaExtractSquid.setTable_name(sourceTable.getTable_name());
            kafkaExtractSquid.setSquidflow_id(SquidFlowId);
            kafkaExtractSquid.setSource_table_id(SourceTableId);
            kafkaExtractSquid.setName(StringUtils.getSquidName(adapter, sourceTable.getTable_name(), SquidFlowId));
            kafkaExtractSquid.setSquid_type(SquidTypeEnum.KAFKAEXTRACT.value());
            kafkaExtractSquid.setKey(StringUtils.generateGUID());
            kafkaExtractSquid.setLocation_x(positionX);
            kafkaExtractSquid.setLocation_y(positionY);
            kafkaExtractSquid.setNumPartitions(1); //默认为1

            //Squid入库
            kafkaExtractSquid.setId(adapter.insert2(kafkaExtractSquid));

            //处理Squid Link
            SquidLink newSquidLink = new SquidLink();
            newSquidLink.setFrom_squid_id(ConnectionSquidId);
            newSquidLink.setSquid_flow_id(SquidFlowId);
            newSquidLink.setTo_squid_id(kafkaExtractSquid.getId());
            newSquidLink.setKey(StringUtils.generateGUID());
            newSquidLink.setId(adapter.insert2(newSquidLink));

            //增加默认的RefColumn与Column


            //更新SourceTable的是否抽取
            if (!sourceTable.isIs_extracted()) {
                sourceTable.setIs_extracted(true);
                adapter.update2(sourceTable);
            }

            outputMap.put("newSquid", kafkaExtractSquid);
            outputMap.put("SquidLink", newSquidLink);

        } catch (Exception e) {
            log.error("Create KafkaExtractSquid error", e);
            out.setMessageCode(MessageCode.INSERT_ERROR);
            try {
                adapter.rollback();
            } catch (SQLException sqlE) {
                log.fatal("rollback error", sqlE);
            }
        } finally {
            adapter.closeSession();
        }
        InfoNewMessagePacket infoPacket = new InfoNewMessagePacket(outputMap, out.getMessageCode().value());
        return infoPacket.toJsonString();
    }

    /**
     * 更新 Kafka Extract SquidModelBase
     * @param info
     * @return
     */
    public String updateKafkaExtractSquid(String info){
        ReturnValue out = new ReturnValue();
        DataAdapterFactory adapterFactory = null;
        IRelationalDataManager adapter = null;

        try {
            adapterFactory = DataAdapterFactory.newInstance();
            adapter = DataAdapterFactory.getDefaultDataManager();
            Map<String, Object> paramsMap = JsonUtil.toHashMap(info);
            KafkaExtractSquid currentSquid = JsonUtil.toGsonBean(String.valueOf(paramsMap.get("KafkaExtractSquid")), KafkaExtractSquid.class);
            adapter.openSession();
            if (currentSquid != null){
                adapter.update2(currentSquid);
            } else {
                out.setMessageCode(MessageCode.NODATA);
            }
        } catch (Exception e){
            log.error("Updated Kafka Connection SquidModelBase error.", e);
            out.setMessageCode(MessageCode.UPDATE_ERROR);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                log.fatal("rollback err!", e1);
            }
        }finally {
            adapter.closeSession();
        }

        InfoNewMessagePacket infoNewMessagePacket = new InfoNewMessagePacket<>(new HashMap<>(), out.getMessageCode().value());
        return  infoNewMessagePacket.toJsonString();
    }

    /**
     * 获取 Kafka Extract SquidModelBase 元数据
     * @param info
     * @return
     */
    public String getKafkaExtractMetadata(String info) {
        ReturnValue out = new ReturnValue();
        DataAdapterFactory adapterFactory = null;
        IRelationalDataManager adapter = null;
        Map<String, Object> outputMap = new HashMap<>();
        try {
            Map<String, Object> paramsMap = JsonUtil.toHashMap(info);
            adapterFactory = DataAdapterFactory.newInstance();
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            KafkaExtractSquid currentSquid = JsonUtil.toGsonBean(String.valueOf(paramsMap.get("KafkaExtractSquid")), KafkaExtractSquid.class);
            SquidLink currentSquidLink = JsonUtil.toGsonBean(String.valueOf(paramsMap.get("SquidLink")), SquidLink.class);


            TransformationService transformationService = new TransformationService(TokenUtil.getToken());

            //重复抽取，删除老的SourceTable数据
            Map<String, String> checkSouceTableDataParam = new HashMap<>();
            checkSouceTableDataParam.put("source_table_id", Integer.toString(currentSquid.getSource_table_id(), 10));
            List<SourceColumn> sourcColumnTempList = adapter.query2List(true, checkSouceTableDataParam, SourceColumn.class);
            if (StringUtils.isNotNull(sourcColumnTempList) && !sourcColumnTempList.isEmpty()) {
                DataShirServiceplug dataShirServiceplug = new DataShirServiceplug(TokenUtil.getToken());
                dataShirServiceplug.deleteSourceColumnList(adapter, currentSquidLink.getFrom_squid_id(), currentSquid.getId(), currentSquid.getSource_table_id(), out);
            }

            //region 创建ReferenceColumn相关的对象
            //创建RefColumn KEY
            SourceColumn tempSourceColumnKey = new SourceColumn(); /*这地方需要先创建SourceColumn去转换ReferenceColumn*/
            tempSourceColumnKey.setName("KEY");
            tempSourceColumnKey.setData_type(SystemDatatype.NVARCHAR.value());
            tempSourceColumnKey.setLength(-1);
            tempSourceColumnKey.setPrecision(0);
            tempSourceColumnKey.setSource_table_id(currentSquid.getSource_table_id());
            tempSourceColumnKey.setRelative_order(1);
            tempSourceColumnKey.setNullable(true);
            tempSourceColumnKey.setId(adapter.insert2(tempSourceColumnKey)); /*入库*/

            //创建RefColumn VALUE
            SourceColumn tempSourceColumnValue = new SourceColumn(); /*这地方需要先创建SourceColumn去转换ReferenceColumn*/
            tempSourceColumnValue.setName("VALUE");
            tempSourceColumnValue.setData_type(SystemDatatype.NVARCHAR.value());
            tempSourceColumnValue.setLength(-1);
            tempSourceColumnValue.setPrecision(0);
            tempSourceColumnValue.setSource_table_id(currentSquid.getSource_table_id());
            tempSourceColumnValue.setRelative_order(1);
            tempSourceColumnValue.setNullable(true);
            tempSourceColumnValue.setId(adapter.insert2(tempSourceColumnValue)); /*入库*/

            //创建RefColumnGroup
            /*获取当前Extract的上游ConnectionSquid*/
            /*Map<String, Object> getConnectionSquidParams = new HashMap<>();
            getConnectionSquidParams.put("id",currentSquidLink.getFrom_squid_id());*/
            String sql="select * from ds_squid where id="+currentSquidLink.getFrom_squid_id();
            KafkaSquid kafkaConnectionSquid = adapter.query2Object(true,sql,null,KafkaSquid.class);
            ReferenceColumnGroup columnGroup = new ReferenceGroupService(TokenUtil.getToken()).mergeReferenceColumnGroup(adapter, currentSquidLink.getFrom_squid_id(), currentSquid.getId());
            /*通过一些列繁琐的参数，入库创建RefColumn，并且返回实体对象*/
            ReferenceColumn refColumnKey = transformationService.initReference_fileFolder(adapter, tempSourceColumnKey, tempSourceColumnKey.getId(), 1, kafkaConnectionSquid, currentSquid.getId(), columnGroup);
            ReferenceColumn refColumnValue = transformationService.initReference_fileFolder(adapter, tempSourceColumnValue, tempSourceColumnValue.getId(), 2, kafkaConnectionSquid, currentSquid.getId(), columnGroup);

            //同步下游
            RepositoryServiceHelper helper = new RepositoryServiceHelper(TokenUtil.getToken(), adapter);
            helper.synchronousInsertRefColumn(adapter, refColumnKey, DMLType.INSERT.value());
            helper.synchronousInsertRefColumn(adapter, refColumnValue, DMLType.INSERT.value());

            //加入返回参数集合
            List<ReferenceColumn> refColumnList = new ArrayList<>();
            refColumnList.add(refColumnKey);
            refColumnList.add(refColumnValue);


            //创建虚拟Trans 通过RefColumn创建
            Transformation startVTransKey = transformationService.initTransformation(adapter, currentSquid.getId(), refColumnKey.getId(), TransformationTypeEnum.VIRTUAL.value(), refColumnKey.getData_type(), 1);
            Transformation startVTransValue = transformationService.initTransformation(adapter, currentSquid.getId(), refColumnValue.getId(), TransformationTypeEnum.VIRTUAL.value(), refColumnValue.getData_type(), 1);

            //加入返回参数集合
            List<Transformation> vTransList = new ArrayList<>();
            vTransList.add(startVTransKey);
            vTransList.add(startVTransValue);
            //endregion

            //region 创建Column相关对象
            //创建Column对象
            Map<String, Object> nameMap = new HashMap<String, Object>(); /*不知道这是干嘛的*/
            Column columnKey = transformationService.initColumn(adapter, tempSourceColumnKey, 1, currentSquid.getId(), nameMap);
            Column columnValue = transformationService.initColumn(adapter, tempSourceColumnValue, 1, currentSquid.getId(), nameMap);

            //加入返回参数集合
            List<Column> columnList = new ArrayList<>();
            columnList.add(columnKey);
            columnList.add(columnValue);
            //给列排序
            int no =1;
            for (int x=0;x<columnList.size();x++){
                adapter.execute("delete from ds_column where id =" + columnList.get(x).getId());
                columnList.get(x).setRelative_order( no++);
                columnList.get(x).setId(adapter.insert2(columnList.get(x)));
            }
            //同步下游
            helper.synchronousInsertColumn(adapter, columnKey.getSquid_id(), columnKey, DMLType.INSERT.value(), false);
            helper.synchronousInsertColumn(adapter, columnValue.getSquid_id(), columnValue, DMLType.INSERT.value(), false);

            //创建Trans对象
            Transformation endVTransKey = transformationService.initTransformation(adapter, currentSquid.getId(), columnKey.getId(), TransformationTypeEnum.VIRTUAL.value(), columnKey.getData_type(), 1);
            Transformation endVTransValue = transformationService.initTransformation(adapter, currentSquid.getId(), columnValue.getId(), TransformationTypeEnum.VIRTUAL.value(), columnValue.getData_type(), 1);

            //加入返回集合
            vTransList.add(endVTransKey);
            vTransList.add(endVTransValue);
            //endregion

            //region 创建Translink
            TransformationLink transLinkKey = transformationService.initTransformationLink(adapter, startVTransKey.getId(), endVTransKey.getId(), 1);
            TransformationLink transLinkValue= transformationService.initTransformationLink(adapter, startVTransValue.getId(), endVTransValue.getId(), 2);

            //加入返回集合
            List<TransformationLink> transLinkList = new ArrayList<>();
            transLinkList.add(transLinkKey);
            transLinkList.add(transLinkValue);
            //endregion

            outputMap.put("Columns", columnList);
            outputMap.put("ReferenceColumns", refColumnList);
            outputMap.put("Transformations", vTransList);
            outputMap.put("TransformationLinks", transLinkList);

        } catch (Exception e) {
            log.error("获取 kafka extract squid元数据异常", e);
            out.setMessageCode(MessageCode.COLUMNLIST_NULL);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                log.fatal("rollback err!", e1);
            }
        }finally {
            adapter.closeSession();
        }
        InfoNewMessagePacket infoPacket = new InfoNewMessagePacket(outputMap, out.getMessageCode().value());
        return infoPacket.toJsonString();
    }


}
