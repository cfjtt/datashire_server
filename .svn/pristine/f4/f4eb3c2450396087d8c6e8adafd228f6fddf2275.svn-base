package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.common.util.ConstantsUtil;
import com.eurlanda.datashire.entity.*;
import com.eurlanda.datashire.enumeration.DMLType;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.enumeration.TransformationTypeEnum;
import com.eurlanda.datashire.enumeration.datatype.SystemDatatype;
import com.eurlanda.datashire.server.utils.TokenUtil;
import com.eurlanda.datashire.sprint7.packet.InfoNewMessagePacket;
import com.eurlanda.datashire.sprint7.packet.PushMessagePacket;
import com.eurlanda.datashire.sprint7.service.squidflow.subservice.ReferenceGroupService;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import com.eurlanda.datashire.utility.StringUtils;
import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Created by yhc on 2/22/2016.
 */
@Service
public class HbaseExtractSquidService {
    private static Log log = LogFactory.getLog(HbaseExtractSquidService.class);

    private String token;//令牌根据令牌得到相应的连接信息
    public String getKey() {
        return key;
    }
    public void setKey(String key) {
        this.key = key;
    }

    private String key;// key值
    public String getToken() {
        return token;
    }
    public void setToken(String token) {
        this.token = token;
    }

    /**
     * 创建 Hbase extract squid
     * @param info
     * @return
     */
    public String createHbaseExtractSquid(String info){
        ReturnValue out = new ReturnValue();
        Map<String, Object> outputMap = new HashMap<>();
        IRelationalDataManager adapter = null;
        DataAdapterFactory adaFactory = null;

        try {
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
            //squid 名称不能超过25位
            String tableName = "";
            if (sourceTable.getTable_name().length() > 25) {
                tableName = sourceTable.getTable_name().substring(0, 25);
            } else {
                tableName = sourceTable.getTable_name();
            }
            //创建实体对象
            HBaseExtractSquid hbaseExtractSquid = new HBaseExtractSquid();
            hbaseExtractSquid.setTable_name(sourceTable.getTable_name());
            hbaseExtractSquid.setSquidflow_id(SquidFlowId);
            hbaseExtractSquid.setSource_table_id(SourceTableId);
            hbaseExtractSquid.setName(StringUtils.getSquidName(adapter, tableName, SquidFlowId));
            hbaseExtractSquid.setSquid_type(SquidTypeEnum.HBASEEXTRACT.value());
            hbaseExtractSquid.setKey(StringUtils.generateGUID());
            hbaseExtractSquid.setLocation_x(positionX);
            hbaseExtractSquid.setLocation_y(positionY);

            //Squid入库
            hbaseExtractSquid.setId(adapter.insert2(hbaseExtractSquid));

            //处理Squid Link
            SquidLink newSquidLink = new SquidLink();
            newSquidLink.setFrom_squid_id(ConnectionSquidId);
            newSquidLink.setSquid_flow_id(SquidFlowId);
            newSquidLink.setTo_squid_id(hbaseExtractSquid.getId());
            newSquidLink.setKey(StringUtils.generateGUID());
            newSquidLink.setId(adapter.insert2(newSquidLink));

            //增加默认的RefColumn与Column

            //更新SourceTable的是否抽取
            if (!sourceTable.isIs_extracted()) {
                sourceTable.setIs_extracted(true);
                adapter.update2(sourceTable);
            }

            outputMap.put("newSquid", hbaseExtractSquid);
            outputMap.put("SquidLink", newSquidLink);

        } catch (Exception e) {
            log.error("Create HbaseExtractSquid error", e);
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
     * 更新Hbase extract squid
     * @param info
     * @return
     */
    public String updateHbaseExtractSquid(String info){
        ReturnValue out = new ReturnValue();
        DataAdapterFactory adapterFactory = null;
        IRelationalDataManager adapter = null;

        try {
            adapterFactory = DataAdapterFactory.newInstance();
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            Map<String, Object> paramsMap = JsonUtil.toHashMap(info);
            HBaseExtractSquid currentSquid = JsonUtil.toGsonBean(String.valueOf(paramsMap.get("HbaseExtractSquid")), HBaseExtractSquid.class);
            if (currentSquid != null){
                adapter.update2(currentSquid);
            } else {
                out.setMessageCode(MessageCode.NODATA);
            }
        } catch (Exception e){
            log.error("Updated Hbase Extract SquidModelBase error.", e);
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
     * 获取 Hbase extract squid 元数据
     * @param info
     * @return
     */
    public String getHbaseExtractSquidMetadata(String info){
        ReturnValue out = new ReturnValue();
        DataAdapterFactory adapterFactory = null;
        IRelationalDataManager adapter = null;
        Map<String, Object> outputMap = new HashMap<>();
        Timer timer = new Timer();
        try {
            key=TokenUtil.getKey();
            token = TokenUtil.getToken();
            final Map<String,Object> returnMap = new HashMap<>();
            returnMap.put("1", 1);
            //增加定时器
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    PushMessagePacket.pushMap(returnMap,DSObjectType.SQUID_FLOW,"1030","0003",key,token,MessageCode.BATCH_CODE.value());
                }
            },25*1000,25*1000);
            Map<String, Object> paramsMap = JsonUtil.toHashMap(info);
            adapterFactory = DataAdapterFactory.newInstance();
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            HBaseExtractSquid currentSquid = JsonUtil.toGsonBean(String.valueOf(paramsMap.get("HbaseExtractSquid")), HBaseExtractSquid.class);
            HBaseConnectionSquid
                    currentConnectionSquid= JsonUtil.toGsonBean(String.valueOf(paramsMap.get("HbaseConnectionSquid")), HBaseConnectionSquid.class);
            SquidLink currentSquidLink = JsonUtil.toGsonBean(String.valueOf(paramsMap.get("SquidLink")), SquidLink.class);
            // 注释掉因为bug6495
            /*SourceTable currentSourceTable = null;
            for (SourceTable st : currentConnectionSquid.getSourceTableList()) {
                if(st.getId() == currentSquid.getSource_table_id()) {
                    currentSourceTable = st;
                    break;
                }
            }

            if (currentSourceTable == null)
                throw new Exception("Source Table为空，必要数据缺失,严重错误");*/

            HbaseService hbaseService = new HbaseService();
            List<Cell> htableCells = hbaseService.getHbaseColumnAndColumnFamilyNames(50, currentConnectionSquid,currentSquid.getTable_name());

            //通过Column Names开始创建元数据
            TransformationService transformationService = new TransformationService(TokenUtil.getToken());
            //重复抽取，删除老的SourceTable数据
            Map<String, String> checkSouceTableDataParam = new HashMap<>();
            checkSouceTableDataParam.put("source_table_id", Integer.toString(currentSquid.getSource_table_id(), 10));
            List<SourceColumn> sourcColumnTempList = adapter.query2List(true, checkSouceTableDataParam, SourceColumn.class);
            if (StringUtils.isNotNull(sourcColumnTempList) && !sourcColumnTempList.isEmpty()) {
                DataShirServiceplug dataShirServiceplug = new DataShirServiceplug(TokenUtil.getToken());
                dataShirServiceplug.hBasedeleteSourceColumnList(adapter, currentSquidLink.getFrom_squid_id(), currentSquid.getId(), currentSquid.getSource_table_id(), out);
            }

            outputMap = this.createExtractSquidMetadata(sourcColumnTempList,htableCells, currentSquid, adapter, transformationService, currentSquidLink);


        } catch (Exception ex){
            timer.purge();
            timer.cancel();
            log.error("获取 hbase extract squid元数据异常", ex);
            log.error(ex);
            ex.printStackTrace();
            out.setMessageCode(MessageCode.ERR_DATABASE_NULL);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                log.fatal("rollback err!", e1);
            }
        }finally {
            timer.purge();
            timer.cancel();
            adapter.closeSession();
            InfoNewMessagePacket infoPacket = new InfoNewMessagePacket(outputMap, out.getMessageCode().value());
            return infoPacket.toJsonString();
        }
    }

    /**
     * 通过名称创建RefColumn 并且入库，并且同步下游
     * @param htableResults
     * @param currentSquid
     * @param adapter
     * @param transformationService
     * @param currentSquidLink
     * @return
     * @throws SQLException
     * @throws Exception
     */
    private Map<String, Object> createExtractSquidMetadata(List<SourceColumn> sourcColumnTempList,List<Cell> htableResults, DataSquid currentSquid,
                                                           IRelationalDataManager adapter, TransformationService transformationService,
                                                           SquidLink currentSquidLink) throws SQLException, Exception {
        RepositoryServiceHelper helper = new RepositoryServiceHelper(TokenUtil.getToken(), adapter);
        List<ReferenceColumn> refColumnList = new ArrayList<>();
        List<Column> columnList = new ArrayList<>();
        List<Transformation> transList = new ArrayList<>();
        List<TransformationLink> transLinkList = new ArrayList<>();
        try{
            //创建Column Group
            ReferenceColumnGroup columnGroup = new ReferenceGroupService(TokenUtil.getToken()).mergeReferenceColumnGroup(adapter,
                    currentSquidLink.getFrom_squid_id(), currentSquid.getId());

            ReferenceColumn refRowKeyColumn=null;
            //创建 rowkey 相关数据
            if(sourcColumnTempList!=null && sourcColumnTempList.size()>0){
                for(SourceColumn sourceColumn:sourcColumnTempList){
                    if(sourceColumn.getName().equals(ConstantsUtil.HBASE_ROW_KEY)){
                        refRowKeyColumn = transformationService.initReference(adapter, sourceColumn, sourceColumn.getId(), 1, currentSquid, currentSquid.getId(), columnGroup);
                        helper.synchronousInsertRefColumn(adapter, refRowKeyColumn, DMLType.INSERT.value());
                    }
                }

            }else{
                  refRowKeyColumn = this.createRefColumn(ConstantsUtil.HBASE_ROW_KEY, SystemDatatype.BINARY.value(),
                        currentSquid, adapter, transformationService, columnList.size(), helper, columnGroup);
            }


            refColumnList.add(refRowKeyColumn);

            Column rowkeyColumn = this.createColumnMetadata(refRowKeyColumn, currentSquid, transformationService, adapter, refColumnList.size(), helper);
            columnList.add(rowkeyColumn);

            Transformation rowkeyFromTrans = this.createVTransMetadataByColumn(refRowKeyColumn, adapter, currentSquid, transformationService);
            Transformation rowkeyToTrans = this.createVTransMetadataByColumn(rowkeyColumn, adapter, currentSquid, transformationService);
            transList.add(rowkeyFromTrans);
            transList.add(rowkeyToTrans);

            TransformationLink rowkeyLink = this.createTranslinkMetadataByTrans(rowkeyFromTrans, rowkeyToTrans, adapter, transformationService, transLinkList.size());
            transLinkList.add(rowkeyLink);

            for(int i = 0; i < htableResults.size(); i++){
                Cell currentCell = htableResults.get(i);
                //region 创建ReferenceColumn相关的对象
                //创建Source Table 中的 sourceColumn 因为创建RefColumn需要这个
                String columnName = Bytes.toString(currentCell.getFamilyArray(), currentCell.getFamilyOffset(), currentCell.getFamilyLength()) + "." +
                        Bytes.toString(currentCell.getQualifierArray(), currentCell.getQualifierOffset(), currentCell.getQualifierLength());
                if(columnName.length()>100){
                    columnName=columnName.substring(0,100);
                }
                ReferenceColumn refColumn=null;
                //如果有sourceColumn就用以前的sourceColumn来创建referenceColumn
                if(sourcColumnTempList!=null && sourcColumnTempList.size()>0){
                    if(!sourcColumnTempList.get(i).getName().equals(ConstantsUtil.HBASE_ROW_KEY)){
                        //创建Ref Column
                        refColumn = transformationService.initReference(adapter, sourcColumnTempList.get(i), sourcColumnTempList.get(i).getId(), 1, currentSquid, currentSquid.getId(), columnGroup);
                        helper.synchronousInsertRefColumn(adapter, refColumn, DMLType.INSERT.value());
                    }
                }else{
                    //先根据columnName和sourceTableId查询sourceColumn表中column Name是否重复
                    String sql="select name from ds_source_column where name='"+columnName+"' and source_table_id="+currentSquid.getSource_table_id();
                    Map<String, Object> map=adapter.query2Object(true,sql,null);
                    if(map!=null){
                        String name=map.get("NAME").toString();
                        if(name.equals(columnName)){
                            columnName=columnName+"_"+i;
                        }
                    }
                    //没有就创建sourceColumn
                     refColumn = this.createRefColumn(columnName, this.getDataTypeByHtableCellValue(currentCell).value(), currentSquid, adapter, transformationService, i+ 2, helper, columnGroup);
                }
                refColumnList.add(refColumn); //加入集合
                Column currentColumn = this.createColumnMetadata(refColumn,currentSquid, transformationService, adapter, i + 2, helper);
                columnList.add(currentColumn);
                Transformation fromTrans = this.createVTransMetadataByColumn(refColumn, adapter, currentSquid, transformationService);
                Transformation toTrans = this.createVTransMetadataByColumn(currentColumn, adapter,currentSquid,transformationService);
                transList.add(fromTrans);
                transList.add(toTrans);
                TransformationLink currentTransLink = this.createTranslinkMetadataByTrans(fromTrans, toTrans, adapter, transformationService, i + 2);
                transLinkList.add(currentTransLink);
            }



        }catch (Exception ex){
            ex.printStackTrace();
            throw ex;
        }finally {
            Map<String, Object> outputMap = new HashMap<>();
            outputMap.put("Columns", columnList);
            outputMap.put("ReferenceColumns", refColumnList);
            outputMap.put("Transformations", transList);
            outputMap.put("TransformationLinks", transLinkList);
            return outputMap;
        }
    }

    private ReferenceColumn createRefColumn(String columnName, int dataType, DataSquid currentSquid, IRelationalDataManager adapter,
                                            TransformationService transformationService, int relativeOrder, RepositoryServiceHelper helper,
                                            ReferenceColumnGroup columnGroup) throws Exception {

        SourceColumn tempSourceColumn = new SourceColumn();
        tempSourceColumn.setName(columnName); /* 拼接column的名称 */
        tempSourceColumn.setData_type(dataType); /* 通过数据判断类型 */
        tempSourceColumn.setLength(-1);
        tempSourceColumn.setPrecision(0);
        tempSourceColumn.setSource_table_id(currentSquid.getSource_table_id());
        tempSourceColumn.setRelative_order(relativeOrder);
        tempSourceColumn.setNullable(true);
        tempSourceColumn.setId(adapter.insert2(tempSourceColumn)); /*入库*/

        //创建Ref Column
        ReferenceColumn refColumn = transformationService.initReference(adapter, tempSourceColumn, tempSourceColumn.getId(), 1, currentSquid, currentSquid.getId(), columnGroup);
        helper.synchronousInsertRefColumn(adapter, refColumn, DMLType.INSERT.value());
        return refColumn;
    }

    /**
     * 通过refColumn创建Column
     * @param refColumn
     * @param currentSquid
     * @param transformationService
     * @param adapter
     * @param helper
     * @return
     * @throws Exception
     */
    private Column createColumnMetadata(ReferenceColumn refColumn, DataSquid currentSquid,
                                        TransformationService transformationService, IRelationalDataManager adapter, int orderColumn,
                                        RepositoryServiceHelper helper) throws Exception {
        //创建Column对象
        Column column = transformationService.initColumn3(adapter, refColumn, orderColumn, currentSquid.getId(), null);

        helper.synchronousInsertColumn(adapter, column.getSquid_id(), column, DMLType.INSERT.value(), false);
        return column;
    }

    /**
     * 通过Column与RefColumn创建虚拟Trans
     * @param column
     * @param adapter
     * @param currentSquid
     * @param transformationService
     * @return
     * @throws Exception
     */
    private Transformation createVTransMetadataByColumn(Column column,  IRelationalDataManager adapter, DataSquid currentSquid,
                                                             TransformationService transformationService) throws Exception {
        //创建Column的Trans
        Transformation startVTrans = transformationService.initTransformation(adapter, currentSquid.getId(),
                    column.getId(), TransformationTypeEnum.VIRTUAL.value(), column.getData_type(), 1);
        return startVTrans;
    }

    /**
     * 通过Trans创建Link
     * @param fromTrans
     * @param toTrans
     * @param adapter
     * @param transformationService
     * @param number
     * @return
     * @throws Exception
     */
    private TransformationLink createTranslinkMetadataByTrans(Transformation fromTrans, Transformation toTrans,
                                                              IRelationalDataManager adapter,
                                                              TransformationService transformationService, int number) throws Exception {
        return transformationService.initTransformationLink(adapter, fromTrans.getId(), toTrans.getId(), number);
    }

    private SystemDatatype getDataTypeByHtableCellValue(Cell htableCell){
        return SystemDatatype.BINARY;
//        String valueString = Bytes.toString(htableCell.getValueArray(), htableCell.getValueOffset(), htableCell.getValueLength());
//        BigDecimal valueBigDecimal =  Bytes.toBigDecimal(htableCell.getValueArray(), htableCell.getValueOffset(), htableCell.getValueLength());
//        Float valueFloat =  Bytes.toFloat(htableCell.getValueArray(), htableCell.getValueOffset());
//        int valueInt = Bytes.toInt(htableCell.getValueArray(), htableCell.getValueOffset(), htableCell.getValueLength());
//
//        if (valueBigDecimal != null){
//            return SystemDatatype.DECIMAL;
//        }else if ( valueFloat != null){
//            return SystemDatatype.FLOAT;
//        } else if (valueString != null){
//            return SystemDatatype.NVARCHAR;
//        } else {
//            return SystemDatatype.BINARY;
//        }
    }

}

