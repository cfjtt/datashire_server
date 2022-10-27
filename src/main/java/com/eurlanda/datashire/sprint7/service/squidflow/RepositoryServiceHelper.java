package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IDBAdapter;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.adapter.db.AdapterDataSourceManager;
import com.eurlanda.datashire.adapter.db.INewDBAdapter;
import com.eurlanda.datashire.dao.*;
import com.eurlanda.datashire.dao.impl.*;
import com.eurlanda.datashire.entity.*;
import com.eurlanda.datashire.entity.dest.DestHDFSColumn;
import com.eurlanda.datashire.entity.dest.DestHiveColumn;
import com.eurlanda.datashire.entity.dest.DestImpalaColumn;
import com.eurlanda.datashire.entity.dest.EsColumn;
import com.eurlanda.datashire.entity.operation.*;
import com.eurlanda.datashire.enumeration.*;
import com.eurlanda.datashire.sprint7.packet.InfoPacket;
import com.eurlanda.datashire.sprint7.plug.TransformationPlug;
import com.eurlanda.datashire.sprint7.service.squidflow.dest.EsColumnService;
import com.eurlanda.datashire.sprint7.service.squidflow.dest.HdfsColumnService;
import com.eurlanda.datashire.sprint7.service.squidflow.dest.ImpalaColumnService;
import com.eurlanda.datashire.sprint7.service.squidflow.subservice.MessageBubbleService;
import com.eurlanda.datashire.sprint7.service.squidflow.subservice.SquidServicesub;
import com.eurlanda.datashire.utility.*;
import com.eurlanda.datashire.utility.objectsql.SelectSQL;
import com.eurlanda.datashire.validator.SquidValidationTask;

import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author dang.lu 2013.11.19
 */
public class RepositoryServiceHelper extends AbstractRepositoryService {

    public RepositoryServiceHelper(String token) {
        super(token);
    }

    public RepositoryServiceHelper(String token, IRelationalDataManager adapter) {
        super(token, adapter);
    }

    public List<InfoPacket> updateReferedColumn(List<ReferenceColumn> list) {
        if (list == null || list.isEmpty()) {
            return null;
        }
        List<InfoPacket> ret = new ArrayList<InfoPacket>();
        adapter.openSession();
        try {
            for (int i = 0; i < list.size(); i++) {
                ReferenceColumn c = list.get(i);
                if (c == null) {
                    continue;
                }
                String sql = "UPDATE DS_REFERENCE_COLUMN SET is_groupby='"
                        + (c.isIs_groupby() ? 'Y' : 'N') + "' WHERE column_id="
                        + c.getId();
                // MessageFormat.format(, c.getId(), c.isIs_groupby()?'Y':'N');
                logger.debug("update reference column : "
                        + adapter.execute(sql));
                InfoPacket info = new InfoPacket();
                info.setCode(MessageCode.SUCCESS.value());
                info.setId(c.getId());
                info.setKey(c.getKey());
                info.setType(DSObjectType.COLUMN);
                ret.add(info);
            }
        } catch (DatabaseException e) {
            logger.error("[save-sql-exception]", e);
            try {
                adapter.rollback();
            } catch (SQLException e1) {
                logger.error("rollback err!", e1);
            }
        } finally {
            adapter.closeSession();
        }
        return ret;
    }

    /**
     * 创建目标列和对应的虚拟变换(从引用列导入,兼容从目标列右键直接创建)
     *
     * @param list
     * @return
     * @throws Exception
     */
    public List<InfoPacket> importColumns(List<TransformationAndCloumn> list) {
        if (list == null || list.isEmpty()) {
            return null;
        }
        List<InfoPacket> infoPackets = new ArrayList<InfoPacket>();
        try {
            adapter.openSession();
            ITransformationInputsDao transInputsDao = new TransformationInputsDaoImpl(adapter);
            for (int i = 0; i < list.size(); i++) {
                Column c = list.get(i).getColumn();
                if (c == null)
                    continue;
                if (c.getId() <= 0) {
                    Transformation t = list.get(i).getTransformation();
                    if (t == null)
                        continue;
                    InfoPacket cp = new InfoPacket();
                    cp.setId(adapter.insert2(c));
                    cp.setKey(c.getKey());
                    cp.setType(DSObjectType.COLUMN);
                    cp.setCode(cp.getId() > 0 ? MessageCode.SUCCESS.value()
                            : MessageCode.SQL_ERROR.value());
                    InfoPacket tp = new InfoPacket();
                    t.setColumn_id(cp.getId());
                    t.setOutput_data_type(c.getData_type());
                    t.setOutput_number(1);
                    int newTransId = adapter.insert2(t);
                    t.setId(newTransId);
                    t.setInputs(transInputsDao.initTransInputs(t,
                            c.getData_type()));
                    tp.setId(newTransId);
                    tp.setKey(t.getKey());
                    tp.setType(DSObjectType.TRANSFORMATION);
                    tp.setCode(tp.getId() > 0 ? MessageCode.SUCCESS.value()
                            : MessageCode.SQL_ERROR.value());
                    // 自动创建源 column 到目标 column
                    // 的直接映射TransformationLink（虚拟tran到虚拟tran）
                    TransformationLink l = list.get(i).getTransformationLink();
                    if (l != null) {
                        InfoPacket lp = new InfoPacket();
                        l.setTo_transformation_id(tp.getId());
                        l.setId(adapter.insert2(l));
                        transInputsDao.updTransInputs(l, null);
                        lp.setId(l.getId());
                        lp.setKey(l.getKey());
                        lp.setType(DSObjectType.TRANSFORMATIONLINK);
                        lp.setCode(lp.getId() > 0 ? MessageCode.SUCCESS.value()
                                : MessageCode.SQL_ERROR.value());
                        infoPackets.add(lp);
                    }
                    infoPackets.add(cp);
                    infoPackets.add(tp);
                } else { // 其实只更新相对位置（因为从引用列选择若干行插入到目标列，可能会打乱原目标列相对位置）
                    adapter.update2(c);
                }
            }
        } catch (Exception e) {
            logger.error("[save-sql-exception]", e);
            try {
                adapter.rollback();
            } catch (SQLException e1) {
                logger.error("rollback err!", e1);
            }
        } finally {
            adapter.closeSession();
        }
        return infoPackets;
    }

    /**
     * 创建目标列和对应的虚拟变换(从引用列导入,兼容从目标列右键直接创建)
     *
     * @param list
     * @return
     * @throws Exception
     */
    /*public List<InfoPacket> dragColumns(List<TransformationAndCloumn> list) {
        if (list == null || list.isEmpty()) {
            return null;
        }
        List<InfoPacket> infoPackets = new ArrayList<InfoPacket>();
        IRelationalDataManager adapter = null;
        try {
        	adapter = DataAdapterFactory.getDefaultDataManager();
        	adapter.openSession();
            ITransformationInputsDao transInputsDao = new TransformationInputsDaoImpl(adapter);
            TransformationService transformationService = new TransformationService(token);
            int squidId = 0;
            Map<String, String> paramsMap = new HashMap<String, String>();
            List<SquidLink> squidLinkList = null;
            SquidLink squidLink = null;
            int toSquidId = 0;
            Column column = null;
            int columnId = 0;
            SquidModelBase squid = null;
            int squidType = 0;
            String sql = null;
            int maxOrder = 0;
            Map<String, Object> orderIdMap = null;
            for (int i = 0; i < list.size(); i++) {
                column = list.get(i).getColumn();
                if (StringUtils.isNull(column)) {
                    continue;
                }
                if (column.getId() <= 0) {
                    Transformation t = list.get(i).getTransformation();
                    if (t == null)
                        continue;
                    InfoPacket cp = new InfoPacket();
                    
                    paramsMap.clear();
                    paramsMap.put("squid_id", String.valueOf(column.getSquid_id()));
                    paramsMap.put("name", column.getName());
                    Column checkCol = adapter.query2Object2(true, paramsMap, Column.class);
                    if (StringUtils.isNotNull(checkCol)){
                    	logger.error("column name 重复：squid_id:["+column.getSquid_id()+"],name["+column.getName()+"]");
                    	return null;
                    }
                    column.setId(adapter.insert2(column));
                    columnId = column.getId();
                    squidId = column.getSquid_id();
                    cp.setId(column.getId());
                    cp.setKey(column.getKey());
                    cp.setType(DSObjectType.COLUMN);
                    cp.setCode(cp.getId() > 0 ? MessageCode.SUCCESS.value()
                            : MessageCode.SQL_ERROR.value());
                    InfoPacket tp = new InfoPacket();
                    t.setColumn_id(cp.getId());
                    t.setOutput_data_type(column.getData_type());
                    t.setOutput_number(1);
                    int newTransId = adapter.insert2(t);
                    t.setInputs(transInputsDao.initTransInputs(
                            newTransId, column.getData_type()));
                    tp.setId(newTransId);
                    tp.setKey(t.getKey());
                    tp.setType(DSObjectType.TRANSFORMATION);
                    tp.setCode(tp.getId() > 0 ? MessageCode.SUCCESS.value()
                            : MessageCode.SQL_ERROR.value());
                    // 自动创建源 column 到目标 column
                    // 的直接映射TransformationLink（虚拟tran到虚拟tran）
                    TransformationLink l = list.get(i).getTransformationLink();
                    if (l != null) {
                        InfoPacket lp = new InfoPacket();
                        l.setTo_transformation_id(tp.getId());
                        l.setId(adapter.insert2(l));
                        transInputsDao.updTransInputs(l, null);
                        lp.setId(l.getId());
                        lp.setKey(l.getKey());
                        lp.setType(DSObjectType.TRANSFORMATIONLINK);
                        lp.setCode(lp.getId() > 0 ? MessageCode.SUCCESS.value()
                                : MessageCode.SQL_ERROR.value());
                        infoPackets.add(lp);
                    }
                    infoPackets.add(cp);
                    infoPackets.add(tp);
                    
                    paramsMap.clear();
                    paramsMap.put("from_squid_id",
                            Integer.toString(squidId, 10));
                    squidLinkList = adapter.query2List(paramsMap,
                            SquidLink.class);
                    ReferenceColumnGroup referenceColumnGroup = null;
                    // 检索当前Squid是否有下游Squid,如果有下游Squid，那么需要更新下游所有的column,
                    // transformation
                    if (StringUtils.isNotNull(squidLinkList)
                            && !squidLinkList.isEmpty()) {

                        paramsMap.clear();
                        paramsMap.put("id", Integer.toString(squidId, 10));
                        SquidModelBase fromSquid = adapter.query2Object(paramsMap,
                                SquidModelBase.class);
                        if(StringUtils.isNull(fromSquid)){
                            continue;
                        }
                        squidType = fromSquid.getSquid_type();
                        // ExtractSquid不允许增加、修改，只允许删除
                        if(SquidTypeEnum.EXTRACT.value() == squidType
                            || SquidTypeEnum.DOC_EXTRACT.value() == squidType
                            || SquidTypeEnum.XML_EXTRACT.value() == squidType
                            || SquidTypeEnum.WEBLOGEXTRACT.value() == squidType
                            || SquidTypeEnum.WEBEXTRACT.value() == squidType
                            || SquidTypeEnum.WEIBOEXTRACT.value() == squidType){
                            continue;
                        }
                        
                        ReferenceColumn referenceColumn = null;
                        Transformation transformationRight = null;
                        Transformation transformationLeft = null;
                        TransformationLink transformationLink = null;
                        
                        for (int j = 0; j < squidLinkList.size(); j++) {
                            squidLink = squidLinkList.get(j);
                            toSquidId = squidLink.getTo_squid_id();
                            paramsMap.clear();
                            paramsMap.put("id", Integer.toString(toSquidId, 10));
                            squid = adapter.query2Object(paramsMap, SquidModelBase.class);
                            if (StringUtils.isNull(squid)) {
                                continue;
                            }
                            // 如果是ReportSquid的话，不需要同步Report的数据
                            if(SquidTypeEnum.REPORT.value() == squid.getSquid_type()){
                                continue;
                            }
                            
                            referenceColumnGroup = new ReferenceGroupService(token).mergeReferenceColumnGroup(adapter, squidLink.getFrom_squid_id(), toSquidId);
                            sql = "select count(relative_order) as cnt from ds_reference_column where reference_squid_id = " + toSquidId + " and host_squid_id = " + fromSquid.getId();
                            orderIdMap = adapter.query2Object(true, sql, null);
                            if(StringUtils.isNotNull(orderIdMap)&&orderIdMap.containsKey("CNT")){
                                maxOrder = Integer.parseInt(orderIdMap.get("CNT").toString(), 10);
                                // 更新下游的ReferenceColumn信息
                                referenceColumn = transformationService
                                        .initReference(adapter, column, columnId,
                                                maxOrder + 1, fromSquid, toSquidId,
                                                referenceColumnGroup);
                            }
                            else {
                                // 更新下游的ReferenceColumn信息
                                referenceColumn = transformationService
                                        .initReference(adapter, column, columnId,
                                                i + 1, fromSquid, toSquidId,
                                                referenceColumnGroup);
                            }
                            // 更新下游的transformation
                            transformationRight = transformationService
                                    .initTransformation(adapter, toSquidId,
                                            referenceColumn.getId(),
                                            TransformationTypeEnum.VIRTUAL
                                                    .value(), referenceColumn
                                                    .getData_type(), 1, i + 1);
                            // 如果是Exception Squid的话，对应的Squid Column也要增加
                            if (SquidTypeEnum.EXCEPTION.value() == squidType) {
                                // 目标ExtractSquid真实列（变换面板左边）
                                column = transformationService
                                        .initColumn(adapter, column, i + 1,
                                                toSquidId, null);
                                transformationLeft = transformationService
                                        .initTransformation(adapter, toSquidId,
                                                column.getId(),
                                                TransformationTypeEnum.VIRTUAL
                                                        .value(), column
                                                        .getData_type(), 1);
                                // 创建 transformation link
                                transformationLink = transformationService
                                        .initTransformationLink(adapter,
                                                transformationRight.getId(),
                                                transformationLeft.getId(),
                                                i + 1);
                                // 更新TransformationInputs
                                transInputsDao.updTransInputs(transformationLink, transformationLeft);
                            }
                        }
                    }

                } else { // 其实只更新相对位置（因为从引用列选择若干行插入到目标列，可能会打乱原目标列相对位置）
                    adapter.update2(column);
                }
                CommonConsts.addValidationTask(new SquidValidationTask(token, adapter,  MessageBubbleService.setMessageBubble(column.getSquid_id(), column.getId(), column.getName(), MessageBubbleCode.ERROR_COLUMN_DATA_TYPE.value())));
                CommonConsts.addValidationTask(new SquidValidationTask(token, adapter,  MessageBubbleService.setMessageBubble(column.getSquid_id(), column.getId(), column.getName(), MessageBubbleCode.ERROR_AGGREGATION_OR_GROUP.value())));
            }
        } catch (Exception e) {
            logger.error("[save-sql-exception]", e);
            try {
                adapter.rollback();
            } catch (SQLException e1) {
                logger.error("rollback err!", e1);
            }
        } finally {
            adapter.closeSession();
        }
        return infoPackets;
    }*/

    /**
     * 创建目标列和对应的虚拟变换(从引用列导入,兼容从目标列右键直接创建)
     *
     * @param list
     * @return
     * @throws Exception
     */
    public List<InfoPacket> dragColumns(List<TransformationAndCloumn> list, ReturnValue out) {
        if (list == null || list.isEmpty()) {
            return null;
        }
        List<InfoPacket> infoPackets = new ArrayList<InfoPacket>();
        IRelationalDataManager adapter = null;
        try {
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            IColumnDao columnDao = new ColumnDaoImpl(adapter);
            ITransformationInputsDao transInputsDao = new TransformationInputsDaoImpl(adapter);
            Column column = null;
            for (int i = 0; i < list.size(); i++) {
                column = list.get(i).getColumn();
                if (StringUtils.isNull(column)) {
                    continue;
                }
                if (column.getId() <= 0) {
                    Transformation t = list.get(i).getTransformation();
                    if (t == null)
                        continue;
                    InfoPacket cp = new InfoPacket();
                    Column checkCol = columnDao.getColumnByParams(column.getSquid_id(), column.getName());
                    if (StringUtils.isNotNull(checkCol)) {
                        logger.error("column name 重复：squid_id:[" + column.getSquid_id() + "],name[" + column.getName() + "]");
                        out.setMessageCode(MessageCode.ERR_COLUMN_DATA_INCOMPLETE);
                        return null;
                    }
                    if (StringUtils.isHavaChinese(column.getName())) {
                        column.setName("defaultName");
                    }
                    int index = 0;
                    while (true) {
                        index++;
                        checkCol = columnDao.getColumnByParams(column.getSquid_id(), column.getName());
                        if (StringUtils.isNotNull(checkCol)) {
                            column.setName("defaultName" + index);
                        } else {
                            break;
                        }
                    }
                    // 插入column
                    //判断当前squid的类型 todo(以后修改) 前台需要改对应接口之后。注释的就可以放开了
//                    ISquidDao squidDao = new SquidDaoImpl(adapter);
//                    int squidType = squidDao.getSquidTypeById(column.getSquid_id());
//
//                    if(SquidTypeEnum.isExtractBySquidType(squidType)){
//                        int dbType=0;
//                        ISquidLinkDao squidLinkDao = new SquidLinkDaoImpl(adapter);
//                       List<SquidLink> squidLinkList= squidLinkDao.getSquidLinkListByToSquid(column.getSquid_id());
//                       if(squidLinkList!=null && squidLinkList.size()>0){
//                           int squidId=squidLinkList.get(0).getFrom_squid_id();
//                           DbSquid dbSquid=squidDao.getObjectById(squidId,DbSquid.class);
//                           if(dbSquid!=null){
//                               dbType=dbSquid.getDb_type();
//                           }
//                       }
//                        column = DataTypeConvertor.getInColumnByColumn(dbType,column);
//                    }
                    column.setId(adapter.insert2(column));
                    cp.setId(column.getId());
                    cp.setKey(column.getKey());
                    cp.setType(DSObjectType.COLUMN);
                    cp.setName(column.getName());
                    cp.setCode(cp.getId() > 0 ? MessageCode.SUCCESS.value()
                            : MessageCode.SQL_ERROR.value());
                    InfoPacket tp = new InfoPacket();
                    t.setColumn_id(cp.getId());
                    t.setOutput_data_type(column.getData_type());
                    t.setOutput_number(1);
                    // 插入transformation
                    int newTransId = adapter.insert2(t);
                    t.setId(newTransId);
                    // transformationInputs
                    t.setInputs(transInputsDao.initTransInputs(
                            t, column.getData_type()));
                    tp.setId(newTransId);
                    tp.setKey(t.getKey());
                    tp.setType(DSObjectType.TRANSFORMATION);
                    tp.setCode(tp.getId() > 0 ? MessageCode.SUCCESS.value()
                            : MessageCode.SQL_ERROR.value());
                    // 自动创建源 column 到目标 column
                    // 的直接映射TransformationLink（虚拟tran到虚拟tran）
                    TransformationLink l = list.get(i).getTransformationLink();
                    if (l != null) {
                        InfoPacket lp = new InfoPacket();
                        l.setTo_transformation_id(tp.getId());
                        l.setId(adapter.insert2(l));
                        transInputsDao.updTransInputs(l, null);
                        lp.setId(l.getId());
                        lp.setKey(l.getKey());
                        lp.setType(DSObjectType.TRANSFORMATIONLINK);
                        lp.setCode(lp.getId() > 0 ? MessageCode.SUCCESS.value()
                                : MessageCode.SQL_ERROR.value());
                        infoPackets.add(lp);
                    }
                    infoPackets.add(cp);
                    infoPackets.add(tp);
                    synchronousInsertColumn(adapter, column.getSquid_id(), column, DMLType.INSERT.value(), false);
                } else { // 其实只更新相对位置（因为从引用列选择若干行插入到目标列，可能会打乱原目标列相对位置）
                    adapter.update2(column);
                }
            }
            //更新column的order顺序
            if (list != null && list.size() > 0) {
                int squidId = list.get(0).getColumn().getSquid_id();
                List<Column> columnList = columnDao.getColumnListBySquidId(squidId);
                List<List<Object>> paramList = new ArrayList<>();
                //批量更新column的relative_order
                if (columnList != null) {
                    for (int i = 0; i < columnList.size(); i++) {
                        Column updateColumn = columnList.get(i);
                        List<Object> params = new ArrayList<>();
                        params.add(i + 1);
                        params.add(updateColumn.getSquid_id());
                        params.add(updateColumn.getId());
                        paramList.add(params);
                    }
                    if (paramList != null && paramList.size() > 0) {
                        adapter.executeBatch("update ds_column set relative_order = ? where squid_id = ? and id = ?", paramList);

                    }
                }


            }

        } catch (Exception e) {
            logger.error("[save-sql-exception]", e);
            try {
                adapter.rollback();
            } catch (SQLException e1) {
                logger.error("rollback err!", e1);
            }
            out.setMessageCode(MessageCode.SQL_ERROR);
            return null;
        } finally {
            adapter.closeSession();
        }
        return infoPackets;
    }

    /**
     * 更新存在Column的定义，比如名字、类型等
     *
     * @param columnList
     * @author bo.dang
     * @date 2014年5月29日
     */
    /*public List<InfoPacket> updateColumns(List<Column> columnList, ReturnValue out) {
        List<InfoPacket> infoPacketList = new ArrayList<InfoPacket>();
        InfoPacket infoPacket = null;
        //System.out.println(JsonUtil.toGsonString(columnList));
        if (StringUtils.isNotNull(columnList) && !columnList.isEmpty()) {
            try {
                Column column = null;
                Transformation transformation = null;
                Map<String, String> paramsMap = new HashMap<String, String>();
                List<SquidLink> squidLinkList = new ArrayList<SquidLink>();
                int squidId = 0;
                SquidLink squidLink = null;
                int toSquidId = 0;
                int columnId = 0;
                int squidType = 0;
                adapter.openSession();
                TransformationService transformationService = new TransformationService(token);
                DataShirServiceplug dataShirServiceplug = new DataShirServiceplug(token);
                for (int i = 0; i < columnList.size(); i++) {
                    column = columnList.get(i);
                    squidId = column.getSquid_id();
                    columnId = column.getId();
                    if (columnId==0){
                    	logger.error("[错误提示]：update column id 为 0, 当前squid["+squidId+"],columnName["+column.getName()+"]");
                    	return null;
                    }
                    infoPacket = new InfoPacket();
                    infoPacket.setId(columnId);
                    infoPacket.setKey(column.getKey());
                    infoPacket.setType(DSObjectType.COLUMN);
                    // 更新Column信息
                    infoPacket
                            .setCode(adapter.update2(column) ? MessageCode.SUCCESS
                                    .value() : MessageCode.SQL_ERROR.value());
                    CommonConsts.addValidationTask(new SquidValidationTask(token, adapter,  MessageBubbleService.setMessageBubble(squidId, columnId, column.getName(), MessageBubbleCode.ERROR_COLUMN_DATA_TYPE.value())));
                    CommonConsts.addValidationTask(new SquidValidationTask(token, adapter,  MessageBubbleService.setMessageBubble(squidId, columnId, column.getName(), MessageBubbleCode.ERROR_AGGREGATION_OR_GROUP.value())));
                    infoPacketList.add(infoPacket);
                    paramsMap.clear();
                    paramsMap.put("column_id", Integer.toString(columnId, 10));
                    paramsMap.put("squid_id", Integer.toString(squidId, 10));

                    transformation = adapter.query2Object2(true, paramsMap,
                            Transformation.class);
                    if(StringUtils.isNull(transformation)){
                        continue;
                    }
                    // 更新Transformation的类型
                    transformation.setOutput_data_type(column.getData_type());
                    infoPacket = new InfoPacket();
                    infoPacket.setId(transformation.getId());
                    infoPacket.setKey(transformation.getKey());
                    infoPacket.setType(DSObjectType.TRANSFORMATION);
                    // 更新TRANSFORMATION信息
                    infoPacket.setCode(adapter.update2(transformation) ? MessageCode.SUCCESS
                                    .value() : MessageCode.SQL_ERROR.value());
                    infoPacketList.add(infoPacket);

                    paramsMap.clear();
                    paramsMap.put("from_squid_id",
                            Integer.toString(squidId, 10));
                    squidLinkList = adapter.query2List(true, paramsMap,
                            SquidLink.class);

                    // 检索当前Squid是否有下游Squid,如果有下游Squid，那么需要更新下游所有的column,
                    // transformation
                    if (StringUtils.isNotNull(squidLinkList)
                            && !squidLinkList.isEmpty()) {

                        ReferenceColumn referenceColumn = null;
                        for (int j = 0; j < squidLinkList.size(); j++) {
                            squidLink = squidLinkList.get(j);
                            toSquidId = squidLink.getTo_squid_id();
                            paramsMap.clear();
                            paramsMap
                                    .put("id", Integer.toString(toSquidId, 10));
                            SquidModelBase squid = adapter.query2Object2(true, paramsMap,
                                    SquidModelBase.class);
                            if (StringUtils.isNull(squid)) {
                                continue;
                            }
                            squidType = squid.getSquid_type();
                            // 如果是ReportSquid或者GISMapSquid的话，需要生成错误的消息泡
                            if (SquidTypeEnum.REPORT.value() == squidType || 
                            		SquidTypeEnum.GISMAP.value() == squidType) {
                                //new MessageBubble(false, toSquidId, toSquidId, squid.getName(), MessageBubbleCode.WARN_REPORT_SQUID_COLUMNS_BY_DELETED.value(), MessageBubbleCode.WARN, "报表squid的源数据被删除，可能导致报表运行不正常");
                                CommonConsts.addValidationTask(new SquidValidationTask(token, adapter, MessageBubbleService.setMessageBubble(toSquidId, toSquidId, squid.getName(), MessageBubbleCode.WARN_REPORT_SQUID_COLUMNS_BY_DELETED.value())));
                                continue;
                            }
                            if (SquidTypeEnum.EXCEPTION.value() == squidType){
                            	continue;
                            }
                            
                            paramsMap.clear();
                            paramsMap.put("column_id",
                                    Integer.toString(columnId, 10));
                            paramsMap.put("reference_squid_id",
                                    Integer.toString(toSquidId, 10));
                            referenceColumn = adapter.query2Object2(true, paramsMap,
                                    ReferenceColumn.class);
                            // 如果ReferenceColumn为null,说明下游ReferenceColumn被删除，那么不需要更新
                            if(StringUtils.isNull(referenceColumn)){
                                continue;
                            }
                            // fixed bug 688 start by bo.dang
                            // 判断Column的类型是否更改，如果更改，需要删除tranformation Link
                            if(column.getData_type() != referenceColumn.getData_type()){
                                paramsMap.clear();
                                paramsMap.put("column_id",
                                        Integer.toString(columnId, 10));
                                paramsMap.put("squid_id",
                                        Integer.toString(toSquidId, 10));
                                transformation = adapter.query2Object2(true, paramsMap,
                                        Transformation.class);
                                dataShirServiceplug.deleteTransformation(adapter, transformation.getId(), 0);
                                transformationService.initTransformation(adapter, toSquidId, columnId, TransformationTypeEnum.VIRTUAL.value(), column.getData_type(), transformation.getOutput_number());
                            }
                            // 更新下游的ReferenceColumn信息
                            transformationService.updateReferenceColumn(
                                    adapter, referenceColumn, column);
                            // fixed bug 688 end by bo.dang
                            
                            if(SquidTypeEnum.STAGE.value() == squidType
                                    || SquidTypeEnum.LOGREG.value() == squidType
                                    || SquidTypeEnum.SVM.value() == squidType
                                    || SquidTypeEnum.NAIVEBAYES.value() == squidType
                                    || SquidTypeEnum.KMEANS.value() == squidType
                                    || SquidTypeEnum.ALS.value() == squidType
                                    || SquidTypeEnum.LINEREG.value() == squidType
                                    || SquidTypeEnum.RIDGEREG.value() == squidType
                                    || SquidTypeEnum.QUANTIFY.value() == squidType
                                    || SquidTypeEnum.DISCRETIZE.value() == squidType
                                    || SquidTypeEnum.DECISIONTREE.value() == squidType){
                                        if(transformation.getTranstype() == TransformationTypeEnum.PREDICT.value() 
                                                && transformation.getModel_squid_id() == squidId){
                                            // Predict 引用的模型有更新
                                            new MessageBubble(false, toSquidId, toSquidId, squid.getName(), MessageBubbleCode.WARN_PREDICT_MODEL_SQUID_BY_UPDATED.value(), MessageBubbleCode.WARN, "引用的模型已经删除");
                                            //CommonConsts.addValidationTask(new SquidValidationTask(token, adapter, MessageBubbleService.setMessageBubble(toSquidId, toSquidId, squid.getName(), MessageBubbleCode.WARN_PREDICT_MODEL_SQUID_BY_UPDATED.value())));
                                        }
                                        
                                    }
                            
                            // 如果是Exception Squid的话，对应的Squid Column也要增加
                            if (SquidTypeEnum.EXCEPTION.value() == squidType) {
                                paramsMap.clear();
                                paramsMap.put("squid_id",
                                        Integer.toString(toSquidId, 10));
                                Column squidColumn = adapter.query2Object2(true, paramsMap, Column.class);
                                // 更新Squid Column
                                // 目标ExtractSquid真实列（变换面板左边）
                                squidColumn.setData_type(column.getData_type());
                                squidColumn.setNullable(column.isNullable());
                                squidColumn.setRelative_order(column
                                        .getRelative_order());
                                squidColumn.setLength(column.getLength());
                                squidColumn.setPrecision(column.getPrecision());
                                squidColumn.setCdc(column.getCdc());
                                squidColumn.setCollation(column.getCollation());
                                squidColumn.setAggregation_type(column
                                        .getAggregation_type());
                                adapter.update2(squidColumn);

                                paramsMap.clear();
                                paramsMap.put("column_id",
                                        Integer.toString(columnId, 10));
                                paramsMap.put("squid_id",
                                        Integer.toString(toSquidId, 10));
                                transformation = adapter.query2Object2(true,
                                        paramsMap, Transformation.class);
                                // 更新Transformation的类型
                                transformation.setOutput_data_type(column
                                        .getData_type());
                                adapter.update2(transformation);
                            }
                        }
                    }

                }
            } catch (Exception e) {
                logger.error("[save-sql-exception]", e);
                try {
                    adapter.rollback();
                } catch (SQLException e1) {
                    logger.error("rollback err!", e1);
                }
                out.setMessageCode(MessageCode.SQL_ERROR);
            } finally {
                adapter.closeSession();
            }
        }
        return infoPacketList;
    }*/

    /**
     * 更新存在Column的定义，比如名字、类型等
     *
     * @param columnList
     * @author bo.dang
     * @date 2014年5月29日
     */
    public List<InfoPacket> updateColumns(List<Column> columnList, boolean IsNeedNameCheck, ReturnValue out) {
        List<InfoPacket> infoPacketList = new ArrayList<InfoPacket>();
        InfoPacket infoPacket = null;
        IRelationalDataManager adapter = DataAdapterFactory.newInstance().getDefaultDataManager();
        if (StringUtils.isNotNull(columnList) && !columnList.isEmpty()) {
            try {
                adapter.openSession();
                ITransformationDao transDao = new TransformationDaoImpl(adapter);
                for (int i = 0; i < columnList.size(); i++) {
                    Column column = columnList.get(i);
                    if (column.getId() == 0) {
                        return null;
                    }
                    infoPacket = new InfoPacket();
                    infoPacket.setId(column.getId());
                    infoPacket.setKey(column.getKey());
                    infoPacket.setType(DSObjectType.COLUMN);
                    infoPacketList.add(infoPacket);
                    // 更新Transformation的类型
                    Transformation transformation = transDao.getTransformationById(column.getSquid_id(), column.getId());
                    if (transformation != null) {
                        infoPacket = new InfoPacket();
                        infoPacket.setId(transformation.getId());
                        infoPacket.setKey(transformation.getKey());
                        infoPacket.setType(DSObjectType.TRANSFORMATION);
                        infoPacketList.add(infoPacket);
                    }
                    //消息泡
                    synchronousUpdateColumn(adapter, column.getSquid_id(), column, DMLType.UPDATE.value(), out, IsNeedNameCheck);
                }
            } catch (Exception e) {
                logger.error("[save-sql-exception]", e);
                try {
                    adapter.rollback();
                } catch (SQLException e1) {
                    logger.error("rollback err!", e1);
                }
                out.setMessageCode(MessageCode.SQL_ERROR);
            } finally {
                adapter.closeSession();
            }
        }
        return infoPacketList;
    }

    /**
     * 修改column，同步下游
     *
     * @param adapter
     * @param squidId
     * @param column
     * @param type
     * @return
     * @throws Exception
     */
    @SuppressWarnings("static-access")
    public Map<String, Object> synchronousUpdateColumn(IRelationalDataManager adapter,
                                                       int squidId, Column column, int type, ReturnValue out, boolean isNeedCheckName) throws Exception {
        ITransformationDao transDao = new TransformationDaoImpl(adapter);
        IColumnDao columnDao = new ColumnDaoImpl(adapter);
        ISquidDao squidDao = new SquidDaoImpl(adapter);
        ISquidLinkDao squidlinkDao = new SquidLinkDaoImpl(adapter);
        IReferenceColumnDao refColumnDao = new ReferenceColumnDaoImpl(adapter);
        ISquidFlowDao squidFlowDao = new SquidFlowDaoImpl(adapter);
        TransformationService transformationService = new TransformationService(token);
        DataShirServiceplug dataShirServiceplug = new DataShirServiceplug(token);
        //修改column
        columnDao.update(column);
        boolean flag = false;
        //获取所有的squid类型，如果存在GroupTaggingSquid则递归更新
        Squid squidForFlow = squidDao.getSquidForCond(column.getSquid_id(), Squid.class);
        //不太明白这里查询这个是干嘛用的，下面所有的该递归更新的判断都有。所以flag 暂时去掉。之前是在synchronousUpdateTagColumn 调用这个方法时判断的。
        List<Map<String, Object>> squidTypeMap = squidFlowDao.getSquidTypeBySquidFlow(squidForFlow.getSquidflow_id());
        if (squidTypeMap != null) {
            for (Map<String, Object> typeMap : squidTypeMap) {
                if (typeMap.containsValue(SquidTypeEnum.GROUPTAGGING.value()) || typeMap.containsValue(SquidTypeEnum.SAMPLINGSQUID.value())) {
                    flag = true;
                    break;
                }
            }
        }
        //相关Trans
        Transformation trans = transDao.getTransformationById(squidId, column.getId());
        if (trans != null) {
            trans.setOutput_data_type(column.getData_type());
            transDao.update(trans);
            //处理input
            ITransformationInputsDao transInputDao = new TransformationInputsDaoImpl(adapter);
            List<TransformationInputs> currentTransInputs = transInputDao.getTransInputListByTransId(trans.getId());
            if (currentTransInputs != null && currentTransInputs.size() > 0) {
                //更新Column时需要处理的是虚拟Trans，虚拟Trans只会有一个input
                TransformationInputs currentInput = currentTransInputs.get(0);
                currentInput.setInput_Data_Type(column.getData_type());
                transInputDao.update(currentInput); /** 更新input */
            }
        }
        List<SquidLink> squidLinks = squidlinkDao.getSquidLinkListByFromSquid(squidId);
        if (squidLinks != null && squidLinks.size() > 0) {
            for (SquidLink squidLink : squidLinks) {
                int toSquidId = squidLink.getTo_squid_id();
                Squid squid = squidDao.getSquidForCond(squidLink.getTo_squid_id(), Squid.class);
                if (squid == null || squid.getSquid_type() == SquidTypeEnum.REPORT.value()
                        || squid.getSquid_type() == SquidTypeEnum.GISMAP.value()
                        || squid.getSquid_type() == SquidTypeEnum.DESTWS.value()
                        || squid.getSquid_type() == SquidTypeEnum.EXCEPTION.value()) {
                    continue;
                }
                ReferenceColumn refColumn = refColumnDao.getReferenceColumnById(squid.getId(),
                        column.getId());
                if (refColumn != null) {
                    //pivotSquid如果没有点击生成列，只有referenceColumn做特殊处理，只用修改referenceColumn
                    if(squid.getSquid_type() == SquidTypeEnum.PIVOTSQUID.value()){
                        Transformation transformation = transDao.getTransformationById(squid.getId(), column.getId());
                        if(transformation == null){
                            transformationService.updateReferenceColumn(
                                    adapter, refColumn, column);
                            this.synchronousUpdateRefColumn(adapter, refColumn, type, out);
                        }
                    }
                    // 判断Column的类型是否更改，如果更改，需要删除tranformation Link,GroupTaggingSquid除外(更改左边),需要将UserDefined，statistic的column设置为0
                    if (column.getData_type() != refColumn.getData_type()) {
                        Transformation transformation = transDao.getTransformationById(squid.getId(), column.getId());
                        //GroupTaggingSquid不需要断线
                        if (squid.getSquid_type() != SquidTypeEnum.GROUPTAGGING.value()
                                && squid.getSquid_type() != SquidTypeEnum.USERDEFINED.value()
                                && squid.getSquid_type() != SquidTypeEnum.STATISTICS.value()
                                && squid.getSquid_type() != SquidTypeEnum.DEST_HIVE.value()
                                && squid.getSquid_type()!=SquidTypeEnum.SAMPLINGSQUID.value()
                                && squid.getSquid_type() != SquidTypeEnum.PIVOTSQUID.value()) {
                            dataShirServiceplug.deleteTransformation(adapter, transformation.getId(), 0);
                            transformationService.initTransformation(adapter, squid.getId(), column.getId(),
                                    TransformationTypeEnum.VIRTUAL.value(), column.getData_type(),
                                    transformation.getOutput_number());
                        } else if (squid.getSquid_type() == SquidTypeEnum.USERDEFINED.value()) {
                            String sql = "update ds_userdefined_datamap_column set column_id=0 where squid_id=" + squid.getId() + " and column_id=" + column.getId();
                            adapter.execute(sql);
                        } else if (squid.getSquid_type() == SquidTypeEnum.STATISTICS.value()) {
                            String sql = "update ds_statistics_datamap_column set column_id=0 where squid_id=" + squid.getId() + " and column_id=" + column.getId();
                            adapter.execute(sql);
                        } else if (squid.getSquid_type() == SquidTypeEnum.DEST_HIVE.value()) {
                            String sql = "update ds_dest_hive_column set column_id=0 where squid_id=" + squid.getId() + " and column_id=" + column.getId();
                            adapter.execute(sql);
                        } else {
                            //更新trans的output
                            transformation.setOutput_data_type(column.getData_type());
                            adapter.update2(transformation);
                            //更新inputs
                        }
                    }
                    transformationService.updateReferenceColumn(
                            adapter, refColumn, column);
                    this.synchronousUpdateRefColumn(adapter, refColumn, type, out);
                    Map<String, Object> returnMap = new HashMap<>();
                    //flag 的判断去掉
                    if (squid.getSquid_type() == SquidTypeEnum.GROUPTAGGING.value() || squid.getSquid_type() == SquidTypeEnum.SAMPLINGSQUID.value()) {
                        returnMap = this.synchronousUpdateTagColumn(adapter, refColumn, type, out, isNeedCheckName);
                    }
                    /**
                     * 2017-11-27
                     *  递归修改  注释掉是因为，当下游有两个groupTagging  其中一个还有一个下游时，递归修改column有错。
                     *  returnMap里面的column 永远是最后一个groupTagging的colum。那么如果第一个groupTagging有下游时。下游column就更新不到。
                     */
                    /*if (flag) {
                        if (returnMap.containsKey("Column")) {
                            synchronousUpdateColumn(adapter, toSquidId, (Column) returnMap.get("Column"), type, out, isNeedCheckName);
                        }
                    }*/
                }
            }
        }
        return null;
    }

    /**
     * 添加column的时候操作
     *
     * @param adapter
     * @param squidId 添加column所属squid id
     * @param column  添加的column
     * @param type    DMLType.INSERT
     * @param flag    true:数据库插入column,并创建virtTrans
     * @return
     * @throws Exception
     */
    @SuppressWarnings("static-access")
    public Map<String, Object> synchronousInsertColumn(IRelationalDataManager adapter,
                                                       int squidId, Column column, int type, boolean flag) throws Exception {
        ITransformationDao transDao = new TransformationDaoImpl(adapter);
        ISquidDao squidDao = new SquidDaoImpl(adapter);
        ISquidLinkDao squidlinkDao = new SquidLinkDaoImpl(adapter);
        IReferenceColumnDao refColumnDao = new ReferenceColumnDaoImpl(adapter);
        IColumnDao columnDao = new ColumnDaoImpl(adapter);
        ISquidFlowDao squidFlowDao = new SquidFlowDaoImpl(adapter);
        TransformationService service = new TransformationService(token);
        // 创建对应的虚拟变换
        //Squid fromSquid = squidDao.getSquidForCond(squidId, Squid.class);
        if (flag) {
            // 创建column,同事赋值id
            column.setId(columnDao.insert2(column));
            Transformation trans = service.initTransformation(adapter,
                    squidId, column.getId(), TransformationTypeEnum.VIRTUAL
                            .value(), column.getData_type(), 1);
            transDao.insert2(trans);
        }
        List<SquidLink> squidLinks = squidlinkDao.getSquidLinkListByFromSquid(squidId);
        if (squidLinks != null && squidLinks.size() > 0) {
            for (SquidLink squidLink : squidLinks) {
                Squid fromsquid = squidDao.getSquidForCond(squidLink.getFrom_squid_id(), Squid.class);
                Squid squid = squidDao.getSquidForCond(squidLink.getTo_squid_id(), Squid.class);
                if (squid == null || squid.getSquid_type() == SquidTypeEnum.REPORT.value()
                        || squid.getSquid_type() == SquidTypeEnum.GISMAP.value()
                        || squid.getSquid_type() == SquidTypeEnum.DESTWS.value()
                        || squid.getSquid_type() == SquidTypeEnum.EXCEPTION.value()) {
                    continue;
                }
                if (squid.getSquid_type() == SquidTypeEnum.DESTES.value()) {
                    // es squid 同步添加列
                    EsColumn esColumn = EsColumnService.genEsColumnByColumn(column, squid.getId());
                    adapter.insert2(esColumn);
                    continue;
                }

                if (squid.getSquid_type() == SquidTypeEnum.DEST_HDFS.value() || squid.getSquid_type() == SquidTypeEnum.DESTCLOUDFILE.value()) {
                    // hdfs squid 同步添加列
                    DestHDFSColumn hdfsColumn = HdfsColumnService.getHDFSColumnByColumn(column, squid.getId());
                    adapter.insert2(hdfsColumn);
                    continue;
                }

                if (squid.getSquid_type() == SquidTypeEnum.DEST_IMPALA.value()) {
                    // hdfs squid 同步添加列
                    DestImpalaColumn impalaColumn = ImpalaColumnService.getImpalaColumnByColumn(column, squid.getId());
                    adapter.insert2(impalaColumn);
                    continue;
                }

                ReferenceColumnGroup group = refColumnDao.getRefColumnGroupBySquidId(squid.getId(), squidId);
                if (group == null) {
                    //添加ReferenceColumnGroup记录
                    int order = 1;
                    List<ReferenceColumnGroup> groupList = refColumnDao.getRefColumnGroupListBySquidId(squid.getId());
                    if (groupList != null && groupList.size() > 0) {
                        order = groupList.size() + 1;
                    }
                    group = new ReferenceColumnGroup();
                    group.setKey(StringUtils.generateGUID());
                    group.setReference_squid_id(squid.getId());
                    group.setRelative_order(order);
                    group.setId(refColumnDao.insert2(group));
                }
                ReferenceColumn refColumn = service.initReference(adapter,
                        column, column.getId(), column.getRelative_order(),
                        fromsquid, squid.getId(), group);
                Transformation refTrans = service.initTransformation(adapter,
                        squid.getId(), refColumn.getColumn_id(), TransformationTypeEnum.VIRTUAL
                                .value(), refColumn.getData_type(), 1);

                this.synchronousInsertRefColumn(adapter, refColumn, type);
                if (fromsquid.getSquid_type() == SquidTypeEnum.SAMPLINGSQUID.value()|| squid.getSquid_type() == SquidTypeEnum.SAMPLINGSQUID.value() || squid.getSquid_type() == SquidTypeEnum.GROUPTAGGING.value() || fromsquid.getSquid_type() == SquidTypeEnum.GROUPTAGGING.value()) {
                    this.synchronousInsertGroupTaggingRefColumn(adapter, refColumn, type,squidLink);
                }

            }

        }
        return null;
    }

    /**
     * 修改ReferenceColumn，同步下游
     *
     * @param adapter
     * @param oldColumn referenceColumn
     * @param type      DmlType
     * @return
     * @throws Exception
     */
    @SuppressWarnings("static-access")
    public Map<String, Object> synchronousUpdateRefColumn(IRelationalDataManager adapter,
                                                          ReferenceColumn oldColumn, int type, ReturnValue out) throws Exception {
        ITransformationDao transDao = new TransformationDaoImpl(adapter);
        ITransformationLinkDao transLinkDao = new TransformationLinkDaoImpl(adapter);
        ITransformationInputsDao transInputsDao = new TransformationInputsDaoImpl(adapter);
        ISquidDao squidDao = new SquidDaoImpl(adapter);
        IColumnDao columnDao = new ColumnDaoImpl(adapter);
        ISquidLinkDao squidlinkDao = new SquidLinkDaoImpl(adapter);
        IReferenceColumnDao refColumnDao = new ReferenceColumnDaoImpl(adapter);

        TransformationService transformationService = new TransformationService(token);
        DataShirServiceplug dataShirServiceplug = new DataShirServiceplug(token);
        String squidName = squidDao.getSquidNameByColumnId(oldColumn.getColumn_id());

        List<SquidLink> squidLinks = squidlinkDao.getSquidLinkListByFromSquid(oldColumn.getReference_squid_id());
        if (squidLinks != null && squidLinks.size() > 0) {
            for (SquidLink squidLink : squidLinks) {
                Squid squid = squidDao.getSquidForCond(squidLink.getTo_squid_id(), Squid.class);
                if (squid != null && (squid.getSquid_type() == SquidTypeEnum.EXCEPTION.value())) {
                    ReferenceColumn refColumn = refColumnDao.getReferenceColumnById(squid.getId(),
                            oldColumn.getColumn_id());
                    Column updColumn = null;
                    //EXCEPTION没有同步（todo）
                    if (refColumn != null) {
                        updColumn = columnDao.getColumnByParams(squid.getId(), squidName + "_" + refColumn.getName());
                    }
                    if (updColumn == null) {
//                        System.out.println(squid.getId() + ":" + squidName + "_" + refColumn.getName());
                        continue;
                    }
                    updColumn.setName(squidName + "_" + oldColumn.getName());
                    updColumn.setData_type(oldColumn.getData_type());
                    updColumn.setLength(oldColumn.getLength());
                    updColumn.setPrecision(oldColumn.getPrecision());
                    updColumn.setScale(oldColumn.getScale());
                    updColumn.setNullable(oldColumn.isNullable());
                    updColumn.setIsPK(oldColumn.isIsPK());
                    updColumn.setIsUnique(oldColumn.isIsUnique());
                    updColumn.setRelative_order(oldColumn.getRelative_order());

                    this.synchronousUpdateColumn(adapter, squid.getId(), updColumn, type, out, false);

                    List<Column> columnList = columnDao.getColumnListBySquidId(squid.getId());
                    if (columnList != null && columnList.size() > 0) {
                        for (Column column2 : columnList) {
                            if (column2.getName().equals("ERROR")) {
                                column2.setRelative_order(columnList.size());
                                columnDao.update(column2);
                            }
                        }
                    }

                    // 判断Column的类型是否更改，如果更改，需要删除tranformation Link
                    TransformationLink transLink = null;
                    if (oldColumn.getData_type() != refColumn.getData_type()) {
                        Transformation toTransformation = transDao.getTransformationById(squid.getId(), updColumn.getId());
                        Transformation transformation = transDao.getTransformationById(squid.getId(), oldColumn.getId());
                        transLink = transLinkDao.getTransLinkByTrans(transformation.getId(), toTransformation.getId());

                        dataShirServiceplug.deleteTransformation(adapter, transformation.getId(), 0);

                        Transformation newTransformation = transformationService.initTransformation(adapter, squid.getId(), oldColumn.getId(),
                                TransformationTypeEnum.VIRTUAL.value(), oldColumn.getData_type(),
                                transformation.getOutput_number());

                        // 更新下游的ReferenceColumn信息
                        transformationService.updateReferenceColumn(
                                adapter, refColumn, oldColumn);

                        // 创建 transformation link
                        TransformationLink transformationLink = transformationService.initTransformationLink(adapter,
                                newTransformation.getId(), toTransformation.getId(),
                                transLink.getIn_order());
                        // 更新TransformationInputs
                        transInputsDao.updTransInputs(transformationLink, null);
                    } else {
                        // 更新下游的ReferenceColumn信息
                        transformationService.updateReferenceColumn(
                                adapter, refColumn, oldColumn);
                    }
                }
            }
        }
        return null;
    }

    /**
     * 同步更新下游的TagColumn的column和左边的transformation（递归所有的下游）
     *
     * @param adapter
     * @param oldColumn
     * @param type
     * @return
     */
    public Map<String, Object> synchronousUpdateTagColumn(IRelationalDataManager adapter,
                                                          ReferenceColumn oldColumn, int type, ReturnValue out, boolean isNeedCheckName) throws Exception {

        ITransformationDao transDao = new TransformationDaoImpl(adapter);
        ITransformationLinkDao transLinkDao = new TransformationLinkDaoImpl(adapter);
        ITransformationInputsDao transInputsDao = new TransformationInputsDaoImpl(adapter);
        ISquidDao squidDao = new SquidDaoImpl(adapter);
        IColumnDao columnDao = new ColumnDaoImpl(adapter);
        ISquidLinkDao squidlinkDao = new SquidLinkDaoImpl(adapter);
        IReferenceColumnDao refColumnDao = new ReferenceColumnDaoImpl(adapter);

        TransformationService transformationService = new TransformationService(token);
        DataShirServiceplug dataShirServiceplug = new DataShirServiceplug(token);
        Map<String, Object> returnMap = new HashMap<>();
        //变换左边(column,Transformation)
        List<SquidLink> squidLinks = squidlinkDao.getSquidLinkListByFromSquid(oldColumn.getHost_squid_id());
        Map<String, String> params = new HashMap<>();
        if (squidLinks != null && squidLinks.size() > 0) {
            for (SquidLink squidLink : squidLinks) {
                params=new HashMap<>();
                Squid squid = squidDao.getSquidForCond(squidLink.getTo_squid_id(), Squid.class);
                if (squid != null) {
                    ReferenceColumn refColumn = refColumnDao.getReferenceColumnById(squid.getId(),
                            oldColumn.getColumn_id());
                    //查询出TransformationLink信息
                    params.put("squid_id", squid.getId() + "");
                    params.put("column_id", oldColumn.getColumn_id() + "");
                    List<Transformation> transformations = adapter.query2List(params, Transformation.class);
                    //List<TransformationLink> transLinks = adapter.query2List(params,TransformationLink.class);
                    if (transformations != null && transformations.size() > 0) {
                        int id = transformations.get(0).getId();
                        params = new HashMap<>();
                        params.put("from_transformation_id", id + "");
                        List<TransformationLink> transformationLinks = adapter.query2List(params, TransformationLink.class);
                        if (transformationLinks != null && transformationLinks.size() > 0) {
                            int toTransId = transformationLinks.get(0).getTo_transformation_id();
                            params = new HashMap<>();
                            params.put("id", toTransId + "");
                            List<Transformation> toTransList = adapter.query2List(params, Transformation.class);
                            if (toTransList != null && toTransList.size() > 0) {
                                int columnId = toTransList.get(0).getColumn_id();
                                params.put("id", columnId + "");
                            }
                            List<Column> leftColumn = adapter.query2List(params, Column.class);
                            //获取所有的column名字
                            String sql = "select name from ds_column where squid_id=" + squid.getId();
                            List<Map<String, Object>> totalColumn = adapter.query2List(true, sql, null);
                            if (leftColumn != null && leftColumn.size() > 0) {
                                //更改column
                                if (refColumn.getData_type() != leftColumn.get(0).getData_type()) {
                                    Transformation transformation = transDao.getTransformationById(squid.getId(), leftColumn.get(0).getId());
                                    //GroupTaggingSquid不需要断线
                                    if (squid.getSquid_type() != SquidTypeEnum.GROUPTAGGING.value()
                                            && squid.getSquid_type() != SquidTypeEnum.USERDEFINED.value()
                                            && squid.getSquid_type() != SquidTypeEnum.STATISTICS.value()
                                            && squid.getSquid_type() != SquidTypeEnum.DEST_HIVE.value()
                                            && squid.getSquid_type() != SquidTypeEnum.SAMPLINGSQUID.value()) {
                                        dataShirServiceplug.deleteTransformation(adapter, transformation.getId(), 0);
                                        transformationService.initTransformation(adapter, squid.getId(), leftColumn.get(0).getId(),
                                                TransformationTypeEnum.VIRTUAL.value(), leftColumn.get(0).getData_type(),
                                                transformation.getOutput_number());

                                    } else if (squid.getSquid_type() == SquidTypeEnum.USERDEFINED.value()) {
                                        sql = "update ds_userdefined_datamap_column set column_id=0 where squid_id=" + squid.getId() + " and column_id=" + leftColumn.get(0).getId();
                                        adapter.execute(sql);
                                    } else if (squid.getSquid_type() == SquidTypeEnum.STATISTICS.value()) {
                                        sql = "update ds_statistics_datamap_column set column_id=0 where squid_id=" + squid.getId() + " and column_id=" + leftColumn.get(0).getId();
                                        adapter.execute(sql);
                                    } else if (squid.getSquid_type() == SquidTypeEnum.DEST_HIVE.value()) {
                                        sql = "update ds_dest_hive_column set column_id=0 where squid_id=" + squid.getId() + " and column_id=" + leftColumn.get(0).getId();
                                        adapter.execute(sql);
                                    } else {
                                        //更新trans的output
                                        transformation.setOutput_data_type(leftColumn.get(0).getData_type());
                                        adapter.update2(transformation);
                                        //更新inputs
                                    }
                                }
                                params.put("id", refColumn.getColumn_id() + "");
                                Column upColumn = adapter.query2Object2(true, params, Column.class);
                                leftColumn.get(0).setData_type(upColumn.getData_type());
                                //修改名字重复
                                String name = upColumn.getName();
                                if (isNeedCheckName) {
                                    int index = 0;
                                    for (int i = 0; i < totalColumn.size(); i++) {
                                        Map<String, Object> nameMap = totalColumn.get(i);
                                        String aleadyName = nameMap.get("NAME") + "";
                                        if (aleadyName.equals(name)) {
                                            if (name.matches("(tag){1}[0-9]*")) {
                                                String noStr = name.substring(name.indexOf("tag") + 3);
                                                noStr = noStr.matches("[0-9]+") ? Integer.parseInt(noStr) + 1 + "" : noStr + 1;
                                                name = "tag" + noStr;
                                            } else {
                                                name += index;
                                            }
                                            //存在名字重复的列名
                                            //out.setMessageCode(MessageCode.COLNAME_ISEXIST);
                                            //throw new Exception("列名已经存在");
                                        }
                                    }
                                } else {
                                    if (leftColumn != null && leftColumn.size() > 0) {
                                        name = leftColumn.get(0).getName();
                                    }
                                }
                                leftColumn.get(0).setName(name);
                                leftColumn.get(0).setIsPK(upColumn.isIsPK());
                                leftColumn.get(0).setIs_Business_Key(upColumn.getIs_Business_Key());
                                leftColumn.get(0).setDescription(upColumn.getDescription());
                                leftColumn.get(0).setAggregation_type(upColumn.getAggregation_type());
                                leftColumn.get(0).setIs_groupby(upColumn.isIs_groupby());
                                leftColumn.get(0).setSort_Level(upColumn.getSort_Level());
                                leftColumn.get(0).setSort_type(upColumn.getSort_type());
                                leftColumn.get(0).setCdc(upColumn.getCdc());
                                leftColumn.get(0).setPrecision(upColumn.getPrecision());
                                leftColumn.get(0).setScale(upColumn.getScale());
                                leftColumn.get(0).setCollation(upColumn.getCollation());
                                leftColumn.get(0).setColumnAttribute(upColumn.getColumnAttribute());
                                leftColumn.get(0).setColumntype(upColumn.getColumntype());
                                leftColumn.get(0).setLength(upColumn.getLength());
                                leftColumn.get(0).setNullable(upColumn.isNullable());
                                if (squid.getSquid_type() == SquidTypeEnum.GROUPTAGGING.value()) {
                                    leftColumn.get(0).setRelative_order(upColumn.getRelative_order() + 1);
                                }
                                adapter.update2(leftColumn.get(0));

                                returnMap.put("Column", leftColumn.get(0));
                                //根据column递归更新下游的referenceColumn  2017-11-27  新加的 之前的老bug
                                synchronousUpdateColumn(adapter,squid.getId(),leftColumn.get(0),type,out,isNeedCheckName);
                                //更新transformation
                                if (refColumn.getData_type() != leftColumn.get(0).getData_type()) {
                                    transformations.get(0).setOutput_data_type(leftColumn.get(0).getData_type());
                                    adapter.update2(transformations.get(0));
                                }
                            }

                        }

                    }

                }

            }
        }
        return returnMap;
    }

    /**
     * 添加ReferenceColumn的时候操作
     * 1.检查下游连接的exceptionSquid
     * 2.为exceptionSquid添加referenceColumn，创建link,column
     * 3.检查下游squid column添加的影响
     *
     * @param adapter
     * @param oldColumn 添加的referenceColumn
     * @param type
     * @return
     * @throws Exception
     */
    @SuppressWarnings("static-access")
    public Map<String, Object> synchronousInsertRefColumn(IRelationalDataManager adapter,
                                                          ReferenceColumn oldColumn, int type) throws Exception {
        ISquidDao squidDao = new SquidDaoImpl(adapter);
        ISquidLinkDao squidlinkDao = new SquidLinkDaoImpl(adapter);
        IReferenceColumnDao refColumnDao = new ReferenceColumnDaoImpl(adapter);
        IColumnDao columnDao = new ColumnDaoImpl(adapter);
        TransformationService transformationService = new TransformationService(token);
        Map<Integer, String> squidNameMap = new HashMap<Integer, String>();
        List<SquidLink> squidLinks = squidlinkDao.getSquidLinkListByFromSquid(oldColumn.getReference_squid_id());
        if (squidLinks != null && squidLinks.size() > 0) {
            for (SquidLink squidLink : squidLinks) {
                Squid squid = squidDao.getSquidForCond(squidLink.getTo_squid_id(), Squid.class);
                if (squid != null && (squid.getSquid_type() == SquidTypeEnum.EXCEPTION.value())) {
                    Squid fromSquid = squidDao.getSquidForCond(oldColumn.getHost_squid_id(), Squid.class);
                    if (!squidNameMap.containsKey(oldColumn.getHost_squid_id())) {
                        String squidName = squidDao.getSquidNameByColumnId(oldColumn.getColumn_id());
                        squidNameMap.put(oldColumn.getHost_squid_id(), squidName);
                    }
                    ReferenceColumnGroup group = refColumnDao.getRefColumnGroupBySquidId(squid.getId(), squidLink.getFrom_squid_id());
                    if (group == null) {
                        int order = 1;
                        List<ReferenceColumnGroup> groupList = refColumnDao.getRefColumnGroupListBySquidId(squid.getId());
                        if (groupList != null && groupList.size() > 0) {
                            order = groupList.size() + 1;
                        }
                        group = new ReferenceColumnGroup();
                        group.setKey(StringUtils.generateGUID());
                        group.setReference_squid_id(squid.getId());
                        group.setRelative_order(order);
                        group.setId(refColumnDao.insert2(group));
                    }
                    int index = 0;
                    List<ReferenceColumn> refColumnList = refColumnDao.getRefColumnListByGroupId(group.getId());
                    if (refColumnList != null && refColumnList.size() > 0) {
                        index = refColumnList.size();
                    }
                    //创建新的ReferenceColumn  客户端右侧
                    ReferenceColumn newRC = transformationService.initReferenceByException(adapter, oldColumn, oldColumn.getColumn_id(),
                            index + 1, squidLink.getFrom_squid_id(), fromSquid.getName(), squid.getId(), group);

                    //创建新的Column, ExceptionSquidColumn 客户端左侧
                    Column newColumn = transformationService.initColumn2(adapter, oldColumn,
                            index + 1, squid.getId(), squidNameMap);

                    //根据Reference创建Transformation 右侧的Trans
                    transformationService.initLinkByColumn(newColumn, newRC, index, squid.getId(),
                            adapter, new ArrayList<Transformation>(), new ArrayList<TransformationLink>());
                    this.synchronousInsertColumn(adapter, squid.getId(), newColumn, type, false);

                    List<Column> columnList = columnDao.getColumnListBySquidId(squid.getId());
                    if (columnList != null && columnList.size() > 0) {
                        for (Column column2 : columnList) {
                            if (column2.getName().equals("ERROR")) {
                                column2.setRelative_order(columnList.size());
                                columnDao.update(column2);
                            }
                        }
                    }
                }
            }
        }
        return null;
    }

    /**
     * 同步GroupTagging下游信息(同步左边的column和左边的transformation)
     *
     * @param adapter
     * @param oldColumn
     * @param type
     * @return
     */
    public Map<String, Object> synchronousInsertGroupTaggingRefColumn(IRelationalDataManager adapter, ReferenceColumn oldColumn, int type,SquidLink squidLink) throws Exception {
        ISquidDao squidDao = new SquidDaoImpl(adapter);
        ISquidLinkDao squidlinkDao = new SquidLinkDaoImpl(adapter);
        IReferenceColumnDao refColumnDao = new ReferenceColumnDaoImpl(adapter);
        IColumnDao columnDao = new ColumnDaoImpl(adapter);
        TransformationService transformationService = new TransformationService(token);

        //Map<Integer, String> squidNameMap = new HashMap<Integer, String>();
        //String squidName = squidDao.getSquidNameByColumnId(oldColumn.getColumn_id());
       // Squid fromSquid = squidDao.getSquidForCond(oldColumn.getHost_squid_id(), Squid.class);

        //List<SquidLink> squidLinks = squidlinkDao.getSquidLinkListByFromSquid(fromSquid.getId());
        //if (squidLinks != null && squidLinks.size() > 0) {
            //for (SquidLink squidLink : squidLinks) {
                Squid squid = squidDao.getSquidForCond(squidLink.getTo_squid_id(), Squid.class);
                if (squid != null && (squid.getSquid_type() == SquidTypeEnum.GROUPTAGGING.value() || squid.getSquid_type() == SquidTypeEnum.SAMPLINGSQUID.value())) {
                    ReferenceColumnGroup group = refColumnDao.getRefColumnGroupBySquidId(squid.getId(), squidLink.getFrom_squid_id());
                    if (group == null) {
                        int order = 1;
                        List<ReferenceColumnGroup> groupList = refColumnDao.getRefColumnGroupListBySquidId(squid.getId());
                        if (groupList != null && groupList.size() > 0) {
                            order = groupList.size() + 1;
                        }
                        group = new ReferenceColumnGroup();
                        group.setKey(StringUtils.generateGUID());
                        group.setReference_squid_id(squid.getId());
                        group.setRelative_order(order);
                        group.setId(refColumnDao.insert2(group));
                    }
                    /*int index = 0;
                    List<ReferenceColumn> refColumnList = refColumnDao.getRefColumnListByGroupId(group.getId());
                    if (refColumnList!=null&&refColumnList.size()>0){
                        index  = refColumnList.size();
                    }*/
                    int index = 0;
                    String sql = "select max(relative_order) as orderNum from ds_column where squid_id=" + squid.getId();
                    Map<String, Object> orderMap = adapter.query2Object(true, sql, null);
                    if (orderMap != null) {
                        index = orderMap.get("ORDERNUM") == null ? 0 : Integer.parseInt(orderMap.get("ORDERNUM") + "");
                    }
                    //创建新的Column, ExceptionSquidColumn 客户端左侧
                    //判断名字是否重复
                    Map<String, Object> paramMap = new HashMap<>();
                    paramMap.put("squid_id", squid.getId());
                    List<Column> columns = adapter.query2List2(true, paramMap, Column.class);
                    if (columns != null && columns.size() > 0) {
                        for (Column aleadyColumn : columns) {
                            if (aleadyColumn.getName().equals(oldColumn.getName())) {
                                if (oldColumn.getName().matches("(tag){1}[0-9]*")) {
                                    String noStr = aleadyColumn.getName().substring(aleadyColumn.getName().indexOf("tag") + 3);
                                    noStr = noStr.matches("[0-9]+") ? Integer.parseInt(noStr) + 1 + "" : noStr + 1;
                                    oldColumn.setName("tag" + noStr);
                                } else {
                                    oldColumn.setName(oldColumn.getName() + 1);
                                }
                            }
                        }
                    }
                    Column newColumn = transformationService.initColumn(adapter, oldColumn,
                            index + 1, squid.getId(), new HashMap<String, Object>());
                    //根据Reference创建Transformation 左侧的Trans
                    transformationService.initLinkByColumn(newColumn, oldColumn, index, squid.getId(),
                            adapter, new ArrayList<Transformation>(), new ArrayList<TransformationLink>());
                    this.synchronousInsertColumn(adapter, squid.getId(), newColumn, type, false);

                //}
            //}
        }
        return null;
    }

    /**
     * 删除ReferenceColumn的时候操作
     *
     * @param adapter
     * @param oldColumn referenceColumn
     * @param type
     * @return
     * @throws Exception
     */
    public Map<String, Object> synchronousDeleteRefColumn(IRelationalDataManager adapter,
                                                          ReferenceColumn oldColumn, int type, List<DataSquidCollectionPropertyId> dataList) throws Exception {

        ITransformationDao transDao = new TransformationDaoImpl(adapter);
        ISquidDao squidDao = new SquidDaoImpl(adapter);
        IColumnDao columnDao = new ColumnDaoImpl(adapter);
        IReferenceColumnDao refColumnDao = new ReferenceColumnDaoImpl(adapter);
        DataShirServiceplug dataShirServiceplug = new DataShirServiceplug(token);
        String sql = "select ds.name from ds_squid ds inner join ds_column dc on ds.id=dc.squid_id where dc.id=" + oldColumn.getColumn_id();
        Map<String, Object> objectMap = adapter.query2Object(false, sql, null);
        String squidName = "";
        if (objectMap != null) {
            squidName = objectMap.get("NAME").toString();
        }
        /*把下游的Squid全部查询出来*/
        List<Squid> squids = squidDao.getNextSquidsForSquidID(oldColumn.getReference_squid_id(), Squid.class);
        //推给前台的数据
        int toSquidId = 0;
        List<Integer> refColumnList = new ArrayList<>();
        List<Integer> delcolumnList = new ArrayList<>();
        List<Integer> tempTransList = new ArrayList<>();
        List<Integer> tempTransLinkList = new ArrayList<>();
        List<TransformationInputs> tempUpdateInputs = new ArrayList<>();
        List<TransformationInputs> tempDeleteInputs = new ArrayList<>();
        DataSquidCollectionPropertyId tempData = null;
        if (squids != null && squids.size() > 0) {
            for (Squid squid : squids) {
                if (squid != null && (squid.getSquid_type() == SquidTypeEnum.EXCEPTION.value())) {
                    ReferenceColumn refColumn = refColumnDao.getReferenceColumnById(squid.getId(),
                            oldColumn.getColumn_id());
                    refColumnList.add(refColumn.getColumn_id());
                    Transformation transformation = transDao.getTransformationById(squid.getId(), oldColumn.getColumn_id());
                    tempTransList.add(transformation.getId());

                    sql = "select * from DS_TRANSFORMATION_LINK where from_transformation_id =" + transformation.getId() +
                            " or to_transformation_id=" + transformation.getId();
                    List<TransformationLink> transLinks = adapter.query2List(true, sql, null, TransformationLink.class);

                    if (transLinks != null && transLinks.size() > 0) {
                        tempTransLinkList.add(transLinks.get(0).getId());
                    }

                    dataShirServiceplug.deleteTransformation(adapter, transformation.getId(), 0);
                    refColumnDao.delReferenceColumnByColumnId(oldColumn.getColumn_id(), squid.getId());
                    Column updColumn = columnDao.getColumnByParams(squid.getId(), squidName + "_" + refColumn.getName());
                    delcolumnList.add(updColumn.getId());
                    if (updColumn != null && updColumn.getId() > 0) {
                        dataShirServiceplug.delColumn(adapter, updColumn.getId(), 0, new ReturnValue()); /* 删除Column */
                    }
                    List<Column> columnList = columnDao.getColumnListBySquidId(squid.getId());
                    if (columnList != null && columnList.size() > 0) {
                        for (Column column2 : columnList) {
                            if (column2.getName().equals("ERROR")) {
                                column2.setRelative_order(columnList.size());
                                columnDao.update(column2);
                            }
                        }
                    }
                    toSquidId = squid.getId();
                }
            }
            tempData = new DataSquidCollectionPropertyId(
                    toSquidId, delcolumnList, refColumnList, tempTransList,
                    tempTransLinkList,
                    tempUpdateInputs, tempDeleteInputs
            );
            dataList=new ArrayList<>();
            dataList.add(tempData);
        }
        return null;
    }

    /**
     * 重新load下游所有的
     *
     * @param squidId
     * @return
     * @author bo.dang
     * @date 2014年5月29日
     */
    public Map<String, Object> reloadColumnsBySquidId(int squidId, ReturnValue out) {
        Map<String, Object> resultMap = new HashMap<String, Object>();
        List<ReloadSquidColumnInfo> reloadSquidColumnInfoList = new ArrayList<ReloadSquidColumnInfo>();
        IRelationalDataManager adapter = null;
        try {
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            ISquidLinkDao squidLinkDao = new SquidLinkDaoImpl(adapter);
            IColumnDao columnDao = new ColumnDaoImpl(adapter);
            //获取到当前squid的column
           /* List<Column> currentColumns=columnDao.getColumnListBySquidId(squidId);
            if(currentColumns!=null){
                ReloadSquidColumnInfo columnInfo = new ReloadSquidColumnInfo();
                columnInfo.setUpdatedSquidID(squidId);
                columnInfo.setUpdatedColumns(currentColumns);
                reloadSquidColumnInfoList.add(columnInfo);
            }*/
            List<SquidLink> squidLinks = squidLinkDao.getSquidLinkListByFromSquid(squidId);
            if (squidLinks != null && squidLinks.size() > 0) {
                for (SquidLink squidLink : squidLinks) {
                    reloadSquidColumnInfoList.addAll(this.getReloadSquidColumnInfoBySquid(adapter,
                            squidLink));
                }
                resultMap.put("ColumnList", reloadSquidColumnInfoList);
                return resultMap;
            }

        } catch (Exception e) {
            logger.error("[reloadColumnsBySquidId-exception]", e);
            try {
                adapter.rollback();
            } catch (SQLException e1) {
                logger.error("rollback err!", e1);
            }
            out.setMessageCode(MessageCode.SQL_ERROR);
        } finally {
            adapter.closeSession();
        }
        return resultMap;
    }

    //递归查询squid的下游信息
    public List<ReloadSquidColumnInfo> getReloadSquidColumnInfoBySquid(
            IRelationalDataManager adapter, SquidLink squidLink) throws Exception {
        List<ReloadSquidColumnInfo> columnInfos = new ArrayList<ReloadSquidColumnInfo>();
        int fromSquidId = squidLink.getFrom_squid_id();
        int toSquidId = squidLink.getTo_squid_id();
        List<ReferenceColumn> refColumns = null;
        if (toSquidId > 0) {
            ISquidDao squidDao = new SquidDaoImpl(adapter);
            ITransformationDao transDao = new TransformationDaoImpl(adapter);
            IColumnDao columnDao = new ColumnDaoImpl(adapter);
            IReferenceColumnDao referenceColumnDao = new ReferenceColumnDaoImpl(adapter);
            ISquidLinkDao squidLinkDao = new SquidLinkDaoImpl(adapter);
            ITransformationInputsDao transInputsDao = new TransformationInputsDaoImpl(adapter);
            ITransformationLinkDao transLinkDao = new TransformationLinkDaoImpl(adapter);

            //过滤不需要的squid类型
            Squid fromSquid = squidDao.getObjectById(fromSquidId, Squid.class);
            Squid squid = squidDao.getObjectById(toSquidId, Squid.class);
            if (squid != null &&
                    (squid.getSquid_type() == SquidTypeEnum.REPORT.value()
                            || squid.getSquid_type() == SquidTypeEnum.GISMAP.value()
                            || squid.getSquid_type() == SquidTypeEnum.DESTWS.value())) {
                return columnInfos;
            }
            //如果下游是tagging.修改GroupColumnIds....属性
            if (squid != null && squid.getSquid_type() == SquidTypeEnum.GROUPTAGGING.value()) {
                GroupTaggingSquid groupSquid = squidDao.getObjectById(toSquidId, GroupTaggingSquid.class);
                groupSquid.setGroupColumnIds("");
                groupSquid.setSortingColumnIds("");
                groupSquid.setTaggingColumnIds("");
                adapter.update2(groupSquid);
            }
            ReloadSquidColumnInfo columnInfo = new ReloadSquidColumnInfo();
            columnInfo.setUpdatedSquidID(toSquidId);
            //column
            columnInfo.setUpdatedColumns(columnDao.getColumnListBySquidId(toSquidId));
            Map<String, Object> paramMap = new HashMap<>();
            paramMap.put("squid_id", squid.getId() + "");
            //查询出所有的dataMapColumn,dest_hive_column
            if (squid.getSquid_type() == SquidTypeEnum.USERDEFINED.value()) {
                List<UserDefinedMappingColumn> dataMapColumns = adapter.query2List2(true, paramMap, UserDefinedMappingColumn.class);
                if (dataMapColumns != null) {
                    columnInfo.setUpdateUserDefinedDataMappingColumns(dataMapColumns);
                }
            } else if (squid.getSquid_type() == SquidTypeEnum.STATISTICS.value()) {
                List<StatisticsDataMapColumn> dataMapColumns = adapter.query2List2(true, paramMap, StatisticsDataMapColumn.class);
                if (dataMapColumns != null) {
                    columnInfo.setUpdateStatisticsDataMappingColumns(dataMapColumns);
                }
            } else if (squid.getSquid_type() == SquidTypeEnum.DEST_HIVE.value()) {
                List<DestHiveColumn> dataMapColumns = adapter.query2List2(true, paramMap, DestHiveColumn.class);
                if (dataMapColumns != null) {
                    columnInfo.setUpdateDestHiveColumns(dataMapColumns);
                }
            }
            //如果上游squid是stage squid 并且下游是exception squid 则通过stageSquidId查询起上游squid的refrenceColumn
            if (fromSquid.getSquid_type() == SquidTypeEnum.STAGE.value() &&
                    squid.getSquid_type() == SquidTypeEnum.EXCEPTION.value()) {
                refColumns = referenceColumnDao.getInitRefColumnListByRefSquid(fromSquidId);
            } else {
                //直接通过当前squidId查询其上游squid的refrenceColumn
                refColumns = referenceColumnDao.getInitRefColumnListByCurrentSquid(toSquidId);
            }
            if (refColumns != null && refColumns.size() > 0) {
                columnInfo.setUpdatedSourceColumns(refColumns);
            }
            //transformation
            List<Transformation> transformations = transDao.getTransListBySquidId(toSquidId);
            if (transformations != null && !transformations.isEmpty()) {
                Map<Integer, List<TransformationInputs>> rsMap = transInputsDao.getTransInputsBySquidId(toSquidId);
                for (int i = 0; i < transformations.size(); i++) {
                    int trans_id = transformations.get(i).getId();
                    if (rsMap != null && rsMap.containsKey(trans_id)) {
                        transformations.get(i).setInputs(rsMap.get(trans_id));
                    }
                }
                columnInfo.setUpdatedTransformations(transformations);
            }
            // 所有transformation link
            List<TransformationLink> transformationLinks = transLinkDao.getTransLinkListBySquidId(toSquidId);
            columnInfo.setUpdatedTransformationLinks(transformationLinks);

            //载入集合中
            columnInfos.add(columnInfo);

            //是否需递归查询
            List<SquidLink> squidLinks = squidLinkDao.getSquidLinkListByFromSquid(toSquidId);
            if (squidLinks != null && squidLinks.size() > 0) {
                for (SquidLink squidLik : squidLinks) {
                    Squid toSquid = squidDao.getSquidForCond(squidLik.getTo_squid_id(), Squid.class);
                    if (squid.getSquid_type() == SquidTypeEnum.EXCEPTION.value()
                            || toSquid.getSquid_type() == SquidTypeEnum.EXCEPTION.value()
                            || toSquid.getSquid_type() == SquidTypeEnum.GROUPTAGGING.value()
                            || squid.getSquid_type() == SquidTypeEnum.GROUPTAGGING.value()
                            ||squid.getSquid_type() == SquidTypeEnum.SAMPLINGSQUID.value()
                            || toSquid.getSquid_type() == SquidTypeEnum.SAMPLINGSQUID.value()  ) {
                        columnInfos.addAll(getReloadSquidColumnInfoBySquid(adapter, squidLik));
                    }
                }
            }
        }
        return columnInfos;
    }

    /**
     * 创建目标列和对应的虚拟变换(界面右键手工创建) （相对位置和坐标由前台计算生成）
     *
     * @param columnList
     * @return
     * @throws Exception
     * @deprecated
     */
    public List<InfoPacket> createColumns(List<Column> columnList)
            throws Exception {
        List<InfoPacket> infoPackets = new ArrayList<InfoPacket>();
        try {
            adapter.openSession();
            for (int i = 0; i < columnList.size(); i++) {
                // 创建目标列
                columnList.get(i).setId(adapter.insert2(columnList.get(i)));
                // 创建对应的虚拟变换
                TransformationService service = new TransformationService(token);
                Transformation trans = service.initTransformation(adapter,
                        columnList.get(i).getSquid_id(), columnList.get(i)
                                .getId(), TransformationTypeEnum.VIRTUAL
                                .value(), columnList.get(i).getData_type(), 1);
                // 封装操作结果返回
                InfoPacket tp = new InfoPacket();
                tp.setId(trans.getId());
                tp.setKey(trans.getKey());
                tp.setType(DSObjectType.TRANSFORMATION);
                tp.setCode(tp.getId() > 0 ? MessageCode.SUCCESS.value()
                        : MessageCode.SQL_ERROR.value());
                infoPackets.add(tp);
                InfoPacket infoPacket = new InfoPacket();
                infoPacket.setId(columnList.get(i).getId());
                infoPacket.setKey(columnList.get(i).getKey());
                infoPacket.setType(DSObjectType.COLUMN);
                infoPacket.setCode(infoPacket.getId() > 0 ? MessageCode.SUCCESS
                        .value() : MessageCode.SQL_ERROR.value());
                infoPackets.add(infoPacket);
            }
        } catch (SQLException e) {
            logger.error("[save-sql-exception]", e);
            try {
                adapter.rollback();
            } catch (SQLException e1) {
                logger.error("rollback err!", e1);
            }
        } finally {
            adapter.closeSession();
        }
        return infoPackets;
    }

    /**
     * 查询指定squid的引用列-组信息
     *
     * @param reference_squid_id
     * @return
     */
    public List<ReferenceColumnGroup> getReferenceColumnGroupList(
            int reference_squid_id) {
        Map<String, String> paramMap = new HashMap<String, String>(1);
        paramMap.put("reference_squid_id",
                Integer.toString(reference_squid_id, 10));
        return adapter.query2List(paramMap, ReferenceColumnGroup.class);
    }

    /**
     * 查询某一组信息
     *
     * @param reference_squid_id
     * @param group_id
     * @return
     */
    public List<ReferenceColumn> getReferenceColumnList(int reference_squid_id,
                                                        int group_id) {
        Map<String, String> paramMap = new HashMap<String, String>(1);
        paramMap.put("reference_squid_id",
                Integer.toString(reference_squid_id, 10));
        paramMap.put("group_id", Integer.toString(group_id, 10));
        return adapter.query2List(paramMap, ReferenceColumn.class);
    }

    public List<Column> getColumnList(int squid_id) {
        Map<String, String> paramMap = new HashMap<String, String>(1);
        paramMap.put("squid_id", Integer.toString(squid_id, 10));
        return adapter.query2List(paramMap, Column.class);
    }

    public List<SourceColumn> getSourceColumnList(boolean inSession,
                                                  int source_squid_id, String table_name) {
        List<String> list = new ArrayList<String>(2);
        list.add(Integer.toString(source_squid_id, 10));
        list.add(table_name);
        return adapter
                .query2List(
                        inSession,
                        "select c.* from DS_SOURCE_TABLE t, DS_SOURCE_COLUMN c"
                                + " where t.id=c.source_table_id and t.source_squid_id=? and t.table_name=?",
                        list, SourceColumn.class);
    }

    /**
     * 新增TransformationLink，同时更改引用列is_referenced='Y'
     *
     * @param link
     * @return
     */
    public Map<String, Object> addTransLink(TransformationInputs currentInput, TransformationLink link, ReturnValue out) {
        if (link != null) {
            Map<String, Object> outputMap = new HashMap<String, Object>();
            IRelationalDataManager adapter3 = null;
            boolean flag = false;
            try {
                adapter3 = DataAdapterFactory.getDefaultDataManager();
                adapter3.openSession();
                ITransformationDao transDao = new TransformationDaoImpl(adapter3);
                ITransformationLinkDao transLinkDao = new TransformationLinkDaoImpl(adapter3);
                ITransformationInputsDao transInputsDao = new TransformationInputsDaoImpl(adapter3);
                IReferenceColumnDao refColumnDao = new ReferenceColumnDaoImpl(adapter3);
                // 计算In_order（前台没计算？）
                //List<TransformationLink> linkList = transLinkDao.getTransLink2ByToTransId(link.getTo_transformation_id());
                List<TransformationLink> linkList = transLinkDao.getTransLinkByToTransId(link.getTo_transformation_id());
                if (linkList != null && !linkList.isEmpty()) {
                    //遍历修改原来，规范原理的order顺序
                    /*String sql = "update ds_transformation_link set in_order = ? where id = ?";
                    List<List<Object>> paramsLists = new ArrayList<>();
                    for(int i=0;i<linkList.size();i++){
                        TransformationLink orderLink = linkList.get(i);
                        if(orderLink.getIn_order() != (i+1)) {
                            List<Object> paramList = new ArrayList<>();
                            paramList.add(i + 1);
                            paramList.add(orderLink.getId());
                            paramsLists.add(paramList);
                        }
                    }
                    adapter3.executeBatch(sql,paramsLists);*/
                    link.setIn_order(linkList.size() + 1);
                } else {
                    link.setIn_order(1);
                }
                Transformation fromTrans = transDao.getObjectById(link.getFrom_transformation_id(),
                        Transformation.class);
                Transformation toTrans = transDao.getObjectById(link.getTo_transformation_id(),
                        Transformation.class);
                if (toTrans != null && toTrans.getId() > 0) {
                    List<TransformationInputs> list = transInputsDao.getTransInputsForColumnByTransId(toTrans.getId());
                    if (list != null) {
                        toTrans.setInputs(list);
                    }
                    //判断trans里面的input是否被选择了
                    if (currentInput != null) {
                        flag = transInputsDao.updTransSelect(currentInput,toTrans);
                    } else {
                        flag = transInputsDao.updTransInputs(link, toTrans);
                    }
                    if (!flag) {
                        out.setMessageCode(MessageCode.ERR_TRANSFORMATION_DATA_TYPE);
                        return null;
                    }
                }
                if (StringUtils.isNull(link.getKey())) {
                    link.setKey(StringUtils.generateGUID());
                }
                int newTransLinkId = transLinkDao.insert2(link);
                if (fromTrans != null &&
                        fromTrans.getTranstype() != TransformationTypeEnum.VIRTUAL.value()) {
                    // 如果不是虚拟的
                    //SquidModelBase fromSquid = squidDao.getObjectById(fromTrans.getSquid_id(), SquidModelBase.class);
                    // 取消transformation消息泡
                    logger.debug("取消transformation消息泡====from");
                    CommonConsts.addValidationTask(new SquidValidationTask(token, adapter3, MessageBubbleService.setMessageBubble(fromTrans.getSquid_id(), fromTrans.getId(), fromTrans.getName(), MessageBubbleCode.ERROR_TRAN_PARAMETERS.value())));
                    CommonConsts.addValidationTask(new SquidValidationTask(token, adapter3, MessageBubbleService.setMessageBubble(fromTrans.getSquid_id(), fromTrans.getId(), fromTrans.getName(), MessageBubbleCode.WARN_TRANSFORMATION_NO_LINK.value())));
                } else if (toTrans != null &&
                        toTrans.getTranstype() != TransformationTypeEnum.VIRTUAL.value()) {// 如果不是虚拟
                    // 取消transformation消息泡
                    logger.debug("取消transformation消息泡====to");
//                    CommonConsts.addValidationTask(new SquidValidationTask(token, adapter3,  MessageBubbleService.setMessageBubble(fromTrans.getSquid_id(), fromTrans.getId(), fromTrans.getName(), MessageBubbleCode.ERROR_TRAN_PARAMETERS.value())));
//                    CommonConsts.addValidationTask(new SquidValidationTask(token, adapter3,  MessageBubbleService.setMessageBubble(toTrans.getSquid_id(), toTrans.getId(), toTrans.getName(), MessageBubbleCode.WARN_TRANSFORMATION_NO_LINK.value())));
                }
                //refColumnDao.updateRefColumnForLink(link.getFrom_transformation_id(), true); 锁表
                outputMap.put("newTransLinkId", newTransLinkId);
                List<TransformationInputs> newTransInputs = transInputsDao.getTransInputsForColumnByTransId(toTrans.getId());
                outputMap.put("updTransInputs", newTransInputs);
                //outputMap.put("updTransformation", );
                return outputMap;
            } catch (Exception e) {
                logger.error("[save-sql-exception]", e);
                try { // 事务回滚，很好很强大
                    adapter3.rollback();
                } catch (SQLException e1) {
                    logger.error("rollback err!", e1);
                }
                out.setMessageCode(MessageCode.SQL_ERROR);
            } finally {
                adapter3.closeSession();
            }
        }
        return null;
    }

    /**
     * @param link
     * @return
     */
    /*public List<InfoPacket> addTransLink(List<TransformationLink> link, ReturnValue out) {
        if (link != null && !link.isEmpty()) {
            List<InfoPacket> info = new ArrayList<InfoPacket>();
            Map<String, String> paramMap = new HashMap<String, String>();
            try {
                adapter.openSession();
                IReferenceColumnDao refColumnDao = new ReferenceColumnDaoImpl(adapter);
                ITransformationInputsDao transInputsDao = new TransformationInputsDaoImpl(adapter);
                for (int i = 0; i < link.size(); i++) {
                    InfoPacket infoPacket = new InfoPacket();
                    paramMap.clear();
                    paramMap.put("to_transformation_id", String.valueOf(link
                            .get(i).getTo_transformation_id()));
                    List<TransformationLink> linkList = adapter.query2List(
                            true, paramMap, TransformationLink.class);
                    if (linkList != null && !linkList.isEmpty()) {
                        link.get(i).setIn_order(linkList.size() + 1);
                    } else {
                        link.get(i).setIn_order(1);
                    }

                    boolean flag = transInputsDao.updTransInputs(link.get(i), null);
                    if (!flag){
                    	out.setMessageCode(MessageCode.ERR_TRANSFORMATION_DATA_TYPE);
                    	return null;
                    }
                    infoPacket.setId(adapter.insert2(link.get(i)));
                    infoPacket.setKey(link.get(i).getKey());
                    infoPacket.setType(DSObjectType.TRANSFORMATIONLINK);
                    info.add(infoPacket);

                    paramMap.clear();
                    paramMap.put("id", String.valueOf(link.get(i)
                            .getFrom_transformation_id()));
                    List<Transformation> fromTrans = adapter.query2List(true,
                            paramMap, Transformation.class);
                    paramMap.clear();
                    paramMap.put("id", String.valueOf(link.get(i)
                            .getTo_transformation_id()));
                    List<Transformation> toTrans = adapter.query2List(true,
                            paramMap, Transformation.class);
                    if (0 != fromTrans.get(0).getTranstype()) {
                        paramMap.clear();
                        paramMap.put("id",
                                String.valueOf(fromTrans.get(0).getSquid_id()));
                        List<SquidModelBase> SquidFrom = adapter.query2List(true,
                                paramMap, SquidModelBase.class);
                        // 鍙栨秷transformation娑堟伅娉�
                        CommonConsts.addValidationTask(new SquidValidationTask(token, adapter,  MessageBubbleService.setMessageBubble(fromTrans.get(0).getSquid_id(), fromTrans.get(0).getId(), fromTrans.get(0).getName(), MessageBubbleCode.ERROR_TRAN_PARAMETERS.value())));
                        CommonConsts.addValidationTask(new SquidValidationTask(token, adapter,  MessageBubbleService.setMessageBubble(fromTrans.get(0).getSquid_id(), fromTrans.get(0).getId(), fromTrans.get(0).getName(), MessageBubbleCode.WARN_TRANSFORMATION_NO_LINK.value())));
                    } else if (0 != toTrans.get(0).getTranstype()) {// 濡傛灉涓嶆槸铏氭嫙
                        paramMap.clear();
                        paramMap.put("id",
                                String.valueOf(toTrans.get(0).getSquid_id()));
                        List<SquidModelBase> SquidTo = adapter.query2List(true,
                                paramMap, SquidModelBase.class);
                        CommonConsts.addValidationTask(new SquidValidationTask(token, adapter,  MessageBubbleService.setMessageBubble(fromTrans.get(0).getSquid_id(), fromTrans.get(0).getId(), fromTrans.get(0).getName(), MessageBubbleCode.ERROR_TRAN_PARAMETERS.value())));
                        CommonConsts.addValidationTask(new SquidValidationTask(token, adapter,  MessageBubbleService.setMessageBubble(toTrans.get(0).getSquid_id(), toTrans.get(0).getId(), toTrans.get(0).getName(), MessageBubbleCode.WARN_TRANSFORMATION_NO_LINK.value())));
                    }
                    refColumnDao.updateRefColumnForLink(link.get(i).getFrom_transformation_id(), true);
                }
                return info;
            } catch (Exception e) {
                logger.error("[save-sql-exception]", e);
                try {
                    adapter.rollback();
                } catch (SQLException e1) {
                    logger.error("rollback err!", e1);
                }
                out.setMessageCode(MessageCode.SQL_ERROR);
            } finally {
                adapter.closeSession();
            }
        }
        return null;
    }*/

    /**
     * 删除TransformationLink，同时更改引用列is_referenced='N'
     *
     * @param TransformationLink_id
     * @return
     */
    public boolean deleteTransLink(int TransformationLink_id) {
        try {
            // 1. 更改引用列is_referenced='N'
            Map<String, String> paramMap = new HashMap<String, String>();
            // Map<String, Object> map =
            // adapter.query2Object("SELECT from_transformation_id FROM DS_TRANSFORMATION_LINK WHERE ID="+TransformationLink_id,
            // null);
            paramMap.put("id", String.valueOf(TransformationLink_id));
            List<TransformationLink> transformationLinks = adapter.query2List(
                    true, paramMap, TransformationLink.class);
            // adapter.openSession();
            if (transformationLinks != null && transformationLinks.size() > 0) {
                int from_transformation_id = transformationLinks.get(0)
                        .getFrom_transformation_id();
                // updateReferenceColumn(from_transformation_id, false);
            }
            // 2. 删除TransformationLink
            paramMap.clear();
            paramMap.put("ID", Integer.toString(TransformationLink_id, 10));
            int cnt = adapter.delete(paramMap, TransformationLink.class);
            logger.debug(cnt + "[id=" + TransformationLink_id
                    + "] TransformationLink(s) removed!");
        } catch (SQLException e) {
            logger.error("[save-sql-exception]", e);
            try {
                adapter.rollback();
            } catch (SQLException e1) {
                logger.error("rollback err!", e1);
            }
            return false;
        }
        return true;
    }

    /**
     * 获得变换序列中的数据
     * <p>
     * 作用描述：
     * </p>
     * <p>
     * 修改说明：
     * </p>
     *
     * @param request
     * @param out
     * @return
     */
    public SquidDataSet queryRuntimeData(SquidRunTimeProperties request,
                                         ReturnValue out) {
        logger.debug(String.format("queryAllTransformationDatas=%s", request));
        SquidDataSet sds = new SquidDataSet();
        Map<String, String> paramMap = new HashMap<String, String>();
        paramMap.put("ID", Integer.toString(request.getSquid_id(), 10));
        List<Squid> list = adapter.query2List(paramMap, Squid.class);
        Squid s = list == null || list.isEmpty() ? null : list.get(0);
        if (s != null) {
            logger.debug(MessageFormat.format("SquidModelBase id={0}, key={1}",
                    s.getId(), s.getKey()));
            DataSquid targetSquid = new DataSquid();
            targetSquid.setId(s.getId());
            targetSquid.setKey(s.getKey());
            sds.setTarget_squid(targetSquid);

            if (request.isAutoRefreshData()) { // 自动推送未完数据
                // SquidTransformationFactory
                // squidTransformationFactory=SquidTransformationFactory.newInstance();
                // squidTransformationFactory.pushMessageData(request, out);
            } else { // 第一次请求数据AutoRefreshData=false，前台获得数据后将其改为true
                GetRowsFromDataSetPar par = new GetRowsFromDataSetPar();
                par.setSquidFlowId(s.getSquidflow_id());
                par.setSquidId(s.getId());
                ResultSets rs = this.queryAll(par, request.getBatchRowNumber());
                if (rs != null) {
                    sds.setDataCount(rs.getDataCount());
                    sds.setColumnList(rs.getColumnList());
                    sds.setDataList(rs.getDataList());
                } else {
                    out.setMessageCode(MessageCode.NODATA);
                }
            }
        } else {
            // query SquidId not exists
            out.setMessageCode(MessageCode.ERR_SQUID_NULL);
        }
        return sds;
    }

    /**
     * 获取关系型数据库的预览信息
     *
     * @param request
     * @param out
     * @return
     */
    public SquidDataSet newQueryDatas(SquidTableInfo request, ReturnValue out) {
        SquidDataSet sds = new SquidDataSet();
        Map<String, String> params = new HashMap<String, String>();
        DBConnectionInfo dbs = new DBConnectionInfo();
        int count = 0;
        try {
            // 表名
            String tableName = request.getTable_name();
            adapter.openSession();
            params.put("id", String.valueOf(request.getSquid_id()));
            DbSquid dbSquid = adapter
                    .query2Object2(true, params, DbSquid.class);
            String columnSql = "SELECT * FROM DS_SOURCE_COLUMN  where  source_table_id =(select  id  from DS_SOURCE_TABLE where table_name='"
                    + tableName
                    + "'   and source_squid_id="
                    + request.getSquid_id() + ")";
            //获取缓存表数据
            List<SourceColumn> sourceColumns = adapter.query2List(true, columnSql, null, SourceColumn.class);
            Map<String, String> tempMap = this.getNameAndType(sourceColumns, dbSquid.getDb_type());
            // 获取数据库类型
            dbs.setDbType(DataBaseType.parse(dbSquid.getDb_type()));
            // 获取username
            dbs.setUserName(dbSquid.getUser_name());
            // 获取password
            dbs.setPassword(dbSquid.getPassword());
            // 主机名
            dbs.setHost(dbSquid.getHost());
            // 数据库名
            dbs.setDbName(dbSquid.getDb_name());
            // 端口
            dbs.setPort(dbSquid.getPort());

            INewDBAdapter iNewDBAdapter = AdapterDataSourceManager
                    .getAdapter(dbs);
            SelectSQL sql = new SelectSQL(tableName);
            if (count > 0) {
                sql.setMaxCount(count);
            } else {
                sql.setMaxCount(20);
            }
            List<Map<String, Object>> dataList = iNewDBAdapter
                    .queryForList(sql, tempMap);
            if (dataList.size() < 1) {
                out.setMessageCode(MessageCode.ERR_VALUEEMPTY);
            } else {
                List<Map<String, String>> stringMaps = new ArrayList<Map<String, String>>();
                for (Map<String, Object> objectMap : dataList) {
                    Map<String, String> newMap = (Map) objectMap;
                    stringMaps.add(newMap);
                }
                sds.setDataList(stringMaps);
            }


        } catch (Exception e) {
            // TODO: handle exception
            logger.error(e);
            out.setMessageCode(MessageCode.ERR_DATEBASECONTENT);
        } finally {
            adapter.closeSession();
        }
        return sds;
    }

    /**
     * 获取列的名称和类型
     *
     * @return
     */
    private Map<String, String> getNameAndType(List<SourceColumn> sourceColumns, int dbType) {
        Map<String, String> map = new HashMap<String, String>();
        for (SourceColumn sourceColumn : sourceColumns) {
            //KEY:列名   value:字段类型+&+长度+&+数据库类型
            map.put(sourceColumn.getName(), sourceColumn.getData_type() + "&" + sourceColumn.getLength() + "&" + dbType);
        }
        return map;
    }

    /**
     * 根据DataSquid的连接信息获得相应的表的数据
     * <p>
     * 作用描述：
     * </p>
     * <p>
     * 修改说明：
     * </p>
     *
     * @param request
     * @param out
     * @return
     */
    public SquidDataSet queryDatas(SquidTableInfo request, ReturnValue out) {
        request.setCurrent_page(Math.max(1, request.getCurrent_page()));
        request.setCount(Math.max(CommonConsts.MinPageSize, request.getCount()));
        request.setCount(Math.min(CommonConsts.MaxPageSize, request.getCount()));

        Squid squid = new SquidServicesub(adapter).getSquid(request
                .getSquid_id());

        SquidDataSet sds = new SquidDataSet();
        DataSquid targetSquid = new DataSquid();
        targetSquid.setId(request.getSquid_id());
        targetSquid.setKey(squid == null ? null : squid.getKey());
        sds.setTarget_squid(targetSquid);

        DbSquid dbSquid = new DBSquidService(adapter).getDBSquid(request
                .getSquid_id());
        if (dbSquid == null) {
            out.setMessageCode(MessageCode.ERR_CONNECTION_NULL);
            return null;
        }
        logger.debug(MessageFormat.format(
                "db_source_squid Connection url={0}:{1}:{2}, usrName={3}",
                dbSquid.getHost(), dbSquid.getPort(), dbSquid.getDb_name(),
                dbSquid.getUser_name()));

        // （理论上）连接任意数据源
        IDBAdapter db_source_adapter = adapterFactory.makeAdapter(dbSquid);
        ResultSets rs = db_source_adapter.queryAll(request.getCurrent_page(),
                request.getCount(), request.getTable_name());
        db_source_adapter.commitAdapter();
        if (rs != null) {
            logger.debug(MessageFormat.format("ResultSets DataCount={0}",
                    rs.getDataCount()));
            sds.setDataCount(rs.getDataCount());
            sds.setColumnList(rs.getColumnList());
            sds.setDataList(rs.getDataList());
        } else {
            out.setMessageCode(MessageCode.NODATA);
        }
        return sds;
    }

    /**
     * 根据id删除ReferenceCloumn
     *
     * @param column_id
     * @return
     * @throws SQLException
     */
    public boolean deleteReferenceCloumn(int column_id, ReturnValue out)
            throws DatabaseException {
        String sql = "delete from DS_REFERENCE_COLUMN where column_id="
                + column_id;
        return adapter.execute(sql) > 0 ? true : false;
    }

    /**
     * 根据id删除ReferenceGroup
     *
     * @param id
     * @return
     * @throws SQLException
     */
    public boolean deleteReferenceGroup(int id, ReturnValue out)
            throws SQLException {
        int excuteResult = 0;
        boolean delteResult = false;
        TransformationPlug transformationPlug = new TransformationPlug(
                DataAdapterFactory.newInstance().getTokenAdapter(token));
        try {
            String delColumnSql = "delete from DS_REFERENCE_COLUMN where group_id="
                    + id;
            String delGroupSql = "delete from DS_REFERENCE_COLUMN_GROUP where id="
                    + id;
            Map<String, String> paramMap = new HashMap<String, String>();
            paramMap.put("GROUP_ID", Integer.toString(id, 10));
            // 根据 group_id查询ReferenceCloumn集合
            List<ReferenceColumn> referenceColumns = adapter.query2List(true,
                    paramMap, ReferenceColumn.class);
            // 根据group_id删除ReferenceCloumn
            excuteResult = adapter.execute(delColumnSql);
            if (excuteResult > 0) {
                // 根据id删除ReferenceGroup
                excuteResult = adapter.execute(delGroupSql);
                if (excuteResult > 0) {
                    if (null != referenceColumns && referenceColumns.size() > 0) {
                        for (ReferenceColumn referenceColumn : referenceColumns) {
                            // 根据host_squid_id和reference_squid_id删除squidlink
                            paramMap.clear();
                            paramMap.put("from_squid_id", Integer
                                    .toString(referenceColumn
                                            .getHost_squid_id()));
                            paramMap.put("to_squid_id", Integer
                                    .toString(referenceColumn
                                            .getReference_squid_id()));
                            excuteResult = adapter.delete(paramMap,
                                    SquidLink.class);
                            if (excuteResult >= 0) {
                                // 根据host_squid_id和reference_squid_id删除squidjoin
                                paramMap.clear();
                                paramMap.put("joined_squid_id", Integer
                                        .toString(referenceColumn
                                                .getHost_squid_id()));
                                paramMap.put("target_squid_id", Integer
                                        .toString(referenceColumn
                                                .getReference_squid_id()));
                                excuteResult = adapter.delete(paramMap,
                                        SquidJoin.class);
                                if (excuteResult >= 0) {
                                    // 根据column_id和transformation_type_id查询出transformation的id,调用删除transformation的方法
                                    paramMap.clear();
                                    paramMap.put("column_id", Integer
                                            .toString(referenceColumn
                                                    .getColumn_id()));
                                    paramMap.put(
                                            "transformation_type_id",
                                            Integer.toString(TransformationTypeEnum.VIRTUAL
                                                    .value()));
                                    List<Transformation> transformations = adapter
                                            .query2List(true, paramMap,
                                                    Transformation.class);
                                    for (Transformation transformation : transformations) {
                                        delteResult = transformationPlug
                                                .deleteTransformation(
                                                        transformation.getId(),
                                                        out, token);
                                        if (!delteResult) {
                                            out.setMessageCode(MessageCode.ERR_DELETE_REFERENCECOLUMNGROUP);
                                            return false;
                                        }
                                    }
                                } else {
                                    out.setMessageCode(MessageCode.ERR_DELETE_REFERENCECOLUMNGROUP);
                                    return false;
                                }
                            } else {
                                out.setMessageCode(MessageCode.ERR_DELETE_REFERENCECOLUMNGROUP);
                                return false;
                            }
                        }
                    }
                }
            } else {
                out.setMessageCode(MessageCode.ERR_DELETE_REFERENCECOLUMNGROUP);
                return false;
            }
        } catch (Exception e) {
            out.setMessageCode(MessageCode.ERR_DELETE_REFERENCECOLUMNGROUP);
            logger.debug("删除ReferenceGroup失败", e);
        }

        return excuteResult >= 0 ? true : false;
    }

    /**
     * 获取运行时数据
     *
     * @param par
     * @param batchRowNumber 每一次推送的最大记录个数
     * @return
     */
    public ResultSets queryAll(GetRowsFromDataSetPar par, int batchRowNumber) {
        ResultSets resultSets = new ResultSets();
        // SquidTransformationFactory
        // squidTransformationFactory=SquidTransformationFactory.newInstance();
        // SquidBasic basic=squidTransformationFactory.getFlowSquidInfor(par,
        // null);
        DataSet s = null; // basic==null?null:basic.getOutDataSet();
        // 获得内存中列的信息
        List<Column> columnList = s == null ? null : s.getColumns();
        // 获得内存中数据信息
        List<List<Object>> cellsList = s == null ? null : s.getCells();
        int colCnt = 0;// 列数
        int rowCnt = 0;// 行数
        if (columnList != null) {
            colCnt = columnList.size();
            // logger.debug("RuntimeData, ColumnInfo: "+colCnt+"\r\n"+columnList);
            Map<String, String> columMap = new HashMap<String, String>(colCnt); // 存储列信息
            for (Column cum : columnList) {
                columMap.put(cum.getName(), String.valueOf(cum.getData_type()));
            }
            resultSets.setColumnList(columMap);
        }
        if (cellsList != null) {
            rowCnt = cellsList.size();
            // logger.debug("RuntimeData, RowInfo: "+rowCnt+"\r\n"+cellsList);
            resultSets.setDataCount(rowCnt);
            rowCnt = Math.min(rowCnt, batchRowNumber);
            List<Map<String, String>> dataList = new ArrayList<Map<String, String>>(
                    rowCnt);
            Map<String, String> map = null;
            for (int i = 0; i < rowCnt; i++) {
                map = new HashMap<String, String>(colCnt);
                for (int j = 0; j < colCnt; j++) {
                    map.put(columnList.get(j).getName(), cellsList.get(i)
                            .get(j) == null ? "" : cellsList.get(i).get(j)
                            .toString());
                }
                dataList.add(map);
            }
            resultSets.setDataList(dataList);
        }
        return resultSets;
    }

}
