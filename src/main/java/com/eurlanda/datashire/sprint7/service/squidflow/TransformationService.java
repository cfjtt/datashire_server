package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.dao.*;
import com.eurlanda.datashire.dao.impl.*;
import com.eurlanda.datashire.entity.*;
import com.eurlanda.datashire.entity.dest.DestHDFSColumn;
import com.eurlanda.datashire.entity.dest.DestImpalaColumn;
import com.eurlanda.datashire.entity.dest.EsColumn;
import com.eurlanda.datashire.entity.operation.DataEntity;
import com.eurlanda.datashire.entity.operation.WhereCondition;
import com.eurlanda.datashire.enumeration.*;
import com.eurlanda.datashire.enumeration.datatype.SystemDatatype;

import com.eurlanda.datashire.server.utils.Constants;
import com.eurlanda.datashire.server.utils.TokenUtil;
import com.eurlanda.datashire.sprint7.packet.InfoPacket;
import com.eurlanda.datashire.sprint7.packet.PushMessagePacket;
import com.eurlanda.datashire.sprint7.plug.GetParam;
import com.eurlanda.datashire.sprint7.plug.SupportPlug;
import com.eurlanda.datashire.sprint7.plug.TransformationPlug;
import com.eurlanda.datashire.sprint7.service.squidflow.dest.EsColumnService;
import com.eurlanda.datashire.sprint7.service.squidflow.dest.HdfsColumnService;
import com.eurlanda.datashire.sprint7.service.squidflow.dest.ImpalaColumnService;
import com.eurlanda.datashire.utility.*;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;

/**
 * Transformation业务处理类
 * Title :
 * Description:
 * Author :赵春花 2013-10-28
 * update :yi.zhou 2014-05-06
 * Department :  JAVA后端研发部
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司
 */
public class TransformationService extends SupportPlug implements ITransformationService {
    static Logger logger = Logger.getLogger(TransformationService.class);// 记录日志
    TransformationPlug transformationPlug = new TransformationPlug(
            adapter);
    private String token;

    public TransformationService(String token) {
        super(token);
        this.token = token;
    }

    /**
     * 创建ReferenceColumn集合
     *
     * @param adapter3        数据库连接对象
     * @param referenceColumn 表字段信息
     * @param newColumnId     表字段主键（单独出来，有可能是Column 或者 复制ReferenceColumn）
     * @param order           排序默认值
     * @param toSquidId       squidId（当前为Exception）
     * @param formSquid
     * @param columnGroup
     * @return
     * @throws SQLException
     */
    public static ReferenceColumn initReference(IRelationalDataManager adapter3, SourceColumn referenceColumn,
                                                int newColumnId, int order, Squid formSquid,
                                                int toSquidId, ReferenceColumnGroup columnGroup) throws SQLException {
        // 创建引用列
        ReferenceColumn ref = new ReferenceColumn();
        ref.setColumn_id(newColumnId);
        ref.setId(ref.getColumn_id());
        ref.setCollation(0);
        ref.setData_type(referenceColumn.getData_type());
        ref.setKey(StringUtils.generateGUID());
        ref.setName(referenceColumn.getName());
        ref.setNullable(referenceColumn.isNullable());
        ref.setLength(referenceColumn.getLength());
        ref.setPrecision(referenceColumn.getPrecision());
        ref.setScale(referenceColumn.getScale());
        ref.setRelative_order(order);
        ref.setSquid_id(formSquid.getId());
        ref.setType(DSObjectType.COLUMN.value());
        ref.setReference_squid_id(toSquidId);
        ref.setHost_squid_id(formSquid.getId());
        ref.setIsPK(referenceColumn.isIspk());
        ref.setIsUnique(referenceColumn.isIsunique());
        ref.setIs_referenced(true);
        ref.setGroup_id(columnGroup.getId());
        adapter3.insert2(ref);
        // 添加新创建的ReferenceColumn信息
        ref.setGroup_name(formSquid.getName());
        ref.setGroup_order(columnGroup.getRelative_order());
        return ref;
    }

    /**
     * 创建ReferenceColumn集合
     *
     * @param adapter3        数据库连接对象
     * @param referenceColumn 表字段信息
     * @param newColumnId     表字段主键（单独出来，有可能是Column 或者 复制ReferenceColumn）
     * @param order           排序默认值
     * @param formSquid
     * @param toSquidId       squidId（当前为Exception）
     * @param columnGroup
     * @return
     * @throws SQLException
     */
    public static ReferenceColumn initReference_fileFolder(IRelationalDataManager adapter3, SourceColumn referenceColumn,
                                                           int newColumnId, int order, Squid formSquid,
                                                           int toSquidId, ReferenceColumnGroup columnGroup) throws SQLException {
        // 创建引用列
        ReferenceColumn ref = new ReferenceColumn();
        ref.setColumn_id(newColumnId);
        ref.setId(ref.getColumn_id());
        ref.setCollation(0);
        //全部替换为nvarchar（max)
        ref.setData_type(SystemDatatype.NVARCHAR.value());
        ref.setLength(-1);
        ref.setPrecision(0);
        ref.setScale(0);
        //ref.setData_type(referenceColumn.getData_type());
        //ref.setLength(referenceColumn.getLength());
        //ref.setPrecision(referenceColumn.getPrecision());
        //ref.setScale(referenceColumn.getScale());
        ref.setKey(StringUtils.generateGUID());
        ref.setName(StringUtils.replaceSource(referenceColumn.getName()));
        ref.setNullable(referenceColumn.isNullable());
        ref.setRelative_order(order);
        ref.setSquid_id(formSquid.getId());
        ref.setType(DSObjectType.COLUMN.value());
        ref.setReference_squid_id(toSquidId);
        ref.setHost_squid_id(formSquid.getId());
        ref.setIsPK(referenceColumn.isIspk());
        ref.setIsUnique(referenceColumn.isIsunique());
        ref.setIs_referenced(true);
        ref.setGroup_id(columnGroup.getId());
        adapter3.insert2(ref);
        // 添加新创建的ReferenceColumn信息
        ref.setGroup_name(formSquid.getName());
        ref.setGroup_order(columnGroup.getRelative_order());
        return ref;
    }

    public static ReferenceColumn initReference(IRelationalDataManager adapter3, Column referenceColumn,
                                                int newColumnId, int order, Squid formSquid,
                                                int toSquidId, ReferenceColumnGroup columnGroup) throws SQLException {
        // 创建引用列
        ReferenceColumn ref = new ReferenceColumn();
        ref.setColumn_id(newColumnId);
        ref.setId(ref.getColumn_id());
        ref.setCollation(0);
        ref.setData_type(referenceColumn.getData_type());
        ref.setKey(StringUtils.generateGUID());
        ref.setName(referenceColumn.getName());
        ref.setNullable(referenceColumn.isNullable());
        ref.setLength(referenceColumn.getLength());
        ref.setPrecision(referenceColumn.getPrecision());
        ref.setScale(referenceColumn.getScale());
        ref.setRelative_order(order);
        ref.setSquid_id(formSquid.getId());
        ref.setType(DSObjectType.COLUMN.value());
        ref.setReference_squid_id(toSquidId);
        ref.setHost_squid_id(formSquid.getId());
        ref.setIsPK(referenceColumn.isIsPK());
        ref.setIsUnique(referenceColumn.isIsUnique());
        ref.setIs_referenced(true);
        ref.setGroup_id(columnGroup.getId());
        adapter3.insert2(ref);
        // 添加新创建的ReferenceColumn信息
        ref.setGroup_name(formSquid.getName());
        ref.setGroup_order(columnGroup.getRelative_order());
        return ref;
    }

    public static ReferenceColumn initReferenceByException(IRelationalDataManager adapter3, Column referenceColumn,
                                                           int newColumnId, int order, int squidId, String fromName,
                                                           int toSquidId, ReferenceColumnGroup columnGroup) throws SQLException {
        // 创建引用列
        ReferenceColumn ref = new ReferenceColumn();
        ref.setColumn_id(newColumnId);
        ref.setId(ref.getColumn_id());
        ref.setCollation(0);
        ref.setData_type(referenceColumn.getData_type());
        ref.setKey(StringUtils.generateGUID());
        ref.setName(referenceColumn.getName());
        ref.setNullable(referenceColumn.isNullable());
        ref.setLength(referenceColumn.getLength());
        ref.setPrecision(referenceColumn.getPrecision());
        ref.setScale(referenceColumn.getScale());
        ref.setRelative_order(order);
        ref.setSquid_id(squidId);
        ref.setType(DSObjectType.COLUMN.value());
        ref.setReference_squid_id(toSquidId);
        ref.setHost_squid_id(squidId);
        ref.setIsPK(referenceColumn.isIsPK());
        ref.setIsUnique(referenceColumn.isIsUnique());
        ref.setIs_referenced(true);
        ref.setGroup_id(columnGroup.getId());
        adapter3.insert2(ref);
        // 添加新创建的ReferenceColumn信息
        ref.setGroup_name(fromName);
        ref.setGroup_order(columnGroup.getRelative_order());
        return ref;
    }

    public static ReferenceColumn initReference(IRelationalDataManager adapter3, Column referenceColumn,
                                                int newColumnId, int order, Squid formSquid,
                                                int toSquidId, int columnGroupId, int columnGroupOrder) throws SQLException {
        // 创建引用列
        ReferenceColumn ref = new ReferenceColumn();
        ref.setColumn_id(newColumnId);
        ref.setId(ref.getColumn_id());
        ref.setCollation(0);
        ref.setData_type(referenceColumn.getData_type());
        ref.setKey(StringUtils.generateGUID());
        ref.setName(referenceColumn.getName());
        ref.setNullable(referenceColumn.isNullable());
        ref.setLength(referenceColumn.getLength());
        ref.setPrecision(referenceColumn.getPrecision());
        ref.setScale(referenceColumn.getScale());
        ref.setRelative_order(order);
        ref.setSquid_id(formSquid.getId());
        ref.setType(DSObjectType.COLUMN.value());
        ref.setReference_squid_id(toSquidId);
        ref.setHost_squid_id(formSquid.getId());
        ref.setIsPK(referenceColumn.isIsPK());
        ref.setIsUnique(referenceColumn.isIsUnique());
        ref.setIs_referenced(true);
        ref.setGroup_id(columnGroupId);
        adapter3.insert2(ref);
        // 添加新创建的ReferenceColumn信息
        ref.setGroup_name(formSquid.getName());
        ref.setGroup_order(columnGroupOrder);
        return ref;
    }

    /**
     * 创建ReferenceColumn集合
     *
     * @param adapter3        数据库连接对象
     * @param referenceColumn 表字段信息
     * @param newColumnId     表字段主键（单独出来，有可能是Column 或者 复制ReferenceColumn）
     * @param order           排序默认值
     * @param toSquidId       squidId（当前为Exception）
     * @param formSquid
     * @param columnGroup
     * @return
     * @throws SQLException
     */
    public static ReferenceColumn mergeReferenceColumn(IRelationalDataManager adapter3, Column referenceColumn,
                                                       int newColumnId, int order, Squid formSquid,
                                                       int toSquidId, ReferenceColumnGroup columnGroup) {
        ReferenceColumn ref = null;
        try {
            Map<String, String> paramsMap = new HashMap<String, String>();
            paramsMap.put("host_squid_id", Integer.toString(formSquid.getId(), 10));
            paramsMap.put("reference_squid_id", Integer.toString(toSquidId, 10));
            paramsMap.put("column_id", Integer.toString(newColumnId, 10));
//        paramsMap.put("squid_id", Integer.toString(formSquid.getId(), 10));
            ref = adapter3.query2Object2(true, paramsMap, ReferenceColumn.class);
            if (StringUtils.isNotNull(ref)) {
                ref.setRelative_order(order);
                ref.setId(newColumnId);
                updateReferenceColumn(adapter3, ref, null);
            } else {
                // 创建引用列
                ref = new ReferenceColumn();
                ref.setColumn_id(newColumnId);
                ref.setId(ref.getColumn_id());
                ref.setCollation(referenceColumn.getCollation());
                ref.setData_type(referenceColumn.getData_type());
                ref.setKey(StringUtils.generateGUID());
                ref.setName(referenceColumn.getName());
                ref.setNullable(referenceColumn.isNullable());
                ref.setLength(referenceColumn.getLength());
                ref.setPrecision(referenceColumn.getPrecision());
                ref.setScale(referenceColumn.getScale());
                ref.setRelative_order(order);
                ref.setSquid_id(formSquid.getId());
                ref.setType(DSObjectType.COLUMN.value());
                ref.setReference_squid_id(toSquidId);
                ref.setHost_squid_id(formSquid.getId());
                ref.setIsPK(referenceColumn.isIsPK());
                ref.setIsUnique(referenceColumn.isIsUnique());
                ref.setIs_referenced(true);
                ref.setGroup_id(columnGroup.getId());
                adapter3.insert2(ref);
            }
            // 添加新创建的ReferenceColumn信息
            ref.setGroup_name(formSquid.getName());
            ref.setGroup_order(columnGroup.getRelative_order());
        } catch (SQLException e) {
            logger.error("更新mergeReferenceColumn异常", e);
            try {
                adapter3.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!", e1);
            }
        }
        return ref;
    }

    /**
     * 更新ReferenceColumn
     *
     * @param adapter3
     * @param referenceColumn
     * @param column
     * @return
     * @throws SQLException
     * @throws DatabaseException
     * @author bo.dang
     * @date 2014年5月29日
     */
    public static ReferenceColumn updateReferenceColumn(
            IRelationalDataManager adapter3,
            ReferenceColumn referenceColumn,
            Column column) {
        // 创建引用列
        String nullAble = null;
        String isUnique = null;
        // fixed bug 843 start by bo.dang
        if (StringUtils.isNotNull(column)) {
            referenceColumn.setName(column.getName());
            referenceColumn.setData_type(column.getData_type());
            referenceColumn.setLength(column.getLength());
            referenceColumn.setPrecision(column.getPrecision());
            referenceColumn.setScale(column.getScale());
            referenceColumn.setRelative_order(column.getRelative_order());
            nullAble = column.isNullable() ? "Y" : "N";
            isUnique = column.isIsUnique() ? "Y" : "N";
            referenceColumn.setCollation(column.getCollation());
            referenceColumn.setDescription(column.getDescription() == null ? "" : column.getDescription());
        } else {
            nullAble = referenceColumn.isNullable() ? "Y" : "N";
            isUnique = referenceColumn.isIsUnique() ? "Y" : "N";
        }

        try {
            String sqlValue = "update ds_reference_column set name=\'" + referenceColumn.getName() + "\' , data_type="
                    + referenceColumn.getData_type() + ",relative_order=" + referenceColumn.getRelative_order() + ", length=" + referenceColumn.getLength() + ", `precision`="
                    + referenceColumn.getPrecision() + ", scale=" + referenceColumn.getScale() + ", nullable=\'" + nullAble + "\', collation=\'" + referenceColumn.getCollation()
                    + "\', isunique=\'" + isUnique + "\', description=\'" + referenceColumn.getDescription()
                    + "\' where column_id=" + referenceColumn.getColumn_id() + " and reference_squid_id=" + referenceColumn.getReference_squid_id();
            adapter3.execute(sqlValue);
            // fixed bug 843 end by bo.dang
        } catch (DatabaseException e) {
            logger.error("更新updateReferenceColumn异常", e);
            try {
                adapter3.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!", e1);
            }
        }
        //adapter3.update2(referenceColumn);
        return referenceColumn;
    }

    public static void main(String[] args) throws EnumException {
        String temp = "[{\"Id\":10,\"Description\":\"被拆分的字符串\",\"SourceTransformationName\":\"\",\"TransformationId\":264,\"Source_Transform_Id\":0,\"Source_Tran_Output_Index\":0,\"Condition\":\"\",\"Relative_Order\":1,\"Input_Data_Type\":0}]";
        List<TransformationInputs> tranInputs = JsonUtil.toGsonList(temp, TransformationInputs.class);
        //TransformationInputs tranInputs = JsonUtil.toGsonList(temp.toLowerCase(), TransformationInputs.class);
        //TransformationInputs tranInputs = (TransformationInputs)JSONObject.toBean(JSONObject.fromObject(temp.toLowerCase()), TransformationInputs.class) ;
        System.out.println(tranInputs);
    }

    /**
     * 创建Transformation对象集合
     * 作用描述：
     * Transformation对象集合不能为空且必须有数据
     * Transformation对象集合里的每个Key必须有值
     * 根据传入的Transformation对象集合创建SquidFLow对象
     * 创建成功——根据Transformation集合对象里的Key查询相对应的ID
     * 修改说明：
     *
     * @param transformations transformation集合对象
     * @param out             异常处理
     * @return
     */
    @Deprecated
    public List<InfoPacket> createTransformations(
            List<Transformation> transformations, ReturnValue out) {
        logger.debug(String.format(
                "createTransformations-TransfromationList.size()=%s",
                transformations.size()));

        boolean create = false;
        List<InfoPacket> infoPackets = new ArrayList<InfoPacket>();
        try {
            // 定义接收返回结果集
            // 调用批量新增
            create = transformationPlug.createTransformations(transformations,
                    out);
            // 新增成功
            if (create) {
                // 根据Key查询出新增的ID
                //int i = 0;
                for (Transformation transformation : transformations) {
                    // 获得Key
                    String key = transformation.getKey();
                    // 根据Key获得ID
                    int id = transformationPlug.getTransformationId(key, out);
                    InfoPacket infoPacket = new InfoPacket();
                    infoPacket.setId(id);
                    infoPacket.setKey(key);
                    infoPacket.setType(DSObjectType.TRANSFORMATION);
                    infoPackets.add(infoPacket);
                    //i++;
                }
                //根据transformation的squid_id查询到DS_SQUID的key
                Squid squid = new RepositoryServiceHelper(TokenUtil.getToken()).getOne(Squid.class, transformations.get(0).getSquid_id());
                logger.debug("根据id查询出的squid对象" + squid.getKey());
                //推送孤立transformation消息泡
                logger.debug("推送孤立transformation消息泡");
                PushMessagePacket.push(
                        new MessageBubble(squid.getKey(), transformations.get(0).getKey(), MessageBubbleCode.WARN_TRANSFORMATION_NO_LINK.value(), false),
                        TokenUtil.getToken());
            }
            logger.debug(String.format("createTransformations-return=%s",
                    infoPackets));
        } catch (Exception e) {
            logger.error("createTransformations-return=%s", e);
            out.setMessageCode(MessageCode.ERR_ARRAYS);
        } finally {
            // 释放连接
            adapter.commitAdapter();
        }
        return infoPackets;
    }

    /**
     * 作用描述：
     * 修改Transformation对象集合
     * 调用方法前确保Transformation对象集合不为空，并且Transformation对象集合里的每个Transformation对象的Id不为空
     * 修改说明：
     *
     * @param transformations transformation集合对象
     * @param out             异常处理
     * @return
     */
    @Deprecated
    public List<InfoPacket> updateTransformations(List<Transformation> transformations, ReturnValue out) {
        logger.debug(String.format("updateTransformations-TransformationList.size()=%s", transformations.size()));
        boolean create = false;
        List<InfoPacket> infoPackets = new ArrayList<InfoPacket>();
        try {
            //封装批量新增数据集
            List<List<DataEntity>> paramList = new ArrayList<List<DataEntity>>();
            //封装条件集合
            List<List<WhereCondition>> whereCondList = new ArrayList<List<WhereCondition>>();
            //调用转换类
            GetParam getParam = new GetParam();
            for (Transformation transformation : transformations) {
                //参数
                List<DataEntity> dataEntitys = new ArrayList<DataEntity>();
                getParam.getTransformation(transformation, dataEntitys);
                paramList.add(dataEntitys);
                //条件
                List<WhereCondition> whereConditions = new ArrayList<WhereCondition>();
                WhereCondition whereCondition = new WhereCondition();
                whereCondition.setAttributeName("ID");
                whereCondition.setDataType(DataStatusEnum.INT);
                whereCondition.setMatchType(MatchTypeEnum.EQ);
                whereCondition.setValue(transformation.getId());

                whereConditions.add(whereCondition);

                whereCondList.add(whereConditions);

            }
            //调用批量新增
            create = adapter.updatBatch(SystemTableEnum.DS_TRANSFORMATION.toString(), paramList, whereCondList, out);
            logger.debug(String.format("updateTransformations-return=%s", create));
            //查询返回结果
            if (create) {
                TransformationPlug transformationPlug = new TransformationPlug(adapter);
                //定义接收返回结果集
                //根据Key查询出新增的ID
                for (Transformation transformation : transformations) {
                    //获得Key
                    String key = transformation.getKey();
                    int id = transformationPlug.getTransformationId(key, out);
                    InfoPacket infoPacket = new InfoPacket();
                    infoPacket.setCode(1);
                    infoPacket.setId(id);
                    infoPacket.setType(DSObjectType.TRANSFORMATIONLINK);
                    infoPacket.setKey(key);
                    infoPackets.add(infoPacket);
                }
            }
        } catch (Exception e) {
            logger.error("updateTransformations-return=%s", e);
            out.setMessageCode(MessageCode.ERR_ARRAYS);
        } finally {
            //释放
            adapter.commitAdapter();
        }
        return infoPackets;
    }

    /**
     * 获取当前transformation的INPUTS集合
     *
     * @param info
     * @param out  新接口 2014-05-09
     * @return
     */
    public Map<String, Object> getTransformationInputById(String info, ReturnValue out) {
        Map<String, Object> outputMap = new HashMap<String, Object>();
        IRelationalDataManager adapter3 = null;
        DataAdapterFactory adapterFactory = null;
        try {
            Map<String, Object> parmsMap = JsonUtil.toHashMap(info);
            int transId = 0;
            if (parmsMap.get("TransformationID") != null) {
                transId = Integer.parseInt(parmsMap.get("TransformationID") + "");
            }
            if (transId > 0) {
                adapterFactory = DataAdapterFactory.newInstance();
                adapter3 = DataAdapterFactory.getDefaultDataManager();
                adapter3.openSession();
                ITransformationInputsDao transInputsDao = new TransformationInputsDaoImpl(adapter3);
                List<TransformationInputs> list = transInputsDao.getTransInputsForColumnByTransId(transId);
                outputMap.put("TransformInputs", list);
                return outputMap;
            } else {
                out.setMessageCode(MessageCode.NODATA);
                return outputMap;
            }
        } catch (Exception e) {
            logger.error("[获取getTransformationInputById=========================================exception]", e);
            try {
                if (adapter3 != null) adapter3.rollback();
            } catch (SQLException e1) {
                logger.error("rollback err!", e1);
            }
            out.setMessageCode(MessageCode.SQL_ERROR);
        } finally {
            if (adapter3 != null) adapter3.closeSession();
        }
        return outputMap;
    }

    /**
     * 更新实体transformation对象及inputs列表
     *
     * @param info
     * @param out
     * @return
     */
    public Map<String, Object> updTransformation(String info, ReturnValue out) {
        Map<String, Object> outputMap = new HashMap<String, Object>();
        IRelationalDataManager adapter3 = null;
        DataAdapterFactory adapterFactory = null;
        try {
            Map<String, Object> parmsMap = JsonUtil.toHashMap(info);
            Transformation trans = JsonUtil.toGsonBean(parmsMap.get("Transformation") + "", Transformation.class);
            if (trans != null) {
                adapter3 = DataAdapterFactory.getDefaultDataManager();
                adapter3.openSession();
                ITransformationDao transDao = new TransformationDaoImpl(adapter3);
                IColumnDao columnDao = new ColumnDaoImpl(adapter3);
                ITransformationInputsDao inputsDao = new TransformationInputsDaoImpl(adapter3);
                ITransformationLinkDao linkDao = new TransformationLinkDaoImpl(adapter3);
                Transformation temp = transDao.getObjectById(trans.getId(), Transformation.class);
                if (temp != null) {
                    if (trans.getTranstype() == TransformationTypeEnum.PREDICT.value()
                            && trans.getModel_squid_id() > 0) {
                        Column col = columnDao.getColumnKeyForDataMingSquid(trans.getModel_squid_id());
                        List<TransformationInputs> inputs = inputsDao.getTransInputListByTransId(trans.getId());
                        if (col != null) {
                            if (inputs != null && inputs.size() == 1) {
                                TransformationInputs transInputs = new TransformationInputs();
                                transInputs.setTransformationId(trans.getId());
                                transInputs.setRelative_order(1);
                                transInputs.setSource_transform_id(0);
                                transInputs.setSource_tran_output_index(0);
                                transInputs.setIn_condition("");
                                transInputs.setInput_Data_Type(col.getData_type());
                                transInputs.setDescription("key");
                                transDao.insert2(transInputs);
                            }
                        } else {
                            if (inputs != null && inputs.size() == 2) {
                                for (TransformationInputs transformationInputs : inputs) {
                                    if (transformationInputs.getRelative_order() == 1) {
                                        transDao.delete(transformationInputs.getId(), TransformationInputs.class);
                                    }
                                }
                            }
                        }
                    }
                    if (trans.getTranstype() == TransformationTypeEnum.RULESQUERY.value()
                            && trans.getModel_squid_id() > 0) {
                        Column col = columnDao.getColumnKeyForDataMingSquid(trans.getModel_squid_id());
                        List<TransformationInputs> inputs = inputsDao.getTransInputListByTransId(trans.getId());
                        if (col != null) {
                            if (inputs != null && inputs.size() > 0 && !inputs.get(inputs.size() - 1).getDescription().equals("key")) {
                                TransformationInputs transInputs = new TransformationInputs();
                                transInputs.setTransformationId(trans.getId());
                                transInputs.setRelative_order(inputs.size());
                                transInputs.setSource_transform_id(0);
                                transInputs.setSource_tran_output_index(0);
                                transInputs.setIn_condition("");
                                transInputs.setInput_Data_Type(col.getData_type());
                                transInputs.setDescription("key");
                                transDao.insert2(transInputs);
                            }
                        } else {
                            if (inputs != null && inputs.size() > 0 && inputs.get(inputs.size() - 1).getDescription().equals("key")) {
                                for (TransformationInputs transformationInputs : inputs) {
                                    if (transformationInputs.getRelative_order() == inputs.get(inputs.size() - 1).getRelative_order()) {
                                        transDao.delete(transformationInputs.getId(), TransformationInputs.class);
                                    }
                                }
                            }
                        }
                    }
                    if (trans.getTranstype() == TransformationTypeEnum.INVERSEQUANTIFY.value()
                            && trans.getModel_squid_id() > 0) {
                        Column col = columnDao.getColumnKeyForDataMingSquid(trans.getModel_squid_id());
                        List<TransformationInputs> inputs = inputsDao.getTransInputListByTransId(trans.getId());
                        if (col != null) {
                            if (inputs != null && inputs.size() > 0 && !inputs.get(inputs.size() - 1).getDescription().equals("key")) {
                                TransformationInputs transInputs = new TransformationInputs();
                                transInputs.setTransformationId(trans.getId());
                                transInputs.setRelative_order(inputs.size());
                                transInputs.setSource_transform_id(0);
                                transInputs.setSource_tran_output_index(0);
                                transInputs.setIn_condition("");
                                transInputs.setInput_Data_Type(col.getData_type());
                                transInputs.setDescription("key");
                                transDao.insert2(transInputs);
                            }
                        } else {
                            if (inputs != null && inputs.size() > 0 && inputs.get(inputs.size() - 1).getDescription().equals("key")) {
                                for (TransformationInputs transformationInputs : inputs) {
                                    if (transformationInputs.getRelative_order() == inputs.get(inputs.size() - 1).getRelative_order()) {
                                        transDao.delete(transformationInputs.getId(), TransformationInputs.class);
                                    }
                                }
                            }
                        }
                    }
                    if (trans.getTranstype() == TransformationTypeEnum.INVERSENORMALIZER.value()
                            && trans.getModel_squid_id() > 0) {
                        Column col = columnDao.getColumnKeyForDataMingSquid(trans.getModel_squid_id());
                        List<TransformationInputs> inputs = inputsDao.getTransInputListByTransId(trans.getId());
                        if (col != null) {
                            if (inputs != null && inputs.size() == 1) {
                                TransformationInputs transInputs = new TransformationInputs();
                                transInputs.setTransformationId(trans.getId());
                                transInputs.setRelative_order(1);
                                transInputs.setSource_transform_id(0);
                                transInputs.setSource_tran_output_index(0);
                                transInputs.setIn_condition("");
                                transInputs.setInput_Data_Type(col.getData_type());
                                transInputs.setDescription("key");
                                transDao.insert2(transInputs);
                            }
                        } else {
                            if (inputs != null && inputs.size() == 2) {
                                for (TransformationInputs transformationInputs : inputs) {
                                    if (transformationInputs.getRelative_order() == 1) {
                                        transDao.delete(transformationInputs.getId(), TransformationInputs.class);
                                    }
                                }
                            }
                        }
                    }

                    List<TransformationInputs> inputs = inputsDao.getTransInputsForColumnByTransId(temp.getId());
                    trans.setInputs(inputs);
                    //对于transformation的input的补充
                    transDao.initTransformationInputDataType(trans);
                    //条件只能500
                    if (trans.getTran_condition() != null) {
                        if (trans.getTran_condition().length() > 500) {
                            trans.setTran_condition(trans.getTran_condition().substring(0, 500));
                        }
                    }
                    transDao.update(trans);
                    for (TransformationInputs input : trans.getInputs()) {
                        inputsDao.update(input);
                    }
                    outputMap.put("updTransformation", trans);
                    return outputMap;
                } else {
                    out.setMessageCode(MessageCode.NODATA);
                    return outputMap;
                }
            } else {
                out.setMessageCode(MessageCode.NODATA);
                return outputMap;
            }
        } catch (Exception e) {
            logger.error("[获取updTransformation=========================================exception]", e);
            try {
                adapter3.rollback();
            } catch (SQLException e1) {
                logger.error("rollback err!", e1);
            }
            out.setMessageCode(MessageCode.SQL_ERROR);
        } finally {
            adapter3.closeSession();
        }
        return outputMap;
    }

    public Map<String, Object> updTransformInputs(String info, ReturnValue out) {
        Map<String, Object> outputMap = new HashMap<String, Object>();
        IRelationalDataManager adapter3 = null;
        try {
            Map<String, Object> parmsMap = JsonUtil.toHashMap(info);
            List<TransformationInputs> tranInputs = JsonUtil.toGsonList(parmsMap.get("TransInputs") + "", TransformationInputs.class);
            if (tranInputs != null && tranInputs.size() > 0) {
                adapter3 = DataAdapterFactory.getDefaultDataManager();
                adapter3.openSession();
                ITransformationInputsDao transInputsDao = new TransformationInputsDaoImpl(adapter3);
                for (TransformationInputs transformationInputs : tranInputs) {
                    TransformationInputs tempInput = transInputsDao.getObjectById(transformationInputs.getId(),
                            TransformationInputs.class);
                    if (tempInput != null && tempInput.getId() > 0) {
                        transInputsDao.update(transformationInputs);
                    } else {
                        out.setMessageCode(MessageCode.ERR_DS_TRANSFORMATION_INPUT);
                        return outputMap;
                    }
                }
                //没有返回值
                return outputMap;
            } else {
                out.setMessageCode(MessageCode.NODATA);
                return outputMap;
            }
        } catch (Exception e) {
            logger.error("[获取updTransformation=========================================exception]", e);
            try {
                adapter3.rollback();
            } catch (SQLException e1) {
                logger.error("rollback err!", e1);
            }
            out.setMessageCode(MessageCode.SQL_ERROR);
        } finally {
            adapter3.closeSession();
        }
        return outputMap;
    }

    /**
     * 新增transformation 新接口（需要判断是否初始化input记录）
     *
     * @param info
     * @param out
     * @return
     */
    public Map<String, Object> createTransAndInputs(String info, ReturnValue out) {
        Map<String, Object> outputMap = new HashMap<String, Object>();
        IRelationalDataManager adapter3 = null;
        try {
            Map<String, Object> parmsMap = JsonUtil.toHashMap(info);
            Transformation trans = JsonUtil.toGsonBean(parmsMap.get("Transformation") + "", Transformation.class);
            if (trans != null) {
                adapter3 = DataAdapterFactory.getDefaultDataManager();
                adapter3.openSession();
                ITransformationDao transDao = new TransformationDaoImpl(adapter3);
                ITransformationInputsDao transInputsDao = new TransformationInputsDaoImpl(adapter3);
                int transTypeid = TransformationTypeEnum.valueOf(trans.getTranstype()).value();
                int output_data_type = transDao.getOutputDataTypeByTransType(transTypeid);
                if (output_data_type != -1) {
                    trans.setOutput_data_type(output_data_type);
                }
                trans.setTranstype(transTypeid);
                int newId = transDao.insert2(trans);
                if (newId > 0) {
                    trans.setId(newId);
                    /*List<TransformationInputs> inputsList = new ArrayList<TransformationInputs>();
                    if (TransformationTypeEnum.isNotTransInputs(transTypeid)) {
                    List<Map<String, Object>> inputsMap = SquidLinkService.Inputscache.get(transTypeid + "");
                    if (inputsMap != null && inputsMap.size() > 0) {
                        for (Map<String, Object> map : inputsMap) {
                            //开始组装TransInputs
                            TransformationInputs transformationInputs = new TransformationInputs();
                            transformationInputs.setInput_Data_Type(Integer.parseInt(map.get("inputDataType").toString()));
                            transformationInputs.setTransformationId(trans.getId());
                            String description = map.get("description").toString();
                            String relative_order = map.get("inputOrder").toString();
                            transformationInputs.setDescription(description);
                            transformationInputs.setRelative_order(Integer.parseInt(relative_order));
                            transformationInputs.setId(transInputsDao.insert2(transformationInputs));
                            inputsList.add(transformationInputs);
                        }
                    }
                }*/
                    outputMap.put("TransformInputs", transInputsDao.initTransInputs(trans, 0));
                }
                outputMap.put("TransformationId", newId);
                return outputMap;
            } else {
                out.setMessageCode(MessageCode.NODATA);
                return outputMap;
            }
        } catch (Exception e) {
            logger.error("[获取createTransAndInputs=========================================exception]", e);
            try {
                adapter3.rollback();
            } catch (SQLException e1) {
                logger.error("rollback err!", e1);
            }
            out.setMessageCode(MessageCode.SQL_ERROR);
        } finally {
            adapter3.closeSession();
        }
        return outputMap;
    }

    /**
     * 拖动transformation打断transformationlink业务处理类  新接口（需要判断是否初始化input记录）
     *
     * @param info
     * @param out
     * @return
     */
    public synchronized Map<String, Object> drapTrans2InputsAndLink(String info, ReturnValue out) {
        Map<String, Object> outputMap = new HashMap<String, Object>();
        IRelationalDataManager adapter3 = null;
        int TransformationId = 0;//transformation的id
        //返回信息
        try {
            Map<String, Object> parmsMap = JsonUtil.toHashMap(info);
            Transformation transformation = JsonUtil.toGsonBean(parmsMap.get("Transformation") + "", Transformation.class);
            // 先记录传过来links的from_transformation_id 和to_transformation_id
            int linkId = Integer.parseInt(parmsMap.get("TransformLinkId") + "");
            if (transformation != null && linkId > 0) {
                adapter3 = DataAdapterFactory.getDefaultDataManager();
                adapter3.openSession();

                ITransformationLinkDao transLinkDao = new TransformationLinkDaoImpl(adapter3);
                ITransformationDao transDao = new TransformationDaoImpl(adapter3);
                ITransformationInputsDao transInputDao = new TransformationInputsDaoImpl(adapter3);

                TransformationLink transformationLinks = transLinkDao.getObjectById(linkId, TransformationLink.class);
                if (transformationLinks == null) {
                    out.setMessageCode(MessageCode.NODATA);
                    return outputMap;
                }
                int from_transformation_id = transformationLinks.getFrom_transformation_id();
                int to_transformation_id = transformationLinks.getTo_transformation_id();
                Integer source_output_index = 0;
                Map<String, Integer> indexMap = new HashMap<String, Integer>();
                // 然后删除link
                boolean flag = transLinkDao.delete(transformationLinks.getId(),
                        TransformationLink.class) >= 0 ? true : false;
                if (flag) {
                    // 更新trans_Inputs
                    transInputDao.resetTransformationInput(from_transformation_id, to_transformation_id, indexMap);
                    if (indexMap.containsKey("index")) {
                        source_output_index = indexMap.get("index");
                    }
                }
                // 如果传过来的transformation有id,则不需要进行新增
                if (0 != transformation.getId()) {
                    TransformationId = transformation.getId();
                } else {
                    // 新增transformation(返回id)
                    int transTypeId = TransformationTypeEnum.valueOf(transformation.getTranstype()).value();
                    int output_data_type = transDao.getOutputDataTypeByTransType(transTypeId);
                    TransformationService service = new TransformationService(TokenUtil.getToken());
                    transformation = service.initTransformation(adapter3, transformation.getSquid_id(),
                            transformation.getName(), transformation.getColumn_id(), transTypeId,
                            output_data_type,
                            //判断是否大于0，否则设置为默认值1  取消(2015-03-19)
                            transformation.getOutput_number(), 0);
                    TransformationId = transformation.getId();
                }
                TransformationLink transformationLinkFrom = new TransformationLink();
                TransformationLink transformationLinkTo = new TransformationLink();
                // 给对象赋值
                this.setTransfromationLink(transformationLinkFrom, from_transformation_id, TransformationId);
                this.setTransfromationLink(transformationLinkTo, TransformationId, to_transformation_id);
                // 创建新的links
                int fromLinkId = transLinkDao.insert2(transformationLinkFrom);
                if (flag) {
                    // 更新trans_Inputs
                    transformationLinkFrom.setSource_input_index(source_output_index);
                    flag = transInputDao.updTransInputs(transformationLinkFrom, null);
                    if (flag) {
                        transformationLinkFrom.setId(fromLinkId);
                        int toLinkId = adapter3.insert2(transformationLinkTo);
                        // 更新trans_Inputs
                        flag = transInputDao.updTransInputs(transformationLinkTo, null);
                        if (flag) {
                            transformationLinkTo.setId(toLinkId);
                            outputMap.put("TransformationId", TransformationId);
                            outputMap.put("TransformLinkFrom", transformationLinkFrom);
                            outputMap.put("TransformLinkTo", transformationLinkTo);
                            List<TransformationInputs> formLists = transInputDao.getTransInputsForColumnByTransId(transformationLinkFrom.getTo_transformation_id());
                            outputMap.put("TransformInputs", formLists);
                            return outputMap;
                        }
                    }
                }
                if (!flag) {
                    adapter3.rollback();
                    out.setMessageCode(MessageCode.ERR_TRANSFORMATION_INPUT_TYPE);
                    return outputMap;
                }
            } else {
                out.setMessageCode(MessageCode.NODATA);
                return outputMap;
            }
        } catch (Exception e) {
            logger.error("[获取drapTrans2InputsAndLink=========================================exception]", e);
            try {
                adapter3.rollback();
            } catch (SQLException e1) {
                logger.error("rollback err!", e1);
            }
            out.setMessageCode(MessageCode.SQL_ERROR);
        } finally {
            if (adapter3 != null) {
                adapter3.closeSession();
            }
        }
        return outputMap;
    }

    /**
     * 对transformationLink对象的赋值
     *
     * @param transformationLink
     * @param from_id
     * @param to_id
     */
    private void setTransfromationLink(TransformationLink transformationLink,
                                       int from_id, int to_id) {
        transformationLink.setFrom_transformation_id(from_id);
        transformationLink.setTo_transformation_id(to_id);
        transformationLink.setKey(StringUtils.generateGUID());
        transformationLink.setIn_order(1);
    }

    /**
     * 新疆Transform
     *
     * @param adapter3
     * @param newSquidId
     * @param column_id
     * @param column_data_Type
     * @return
     * @throws Exception
     */
    public Transformation initTransformation(IRelationalDataManager adapter3,
                                             int newSquidId, int column_id, int transTypeId,
                                             int column_data_Type, int output_number) throws Exception {
        // 创建新增列对应的虚拟变换
        return initTransformation(adapter3, newSquidId, column_id, transTypeId, column_data_Type, output_number, 0);
/*		Transformation newTrans = new Transformation();
        newTrans.setColumn_id(column_id);
		newTrans.setKey(StringUtils.generateGUID());
		newTrans.setSquid_id(newSquidId);
		newTrans.setTranstype(transTypeId);
		newTrans.setOutput_data_type(column_data_Type);
		newTrans.setOutput_number(output_number);
		newTrans.setId(adapter3.insert2(newTrans));
		newTrans.setInputs(initTransInputs(adapter3, newTrans.getId()));
		return newTrans;*/
    }

    /**
     * 新建Transform（带坐标）
     *
     * @param adapter3
     * @param newSquidId       transformation所属squid id
     * @param transTypeId      tran类型
     * @param column_id        如果tran为virt,则需要指明column_id
     * @param column_data_Type transformation输出类型
     * @param output_number    transformation输出数量
     * @param index            坐标使用，现已无用
     * @return
     * @throws Exception
     */
    public Transformation initTransformation(IRelationalDataManager adapter3,
                                             int newSquidId, int column_id, int transTypeId,
                                             int column_data_Type, int output_number, int index) throws Exception {
        // 创建新增列对应的虚拟变换
        ITransformationDao transformationDao = new TransformationDaoImpl(adapter3);
        ITransformationInputsDao transInputsDao = new TransformationInputsDaoImpl(adapter3);
        Transformation newTrans = new Transformation();
        if (transTypeId == TransformationTypeEnum.VIRTUAL.value()) {
            newTrans.setColumn_id(column_id);
        }
        newTrans.setKey(StringUtils.generateGUID());
        newTrans.setSquid_id(newSquidId);
        newTrans.setTranstype(transTypeId);
        newTrans.setOutput_data_type(column_data_Type);
        newTrans.setOutput_number(output_number);
//        if (index>0){
//            newTrans.setLocation_x(0);
//            newTrans.setLocation_y((index) * 25 + 25 / 2);
//        }
        newTrans.setId(transformationDao.insert2(newTrans));
       /* List<TransformationInputs> inputsList=new ArrayList<>();
        if (transTypeId != TransformationTypeEnum.CONCATENATE.value()
                &&transTypeId != TransformationTypeEnum.CHOICE.value()
                && transTypeId != TransformationTypeEnum.CSVASSEMBLE.value()
                && transTypeId != TransformationTypeEnum.NUMASSEMBLE.value()) {
            List<Map<String, Object>> inputsMap = SquidLinkService.Inputscache.get(transTypeId + "");
            if (inputsMap != null && inputsMap.size() > 0) {
                for (Map<String, Object> map : inputsMap) {
                    //开始组装TransInputs
                    TransformationInputs transformationInputs = new TransformationInputs();
                    if(column_data_Type!=0){
                        transformationInputs.setInput_Data_Type(column_data_Type);
                    }else {
                        transformationInputs.setInput_Data_Type(Integer.parseInt(map.get("inputDataType").toString()));
                    }
                    transformationInputs.setTransformationId(newTrans.getId());
                    String description = map.get("description").toString();
                    String relative_order = map.get("inputOrder").toString();
                    transformationInputs.setDescription(description);
                    transformationInputs.setRelative_order(Integer.parseInt(relative_order));
                    transformationInputs.setId(transInputsDao.insert2(transformationInputs));
                    inputsList.add(transformationInputs);
                }
            }
        }
       newTrans.setInputs(inputsList);*/
        newTrans.setInputs(transInputsDao.initTransInputs(newTrans, column_data_Type));
        return newTrans;
    }

    /**
     * 新建Transform（带坐标）
     *
     * @param adapter3
     * @param newSquidId
     * @param column_id
     * @param column_data_Type
     * @return
     * @throws Exception
     */
    public Transformation initTransformation(IRelationalDataManager adapter3,
                                             int newSquidId, String newName, int column_id, int transTypeId,
                                             int column_data_Type, int output_number, int index) throws Exception {
        // 创建新增列对应的虚拟变换
        ITransformationDao transformationDao = new TransformationDaoImpl(adapter3);
        ITransformationInputsDao transInputsDao = new TransformationInputsDaoImpl(adapter3);
        Transformation newTrans = new Transformation();
        if (transTypeId == TransformationTypeEnum.VIRTUAL.value()) {
            newTrans.setColumn_id(column_id);
        }
        newTrans.setName(newName);
        newTrans.setKey(StringUtils.generateGUID());
        newTrans.setSquid_id(newSquidId);
        newTrans.setTranstype(transTypeId);
        newTrans.setOutput_data_type(column_data_Type);
        newTrans.setOutput_number(output_number);
        if (index > 0) {
            newTrans.setLocation_x(0);
            newTrans.setLocation_y((index) * 25 + 25 / 2);
        }
        newTrans.setId(transformationDao.insert2(newTrans));
        newTrans.setInputs(transInputsDao.initTransInputs(newTrans, column_data_Type));
        return newTrans;
    }

    /**
     * 新建Transform（带坐标）
     *
     * @param adapter3
     * @param newSquidId
     * @param column_id
     * @param column_data_Type
     * @return
     * @throws Exception
     */
    public Transformation mergeTransformation(IRelationalDataManager adapter3,
                                              int newSquidId, int column_id, int transTypeId,
                                              int column_data_Type, int output_number, int index) throws Exception {
        ITransformationDao transformationDao = new TransformationDaoImpl(adapter3);
        ITransformationInputsDao transInputsDao = new TransformationInputsDaoImpl(adapter3);
        Transformation newTrans = transformationDao.getTransformationById(newSquidId, column_id);
        if (StringUtils.isNull(newTrans)) {
            // 创建新增列对应的虚拟变换
            newTrans = new Transformation();
            if (transTypeId == TransformationTypeEnum.VIRTUAL.value()) {
                newTrans.setColumn_id(column_id);
            }
            newTrans.setKey(StringUtils.generateGUID());
            newTrans.setSquid_id(newSquidId);
            newTrans.setTranstype(transTypeId);
            newTrans.setOutput_data_type(column_data_Type);
            newTrans.setOutput_number(output_number);
            if (index > 0) {
                newTrans.setLocation_x(0);
                newTrans.setLocation_y((index) * 25 + 25 / 2);
            }
            newTrans.setId(transformationDao.insert2(newTrans));
            newTrans.setInputs(transInputsDao.initTransInputs(newTrans, column_data_Type));
        } else {
            newTrans.setOutput_data_type(column_data_Type);
            newTrans.setOutput_number(output_number);
            if (index > 0) {
                newTrans.setLocation_x(0);
                newTrans.setLocation_y((index) * 25 + 25 / 2);
            }
            transformationDao.update(newTrans);
            List<TransformationInputs> tempInputsList = transInputsDao.getTransInputListByTransId(newTrans.getId());
            if (StringUtils.isNotNull(tempInputsList)) {
                for (int i = 0; i < tempInputsList.size(); i++) {
                    tempInputsList.get(i).setInput_Data_Type(column_data_Type);
                    transInputsDao.update(tempInputsList.get(i));
                }
            }
            newTrans.setInputs(tempInputsList);
        }
        return newTrans;
    }

    /**
     * 新增Column对象
     *
     * @param adapter3
     * @param columnInfo
     * @param order
     * @param toSquidId
     * @return
     * @throws SQLException
     */
    public Column initColumn(IRelationalDataManager adapter3,
                             Column columnInfo, int order,
                             int toSquidId, Map<String, Object> nameColumn) throws SQLException {
        // 目标ExtractSquid真实列（变换面板左边）
        String newName = null;
        if (StringUtils.isNotNull(nameColumn)) {
            newName = getColumnName(nameColumn, columnInfo.getName());
        } else {
            newName = columnInfo.getName();
        }
        Column newColumn = new Column();
        newColumn.setCollation(0);
        //newColumn.setData_type(DataTypeManager.decode(columnInfo.getData_type()));
        newColumn.setData_type(columnInfo.getData_type());
        newColumn.setLength(columnInfo.getLength());
        newColumn.setPrecision(columnInfo.getPrecision());
        newColumn.setScale(columnInfo.getScale());
        newColumn.setKey(StringUtils.generateGUID());
        //目标列中文转英文
        if (StringUtils.isHavaChinese(newName)) {
            newColumn.setName("col_" + order);
        } else {
            newColumn.setName(newName);
        }
        newColumn.setNullable(columnInfo.isNullable());
        newColumn.setIsPK(columnInfo.isIsPK());
        if (columnInfo.isIsPK()) {
            newColumn.setNullable(false);
        }
        newColumn.setIsUnique(columnInfo.isIsUnique());
        newColumn.setIs_Business_Key(columnInfo.getIs_Business_Key());
        newColumn.setRelative_order(order);
        newColumn.setAggregation_type(columnInfo.getAggregation_type());
        newColumn.setCdc(columnInfo.getCdc());
        newColumn.setIs_groupby(columnInfo.isIs_groupby());
        newColumn.setDescription(columnInfo.getDescription());
        newColumn.setSquid_id(toSquidId);
        newColumn.setSort_Level(columnInfo.getSort_Level());
        newColumn.setSort_type(columnInfo.getSort_type());
        newColumn.setId(adapter3.insert2(newColumn));
        return newColumn;
    }

    /**
     * 添加新的column
     *
     * @param adapter3
     * @param columnName
     * @param dataType
     * @param order
     * @param toSquidId
     * @return
     * @throws SQLException
     */
    public Column initColumn(IRelationalDataManager adapter3,
                             String columnName, int dataType, int length, int order,
                             int toSquidId) throws Exception {
        // 目标ExtractSquid真实列（变换面板左边）
        IColumnDao columnDao = new ColumnDaoImpl(adapter3);
        Column newColumn = new Column();
        newColumn.setCollation(0);
        //newColumn.setData_type(DataTypeManager.decode(columnInfo.getData_type()));
        newColumn.setData_type(dataType);
        newColumn.setLength(length);
        newColumn.setPrecision(0);
        newColumn.setScale(0);
        newColumn.setKey(StringUtils.generateGUID());
        newColumn.setName(columnName);
        newColumn.setNullable(true);
        newColumn.setIsPK(false);
        newColumn.setIsUnique(false);
        newColumn.setIs_Business_Key(0);
        newColumn.setRelative_order(order);
        newColumn.setAggregation_type(0);
        newColumn.setCdc(0);
        newColumn.setIs_groupby(false);
        newColumn.setDescription("");
        newColumn.setSquid_id(toSquidId);

        List ColumnLists = columnDao.getColumnListBySquidId(toSquidId);
        List ColLists = new ArrayList();
        for (int i = 0; i < ColumnLists.size(); i++) {
            Column col = (Column) ColumnLists.get(i);
            ColLists.add(col.getName());
        }
        if (ColLists.contains(newColumn.getName())) {
        } else {
            newColumn.setId(columnDao.insert2(newColumn));
        }
        return newColumn;
    }

    /**
     * 新增Column对象
     *
     * @param adapter3
     * @param columnInfo
     * @param order
     * @param toSquidId
     * @return
     * @throws SQLException
     */
    public Column initColumn2(IRelationalDataManager adapter3,
                              ReferenceColumn columnInfo, int order,
                              int toSquidId, Map<Integer, String> nameColumn) throws SQLException {
        // 目标ExtractSquid真实列（变换面板左边）
        String newName = null;
        if (StringUtils.isNotNull(nameColumn)) {
            newName = nameColumn.get(columnInfo.getHost_squid_id()) + "_" + columnInfo.getName();
        } else {
            newName = columnInfo.getName();
        }
        Column newColumn = new Column();
        newColumn.setCollation(0);
        //newColumn.setData_type(DataTypeManager.decode(columnInfo.getData_type()));
        newColumn.setData_type(columnInfo.getData_type());
        newColumn.setLength(columnInfo.getLength());
        newColumn.setPrecision(columnInfo.getPrecision());
        newColumn.setScale(columnInfo.getScale());
        newColumn.setKey(StringUtils.generateGUID());
        if (StringUtils.isHavaChinese(newName)) {
            newName = "col_" + order;
        }
        newColumn.setName(newName);
        newColumn.setNullable(columnInfo.isNullable());
        newColumn.setIsPK(columnInfo.isIsPK());
        newColumn.setIsUnique(columnInfo.isIsUnique());
        newColumn.setIs_Business_Key(columnInfo.getIs_Business_Key());
        newColumn.setRelative_order(order);
        newColumn.setAggregation_type(columnInfo.getAggregation_type());
        newColumn.setCdc(columnInfo.getCdc());
        newColumn.setIs_groupby(columnInfo.isIs_groupby());
        newColumn.setDescription(columnInfo.getDescription());
        newColumn.setSquid_id(toSquidId);
        newColumn.setId(adapter3.insert2(newColumn));
        return newColumn;
    }

    /**
     * 新增Column对象,并且对名称进行替换处理
     *
     * @param adapter3
     * @param columnInfo
     * @param order
     * @param toSquidId
     * @return
     * @throws SQLException
     */
    public Column initColumn3(IRelationalDataManager adapter3,
                              ReferenceColumn columnInfo, int order,
                              int toSquidId, Map<Integer, String> nameColumn) throws SQLException {
        // 目标ExtractSquid真实列（变换面板左边）
        String newName = null;
        if (StringUtils.isNotNull(nameColumn)) {
            newName = nameColumn.get(columnInfo.getHost_squid_id()) + "_" + columnInfo.getName();
        } else {
            newName = StringUtils.replace(columnInfo.getName());
        }
        Column newColumn = new Column();
        newColumn.setCollation(0);
        //newColumn.setData_type(DataTypeManager.decode(columnInfo.getData_type()));
        newColumn.setData_type(columnInfo.getData_type());
        newColumn.setLength(columnInfo.getLength());
        newColumn.setPrecision(columnInfo.getPrecision());
        newColumn.setScale(columnInfo.getScale());
        newColumn.setKey(StringUtils.generateGUID());
        newColumn.setName(newName);
        newColumn.setNullable(columnInfo.isNullable());
        newColumn.setIsPK(columnInfo.isIsPK());
        newColumn.setIsUnique(columnInfo.isIsUnique());
        newColumn.setIs_Business_Key(columnInfo.getIs_Business_Key());
        newColumn.setRelative_order(order);
        newColumn.setAggregation_type(columnInfo.getAggregation_type());
        newColumn.setCdc(columnInfo.getCdc());
        newColumn.setIs_groupby(columnInfo.isIs_groupby());
        newColumn.setDescription(columnInfo.getDescription());
        newColumn.setSquid_id(toSquidId);
        newColumn.setId(adapter3.insert2(newColumn));
        return newColumn;
    }

    /**
     * 新增Column对象
     *
     * @param adapter3
     * @param order
     * @param toSquidId
     * @return
     * @throws SQLException
     * @author bo.dang
     */
    public Column initColumn(IRelationalDataManager adapter3,
                                 SourceColumn sourceColumn, int order,
                             int toSquidId, Map<String, Object> nameColumn) throws SQLException {
        // 目标ExtractSquid真实列（变换面板左边）
        String newName = null;
        if (StringUtils.isNotNull(nameColumn)) {
            newName = getColumnName(nameColumn, sourceColumn.getName());
        } else {
            newName = sourceColumn.getName();

        }
        //防止替换特殊字符后，出现name为null的情况
        if (StringUtils.isNull(newName)) {
            newName = "defaultName";
            //判断是否有重复名字
            newName = getColumnName(nameColumn, newName);
        }
        Column newColumn = new Column();
        newColumn.setCollation(0);
//      newColumn.setData_type(DataTypeManager.decode(sourceColumn.getData_type()));
        //全部转化成NVARCHAR
        //newColumn.setData_type(SystemDatatype.NVARCHAR.value());
        //如果是datetime类型转化成NVARCHAR类型，增加判断必须是上游必须是mongodb
        Map<String,String> param = new Hashtable<>();
        param.put("id",toSquidId+"");
        Squid squid = adapter3.query2Object2(true,param,Squid.class);

        newColumn.setLength(sourceColumn.getLength());
        if((SystemDatatype.parse(sourceColumn.getData_type()) == SystemDatatype.DATETIME
                || SystemDatatype.parse(sourceColumn.getData_type()) == SystemDatatype.DATE)
                && SquidTypeEnum.parse(squid.getSquid_type()) == SquidTypeEnum.MONGODBEXTRACT){
            newColumn.setData_type(SystemDatatype.NVARCHAR.value());
            newColumn.setLength(255);
        }else {
            newColumn.setData_type(sourceColumn.getData_type());
        }
        newColumn.setPrecision(sourceColumn.getPrecision());
        newColumn.setScale(sourceColumn.getScale());
        newColumn.setKey(StringUtils.generateGUID());
        newColumn.setName(newName);
        newColumn.setNullable(sourceColumn.isNullable());
        newColumn.setIsPK(sourceColumn.isIspk());
        if (sourceColumn.isIspk()) {
            newColumn.setNullable(false);
        }
        newColumn.setIsUnique(sourceColumn.isIsunique());
        newColumn.setRelative_order(order);
        newColumn.setSquid_id(toSquidId);
        // fixed bug 905 start by bo.dang
        newColumn.setIs_groupby(false);
        newColumn.setAggregation_type(-1);

        //目标列去换行
        if (StringUtils.isNotNull(newColumn.getName())) {
            if (newColumn.getName().contains("\n")) {
                newColumn.setName(newColumn.getName().replaceAll("\n", ""));
            } else {
                newColumn.setName(newColumn.getName().replaceAll("\r\n", ""));
            }
            //目标列中文转英文
            if (StringUtils.isHavaChinese(newColumn.getName())) {
                newColumn.setName("col_" + order);
                //判断名字是否重复
                newColumn.setName(getColumnName(nameColumn, newColumn.getName()));
            }
        }


        // fixed bug 905 end by bo.dang
        int n = adapter3.insert2(newColumn);
        newColumn.setId(n);
        return newColumn;
    }

    /**
     * Merge
     *
     * @param adapter3
     * @param sourceColumn
     * @param order
     * @param toSquidId
     * @param nameColumn
     * @return
     * @throws SQLException
     * @author bo.dang
     * @date 2014年6月23日
     */
    public Column mergeColumn(IRelationalDataManager adapter3,
                              Column sourceColumn, int order,
                              int toSquidId, Map<String, Object> nameColumn) throws SQLException {
        // 目标ExtractSquid真实列（变换面板左边）
        String newName = null;
        if (StringUtils.isNotNull(nameColumn)) {
            newName = getColumnName(nameColumn, sourceColumn.getName());
        } else {
            newName = sourceColumn.getName();
        }
        Column newColumn = null;
        Map<String, String> paramsMap = new HashMap<String, String>();
        // 目标ExtractSquid真实列（变换面板左边）
        paramsMap.put("name", sourceColumn.getName());
        paramsMap.put("squid_id", Integer.toString(toSquidId, 10));
        newColumn = adapter3.query2Object2(true, paramsMap, Column.class);
        if (StringUtils.isNotNull(newColumn)) {
            adapter3.update2(newColumn);
        } else {
            newColumn = new Column();
            newColumn.setCollation(0);
//        newColumn.setData_type(DataTypeManager.decode(sourceColumn.getData_type()));
            newColumn.setData_type(sourceColumn.getData_type());
            newColumn.setLength(sourceColumn.getLength());
            newColumn.setPrecision(sourceColumn.getPrecision());
            newColumn.setScale(sourceColumn.getScale());
            newColumn.setKey(StringUtils.generateGUID());
            newColumn.setName(newName);
            newColumn.setNullable(sourceColumn.isNullable());
            newColumn.setIsPK(sourceColumn.isIsPK());
            newColumn.setIsUnique(sourceColumn.isIsUnique());
            newColumn.setIs_Business_Key(sourceColumn.getIs_Business_Key());
            newColumn.setAggregation_type(sourceColumn.getAggregation_type());
            newColumn.setCdc(sourceColumn.getCdc());
            newColumn.setIs_groupby(sourceColumn.isIs_groupby());
            newColumn.setDescription(sourceColumn.getDescription());
            newColumn.setRelative_order(order);
            newColumn.setSquid_id(toSquidId);
            newColumn.setId(adapter3.insert2(newColumn));
        }
        return newColumn;
    }

    /**
     * @param adapter3
     * @param sourceColumn
     * @param order
     * @param toSquidId
     * @param nameColumn
     * @return
     * @throws SQLException
     * @author bo.dang
     * @date 2014年6月23日
     */
    public Column mergeColumn(IRelationalDataManager adapter3,
                              SourceColumn sourceColumn, int order,
                              int toSquidId, Map<String, Object> nameColumn) throws SQLException {
        // 目标ExtractSquid真实列（变换面板左边）
        String newName = null;
        if (StringUtils.isNotNull(nameColumn)) {
            newName = getColumnName(nameColumn, sourceColumn.getName());
        } else {
            newName = sourceColumn.getName();
        }
        Column newColumn = null;
        Map<String, String> paramsMap = new HashMap<String, String>();
        // 目标ExtractSquid真实列（变换面板左边）
        paramsMap.put("name", sourceColumn.getName());
        paramsMap.put("squid_id", Integer.toString(toSquidId, 10));
        newColumn = adapter3.query2Object2(true, paramsMap, Column.class);
        if (StringUtils.isNotNull(newColumn)) {
            adapter3.update2(newColumn);
        } else {
            newColumn = new Column();
            newColumn.setCollation(0);
//        newColumn.setData_type(DataTypeManager.decode(sourceColumn.getData_type()));
            newColumn.setData_type(sourceColumn.getData_type());
            newColumn.setLength(sourceColumn.getLength());
            newColumn.setPrecision(sourceColumn.getPrecision());
            newColumn.setKey(StringUtils.generateGUID());
            newColumn.setName(newName);
            newColumn.setNullable(sourceColumn.isNullable());
            newColumn.setIsPK(sourceColumn.isIspk());
            newColumn.setIsUnique(sourceColumn.isIsunique());
            newColumn.setRelative_order(order);
            newColumn.setSquid_id(toSquidId);
            // fixed bug 905 start by bo.dang
            newColumn.setIs_groupby(false);
            newColumn.setAggregation_type(-1);
            // fixed bug 905 end by bo.dang
            newColumn.setId(adapter3.insert2(newColumn));
        }
        return newColumn;
    }

    /**
     * 添加自定义的Column
     *
     * @param adapter3
     * @param index
     * @param entry
     * @param squidId
     * @return
     * @throws SQLException
     * @author yi.zhou
     */
    public Column initColumn(IRelationalDataManager adapter3, int index,
                             Entry<String, Integer> entry, int squidId) throws SQLException {
        Column newColumn = new Column();
        newColumn.setCollation(0);
        newColumn.setData_type(entry.getValue());
        if (entry.getKey().toLowerCase().equals("model")
                || entry.getKey().toLowerCase().equals("trees")) {
            newColumn.setLength(9999);
        }
        if (entry.getKey().toLowerCase().equals("uid")) {
            newColumn.setLength(64);
        }
        //newColumn.setLength(columnInfo.getLength());
        //newColumn.setPrecision(columnInfo.getPrecision());
        newColumn.setKey(StringUtils.generateGUID());
        newColumn.setName(entry.getKey());
        newColumn.setNullable(false);
        newColumn.setIsUnique(false);
        newColumn.setCdc(0);
        newColumn.setRelative_order(index);
        newColumn.setSquid_id(squidId);
        return newColumn;
    }

    /**
     * initSourceColum
     * 2014-12-5
     *
     * @param dataType
     * @param length
     * @param precision
     * @param name
     * @param nullable
     * @param isPk
     * @param isUnique
     * @return
     * @author Akachi
     * @E-Mail zsts@hotmail.com
     */
    public SourceColumn initSourceColumn(SystemDatatype dataType, Integer length, int precision, String name, boolean nullable, boolean isPk, boolean isUnique) {
        SourceColumn source = new SourceColumn();
        source.setData_type(dataType.value());
        source.setLength(length);
        source.setPrecision(precision);
        source.setName(name);
        source.setNullable(nullable);
        source.setIspk(isPk);
        source.setIsunique(isUnique);
        return source;
    }

    /**
     * 设置重复名称后自动创建序号
     *
     * @param nameColumn
     * @param ColumnName
     * @return
     */
    public String getColumnName(Map<String, Object> nameColumn, String ColumnName) {
        String tempName = ColumnName;
        int cnt = 0;
        while (true) {
            if (nameColumn.containsKey(tempName.toLowerCase())) {
                tempName = ColumnName + cnt;
            } else {
                nameColumn.put(tempName.toLowerCase(), tempName);
                return tempName;
            }
            cnt++;
        }
    }

    /**
     * 在创建link，生成对应的Tranformation获取到数据后在添加link数据
     *
     * @param column
     * @param referenceColumn
     * @param tmpIndex
     * @param adapter3
     * @param transformations
     * @param transformationLinks
     * @throws Exception
     */
    public void initLinkByColumn(Column column, ReferenceColumn referenceColumn, int tmpIndex,
                                 int squidId, IRelationalDataManager adapter3,
                                 List<Transformation> transformations,
                                 List<TransformationLink> transformationLinks) throws Exception {
        // 新增Transformation
        ITransformationDao transDao = new TransformationDaoImpl(adapter3);
        ITransformationInputsDao transInputsDao = new TransformationInputsDaoImpl(adapter3);
        int tmpFromTransId;
        // 创建目标 transformation
        Transformation newTrans = this.initTransformation(adapter3, squidId,
                column.getId(), TransformationTypeEnum.VIRTUAL.value(),
                column.getData_type(), 1);
        transformations.add(newTrans);
        int tmpToTransId = newTrans.getId();
        Transformation tmpTrans = transDao.getTransformationById(squidId,
                referenceColumn.getColumn_id());
        if (StringUtils.isNull(tmpTrans)) {
            // 创建来源 transformation
            newTrans = this.initTransformation(adapter3, squidId,
                    referenceColumn.getColumn_id(), TransformationTypeEnum.VIRTUAL.value(),
                    referenceColumn.getData_type(), 1);
            transformations.add(newTrans);
            tmpFromTransId = newTrans.getId();
        } else {
            tmpFromTransId = tmpTrans.getId();
        }
        // 创建 transformation link
        TransformationLink transLink = new TransformationLink();
        transLink.setIn_order(tmpIndex + 1);
        transLink.setFrom_transformation_id(tmpFromTransId);
        transLink.setTo_transformation_id(tmpToTransId);
        transLink.setKey(StringUtils.generateGUID());
        transLink.setId(adapter3.insert2(transLink));
        //更新SourceTrans
        transInputsDao.updTransInputs(transLink, null);
        transformationLinks.add(transLink);
    }

    /**
     * 创建TransformationLink
     *
     * @param adapter2
     * @param transformationFromId
     * @param transformationToId
     * @param order
     * @return
     * @throws Exception
     * @author bo.dang
     * @date 2014年5月24日
     */
    public TransformationLink initTransformationLink(IRelationalDataManager adapter2, int transformationFromId, int transformationToId, int order) throws Exception {
        TransformationLink transformationLink = new TransformationLink();
        transformationLink.setIn_order(order);
        transformationLink.setFrom_transformation_id(transformationFromId);
        transformationLink.setTo_transformation_id(transformationToId);
        transformationLink.setKey(StringUtils.generateGUID());
        transformationLink.setId(adapter2.insert2(transformationLink));
        return transformationLink;
    }

    /**
     * 创建TransformationLink
     *
     * @param adapter2
     * @param transformationFromId
     * @param transformationToId
     * @param order
     * @return
     * @throws Exception
     * @author bo.dang
     * @date 2014年5月24日
     */
    public TransformationLink mergeTransformationLink(IRelationalDataManager adapter2, int transformationFromId, int transformationToId, int order) throws Exception {
        Map<String, String> paramsMap = new HashMap<String, String>();
        paramsMap.put("from_transformation_id", Integer.toString(transformationFromId, 10));
        paramsMap.put("to_transformation_id", Integer.toString(transformationToId, 10));
        TransformationLink transformationLink = adapter2.query2Object2(true, paramsMap, TransformationLink.class);
        if (StringUtils.isNotNull(transformationLink)) {
            adapter2.update2(transformationLink);
        } else {
            transformationLink = new TransformationLink();
            transformationLink.setIn_order(order);
            transformationLink.setFrom_transformation_id(transformationFromId);
            transformationLink.setTo_transformation_id(transformationToId);
            transformationLink.setKey(StringUtils.generateGUID());
            transformationLink.setId(adapter2.insert2(transformationLink));
        }
        return transformationLink;
    }

    /**
     * 修改Trans坐标
     *
     * @param info
     * @param out
     * @return
     */
    public Map<String, Object> updateTransLocations(String info, ReturnValue out) {
        Map<String, Object> outputMap = new HashMap<String, Object>();
        IRelationalDataManager adapter3 = null;
        DataAdapterFactory adapterFactory = null;
        try {
            Map<String, Object> inputMap = JsonUtil.toHashMap(info);
            List<CommonLocationInfo> list = JsonUtil.toGsonList(inputMap.get("Positions") + "", CommonLocationInfo.class);
            if (list != null && list.size() > 0) {
                adapterFactory = DataAdapterFactory.newInstance();
                adapter3 = DataAdapterFactory.getDefaultDataManager();
                adapter3.openSession();
                for (CommonLocationInfo com : list) {
                    if (com.getId() > 0) {
                        String sql = "update ds_transformation set location_x=" + com.getX() + ",location_y=" + com.getY() + " where id=" + com.getId();
                        adapter3.execute(sql);
                    }
                }
                return outputMap;
            } else {
                out.setMessageCode(MessageCode.NODATA);
            }
        } catch (Exception e) {
            try {
                if (adapter3 != null) {
                    adapter3.rollback();
                }
            } catch (SQLException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            logger.error("updateTransLocations error, Message：" + e.getMessage());
            out.setMessageCode(MessageCode.SQL_ERROR);
        } finally {
            if (adapter3 != null) {
                adapter3.closeSession();
            }
        }
        return null;
    }

    /**
     * 通过transformation创建column和Link
     *
     * @param info
     * @return map "VTransformation:Transformation NewColumn:Column NewLink:TransformationLink"
     */
    public Map<String, Object> createColumnAndLinkByTrans(String info, ReturnValue out) {
        logger.info("==== info ===" + info);
        Map<String, Object> infoMap = JsonUtil.toHashMap(info);
        int transformationID = Integer.parseInt(infoMap.get("TransformationID").toString());
        int squidId = Integer.parseInt(infoMap.get("SquidId").toString());
        IRelationalDataManager adapter3 = DataAdapterFactory.getDefaultDataManager();
        adapter3.openSession();
        // 1.获取transformation
        // 2.获取tranformation 的输出类型
        // 3.根据输出类型生成column
        // 4.生成column上的虚拟transformation
        // 5.创建transformationLink
        // 6.返回值 {VTransformation:Transformation,NewColumn:Column,NewLink:TransformationLink}
        try {
            // 1.获取transformation
            ITransformationDao transformationDao = new TransformationDaoImpl(adapter3);
            Transformation transformation = transformationDao.getObjectById(transformationID, Transformation.class);
            // 2.获取tranformation 的输出类型
            int outNum = transformation.getOutput_number();
            int dataType = transformation.getOutput_data_type();
            List<Column> columns = new ArrayList<>();
            Column column = new Column();

            // 3.根据tranformation输出类型生成column中的一些默认值
            column.setData_type(dataType);
            if (dataType == SystemDatatype.NVARCHAR.value()) {
                column.setLength(256);
                //	column.setLength(-1);  -1表示最大长度

                // ---修复BUG:生成新列时，一些类型需要给个默认的长度.
            } else if (dataType == SystemDatatype.NCHAR.value()) {
                column.setLength(256);
            } else if (dataType == SystemDatatype.BINARY.value()) {
                column.setLength(256);
            } else if (dataType == SystemDatatype.VARBINARY.value()) {
                column.setLength(256);
            } else if (dataType == SystemDatatype.CSN.value()) {
                column.setLength(256);
            } else if (dataType == SystemDatatype.DECIMAL.value()) {
                column.setPrecision(19);
                column.setScale(4);
            } else if (dataType == SystemDatatype.CSV.value()) {
                column.setLength(256);
            }
            //	增加以上代码就OK	----zhengpeng.deng

            column.setName(CommonConsts.DEFAULT_COLUMN_NAME + "1");
            column.setKey(StringUtils.generateGUID());
            IColumnDao columnDao = new ColumnDaoImpl(adapter3);
            List<Column> columnList = columnDao.getColumnListBySquidId(squidId);
            boolean flag = false;
            int i = 1;
            do {
                flag = false;
                for (Column c : columnList) {
                    if (c.getName().equalsIgnoreCase(column.getName())) {
                        flag = true;
                        column.setName(CommonConsts.DEFAULT_COLUMN_NAME + i);
                        i++;
                        break;
                    }
                }
            }
            while (flag);
            // 保存column
            column.setSquid_id(squidId);
            // 查找最大的order
            int maxOrder = 0;
            for (Column c : columnList) {
                if (c.getRelative_order() > maxOrder) {
                    maxOrder = c.getRelative_order();
                }
            }
            column.setRelative_order(++maxOrder);

            int columnId = columnDao.insert2(column);
            // 查询，以获取数据库生成的ID
//			column = columnDao.getColumnByKey(column.getKey());
            column.setId(columnId);

            // 创建virtual transformation
            Transformation virtTransformation = new Transformation();
            virtTransformation.setColumn_id(column.getId());
            virtTransformation.setKey(StringUtils.generateGUID());
            virtTransformation.setSquid_id(squidId);
            virtTransformation.setLocation_x(0);
            virtTransformation.setLocation_y(columnList.size() * 25 + 25 / 2);
            virtTransformation.setTranstype(TransformationTypeEnum.VIRTUAL.value());
            virtTransformation.setOutput_data_type(column.getData_type());
            int transId = transformationDao.insert2(virtTransformation);
            // 查询，以获取数据库生成的ID
//			virtTransformation = transformationDao.getTransformationByKey(virtTransformation.getKey());;
            // 给transformation输入值
            virtTransformation.setId(transId);
            TransformationInputs transformationInputs = new TransformationInputs();
            transformationInputs.setInput_Data_Type(column.getData_type());
            transformationInputs.setTransformationId(virtTransformation.getId());
            transformationInputs.setSource_transform_id(transformation.getId());
            transformationInputs.setSource_tran_output_index(0);
            transformationInputs.setSourceTransformationName(transformation.getName());
            transformationInputs.setKey(StringUtils.generateGUID());
            ITransformationInputsDao transformationInputsDao = new TransformationInputsDaoImpl(adapter3);
            int inputsId = transformationInputsDao.insert2(transformationInputs);
            // 查询，以获取数据库生成的ID
            transformationInputs.setId(inputsId);
            List<TransformationInputs> inputsList = new ArrayList<>();
            inputsList.add(transformationInputs);
            virtTransformation.setInputs(inputsList);

            // 创建Link
            TransformationLink transformationLink = new TransformationLink();
            transformationLink.setFrom_transformation_id(transformation.getId());
            transformationLink.setTo_transformation_id(virtTransformation.getId());
            transformationLink.setKey(StringUtils.generateGUID());
            transformationLink.setIn_order(1);
            ITransformationLinkDao transformationLinkDao = new TransformationLinkDaoImpl(adapter3);
            int linkId = transformationLinkDao.insert2(transformationLink);
            // 查询，以获取数据库生成的ID
//			transformationLink = transformationLinkDao.getTransformationLinkByKey(transformationLink.getKey());
            transformationLink.setId(linkId);

            // 为column 创建referenceColumn
//			ReferenceColumnService referenceColumnService = new ReferenceColumnService(token);
//			referenceColumnService.createReferenceColumn(column, adapter3);
            RepositoryServiceHelper helper = new RepositoryServiceHelper(TokenUtil.getToken());
            helper.synchronousInsertColumn(adapter3, squidId, column, DMLType.INSERT.value(), false);
            Map<String, Object> map = new HashMap<>();
            map.put("VTransformation", virtTransformation);
            map.put("NewColumn", column);
            map.put("NewLink", transformationLink);
            return map;
        } catch (Exception e) {
            out.setMessageCode(MessageCode.SQL_ERROR);
            try {
                adapter3.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
            return null;
        } finally {
            if (adapter3 != null) {
                adapter3.closeSession();
            }
        }
    }

    /**
     * 创建 id column
     *
     * @param info
     * @return
     */
    public Map<String, Object> createIDColumn(String info, ReturnValue out) throws Exception {
        IRelationalDataManager adapter = DataAdapterFactory.getDefaultDataManager();
        Map<String, Object> outputMap = new HashMap<>();
        adapter.openSession();
        try {
            Map<String, Object> paramsMap = JsonUtil.toHashMap(info);
            StageSquid currentSquid = JsonUtil.toGsonBean(String.valueOf(paramsMap.get("StageSquid")), StageSquid.class);
            ISquidFlowDao squidFlowDao = new SquidFlowDaoImpl(adapter);
            //判断该squidflow是否有GroupTaggingSquid
            boolean flag = false;
            List<Map<String, Object>> squidTypeMaps = squidFlowDao.getSquidTypeBySquidFlow(currentSquid.getSquidflow_id());
            if (squidTypeMaps != null) {
                for (Map<String, Object> typeMap : squidTypeMaps) {
                    if (typeMap.containsValue(SquidTypeEnum.GROUPTAGGING.value())) {
                        flag = true;
                        break;
                    }
                }
            }
            for (Column item : currentSquid.getColumns()) {
                if (item.getName().equals("id")) {
                    out.setMessageCode(MessageCode.ERR_ID_COLUMN_EXIST);
                    return outputMap;
                }
            }
            // 增加以下 column id 类型为int 型，从0 每次加 1 自增
            Column newColumn = new Column();
            newColumn.setCollation(0);
            newColumn.setData_type(SystemDatatype.BIGINT.value());
            newColumn.setName("id");
            newColumn.setLength(0);
            newColumn.setNullable(false);
            newColumn.setIsUnique(true);
            newColumn.setCdc(0);
            newColumn.setPrecision(0);
            newColumn.setRelative_order(currentSquid.getColumns().size() + 1);
            newColumn.setSquid_id(currentSquid.getId());
            newColumn.setKey(StringUtils.generateGUID());
            newColumn.setAggregation_type(-1);
            newColumn.setIs_groupby(false);
            newColumn.setId(adapter.insert2(newColumn));
            ISquidDao squidDao = new SquidDaoImpl(adapter);
            // 创建新增列对应的虚拟变换
            TransformationService transformationService = new TransformationService(TokenUtil.getToken());
            Transformation newTrans = transformationService.initTransformation(adapter, currentSquid.getId(),
                    newColumn.getId(), TransformationTypeEnum.VIRTUAL.value(),
                    newColumn.getData_type(), 1);
            //同步下游(左边的reference,transformation)
            synchronizedCreateIdColumn(adapter, currentSquid, flag, out, newColumn);
            outputMap.put("idColumn", newColumn);
            outputMap.put("idTrans", newTrans);
        } catch (Exception e) {
            logger.error("创建id column异常", e);
            out.setMessageCode(MessageCode.ERR_COLUMN_NULL);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!", e1);
            }
        } finally {
            adapter.closeSession();
            return outputMap;
        }
    }

    /**
     * 新的创建id column(GroupTaggingSquidtong递归同步)
     *
     * @param out
     * @return
     */
    public void synchronizedCreateIdColumn(IRelationalDataManager adapter, Squid currentSquid, boolean flag, ReturnValue out, Column newColumn) throws Exception {
        ISquidDao squidDao = new SquidDaoImpl(adapter);
        TransformationService transformationService = new TransformationService(TokenUtil.getToken());
        Map<String, Object> params = new HashMap<>();
        Column column = null;
        if (newColumn == null) {
            return;
        }


        //查询出下游的squid对象,新增column对象
        List<Squid> squidLists = squidDao.getNextSquidsForSquidID(currentSquid.getId(), Squid.class);
        if (squidLists != null) {
            for (Squid squid : squidLists) {
                params.put("squid_id", squid.getId());
                List<Column> columns = adapter.query2List2(true, params, Column.class);
                if (columns != null) {
                    for (Column item : columns) {
                        //现在只有groupTagging同步下游时，需要增加column，其余的都是增加referenceColumn
                        if (squid.getSquid_type() == SquidTypeEnum.GROUPTAGGING.value()) {
                            if (item.getName().equals("id")) {
                                out.setMessageCode(MessageCode.ERR_ID_COLUMN_EXIST);
                                return;
                            }
                        }
                    }
                }
                //根据squid新增ds——column，和相对应的落地column
                if (squid.getSquid_type() == SquidTypeEnum.DEST_HDFS.value() || squid.getSquid_type() == SquidTypeEnum.DESTCLOUDFILE.value()) {
                    DestHDFSColumn hdfsColumn = HdfsColumnService.getHDFSColumnByColumn(newColumn, squid.getId());
                    adapter.insert2(hdfsColumn);
                } else if (squid.getSquid_type() == SquidTypeEnum.DEST_IMPALA.value()) {
                    DestImpalaColumn impalaColumn = ImpalaColumnService.getImpalaColumnByColumn(newColumn, squid.getId());
                    adapter.insert2(impalaColumn);
                } else if (squid.getSquid_type() == SquidTypeEnum.DESTES.value()) {
                    EsColumn destColumn = EsColumnService.genEsColumnByColumn(newColumn, squid.getId());
                    adapter.insert2(destColumn);
                } else if (squid.getSquid_type() == SquidTypeEnum.STAGE.value()
                        || squid.getSquid_type() == SquidTypeEnum.STREAM_STAGE.value()
                        || squid.getSquid_type() == SquidTypeEnum.GROUPTAGGING.value()) {
                    //增加Reference
                    IReferenceColumnDao reference = new ReferenceColumnDaoImpl(adapter);
                    List<ReferenceColumn> referenceList = reference.getRefColumnListByRefSquidId(squid.getId());
                    //增加TransformationGroupId
                    ReferenceColumnGroup columnGroup = new ReferenceColumnGroup();
                    columnGroup.setReference_squid_id(squid.getId());
                    columnGroup.setRelative_order((referenceList == null || referenceList.size() == 0) ? 1 : referenceList.size() + 1);
                    columnGroup.setKey(StringUtils.generateGUID());
                    columnGroup.setId(adapter.insert2(columnGroup));

                    ReferenceColumn referenceColumn = new ReferenceColumn();
                    referenceColumn.setColumn_id(newColumn.getId());
                    referenceColumn.setRelative_order((referenceList == null || referenceList.size() == 0) ? 1 : referenceList.size() + 1);
                    referenceColumn.setReference_squid_id(squid.getId());
                    referenceColumn.setName(newColumn.getName());
                    referenceColumn.setData_type(SystemDatatype.BIGINT.value());
                    referenceColumn.setCollation(0);
                    referenceColumn.setNullable(false);
                    referenceColumn.setLength(0);
                    referenceColumn.setPrecision(0);
                    referenceColumn.setScale(0);
                    referenceColumn.setIsUnique(true);
                    referenceColumn.setIsPK(true);
                    referenceColumn.setCdc(0);
                    referenceColumn.setIs_Business_Key(0);
                    referenceColumn.setIs_referenced(true);
                    referenceColumn.setHost_squid_id(newColumn.getSquid_id());
                    referenceColumn.setGroup_id(columnGroup.getId());
                    referenceColumn.setId(adapter.insert2(referenceColumn));
                    //增加虚拟转换
                    Transformation leftTrans = transformationService.initTransformation(adapter, squid.getId(),
                            referenceColumn.getColumn_id(), TransformationTypeEnum.VIRTUAL.value(),
                            newColumn.getData_type(), 1);

                    if (squid.getSquid_type() == SquidTypeEnum.GROUPTAGGING.value()) {
                        //创建对应的column,transformation，transformationLink
                        params = new HashMap<>();
                        params.put("squid_id", squid.getId());
                        List<Column> columnList = adapter.query2List2(true, params, Column.class);
                        column = new Column();
                        column.setCollation(0);
                        column.setData_type(SystemDatatype.BIGINT.value());
                        column.setName("id");
                        column.setLength(0);
                        column.setNullable(false);
                        column.setIsUnique(true);
                        column.setCdc(0);
                        column.setPrecision(0);
                        column.setRelative_order(columnList == null ? 1 : columnList.size() + 1);
                        column.setSquid_id(squid.getId());
                        column.setKey(StringUtils.generateGUID());
                        column.setAggregation_type(-1);
                        column.setIs_groupby(false);
                        column.setId(adapter.insert2(column));
                        //创建对应的transformation
                        Transformation rightTrans = transformationService.initTransformation(adapter, squid.getId(), column.getId(), TransformationTypeEnum.VIRTUAL.value(), newColumn.getData_type(), 1);
                        //创建对应的transLink
                        transformationService.initTransformationLink(adapter, leftTrans.getId(), rightTrans.getId(), referenceColumn.getRelative_order());
                    }
                }
                //递归调用
                if (flag) {
                    //获取下游的squid
                    synchronizedCreateIdColumn(adapter, squid, flag, out, column);
                }
            }
        }
    }

    /**
     * 创建extraction date column
     *
     * @param info
     * @param out
     * @return
     */
    public Map<String, Object> createExtractionDateColumn(String info, ReturnValue out) {
        IRelationalDataManager adapter = null;
        DataAdapterFactory adaFactory = null;
        Map<String, Object> outputMap = new HashMap<>();
        try {
            adapter = DataAdapterFactory.getDefaultDataManager();
            Map<String, Object> paramsMap = JsonUtil.toHashMap(info);
            DataSquid currentSquid = JsonUtil.toGsonBean(String.valueOf(paramsMap.get("DataSquid")), DataSquid.class);
            ISquidLinkDao squidLinkDao = new SquidLinkDaoImpl(adapter);
            for (Column item : currentSquid.getColumns()) {
                if (item.getName().equals(Constants.DEFAULT_EXTRACT_COLUMN_NAME)) {
                    out.setMessageCode(MessageCode.ERR_ID_COLUMN_EXIST);
                    return outputMap;
                }
            }

            adapter.openSession();
            // 创建默认的Column and Tansformation
            // TODO 新增column：extraction_date
            Column newColumn = new Column();
            newColumn.setCollation(0);
            newColumn.setData_type(SystemDatatype.DATETIME.value());
            newColumn.setName(Constants.DEFAULT_EXTRACT_COLUMN_NAME);
            newColumn.setLength(0);
            newColumn.setNullable(true);
            //修改bug5153
            newColumn.setIsUnique(true);
            newColumn.setCdc(0);
            newColumn.setPrecision(0);
            newColumn.setRelative_order(currentSquid.getColumns() == null ? 1 : currentSquid.getColumns().size() + 1);
            newColumn.setSquid_id(currentSquid.getId());
            newColumn.setKey(StringUtils.generateGUID());
            newColumn.setCollation(0);
            // fixed bug 905 start by bo.dang
            newColumn.setIs_groupby(false);
            newColumn.setAggregation_type(-1);
            // fixed bug 905 end by bo.dang
            newColumn.setId(adapter.insert2(newColumn));

            //同步下游
            RepositoryServiceHelper helper = new RepositoryServiceHelper(TokenUtil.getToken(), adapter);
            helper.synchronousInsertColumn(adapter, newColumn.getSquid_id(), newColumn, DMLType.INSERT.value(), false);

            // 创建新增列对应的虚拟变换
            TransformationService transformationService = new TransformationService(TokenUtil.getToken());
            Transformation transformation = transformationService.mergeTransformation(
                    adapter, currentSquid.getId(), newColumn.getId(),
                    TransformationTypeEnum.VIRTUAL.value(),
                    SystemDatatype.OBJECT.value(), 1, 1);

            outputMap.put("ExtractDateColumn", newColumn);
            outputMap.put("ExtractDateTrans", transformation);

        } catch (Exception e) {
            logger.error("创建extraction date column异常", e);
            out.setMessageCode(MessageCode.ERR_COLUMN_NULL);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!", e1);
            }
        } finally {
            adapter.closeSession();
            return outputMap;
        }
    }
}
