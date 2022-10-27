package com.eurlanda.datashire.userDefinedService;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.common.util.ConstantsUtil;
import com.eurlanda.datashire.dao.IColumnDao;
import com.eurlanda.datashire.dao.IReferenceColumnDao;
import com.eurlanda.datashire.dao.ISquidDao;
import com.eurlanda.datashire.dao.ISquidLinkDao;
import com.eurlanda.datashire.dao.ITransformationDao;
import com.eurlanda.datashire.dao.impl.ColumnDaoImpl;
import com.eurlanda.datashire.dao.impl.ReferenceColumnDaoImpl;
import com.eurlanda.datashire.dao.impl.SquidDaoImpl;
import com.eurlanda.datashire.dao.impl.SquidLinkDaoImpl;
import com.eurlanda.datashire.dao.impl.TransformationDaoImpl;
import com.eurlanda.datashire.entity.*;
import com.eurlanda.datashire.entity.dest.DestHDFSColumn;
import com.eurlanda.datashire.entity.dest.DestImpalaColumn;
import com.eurlanda.datashire.entity.dest.EsColumn;
import com.eurlanda.datashire.entity.operation.BeyondSquidException;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.enumeration.TransformationTypeEnum;
import com.eurlanda.datashire.enumeration.datatype.SystemDatatype;
import com.eurlanda.datashire.server.utils.TokenUtil;
import com.eurlanda.datashire.sprint7.service.squidflow.TransformationService;
import com.eurlanda.datashire.sprint7.service.squidflow.dest.EsColumnService;
import com.eurlanda.datashire.sprint7.service.squidflow.dest.HdfsColumnService;
import com.eurlanda.datashire.sprint7.service.squidflow.dest.ImpalaColumnService;
import com.eurlanda.datashire.utility.DatabaseException;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import com.eurlanda.datashire.utility.StringUtils;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Eurlanda on 2017/4/24.
 */
public class UserDefinedServiceImpl {
    private Logger logger = Logger.getLogger(UserDefinedServiceImpl.class);

    /**
     * 读取类的所有信息
     *
     * @return
     */
    public List<String> getAllUserDefinedClass() {
        return null;
    }

    /**
     * 读取指定类的信息
     */
    public List<UserDefinedSquid> getSelectOneUserDefined(String classNames) {
        return null;
    }


    /**
     * 创建squid
     *
     * @param info
     * @return
     */
    public String createUserDefinedSquid(String info) {
        ReturnValue out = new ReturnValue();
        Map<String, Object> map = new HashMap<String, Object>();
        IRelationalDataManager adapter = null;
        try {
            Map<String, Object> data = JsonUtil.toHashMap(info);
            UserDefinedSquid definedSquid = JsonUtil.toGsonBean(data.get("UserDefinedSquid") + "", UserDefinedSquid.class);
            if (definedSquid != null) {
                adapter = DataAdapterFactory.getDefaultDataManager();
                adapter.openSession();
                int squidId = adapter.insert2(definedSquid);
                //读取所有的类的相关信息()
                String sql = "select alias_name from THIRD_JAR_DEFINITION";
                List<Map<String, Object>> classNames = adapter.query2List(true, sql, null);
                List<String> classNameList = new ArrayList<>();
                for (Map<String, Object> className : classNames) {
                    classNameList.add(className.get("ALIAS_NAME") + "");
                }
                map.put("SquidId", squidId);
                map.put("UserDefinedClassNameList", classNameList);
            } else {
                out.setMessageCode(MessageCode.DESERIALIZATION_FAILED);
            }
        } catch (BeyondSquidException e) {
            try {
                if (adapter != null) adapter.rollback();
            } catch (SQLException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            out.setMessageCode(MessageCode.ERR_SQUID_COUNT_MAX);
            logger.error("创建UserDefinedSquid异常", e);
        } catch (Exception e) {
            e.printStackTrace();
            try {
                if (adapter != null) adapter.rollback();
            } catch (SQLException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            out.setMessageCode(MessageCode.INSERT_ERROR);
            logger.error("创建UserDefinedSquid异常", e);
        } finally {
            if (adapter != null) adapter.closeSession();
        }
        return JsonUtil.toJsonString(map, DSObjectType.USERDEFINED, out.getMessageCode());
    }

    /**
     * 更新squid
     *
     * @param info
     * @return
     */
    public String updateUserDefinedSquid(String info) {
        ReturnValue out = new ReturnValue();
        IRelationalDataManager adapter = null;
        Map<String, Object> paramsMap = JsonUtil.toHashMap(info);
        try {
            UserDefinedSquid definedSquid = JsonUtil.toGsonBean(paramsMap.get("UserDefinedSquid") + "", UserDefinedSquid.class);
            if (null != definedSquid) {
                adapter = DataAdapterFactory.getDefaultDataManager();
                adapter.openSession();
                boolean flag = adapter.update2(definedSquid);
                if (!flag) {
                    out.setMessageCode(MessageCode.UPDATE_ERROR);
                }
            } else {
                out.setMessageCode(MessageCode.ERR_SQUID_NULL);
            }
        } catch (Exception e) {
            try {
                if (adapter != null) {
                    adapter.rollback();
                }
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
            out.setMessageCode(MessageCode.UPDATE_ERROR);
            logger.error("更新UserDefinedSquid异常", e);
        } finally {
            if (adapter != null) {
                adapter.closeSession();
            }
        }
        return JsonUtil.toJsonString(new HashMap<String, Object>(), DSObjectType.USERDEFINED, out.getMessageCode());
    }

    /**
     * 更新dataMap
     *
     * @param info
     * @return
     */
    public String updateUserDefinedMappingColumns(String info) {
        ReturnValue out = new ReturnValue();
        IRelationalDataManager adapter = null;
        try {
            List<UserDefinedMappingColumn> userDefinedMappingColumns = JsonUtil.toGsonList(JsonUtil.toHashMap(info).get("UserDefinedMappingColumnList") + "", UserDefinedMappingColumn.class);
            if (null != userDefinedMappingColumns && userDefinedMappingColumns.size() > 0) {
                adapter = DataAdapterFactory.getDefaultDataManager();
                adapter.openSession();
                for (UserDefinedMappingColumn mappingColumn : userDefinedMappingColumns) {
                    adapter.update2(mappingColumn);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            try {
                if (adapter != null) {
                    adapter.rollback();
                }
            } catch (Exception e1) {
                e1.printStackTrace();
            }
            out.setMessageCode(MessageCode.UPDATE_ERROR);
            logger.error("修改UserDefinedMappingColumns异常", e);

        } finally {
            if (adapter != null) {
                adapter.closeSession();
            }
        }
        return JsonUtil.toJsonString(new HashMap<>(), DSObjectType.USERDEFINED, out.getMessageCode());
    }

    /**
     * 更新parameterMap
     *
     * @param info
     * @return
     */
    public String updateUserDefinedParameterColumns(String info) {
        ReturnValue out = new ReturnValue();
        IRelationalDataManager adapter = null;
        try {
            List<UserDefinedParameterColumn> userDefinedParameterColumn = JsonUtil.toGsonList(JsonUtil.toHashMap(info).get("UserDefinedParameterColumnList") + "", UserDefinedParameterColumn.class);
            if (userDefinedParameterColumn != null && userDefinedParameterColumn.size() > 0) {
                adapter = DataAdapterFactory.getDefaultDataManager();
                adapter.openSession();
                for (UserDefinedParameterColumn definedParameterColumn : userDefinedParameterColumn) {
                    adapter.update2(definedParameterColumn);
                }
            } else {
                out.setMessageCode(MessageCode.ERR_COLUMN_NULL);
            }
        } catch (Exception e) {
            try {
                if (adapter != null) {
                    adapter.rollback();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
            out.setMessageCode(MessageCode.UPDATE_ERROR);
            logger.error("更新UserDefinedParameterColumns异常", e);
        } finally {
            if (adapter != null) {
                adapter.closeSession();
            }
        }
        return JsonUtil.toJsonString(new HashMap<String, Object>(), DSObjectType.USERDEFINED, out.getMessageCode());
    }

    /**
     * 创建squidLink
     *
     * @param info
     * @param out
     * @return
     */
    public String createSquidLink(String info, ReturnValue out) {
        IRelationalDataManager adapter = null;
        Map<String, Object> returnMap = new HashMap<>();
        int squidType = 0;
        try {
            Map<String, Object> paramMap = JsonUtil.toHashMap(info);
            SquidLink squidLink = JsonUtil.toGsonBean(paramMap.get("SquidLink") + "", SquidLink.class);
            if (squidLink == null) {
                out.setMessageCode(MessageCode.ERR_SQUIDlINK_LIST_NULL);
                logger.debug(String.format("创建自定义squidLink异常", out.getMessageCode()));
                return JsonUtil.toJsonString(returnMap, DSObjectType.SQUID, out.getMessageCode());
            } else {
                adapter = DataAdapterFactory.getDefaultDataManager();
                adapter.openSession();
                int squidLinkId = adapter.insert2(squidLink);
                returnMap.put("SquidLinkId", squidLinkId);
                //创建referenceColumn
                IColumnDao columnDao = new ColumnDaoImpl(adapter);
                ISquidDao squidDao = new SquidDaoImpl(adapter);
                IReferenceColumnDao refColumnDao = new ReferenceColumnDaoImpl(adapter);
                TransformationService service = new TransformationService(TokenUtil.getToken());
                List<Column> columnList = columnDao.getColumnListBySquidId(squidLink.getFrom_squid_id());
                List<ReferenceColumn> refColumnList = new ArrayList<>();
                List<Transformation> transformationList = new ArrayList<>();
                if (columnList != null && columnList.size() > 0) {
                    List<ReferenceColumnGroup> rg = refColumnDao.getRefColumnGroupListBySquidId(squidLink.getTo_squid_id());
                    ReferenceColumnGroup columnGroup = new ReferenceColumnGroup();
                    columnGroup.setKey(StringUtils.generateGUID());
                    columnGroup.setReference_squid_id(squidLink.getTo_squid_id());
                    columnGroup.setRelative_order(rg == null || rg.isEmpty() ? 1 : rg.size() + 1);
                    columnGroup.setId(refColumnDao.insert2(columnGroup));
                    Squid fromSquid = squidDao.getObjectById(squidLink.getFrom_squid_id(), Squid.class);
                    squidType = squidDao.getSquidTypeById(squidLink.getTo_squid_id());
                    for (Column column : columnList) {
                        ReferenceColumn refColumn = service.initReference(adapter, column, column.getId(), column.getRelative_order(), fromSquid, squidLink.getTo_squid_id(), columnGroup);
                        refColumnList.add(refColumn);
                        Transformation transformation = service.initTransformation(adapter, squidLink.getTo_squid_id(), column.getId(), TransformationTypeEnum.VIRTUAL.value(), column.getData_type(), column.getRelative_order());
                        transformationList.add(transformation);
                    }
                }
                returnMap.put("ReferenceColumnList", refColumnList);
                returnMap.put("TransformationList", transformationList);
            }
        } catch (Exception e) {
            e.printStackTrace();
            try {
                adapter.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
            out.setMessageCode(MessageCode.SQL_ERROR);
        } finally {
            if (adapter != null) {
                adapter.closeSession();
            }
        }
        return JsonUtil.toJsonString(returnMap, DSObjectType.parse(squidType), out.getMessageCode());
    }

    /**
     * 删除squidlink，需要同步下游
     *
     * @param info
     * @return
     */
    public String deleteSquidLink(String info) throws Exception {
        IRelationalDataManager adapter = null;
        ReturnValue out = new ReturnValue();
        SquidLink squidLink = JsonUtil.toGsonBean(JsonUtil.toHashMap(info).get("SquidLink") + "", SquidLink.class);
        Map<String, Object> returnMap = new HashMap<>();
        int squidTypeId = 0;
        if (squidLink != null) {
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            ISquidDao squidDao = new SquidDaoImpl(adapter);
            IColumnDao columnDao = new ColumnDaoImpl(adapter);
            squidTypeId = squidDao.getSquidTypeById(squidLink.getTo_squid_id());
            //删除squidLink
            String sql = "delete from DS_SQUID_LINK where id=" + squidLink.getId();
            try {
                adapter.execute(sql);
                //将dataMap里面的columnId更新为空
                Map<String, Object> params = new HashMap<>();
                if (squidTypeId == SquidTypeEnum.USERDEFINED.value()) {
                    sql = "update ds_userdefined_datamap_column set column_id = 0 where squid_id=" + squidLink.getTo_squid_id();
                } else if (squidTypeId == SquidTypeEnum.STATISTICS.value()) {
                    sql = "update ds_statistics_datamap_column set column_id = 0 where squid_id=" + squidLink.getTo_squid_id();
                }
                adapter.execute(sql);

                params.clear();
                params.put("squid_id", squidLink.getTo_squid_id() + "");
                if (squidTypeId == SquidTypeEnum.USERDEFINED.value()) {
                    List<UserDefinedMappingColumn> dataMapList = adapter.query2List2(true, params, UserDefinedMappingColumn.class);
                    returnMap.put("UserDefinedMappingColumnList", dataMapList);
                } /*else if(squidTypeId == SquidTypeEnum.STATISTICS.value()){
                    List<StatisticsDataMapColumn> dataMapList = adapter.query2List2(true,params, StatisticsDataMapColumn.class);
                    returnMap.put("StatisticMappingColumnList",dataMapList);
                }*/
                //将referenceColumn删掉
                params.clear();
                params.put("reference_squid_id", squidLink.getTo_squid_id() + "");
                sql = "delete from ds_reference_column where reference_squid_id = " + squidLink.getTo_squid_id();
                adapter.execute(sql);
                //将transformation删掉
                List<Column> columnList = columnDao.getColumnListBySquidId(squidLink.getFrom_squid_id());
                if (columnList != null) {
                    for (Column column : columnList) {
                        sql = "delete from ds_transformation where squid_id=" + squidLink.getTo_squid_id() + " and column_id=" + column.getId();
                        adapter.execute(sql);
                    }
                }
            } catch (DatabaseException e) {
                e.printStackTrace();
                try {
                    adapter.rollback();
                } catch (SQLException e1) {
                    e1.printStackTrace();
                }
            } catch (Exception e) {
                e.printStackTrace();
                try {
                    adapter.rollback();
                } catch (SQLException e1) {
                    e1.printStackTrace();
                }
            } finally {
                if (adapter != null) {
                    adapter.closeSession();
                }
            }

        }
        return JsonUtil.toJsonString(returnMap, DSObjectType.parse(squidTypeId), out.getMessageCode());
    }

    /**
     * 改变类的名字(需要同步下游)
     *
     * @param info
     * @return
     */
    public String changeUserDefinedClassName(String info) {
        IRelationalDataManager adapter = null;
        ReturnValue out = new ReturnValue();
        try {
            Map<String, Object> paramMap = JsonUtil.toHashMap(info);
            int squidId = (int) paramMap.get("SquidId");
            String aliasName = paramMap.get("UserDefinedClassName") + "";
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            IColumnDao columnDao = new ColumnDaoImpl(adapter);
            ITransformationDao transDao = new TransformationDaoImpl(adapter);
            ISquidLinkDao squidLinkDao = new SquidLinkDaoImpl(adapter);
            //将之前的所有的dataMap,parameter,referenceColumn，column删掉
            String sql = "delete from ds_userdefined_datamap_column where squid_id=" + squidId;
            adapter.execute(sql);
            sql = "delete from ds_userdefined_parameters_column where squid_id=" + squidId;
            adapter.execute(sql);
            //删除column的Transformation
            List<Column> oldColumnList = columnDao.getColumnListBySquidId(squidId);
            if (oldColumnList != null) {
                for (Column column : oldColumnList) {
                    sql = "delete from ds_transformation where squid_id=" + squidId + " and column_id=" + column.getId();
                    adapter.execute(sql);
                }
            }
            //删除column
            sql = "delete from ds_column where squid_id=" + squidId;
            adapter.execute(sql);
            //更新squid
            String updateName = "update ds_squid set selectClassName = ? where id= ?";
            List<Object> paramsList = new ArrayList<>();
            paramsList.add(aliasName);
            paramsList.add(squidId);
            adapter.execute(updateName, paramsList);
            //根据类名获取类中dataMap，parameter，outMaping相关信息，生成mappingColumn信息，referenceColumn,column信息
            sql = "select * from third_jar_definition where alias_name =  ?";
            List<Object> sqlMap = new ArrayList<>();
            sqlMap.add(aliasName);
            logger.debug("别名为:" + aliasName);
            Map<String, Object> returnMap = adapter.query2Object(true, sql, sqlMap);
            List<Map<String, Object>> dataMapList = null;
            List<Map<String, Object>> parameterList = null;
            List<Map<String, Object>> outMapList = null;
            if (returnMap.get("DATA_MAPPING") != null) {
                dataMapList = JsonUtil.toList(returnMap.get("DATA_MAPPING"));
            }
            if (returnMap.get("PARAMETER") != null) {
                parameterList = JsonUtil.toList(returnMap.get("PARAMETER"));
            }
            if (returnMap.get("OUTPUT_MAPPING") != null) {
                outMapList = JsonUtil.toList(returnMap.get("OUTPUT_MAPPING"));
            }
            List<UserDefinedMappingColumn> userDefinedMappingColumnList = new ArrayList<>();
            List<UserDefinedParameterColumn> parameterColumnList = new ArrayList<>();
            List<Column> newcolumnList = new ArrayList<>();
            List<Transformation> transformationList = new ArrayList<>();
            TransformationService service = new TransformationService(TokenUtil.getToken());
            //生成dataMappingColumn信息
            if (dataMapList != null) {
                for (Map<String, Object> dataMap : dataMapList) {
                    UserDefinedMappingColumn dataMapColumn = new UserDefinedMappingColumn();
                    if (dataMap.containsKey("name")) {
                        dataMapColumn.setName(dataMap.get("name") + "");
                    }
                    if (dataMap.containsKey("type")) {
                        dataMapColumn.setType(Integer.parseInt(dataMap.get("type") + ""));
                    }
                    if (dataMap.containsKey("description")) {
                        dataMapColumn.setDescription(dataMap.get("description") + "");
                    }
                    if (dataMap.containsKey("precision")) {
                        dataMapColumn.setPrecision((int) dataMap.get("precision"));
                    }
                    if (dataMap.containsKey("scale")) {
                        dataMapColumn.setScale((int) dataMap.get("scale"));
                    }
                    dataMapColumn.setSquid_id(squidId);
                    dataMapColumn.setId(adapter.insert2(dataMapColumn));
                    userDefinedMappingColumnList.add(dataMapColumn);
                }
            }
            //生成parameter信息
            if (parameterList != null) {
                for (Map<String, Object> paramsMap : parameterList) {
                    UserDefinedParameterColumn parameterColumn = new UserDefinedParameterColumn();
                    if (paramsMap.containsKey("name")) {
                        parameterColumn.setName(paramsMap.get("name") + "");
                    }
                    if (paramsMap.containsKey("description")) {
                        parameterColumn.setDescription(paramsMap.get("description") + "");
                    }
                    parameterColumn.setSquid_id(squidId);
                    parameterColumn.setId(adapter.insert2(parameterColumn));
                    parameterColumnList.add(parameterColumn);
                }
            }
            //生成column
            int i = 0;
            if (outMapList != null) {
                for (Map<String, Object> outMap : outMapList) {
                    Column column = new Column();
                    i++;
                    if (outMap.containsKey("name")) {
                        column.setName(outMap.get("name") + "");
                    }
                    if (outMap.containsKey("description")) {
                        column.setDescription(outMap.get("description") + "");
                    }
                    if (outMap.containsKey("type")) {
                        if (Integer.parseInt(outMap.get("type") + "") == SystemDatatype.NVARCHAR.value()) {
                            column.setLength(-1);
                        }
                        if (Integer.parseInt(outMap.get("type") + "") == SystemDatatype.NCHAR.value()) {
                            column.setLength(-1);
                        }

                        column.setData_type(Integer.parseInt(outMap.get("type") + ""));
                    }
                    if (outMap.containsKey("precision")) {
                        column.setPrecision(Integer.parseInt(outMap.get("precision") + ""));
                    }
                    if (outMap.containsKey("scale")) {
                        column.setScale(Integer.parseInt(outMap.get("scale") + ""));
                    }
                    column.setSquid_id(squidId);
                    column.setRelative_order(i);
                    column.setId(adapter.insert2(column));
                    newcolumnList.add(column);
                    //生成Transformation
                    Transformation transformation = service.initTransformation(adapter, squidId, column.getId(), TransformationTypeEnum.VIRTUAL.value(), column.getData_type(), i);
                    transformationList.add(transformation);
                }
            }
            returnMap.clear();
            returnMap.put("UserDefinedMappingColumnList", userDefinedMappingColumnList);
            returnMap.put("UserDefinedParameterColumnList", parameterColumnList);
            returnMap.put("ColumnList", newcolumnList);
            //查询出所有的transformation
            transformationList.addAll(transDao.getTransListBySquidId(squidId));
            returnMap.put("TransformationList", transformationList);

            //查询出squid信息，同步下游
            Map<String, Object> paramaMap = new HashMap<>();
            paramaMap.put("from_squid_id", squidId);
            List<SquidLink> squidLinkList = adapter.query2List2(true, paramaMap, SquidLink.class);
            if (squidLinkList != null) {
                for (SquidLink squidLink : squidLinkList) {
                    synchronizedNextSquidColumn(squidLink, adapter, oldColumnList);
                }
            }
            return JsonUtil.toJsonString(returnMap, DSObjectType.USERDEFINED, out.getMessageCode());

        } catch (Exception e) {
            e.printStackTrace();
            try {
                adapter.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
        } finally {
            if (adapter != null) {
                adapter.closeSession();
            }
        }
        return null;
    }

    /**
     * 同步下游
     *
     * @param squidLink
     * @param adapter
     */
    public void synchronizedNextSquidColumn(SquidLink squidLink, IRelationalDataManager adapter, List<Column> upColumn) throws Exception {
        if (squidLink != null) {
            int fromSquid = squidLink.getFrom_squid_id();
            int toSquid = squidLink.getTo_squid_id();
            ISquidDao squidDao = new SquidDaoImpl(adapter);
            Squid upSquid = squidDao.getSquidForCond(fromSquid, Squid.class);
            Squid nextSquid = squidDao.getSquidForCond(toSquid, Squid.class);
            //获取上游的column
            IColumnDao columnDao = new ColumnDaoImpl(adapter);
            TransformationService service = new TransformationService(TokenUtil.getToken());
            List<Column> columnList = columnDao.getColumnListBySquidId(fromSquid);
            //判断下游是否是GroupTagSquid
            if (nextSquid.getSquid_type() == SquidTypeEnum.GROUPTAGGING.value()) {
                Map<String, Object> paraMap = new HashMap<>();
                paraMap.put("reference_squid_id", toSquid);
                List<ReferenceColumnGroup> refGroupList = adapter.query2List2(true, paraMap, ReferenceColumnGroup.class);
                //添加ReferenceColumn（先将之前的清除掉）
                String sql = "delete from ds_reference_column where reference_squid_id = " + toSquid;
                adapter.execute(sql);
                //删除TransformationLink
                sql = "delete from ds_transformation_link where  from_transformation_id in (select id from ds_transformation where squid_id=" + toSquid + ")";
                adapter.execute(sql);
                //删除transformation
                sql = "delete from ds_transformation where squid_id = " + toSquid + " and column_id != (select id from ds_column where squid_id=" + toSquid + " and name=?)";
                List<Object> sqlMap = new ArrayList<>();
                sqlMap.add(ConstantsUtil.CN_GROUP_TAG);
                adapter.execute(sql, sqlMap);
                //删除column（不包括tag）
                sql = "delete from ds_column where squid_id = " + toSquid + " and name != ?";
                adapter.execute(sql, sqlMap);
                //将tag类的order修改为1
                sql = "update ds_column set relative_order = 1 where name= ?";
                adapter.execute(sql, sqlMap);

                Map<String, String> columnName = new HashMap<>();
                columnName.put(ConstantsUtil.CN_GROUP_TAG, ConstantsUtil.CN_GROUP_TAG);
                if (refGroupList == null || refGroupList.size() == 0) {
                    ReferenceColumnGroup refGroup = new ReferenceColumnGroup();
                    refGroup.setReference_squid_id(toSquid);
                    refGroup.setRelative_order(1);
                    refGroup.setId(adapter.insert2(refGroup));
                    refGroupList = new ArrayList<>();
                    refGroupList.add(refGroup);
                }
                //生成ReferenceColumn
                for (Column column : columnList) {
                    ReferenceColumn refColumn = service.initReference(adapter, column, column.getId(), column.getRelative_order(), upSquid, toSquid, refGroupList.get(0));
                    //生成左边的Transformation
                    Transformation rightTrans = service.initTransformation(adapter, toSquid, column.getId(), TransformationTypeEnum.VIRTUAL.value(), column.getData_type(), column.getRelative_order());
                    //生成左边的column
                    Column leftColumn = new Column();
                    leftColumn.setRelative_order(column.getRelative_order() + 1);
                    leftColumn.setSquid_id(toSquid);
                    //判断名字是否重复
                    while (columnName.containsKey(column.getName())) {
                        if (column.getName().matches("(tag)[0-9]*")) {
                            int no = Integer.parseInt(StringUtils.isNull(column.getName().substring(3)) ? "0" : column.getName().substring(3)) + 1;
                            column.setName(ConstantsUtil.CN_GROUP_TAG + no);
                        } else {
                            column.setName(column.getName() + 1);
                        }
                    }
                    columnName.put(column.getName(), column.getName());
                    leftColumn.setName(column.getName());
                    leftColumn.setData_type(column.getData_type());
                    leftColumn.setCollation(column.getCollation());
                    leftColumn.setNullable(column.isNullable());
                    leftColumn.setLength(column.getLength());
                    leftColumn.setPrecision(column.getPrecision());
                    leftColumn.setScale(column.getScale());
                    leftColumn.setIs_groupby(column.isIs_groupby());
                    leftColumn.setAggregation_type(column.getAggregation_type());
                    leftColumn.setDescription(column.getDescription());
                    leftColumn.setIsUnique(column.isIsUnique());
                    leftColumn.setIsPK(column.isIsPK());
                    leftColumn.setCdc(column.getCdc());
                    leftColumn.setIs_Business_Key(column.getIs_Business_Key());
                    leftColumn.setSort_Level(column.getSort_Level());
                    leftColumn.setSort_type(column.getSort_type());
                    leftColumn.setId(adapter.insert2(leftColumn));
                    //生成左边的Transformation
                    Transformation leftTrans = service.initTransformation(adapter, toSquid, leftColumn.getId(), TransformationTypeEnum.VIRTUAL.value(), leftColumn.getData_type(), column.getRelative_order() + 1);
                    //生成TransformationLink
                    service.initTransformationLink(adapter, rightTrans.getId(), leftTrans.getId(), column.getRelative_order());
                }

                //同步下游
                Map<String, Object> paramaMap = new HashMap<>();
                paramaMap.put("from_squid_id", toSquid);
                List<SquidLink> squidLinkList = adapter.query2List2(true, paramaMap, SquidLink.class);
                if (squidLinkList != null) {
                    for (SquidLink nextSquidLink : squidLinkList) {
                        synchronizedNextSquidColumn(nextSquidLink, adapter, columnList);
                    }
                }
            } else if (nextSquid.getSquid_type() == SquidTypeEnum.EXCEPTION.value()) {
                //获取上上游的column信息
            } else if (nextSquid.getSquid_type() == SquidTypeEnum.DEST_IMPALA.value()) {
                //删除原来的信息
                String sql = "delete from ds_dest_impala_column where squid_id=" + toSquid;
                adapter.execute(sql);
                //添加column
                for (Column column : columnList) {
                    DestImpalaColumn impalaColumn = ImpalaColumnService.getImpalaColumnByColumn(column, toSquid);
                    adapter.insert2(impalaColumn);
                }
            } else if (nextSquid.getSquid_type() == SquidTypeEnum.DEST_HDFS.value()
                    || nextSquid.getSquid_type() == SquidTypeEnum.DESTCLOUDFILE.value()) {
                //删除原来的信息
                String sql = "delete from ds_dest_hdfs_column where squid_id=" + toSquid;
                adapter.execute(sql);
                //添加column
                for (Column column : columnList) {
                    DestHDFSColumn hdfsColumn = HdfsColumnService.getHDFSColumnByColumn(column, toSquid);
                    adapter.insert2(hdfsColumn);
                }

            } else if (nextSquid.getSquid_type() == SquidTypeEnum.DESTES.value()) {
                //删除原来的信息
                String sql = "delete from DS_ES_COLUMN where squid_id=" + toSquid;
                adapter.execute(sql);
                //添加column
                for (Column column : columnList) {
                    EsColumn esColumn = EsColumnService.genEsColumnByColumn(column, toSquid);
                    adapter.insert2(esColumn);
                }

            } else if (SquidTypeEnum.isDataMingSquid(nextSquid.getSquid_type()) || SquidTypeEnum.STAGE.value() == nextSquid.getSquid_type()) {
                //清除referenceColumn和Transformation
                IReferenceColumnDao refColumnDao = new ReferenceColumnDaoImpl(adapter);
                List<ReferenceColumn> refColumnList = refColumnDao.getRefColumnListByRefSquidId(toSquid);
                List<ReferenceColumnGroup> groupList = new ArrayList<>();
                ReferenceColumnGroup group = refColumnDao.getRefColumnGroupBySquidId(toSquid, fromSquid);
                if (group != null) {
                    groupList.add(group);
                }
                //删除referenceColumn
                String sql = null;

                //删除reference
                for (Column column : upColumn) {
                    sql = "delete from ds_reference_column where reference_squid_id=" + toSquid + " and column_id = " + column.getId();
                    adapter.execute(sql);
                }
                if (refColumnList != null) {
                    List<Map<String, Object>> idMaps = new ArrayList<>();
                    for (ReferenceColumn referenceColumn : refColumnList) {
                        sql = "select id from ds_transformation where squid_id=" + toSquid + " and column_id=" + referenceColumn.getColumn_id() + "";
                        //删除link
                        List<Map<String, Object>> idMap = adapter.query2List(true, sql, null);
                        if (idMap != null && idMap.size() > 0) {
                            idMaps.addAll(idMap);
                        }
//                        sql = "delete from ds_transformation where squid_id=" + toSquid + " and column_id=" + referenceColumn.getColumn_id();
//                        adapter.execute(sql);
                    }

                    //删除referenceColumn
                    List<Integer> ids = new ArrayList<>();
                    for (Map<String, Object> idMap : idMaps) {
                        ids.add(Integer.parseInt(idMap.get("ID") + ""));
                    }
                    if(ids!=null&&ids.size()>0){
                        String sqlId = JsonUtil.toJSONString(ids).replaceAll("\\[", "(").replaceAll("]", ")");
                        //更新左边的tranformationInput的source_transform_id=0
                        sql="update ds_tran_inputs set source_transform_id=0 where transformation_id in (select dt.id from ds_transformation dt,ds_transformation_link dtl where dt.id = dtl.to_transformation_id and dtl.from_transformation_id in "+sqlId+")";
                        adapter.execute(sql);
                        sql = "delete from ds_transformation_link where from_transformation_id in " + sqlId;
                        adapter.execute(sql);
                        sql = "delete from ds_transformation where id in" + sqlId;
                        adapter.execute(sql);
                        //删除右边transformationInput
                        sql = "delete from ds_tran_inputs where transformation_id in "+sqlId;
                        adapter.execute(sql);

                    }
                }
                //添加referenceColumn
                if (groupList == null || groupList.size() == 0) {
                    ReferenceColumnGroup refGroup = new ReferenceColumnGroup();
                    refGroup.setReference_squid_id(toSquid);
                    refGroup.setRelative_order(1);
                    refGroup.setId(adapter.insert2(refGroup));
                    groupList.add(refGroup);
                }
                for (Column column : columnList) {
                    service.initReference(adapter, column, column.getId(), column.getRelative_order(), upSquid, toSquid, groupList.get(0));
                    service.initTransformation(adapter, toSquid, column.getId(), TransformationTypeEnum.VIRTUAL.value(), column.getData_type(), column.getRelative_order());
                }

            }

        }
    }
}
