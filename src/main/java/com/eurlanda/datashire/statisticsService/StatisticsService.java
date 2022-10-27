package com.eurlanda.datashire.statisticsService;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.dao.IColumnDao;
import com.eurlanda.datashire.dao.ISquidLinkDao;
import com.eurlanda.datashire.dao.ITransformationDao;
import com.eurlanda.datashire.dao.impl.ColumnDaoImpl;
import com.eurlanda.datashire.dao.impl.SquidLinkDaoImpl;
import com.eurlanda.datashire.dao.impl.TransformationDaoImpl;
import com.eurlanda.datashire.entity.*;
import com.eurlanda.datashire.entity.operation.BeyondSquidException;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.enumeration.TransformationTypeEnum;
import com.eurlanda.datashire.enumeration.datatype.SystemDatatype;
import com.eurlanda.datashire.server.utils.TokenUtil;
import com.eurlanda.datashire.sprint7.service.squidflow.TransformationService;
import com.eurlanda.datashire.userDefinedService.UserDefinedServiceImpl;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Eurlanda on 2017/5/5.
 */
@Service
public class StatisticsService {
    private Logger logger = Logger.getLogger(StatisticsService.class);
    private String key;
    private String token;

    /**
     * 创建squid（根据name，生成的dataMapColumn,parameterColumn,column）
     *
     * @param info
     * @return
     */
    public String createStatisticsSquid(String info) {
        ReturnValue out = new ReturnValue();
        Map<String, Object> map = new HashMap<String, Object>();
        IRelationalDataManager adapter = null;
        try {
            Map<String, Object> data = JsonUtil.toHashMap(info);
            StatisticsSquid statisticsSquid = JsonUtil.toGsonBean(data.get("StatisticsSquid") + "", StatisticsSquid.class);
            if (statisticsSquid != null) {
                adapter = DataAdapterFactory.getDefaultDataManager();
                adapter.openSession();
                statisticsSquid.setId(adapter.insert2(statisticsSquid));
                //查询出所有的算法名称
                //读取所有的类的相关信息()
                String sql = "select statistics_name from ds_statistics_definition";
                List<Map<String, Object>> classNames = adapter.query2List(true, sql, null);
                List<String> classNameList = new ArrayList<>();
                if (classNames != null)  {
                    for (Map<String, Object> className : classNames) {
                        classNameList.add(className.get("STATISTICS_NAME") + "");
                    }
                }
                statisticsSquid.setStatisticsNames(classNameList);
                map.put("StatisticsSquid", statisticsSquid);
            } else {
                out.setMessageCode(MessageCode.DESERIALIZATION_FAILED);
            }
        } catch (BeyondSquidException e) {
            try {
                if (adapter != null) adapter.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
            out.setMessageCode(MessageCode.ERR_SQUID_COUNT_MAX);
            logger.error("创建StatisticsSquid异常", e);
        } catch (Exception e) {
            e.printStackTrace();
            try {
                if (adapter != null) adapter.rollback();
            } catch (SQLException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            out.setMessageCode(MessageCode.INSERT_ERROR);
            logger.error("创建StatisticsSquid异常", e);
        } finally {
            if (adapter != null) adapter.closeSession();
        }
        return JsonUtil.toJsonString(map, DSObjectType.STATISTICS, out.getMessageCode());
    }

    /**
     * 更新squid
     *
     * @param info
     * @return
     */
    public String updateStatisticsSquid(String info) {
        ReturnValue out = new ReturnValue();
        IRelationalDataManager adapter = null;
        Map<String, Object> paramsMap = JsonUtil.toHashMap(info);
        try {
            StatisticsSquid statisticsSquid = JsonUtil.toGsonBean(paramsMap.get("StatisticsSquid") + "", StatisticsSquid.class);
            if (null != statisticsSquid) {
                adapter = DataAdapterFactory.getDefaultDataManager();
                adapter.openSession();
                boolean flag = adapter.update2(statisticsSquid);
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
            } catch (Exception e1) {
                e1.printStackTrace();
            }
            out.setMessageCode(MessageCode.UPDATE_ERROR);
            logger.error("更新StatisticsSquid异常", e);
        } finally {
            if (adapter != null) {
                adapter.closeSession();
            }
        }
        return JsonUtil.toJsonString(new HashMap<String, Object>(), DSObjectType.STATISTICS, out.getMessageCode());
    }

    /**
     * 创建squidLink（同步下游创建referenceColumn）
     *
     * @param info
     * @return
     */
    public String createStatisticsSquidLink(String info) {
        ReturnValue out = new ReturnValue();
        UserDefinedServiceImpl impl = new UserDefinedServiceImpl();
        return impl.createSquidLink(info, out);
    }

    /**
     * 删除squidLink(将dataMapColumn置为空，同时将referenceColumn,Transformation删除)
     *
     * @param info
     * @return
     */
    public String deleteStatisticsSquidLink(String info) throws Exception {
        UserDefinedServiceImpl impl = new UserDefinedServiceImpl();
        return impl.deleteSquidLink(info);
    }

    /**
     * 更新dataMapColumn
     *
     * @param info
     * @return
     */
    public String updateStatisticsMappingColumns(String info) {
        ReturnValue out = new ReturnValue();
        IRelationalDataManager adapter = null;
        Map<String, Object> paramsMap = JsonUtil.toHashMap(info);
        try {
            List<StatisticsDataMapColumn> statisticsDataMapColumns = JsonUtil.toGsonList(paramsMap.get("UpdateColumns") + "", StatisticsDataMapColumn.class);
            if (statisticsDataMapColumns != null && statisticsDataMapColumns.size() > 0) {
                for (StatisticsDataMapColumn statisticsDataMapColumn : statisticsDataMapColumns) {
                    adapter = DataAdapterFactory.getDefaultDataManager();
                    adapter.openSession();
                    adapter.update2(statisticsDataMapColumn);
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
            logger.error("更新StatisticsMapColumn异常", e);
        } finally {
            if (adapter != null) {
                adapter.closeSession();
            }
        }
        return JsonUtil.toJsonString(new HashMap<String, Object>(), DSObjectType.STATISTICS, out.getMessageCode());
    }

    /**
     * 更新paramColumn
     *
     * @param info
     * @return
     */
    public String updateStatisticsParameterColumns(String info) {
        ReturnValue out = new ReturnValue();
        IRelationalDataManager adapter = null;
        Map<String, Object> paramsMap = JsonUtil.toHashMap(info);
        try {
            List<StatisticsParameterColumn> statisticsParameterColumns = JsonUtil.toGsonList(paramsMap.get("UpdateColumns") + "", StatisticsParameterColumn.class);
            if (statisticsParameterColumns != null && statisticsParameterColumns.size() > 0) {
                for (StatisticsParameterColumn statisticsParameterColumn : statisticsParameterColumns) {
                    adapter = DataAdapterFactory.getDefaultDataManager();
                    adapter.openSession();
                    adapter.update2(statisticsParameterColumn);
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
            logger.error("更新StatisticsParameterColumn异常", e);
        } finally {
            if (adapter != null) {
                adapter.closeSession();
            }
        }
        return JsonUtil.toJsonString(new HashMap<String, Object>(), DSObjectType.STATISTICS, out.getMessageCode());
    }

    /**
     * 创建dataMapColumn
     *
     * @param info
     * @return
     */
    public String createDataMappingColumn(String info) {
        ReturnValue out = new ReturnValue();
        IRelationalDataManager adapter = null;
        Map<String, Object> returnMap = new HashMap<>();
        try {
            Map<String, Object> paramMap = JsonUtil.toHashMap(info);
            StatisticsDataMapColumn dataMapColumn = JsonUtil.toGsonBean(paramMap.get("StatisticsDatamapColumn") + "", StatisticsDataMapColumn.class);
            if (dataMapColumn != null) {
                adapter = DataAdapterFactory.getDefaultDataManager();
                adapter.openSession();
                int newColumnId = adapter.insert2(dataMapColumn);
                returnMap.put("newColumnId", newColumnId);
            }
        } catch (Exception e) {
            e.printStackTrace();
            out.setMessageCode(MessageCode.SQL_ERROR);
        } finally {
            if (adapter != null) {
                adapter.closeSession();
            }
        }
        return JsonUtil.toJsonString(returnMap, DSObjectType.STATISTICS, out.getMessageCode());
    }

    /**
     * 删除dataMapColumn
     *
     * @param info
     * @return
     */
    public String deleteDataMappingColumn(String info) {
        ReturnValue out = new ReturnValue();
        IRelationalDataManager adapter = null;
        try {
            Map<String, Object> paramMap = JsonUtil.toHashMap(info);
            StatisticsDataMapColumn dataMapColumn = JsonUtil.toGsonBean(paramMap.get("StatisticsDatamapColumn") + "", StatisticsDataMapColumn.class);
            if (dataMapColumn != null) {
                adapter = DataAdapterFactory.getDefaultDataManager();
                adapter.openSession();
                Map<String, String> returnMap = new HashMap<>();
                returnMap.put("id", dataMapColumn.getId() + "");
                adapter.delete(returnMap, StatisticsDataMapColumn.class);
            }
        } catch (Exception e) {
            e.printStackTrace();
            out.setMessageCode(MessageCode.SQL_ERROR);
        } finally {
            if (adapter != null) {
                adapter.closeSession();
            }
        }
        return JsonUtil.toJsonString(new HashMap<>(), DSObjectType.STATISTICS, out.getMessageCode());
    }

    /**
     * 更改算法
     *
     * @param info
     * @return
     */
    public String changeStatisticsSquid(String info) {
        IRelationalDataManager adapter = null;
        ReturnValue out = new ReturnValue();
        Map<String, Object> returnMap = new HashMap<>();
        try {
            Map<String, Object> paramMap = JsonUtil.toHashMap(info);
            int squidId = (int) paramMap.get("squidId");
            String statisticsName = paramMap.get("StatisticsName") + "";
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            IColumnDao columnDao = new ColumnDaoImpl(adapter);
            ITransformationDao transDao = new TransformationDaoImpl(adapter);
            ISquidLinkDao squidLinkDao = new SquidLinkDaoImpl(adapter);
            //将之前的所有的dataMap,parameter,referenceColumn，column删掉
            String sql = "delete from ds_statistics_datamap_column where squid_id=" + squidId;
            adapter.execute(sql);
            sql = "delete from ds_statistics_parameters_column where squid_id=" + squidId;
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
            String updateName = "update ds_squid set statistics_name = ? where id= ?";
            List<Object> paramsList = new ArrayList<>();
            paramsList.add(statisticsName);
            paramsList.add(squidId);
            adapter.execute(updateName, paramsList);
            //根据类名获取类中dataMap，parameter，outMaping相关信息，生成mappingColumn信息，referenceColumn,column信息
            sql = "select * from ds_statistics_definition where statistics_name =  ?";
            List<Object> sqlMap = new ArrayList<>();
            sqlMap.add(statisticsName);
            logger.debug("算法名为:" + statisticsName);
            returnMap = adapter.query2Object(true, sql, sqlMap);
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
            List<StatisticsDataMapColumn> statisticsDataMapColumnList = new ArrayList<>();
            List<StatisticsParameterColumn> parameterColumnList = new ArrayList<>();
            List<Column> newColumnList = new ArrayList<>();
            List<Transformation> transformationList = new ArrayList<>();
            TransformationService service = new TransformationService(TokenUtil.getToken());
            //生成dataMappingColumn信息
            if (dataMapList != null) {
                for (Map<String, Object> dataMap : dataMapList) {
                    StatisticsDataMapColumn dataMapColumn = new StatisticsDataMapColumn();
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
                    statisticsDataMapColumnList.add(dataMapColumn);
                }
            }
            //生成parameter信息
            if (parameterList != null) {
                for (Map<String, Object> paramsMap : parameterList) {
                    StatisticsParameterColumn parameterColumn = new StatisticsParameterColumn();
                    if (paramsMap.containsKey("name")) {
                        parameterColumn.setName(paramsMap.get("name") + "");
                    }
                    if (paramsMap.containsKey("description")) {
                        parameterColumn.setDescription(paramsMap.get("description") + "");
                    }
                    if (paramsMap.containsKey("type")) {
                        parameterColumn.setType(Integer.parseInt(paramsMap.get("type") + ""));
                    }
                    if (paramsMap.containsKey("precision")) {
                        parameterColumn.setPrecision((int) paramsMap.get("precision"));
                    }
                    if (paramsMap.containsKey("scale")) {
                        parameterColumn.setScale((int) paramsMap.get("scale"));
                    }
                    if ("OneWayANOVA".equals(statisticsName)) {
                        parameterColumn.setValue("0.05");
                    }
                    if ("PCA".equals(statisticsName)) {
                        parameterColumn.setValue("1");
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
                    if (outMap.containsKey("length")) {
                        column.setLength(Integer.parseInt(outMap.get("length") + ""));
                    }
                    column.setSquid_id(squidId);
                    column.setRelative_order(i);
                    column.setId(adapter.insert2(column));
                    newColumnList.add(column);
                    //生成Transformation
                    Transformation transformation = service.initTransformation(adapter, squidId, column.getId(), TransformationTypeEnum.VIRTUAL.value(), column.getData_type(), i);
                    transformationList.add(transformation);
                }
            }
            returnMap.clear();
            returnMap.put("StatisticsDatamapColumns", statisticsDataMapColumnList);
            returnMap.put("StatisticsParametersColumns", parameterColumnList);
            returnMap.put("ColumnList", newColumnList);
            //查询出所有的transformation
            transformationList.addAll(transDao.getTransListBySquidId(squidId));
            returnMap.put("TransformationList", transformationList);

            //查询出squid信息，同步下游
            Map<String, Object> paramaMap = new HashMap<>();
            paramaMap.put("from_squid_id", squidId);
            List<SquidLink> squidLinkList = adapter.query2List2(true, paramaMap, SquidLink.class);
            if (squidLinkList != null) {
                UserDefinedServiceImpl impl = new UserDefinedServiceImpl();
                for (SquidLink squidLink : squidLinkList) {
                    impl.synchronizedNextSquidColumn(squidLink, adapter, oldColumnList);
                }
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
        if (returnMap == null) {
            returnMap = new HashMap<>();
        }
        return JsonUtil.toJsonString(returnMap, DSObjectType.STATISTICS, out.getMessageCode());
    }
}
