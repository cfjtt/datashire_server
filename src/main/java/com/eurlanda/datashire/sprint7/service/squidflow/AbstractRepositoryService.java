package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.dao.IColumnDao;
import com.eurlanda.datashire.dao.ITransformationInputsDao;
import com.eurlanda.datashire.dao.impl.ColumnDaoImpl;
import com.eurlanda.datashire.dao.impl.TransformationInputsDaoImpl;
import com.eurlanda.datashire.entity.*;
import com.eurlanda.datashire.entity.operation.BeyondSquidException;
import com.eurlanda.datashire.enumeration.DMLType;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.enumeration.MessageBubbleCode;
import com.eurlanda.datashire.enumeration.TransformationTypeEnum;
import com.eurlanda.datashire.server.utils.Constants;
import com.eurlanda.datashire.sprint7.packet.InfoPacket;
import com.eurlanda.datashire.sprint7.service.squidflow.subservice.CheckExtractService;
import com.eurlanda.datashire.sprint7.service.squidflow.subservice.ExtractService;
import com.eurlanda.datashire.sprint7.service.squidflow.subservice.MessageBubbleService;
import com.eurlanda.datashire.sprint7.service.user.subservice.AbstractService;
import com.eurlanda.datashire.utility.AnnotationHelper;
import com.eurlanda.datashire.utility.CommonConsts;
import com.eurlanda.datashire.utility.DatabaseException;
import com.eurlanda.datashire.utility.DateUtil;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import com.eurlanda.datashire.utility.StringUtil;
import com.eurlanda.datashire.utility.StringUtils;
import com.eurlanda.datashire.validator.SquidValidationTask;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 原子业务抽象类(仓库相关)
 *
 * @author dang.lu 2013.11.12
 */
public abstract class AbstractRepositoryService {
    protected static final Logger logger = LogManager
            .getLogger(AbstractRepositoryService.class);
    /**
     * adapter 静态工厂，子类可以用来产生任意数据源
     */
    protected static final DataAdapterFactory adapterFactory = DataAdapterFactory
            .newInstance();
    /**
     * 系统数据库adapter，不同仓库需根据用户当前token获取
     */
    protected final IRelationalDataManager adapter;
    protected final String token;

    /**
     * 只提供子类实现，通过token获取adapter
     */
    protected AbstractRepositoryService(String token) {
        if (StringUtils.isNull(token)) {
            logger.error("token is null!");
        }
        this.token = token;
        this.adapter = DataAdapterFactory.getDefaultDataManager();
    }

    /**
     * 只提供子类实现，通过参数传递，防止重复生成
     */
    protected AbstractRepositoryService(IRelationalDataManager adapter) {
        if (adapter == null) {
            logger.error("adapter is null!");
        }
        this.token = null;
        this.adapter = adapter;
    }

    /**
     * 只提供子类实现，通过参数传递，防止重复生成
     */
    protected AbstractRepositoryService(String token,
                                        IRelationalDataManager adapter) {
        if (StringUtils.isNull(token) && adapter == null) {
            logger.error("token and adapter is null!");
        }
        this.token = token;
        if (adapter == null) {
            this.adapter = DataAdapterFactory.getDefaultDataManager();
        } else {
            this.adapter = adapter;
        }
    }

    /**
     * 通用查询全部
     */
    public List getAll(Class<? extends DSBaseModel> c) {
        String clsName = c.getName();
        if (AbstractService.SQL_Cache.get(clsName) == null) {
            AbstractService.SQL_Cache.put(clsName,
                    "SELECT " + AnnotationHelper.cols2Str(c) + " FROM "
                            + AnnotationHelper.getTableName(c));
        }
        return adapter.query2List(AbstractService.SQL_Cache.get(clsName), null,
                c);
    }

    /**
     * 通用查询全部
     */
    public <T> List<T> getAll(String sql, Class<T> c) {
        return adapter.query2List(sql, null, c);
    }

    /**
     * 根据ID查找对象
     *
     * @param c
     * @param id
     * @return
     */
    public <T> T getOne(Class<T> c, int id) {
        Map<String, String> params = new HashMap<String, String>();
        params.put("ID", Integer.toString(id, 10));
        return adapter.query2Object(params, c);
    }

    public <T> List<T> getIdByKey(List<String> key,
                                  Class<? extends DSBaseModel> c) {
        return null;
    }

    public int getIdByKey(String key, Class c) {
        return -1;
    }

    /**
     * 通用添加/修改 支持混合对象处理(即入参列表可以不是同一种类型对象)
     *
     * @param beans
     * @return List<InfoPacket> (包括对象ID、key、code、type等信息)
     */
    public List<InfoPacket> save(List<? extends DSBaseModel> beans, int t) {
        List<InfoPacket> list = new ArrayList<InfoPacket>();
        if (beans == null || beans.isEmpty())
            return list; // 如果校验入参为空，或操作类型不支持，则不处理直接返回
        DSBaseModel bean;
        InfoPacket info;
        boolean succeed = true;
        try {
            adapter.openSession();
            for (int i = beans.size() - 1; i >= 0; i--) {
                bean = beans.get(i);
                logger.info(MessageFormat.format("{0}: {1}", DMLType.parse(t),
                        bean));
                if (bean == null)
                    continue;
                info = new InfoPacket(); // AvoidInstantiatingObjectsInLoops,Avoid
                // this whenever you can, it's really
                // expensive
                info.setKey(bean.getKey());
                info.setType(AbstractService.getType(bean));
                if (t == DMLType.INSERT.value()) {
                    info.setId(adapter.insert2(bean));
                    info.setCode((info.getId() > 0 ? MessageCode.SUCCESS
                            : MessageCode.SQL_ERROR).value());
                    CommonConsts.addValidationTask(new SquidValidationTask(token, adapter, MessageBubbleService.setMessageBubble(info.getId(), info.getId(), null, MessageBubbleCode.WARN_SQUID_NO_LINK.value())));
                    CommonConsts.addValidationTask(new SquidValidationTask(token, adapter, MessageBubbleService.setMessageBubble(info.getId(), info.getId(), bean.getName(), MessageBubbleCode.ERROR_SQL_SYNTAX_FILTER.value())));
                } else if (t == DMLType.UPDATE.value()) {
                    info.setId(bean.getId());
                    info.setCode(adapter.update2(bean) ? MessageCode.SUCCESS
                            .value() : MessageCode.SQL_ERROR.value());
                    // extract 过滤条件校验
                    if (!DSObjectType.TRANSFORMATION.equals(info.getType())) {
                        CommonConsts.addValidationTask(new SquidValidationTask(token, adapter, MessageBubbleService.setMessageBubble(info.getId(), info.getId(), bean.getName(), MessageBubbleCode.ERROR_SQL_SYNTAX_INCREMENT.value())));
                        CommonConsts.addValidationTask(new SquidValidationTask(token, adapter, MessageBubbleService.setMessageBubble(info.getId(), info.getId(), bean.getName(), MessageBubbleCode.WARN_DATASQUID_NO_SQUIDINDEXES.value())));
                        //if (DSObjectType.EXTRACT.equals(info.getType()) || DSObjectType.STAGE.equals(info.getType())) {
                        CommonConsts.addValidationTask(new SquidValidationTask(token, adapter, MessageBubbleService.setMessageBubble(info.getId(), info.getId(), bean.getName(), MessageBubbleCode.ERROR_SQL_SYNTAX_FILTER.value())));
                        //}
                    }
                    if (DSObjectType.STAGE.equals(info.getType())
                            || DSObjectType.DATAMINING.equals(info.getType())) {
                        CommonConsts.addValidationTask(new SquidValidationTask(token, adapter, MessageBubbleService.setMessageBubble(info.getId(), info.getId(), bean.getName(), MessageBubbleCode.WARN_DATA_SQUID_NO_EXCEPTION_SQUID.value())));


                    }
                }
                list.add(info);
            }
        } catch (SQLException e) {
            succeed = false;
            logger.error("[save-sql-exception]", e);
            try { // 事务回滚，很好很强大
                adapter.rollback();
            } catch (SQLException e1) {
                logger.error("rollback err!", e1);
            }
        } finally {
            adapter.closeSession();
            if (!succeed) { // 如果新增/更新失败，则封装原信息返回，并将code设为：SQL_ERROR
                list.clear();
                for (int i = beans.size() - 1; i >= 0; i--) {
                    bean = beans.get(i);
                    info = new InfoPacket();
                    info.setKey(bean.getKey());
                    info.setType(AbstractService.getType(bean));
                    info.setId(bean.getId());
                    info.setCode(MessageCode.SQL_ERROR.value());
                    list.add(info);
                }
            }
        }
        return list;
    }

    /**
     * 通用添加/修改 支持混合对象处理(即入参列表可以不是同一种类型对象)
     *
     * @param beans
     * @return List<InfoPacket> (包括对象ID、key、code、type等信息)
     */
    public void saveAll(List<? extends DSBaseModel> beans, int t) {
        DSBaseModel bean;
        int execuResult = 0;
        boolean succeed = false;
        try {
            adapter.openSession();
            for (int i = beans.size() - 1; i >= 0; i--) {
                bean = beans.get(i);
                logger.info(MessageFormat.format("{0}: {1}", DMLType.parse(t),
                        bean));
                if (t == DMLType.INSERT.value()) {
                    execuResult = adapter.insert2(bean);
                } else if (t == DMLType.UPDATE.value()) {
                    succeed = adapter.update2(bean);
                }
            }
        } catch (SQLException e) {
            logger.error("[save-sql-exception]", e);
            try { // 事务回滚，很好很强大
                adapter.rollback();
            } catch (SQLException e1) {
                logger.error("rollback err!", e1);
            }
        } finally {
            adapter.closeSession();
            if (execuResult <= 0 || !succeed) {
                logger.error("[=====sql failed!=====]");
            }
        }

    }

    /**
     * 单对象更新
     *
     * @param obj
     * @return
     */
    public InfoPacket update(DSBaseModel obj) {
        InfoPacket info = new InfoPacket();
        if (obj == null)
            return info; // 如果校验入参为空，或操作类型不支持，则不处理直接返回
        try {
            logger.info(MessageFormat.format("Update: {0}", obj));
            info.setKey(obj.getKey());
            info.setType(AbstractService.getType(obj));
            info.setId(obj.getId());
            adapter.openSession();
            info.setCode(adapter.update2(obj) ? MessageCode.SUCCESS.value()
                    : MessageCode.SQL_ERROR.value());
            // 推送消息泡
            CommonConsts.addValidationTask(new SquidValidationTask(token, adapter, MessageBubbleService.setMessageBubble(obj.getId(), obj.getId(), obj.getName(), MessageBubbleCode.ERROR_SQL_SYNTAX_FILTER.value())));
            CommonConsts.addValidationTask(new SquidValidationTask(token, adapter, MessageBubbleService.setMessageBubble(obj.getId(), obj.getId(), obj.getName(), MessageBubbleCode.ERROR_SQUID_CONNECTION_NO_HOST.value())));
            CommonConsts.addValidationTask(new SquidValidationTask(token, adapter, MessageBubbleService.setMessageBubble(obj.getId(), obj.getId(), obj.getName(), MessageBubbleCode.ERROR_SQUID_CONNECTION_NO_RDBMSTYPE.value())));
            CommonConsts.addValidationTask(new SquidValidationTask(token, adapter, MessageBubbleService.setMessageBubble(obj.getId(), obj.getId(), obj.getName(), MessageBubbleCode.ERROR_SQUID_CONNECTION_NO_DATABASE_NAME.value())));
        } catch (SQLException e) {
            logger.error("[save-sql-exception]", e);
            try { // 事务回滚，很好很强大
                adapter.rollback();
            } catch (SQLException e1) {
                logger.error("rollback err!", e1);
            }
        } finally {
            adapter.closeSession();
        }
        return info;
    }

    /**
     * 单对象新增
     *
     * @param obj
     * @return
     */
    public InfoPacket add(DSBaseModel obj) {
        InfoPacket info = new InfoPacket();
        if (obj == null)
            return info; // 如果校验入参为空，或操作类型不支持，则不处理直接返回
        try {
            logger.info(MessageFormat.format("Add: {0}", obj));
            info.setKey(obj.getKey());
            info.setType(AbstractService.getType(obj));
            adapter.openSession();
            info.setId(adapter.insert2(obj));
            info.setCode((info.getId() > 0 ? MessageCode.SUCCESS
                    : MessageCode.SQL_ERROR).value());
        } catch (SQLException e) {
            logger.error("[save-sql-exception]", e);
            try { // 事务回滚，很好很强大
                adapter.rollback();
            } catch (SQLException e1) {
                logger.error("rollback err!", e1);
            }
        } finally {
            adapter.closeSession();
        }
        return info;
    }

    /**
     * 通用添加/修改 支持混合对象处理(即入参列表可以不是同一种类型对象)
     * 目前支持ExtractSquid, SquidLink
     *
     * @param <T>
     * @param executeTtype .INSERT/UPDATE
     * @return List<InfoPacket> (包括对象ID、key、code、type等信息)
     */
    public <T> Map<String, Object> execute(String info,
                                           Map<String, Class<?>> paramsMap,
                                           int executeTtype,
                                           ReturnValue out) {
/*        Object obj = JsonUtil.object2HashMap(info, mainClass);
        obj.getClass();*/
        Map<String, Object> resultMap = new HashMap<String, Object>();
        DSBaseModel bean = null;
        DataSquid dataSquid = null;
        boolean isIndexed = false;
        boolean succeed = true;
        // squid的Type
        int dsObjectType = 0;
        // 新增Squid的ID
        int newSquidId = 0;
        // 新增的SquidLink的Id
        int newSquidLinkId = 0;
        int squidId = 0;
        String name = null;
        // 新增SquidLink
        SquidLink squidLink = null;
        try {
            adapter.openSession();
            Object obj;
            int count = 0;
            int sourceTableId = 0;
            Boolean updateFlag = false;
            for (Map.Entry<String, Class<?>> entry : paramsMap.entrySet()) {
                count++;
                updateFlag = false;
                //String key = entry.getKey().toString();
                //Object ob = entry.getValue();
                obj = JsonUtil.object2HashMap(info, entry.getValue());
                //logger.info(MessageFormat.format("{0}: {1}", DMLType.parse(executeTtype), obj));
                dsObjectType = AbstractService.getDSObjectType(obj).value();
                // 如果是新增ExtractSquid
                if (executeTtype == DMLType.INSERT.value()
                        && (dsObjectType == DSObjectType.DOC_EXTRACT.value()
                        || dsObjectType == DSObjectType.XML_EXTRACT.value()
                        || dsObjectType == DSObjectType.WEBLOGEXTRACT.value()
                        || dsObjectType == DSObjectType.WEIBOEXTRACT.value()
                        || dsObjectType == DSObjectType.WEBEXTRACT.value()
                        || dsObjectType == DSObjectType.DATAMINING.value()
                        || dsObjectType == DSObjectType.SQUIDINDEXS.value()
                        || dsObjectType == DSObjectType.REPORT.value()
                        || dsObjectType == DSObjectType.HTTPEXTRACT.value()
                        || dsObjectType == DSObjectType.WEBSERVICEEXTRACT.value()
                        || dsObjectType == DSObjectType.GISMAP.value())) {
                    newSquidId = adapter.insert2(obj);
                    resultMap.put("newSquidId", newSquidId);
                    if (dsObjectType != DSObjectType.SQUIDINDEXS.value()) {
                        bean = (DSBaseModel) obj;
                        // 消息泡验证
                        // 更新SourceTabel中的is_extracted
                        if (dsObjectType == DSObjectType.DOC_EXTRACT.value()) {
                            DocExtractSquid docExtractSquid = (DocExtractSquid) obj;
                            sourceTableId = docExtractSquid.getSource_table_id();
                            updateFlag = true;
                        } else if (dsObjectType == DSObjectType.XML_EXTRACT.value()
                                || dsObjectType == DSObjectType.WEBLOGEXTRACT.value()
                                || dsObjectType == DSObjectType.WEIBOEXTRACT.value()
                                || dsObjectType == DSObjectType.WEBEXTRACT.value()
                                || dsObjectType == DSObjectType.HTTPEXTRACT.value()
                                || dsObjectType == DSObjectType.WEBSERVICEEXTRACT.value()) {
                            dataSquid = (DataSquid) obj;
                            sourceTableId = dataSquid.getSource_table_id();
                            updateFlag = true;
                        }
                        if (sourceTableId > 0) {
                            String sql = "update ds_source_table set is_extracted = 'Y' where id = " + sourceTableId;
                            if (updateFlag) {
                                adapter.execute(sql);
                            }
                        }
                        CommonConsts.addValidationTask(new SquidValidationTask(token, adapter, MessageBubbleService.setMessageBubble(newSquidId, newSquidId, bean.getName(), MessageBubbleCode.WARN_SQUID_NO_LINK.value())));
                    }
                    //创建cloumn针对WebServiceExtract
                    if (DSObjectType.WEBSERVICEEXTRACT.value() == dsObjectType) {//如果是webservice择创建column :t1 判断是否为restful
                        squidLink = JsonUtil.object2HashMap(info, SquidLink.class);
                        WebserviceExtractSquid wes = (WebserviceExtractSquid) obj;
                        if (squidLink != null && wes != null) {
                            Map<String, String> params = new HashMap<String, String>();
                            params.put("id", squidLink.getFrom_squid_id() + "");
                            WebserviceSquid ws = adapter.query2Object2(true, params, WebserviceSquid.class);
                            wes.setId(newSquidId);
                            if (ws != null && !ws.getIs_restful()) {//:t1
//		                       	   wes.setId(newSquidId);///////操操操操操操！
                                IColumnDao columnDao = new ColumnDaoImpl(adapter);
                                List<SourceColumn> sourceColumnList = columnDao.findSourceColumnBySourceTableId(sourceTableId);
                                //这里使用link查询父节点来进行判断是否需要现在创建column
                                this.createColumnBySourceColumn(wes, sourceColumnList, adapter, token);
                            }
                            this.createThirdPartyParams(wes);
                            ExtractService.setDSFCS(adapter, wes);
                            resultMap.put("HeaderParams", wes.getHeaderParams());
                            resultMap.put("ContentParams", wes.getContentParams());
                            resultMap.put("UrlParams", wes.getUrlParams());
                        }
                    } else if (DSObjectType.HTTPEXTRACT.value() == dsObjectType) {
                        squidLink = JsonUtil.object2HashMap(info, SquidLink.class);
                        HttpExtractSquid hess = (HttpExtractSquid) obj;
                        if (squidLink != null && hess != null) {
                            Map<String, String> params = new HashMap<String, String>();
                            params.put("id", squidLink.getFrom_squid_id() + "");
                            hess.setId(newSquidId);
                            this.createThirdPartyParams(hess);
                            ExtractService.setDSFCS(adapter, hess);
                            resultMap.put("ContentParams", hess.getContentParams());
                            resultMap.put("UrlParams", hess.getUrlParams());
                        }
                    }
                    if (dsObjectType == DSObjectType.WEBLOGEXTRACT.value()) {
                        CommonConsts.addValidationTask(new SquidValidationTask(token, adapter, MessageBubbleService.setMessageBubble(newSquidId, newSquidId, bean.getName(), MessageBubbleCode.ERROR_WEBLOG_EXTRACT_SQUID_NO_SOURCE_DATA.value())));
                    }
                    if (dsObjectType == DSObjectType.REPORT.value()) {
                        CommonConsts.addValidationTask(new SquidValidationTask(token, adapter, MessageBubbleService.setMessageBubble(newSquidId, newSquidId, bean.getName(), MessageBubbleCode.WARN_REPORT_SQUID_NO_PUBLISHING_FOLDER.value())));
                        CommonConsts.addValidationTask(new SquidValidationTask(token, adapter, MessageBubbleService.setMessageBubble(newSquidId, newSquidId, bean.getName(), MessageBubbleCode.ERROR_REPORT_SQUID_NO_TEMPLATE_DEFINITION.value())));
                    }
                    if (dsObjectType == DSObjectType.GISMAP.value()) {
                        CommonConsts.addValidationTask(new SquidValidationTask(token, adapter, MessageBubbleService.setMessageBubble(newSquidId, newSquidId, bean.getName(), MessageBubbleCode.WARN_GISMAP_SQUID_NO_PUBLISHING_FOLDER.value())));
                        CommonConsts.addValidationTask(new SquidValidationTask(token, adapter, MessageBubbleService.setMessageBubble(newSquidId, newSquidId, bean.getName(), MessageBubbleCode.ERROR_GISMAP_SQUID_NO_TEMPLATE_DEFINITION.value())));
                    }
                } else if (executeTtype == DMLType.UPDATE.value()) {
                    adapter.update2(obj);
                    if (obj instanceof DSBaseModel) {
                        bean = (DSBaseModel) obj;
                        //info.setId(bean.getId());
                        //info.setCode(adapter.update2(bean)?MessageCode.SUCCESS.value():MessageCode.SQL_ERROR.value());
                        newSquidId = bean.getId();
                        resultMap.put("newSquidId", newSquidId);
                    }
                    if (dsObjectType == DSObjectType.DATAMINING.value()) {
                        CommonConsts.addValidationTask(new SquidValidationTask(token, adapter, MessageBubbleService.setMessageBubble(newSquidId, newSquidId, bean.getName(), MessageBubbleCode.WARN_DATA_MININGSQUID_TRAINING_PERCENTAGE.value())));
                    } else if (dsObjectType == DSObjectType.DOC_EXTRACT.value()
                            || dsObjectType == DSObjectType.XML_EXTRACT.value()
                            || dsObjectType == DSObjectType.WEBLOGEXTRACT.value()
                            || dsObjectType == DSObjectType.WEIBOEXTRACT.value()
                            || dsObjectType == DSObjectType.WEBEXTRACT.value()
                            || dsObjectType == DSObjectType.DATAMINING.value()
                            || dsObjectType == DSObjectType.SQUIDINDEXS.value()
                            || dsObjectType == DSObjectType.HTTPEXTRACT.value()
                            || dsObjectType == DSObjectType.WEBSERVICEEXTRACT.value()) {
                        // 校验SquidIndexes
                        CommonConsts.addValidationTask(new SquidValidationTask(token, adapter, MessageBubbleService.setMessageBubble(newSquidId, newSquidId, bean.getName(), MessageBubbleCode.ERROR_SQL_SYNTAX_FILTER.value())));
                        CommonConsts.addValidationTask(new SquidValidationTask(token, adapter, MessageBubbleService.setMessageBubble(newSquidId, newSquidId, bean.getName(), MessageBubbleCode.WARN_DATASQUID_NO_SQUIDINDEXES.value())));
                    }
                    // Business Key 校验
                    // dataSquid索引校验
                           /*if(isIndexed){
                      		new MessageBubbleService(token).validateSquidIndexes(squidId, squidId, name, isIndexed);
                  		}*/
                    // extract 过滤条件校验
                    if (DSObjectType.EXTRACT.value() == dsObjectType
                            || DSObjectType.DOC_EXTRACT.value() == dsObjectType) {
                        //new SQLValidator(token, adapter).checkSquidFilter(bean.getId());
                    }
                    if (dsObjectType == DSObjectType.REPORT.value()) {
                        CommonConsts.addValidationTask(new SquidValidationTask(token, adapter, MessageBubbleService.setMessageBubble(newSquidId, newSquidId, bean.getName(), MessageBubbleCode.WARN_REPORT_SQUID_NO_PUBLISHING_FOLDER.value())));
                        CommonConsts.addValidationTask(new SquidValidationTask(token, adapter, MessageBubbleService.setMessageBubble(newSquidId, newSquidId, bean.getName(), MessageBubbleCode.ERROR_REPORT_SQUID_NO_TEMPLATE_DEFINITION.value())));
                    }
                    if (dsObjectType == DSObjectType.GISMAP.value()) {
                        CommonConsts.addValidationTask(new SquidValidationTask(token, adapter, MessageBubbleService.setMessageBubble(newSquidId, newSquidId, bean.getName(), MessageBubbleCode.WARN_GISMAP_SQUID_NO_PUBLISHING_FOLDER.value())));
                        CommonConsts.addValidationTask(new SquidValidationTask(token, adapter, MessageBubbleService.setMessageBubble(newSquidId, newSquidId, bean.getName(), MessageBubbleCode.ERROR_GISMAP_SQUID_NO_TEMPLATE_DEFINITION.value())));
                    }
                }
                // 如果是新增SquidLink的话，就必须要新增ExtractSquid,得到ExtractSquid的Id,然后设置给SquidLink的to_squid_id
                if (count == paramsMap.size() && executeTtype == DMLType.INSERT.value()
                        && paramsMap.containsKey(SquidLink.class.getSimpleName())) {
                    squidLink = JsonUtil.object2HashMap(info, SquidLink.class);
                    // 设置ExtractSquid的Id
                    squidLink.setTo_squid_id(newSquidId);
                    newSquidLinkId = adapter.insert2(squidLink);
                    resultMap.put("newSquidLinkId", newSquidLinkId);
                    squidLink.setId(newSquidLinkId);
                    //new MessageBubbleService(token).isolateSquid(DMLType.INSERT, DSObjectType.SQUIDLINK, squidLink.getFrom_squid_id(), newSquidId, null, squidLink);
                    CommonConsts.addValidationTask(new SquidValidationTask(token, adapter, MessageBubbleService.setMessageBubble(squidLink.getFrom_squid_id(), squidLink.getFrom_squid_id(), bean.getName(), MessageBubbleCode.WARN_SQUID_NO_LINK.value())));
                    CommonConsts.addValidationTask(new SquidValidationTask(token, adapter, MessageBubbleService.setMessageBubble(newSquidId, newSquidId, bean.getName(), MessageBubbleCode.WARN_SQUID_NO_LINK.value())));
                }
            }
        } catch (BeyondSquidException e) {
            try {
                if (adapter != null) adapter.rollback();
            } catch (SQLException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            out.setMessageCode(MessageCode.ERR_SQUID_COUNT_MAX);
            logger.error("创建异常", e);
        } catch (SQLException e) {
            succeed = false;
            logger.error("[save-sql-exception]", e);
            try {  // 事务回滚，很好很强大
                adapter.rollback();
            } catch (SQLException e1) {
                logger.error("rollback err!", e1);
            }
            out.setMessageCode(MessageCode.SQL_ERROR);
        } catch (DatabaseException e) {
            // TODO Auto-generated catch block
            logger.error("[save-sql-exception]", e);
            try {  // 事务回滚，很好很强大
                adapter.rollback();
            } catch (SQLException e1) {
                logger.error("rollback err!", e1);
            }
            out.setMessageCode(MessageCode.SQL_ERROR);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            logger.error("[save-sql-exception]", e);
            try {  // 事务回滚，很好很强大
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

    /**
     * 创建并装载第三方参数ByWebServiceiceExtractSquid
     * 2014-12-9
     *
     * @param wses
     * @return
     * @throws SQLException
     * @author Akachi
     * @E-Mail zsts@hotmail.com
     */
    public int createThirdPartyParams(DataSquid wses) throws SQLException {
        Integer sourceTableId = wses.getSource_table_id();
        Map<String, String> param = new HashMap<String, String>();
        param.put("id", sourceTableId + "");
        DBSourceTable dst = adapter.query2Object2(true, param, DBSourceTable.class);
        String[] headerXmlParam = StringUtil.getParam(dst.getHeader_xml() == null ? "" : dst.getHeader_xml(), "@", ";");//2
        String[] paramsXmlParam = StringUtil.getParam(dst.getParams_xml() == null ? "" : dst.getParams_xml(), "@", ";");//2
        String[] postParam = StringUtil.getParam(dst.getPost_params() == null ? "" : dst.getPost_params(), "@", ";");//1
        String[] urlParam = StringUtil.getParam(dst.getUrl() == null ? "" : dst.getUrl(), "@", ";");//0
        List<String> listPost = new ArrayList<String>();
        for (int i = 0; i < postParam.length; i++) {//URL与post中参数不能存在相同，因为在WADL中会表明已存在在URL中的参数，如在WSDL中择允许CONTACT与HEADER中同时存在相同参数
            boolean b = false;//默认不存在
            for (int j = 0; j < urlParam.length; j++) {
                if (urlParam[j].equals(postParam[i])) {
                    b = true;
                }
            }
            if (!b) {//如果不存在就加入
                listPost.add(postParam[i]);
            }
        }
        postParam = new String[listPost.size()];
        for (int i = 0; i < listPost.size(); i++) {
            postParam[i] = listPost.get(i);
        }
        List<ThirdPartyParams> thirdPartyParamslist = initThirdPartyParams(wses, headerXmlParam, 1);
        initThirdPartyParams(wses, paramsXmlParam, 2, thirdPartyParamslist);
        initThirdPartyParams(wses, urlParam, 0, thirdPartyParamslist);
        initThirdPartyParams(wses, postParam, 2, thirdPartyParamslist);
        for (ThirdPartyParams thirdPartyParams : thirdPartyParamslist) {
            int id = adapter.insert2(thirdPartyParams);
            thirdPartyParams.setId(id);//tage
        }
        return 0;
    }

    /**
     * 初始化第三方参数创建新的List
     * 2014-12-11
     *
     * @param wses  webservice的实体bean
     * @param names
     * @param type  类型 0=get,1=post,2=content(适用于soap请求)
     * @return
     * @author Akachi
     * @E-Mail zsts@hotmail.com
     */
    private List<ThirdPartyParams> initThirdPartyParams(DataSquid wses, String[] names, int type) {
        List<ThirdPartyParams> ThirdPartyParamslist = new ArrayList<ThirdPartyParams>();
        for (String name : names) {
            ThirdPartyParamslist.add(initThirdPartyParam(wses, name, type));
        }
        return ThirdPartyParamslist;
    }

    /**
     * 初始化第三方参数
     * 2014-12-10
     *
     * @param wses                 webservice的实体bean
     * @param names
     * @param type                 类型 0=get,1=post,2=content(适用于soap请求 ,也使用与restful 只是restful是手动添加的)
     * @param thirdPartyParamslist 已初始化好的第三方参数list
     * @author Akachi
     * @E-Mail zsts@hotmail.com
     */
    private void initThirdPartyParams(DataSquid wses, String[] names, int type, List<ThirdPartyParams> thirdPartyParamslist) {
        for (String name : names) {
            thirdPartyParamslist.add(initThirdPartyParam(wses, name, type));
        }
    }

    /**
     * 初始化第三方参数
     * 2014-12-10
     *
     * @param wses webservice的实体bean
     * @param name
     * @param type 类型 0=get,1=post,2=content(适用于soap请求)
     * @return 返回一个新的结合
     * @author Akachi
     * @E-Mail zsts@hotmail.com
     */
    private ThirdPartyParams initThirdPartyParam(DataSquid wses, String name, int type) {

        ThirdPartyParams tpp = new ThirdPartyParams();
        tpp.setCreation_date(DateUtil.DATE_FULL_TIME_FORMAT.format(new Date()));
        tpp.setKey(StringUtils.generateGUID());
        tpp.setType(DSObjectType.THIRDPARTYPARAMS.value());
        tpp.setName(name);
        tpp.setParams_type(type);
        tpp.setSource_table_id(wses.getSource_table_id());
        tpp.setSquid_id(wses.getId());
        tpp.setVal("");
        //tpp.setVariable_id(0);
        tpp.setValue_type(0);
        return tpp;
    }

    /**
     * 删除所有Column
     * 2014-12-12
     *
     * @param wes
     * @param adapter
     * @return
     * @throws SQLException
     * @author Akachi
     * @E-Mail zsts@hotmail.com
     */
    public static int deleteAllColumnByWebServiceExtract(DataSquid wes, IRelationalDataManager adapter) throws SQLException {
        Map<String, String> param = new HashMap<String, String>();
        param.put("reference_squid_id", wes.getId() + "");
        adapter.delete(param, ReferenceColumn.class);
        param.clear();
        param.put("reference_squid_id", wes.getId() + "");
        adapter.delete(param, ReferenceColumnGroup.class);
        param.clear();
        param.put("squid_id", wes.getId() + "");
        adapter.delete(param, Column.class);
        return 0;
    }

    /**
     * 创建columnBySourceColumn
     * 2014-12-8
     *
     * @param extractSquid
     * @param sourceColumns
     * @return
     * @throws Exception
     * @author Akachi
     * @E-Mail zsts@hotmail.com
     */
    public static int createColumnBySourceColumn(DataSquid extractSquid, List<SourceColumn> sourceColumns, IRelationalDataManager adapter, String token) throws Exception {
        // 创建引用列组
        ReferenceColumnGroup columnGroup = new ReferenceColumnGroup();
        columnGroup.setKey(StringUtils.generateGUID());
        columnGroup.setReference_squid_id(extractSquid.getId());
        columnGroup.setRelative_order(1);
        columnGroup.setGroup_name(extractSquid.getName());
//        extractSquid.getName()
//        columnGroup.setRelative_order(relative_order)
        columnGroup.setId(adapter.insert2(columnGroup));
        List<ReferenceColumn> referenceColumns = new ArrayList<ReferenceColumn>();
        List<Column> columns = new ArrayList<Column>();
        List<Transformation> transformations = new ArrayList<Transformation>();
        List<TransformationLink> transformationLinks = new ArrayList<TransformationLink>();
        int sourceColumnSize = sourceColumns.size();
        Column column = null;
        SourceColumn columnInfo = null;
        ReferenceColumn referenceColumn = null;
        int extractSquidId = extractSquid.getId();
        //Transformation transformation = null;
        int transformationToId = 0;
        TransformationService transformationService = new TransformationService(token);
        int relativeOrder = 1;
        int count = 0;
        ITransformationInputsDao transInputsDao = new TransformationInputsDaoImpl(adapter);
        // updated by yi.zhou bug 1911
        Map<String, String> tempMap = new HashMap<String, String>();
        //删除现有的Transformation transformation;
        cleanTransformation(extractSquidId, transInputsDao);
        // 源列和目标列初始相同，生成后用户可以对目标列进行调整
        for (int tmpIndex = 0; tmpIndex < sourceColumnSize; tmpIndex++) {
            relativeOrder++;
            count = relativeOrder;
            columnInfo = sourceColumns.get(tmpIndex);
            // update DbBaseDataType start by yi.zhou
            // 目标ExtractSquid引用列（变换面板右边，引用db_source_squid）
            referenceColumn = transformationService.initReference(adapter, columnInfo, columnInfo.getId(), count, extractSquid, extractSquidId, columnGroup);
            // 目标ExtractSquid真实列（变换面板左边）
            // 替换掉特殊字符
            columnInfo.setName(StringUtils.replace(columnInfo.getName()));

//			fixed bug 980 end by bo.dang
            column = transformationService.initColumn(adapter, columnInfo, count, extractSquidId, null);
            Transformation transformationLeft = transformationService.initTransformation(adapter, extractSquidId, column.getId(),
                    TransformationTypeEnum.VIRTUAL.value(),
                    column.getData_type(), 1);
            columns.add(column);
            transformations.add(transformationLeft);

            //目标ExtractSquid的引用列类型
            Transformation transformationRight = transformationService.initTransformation(
                    adapter, extractSquidId, referenceColumn.getId(),
                    TransformationTypeEnum.VIRTUAL.value(),
                    referenceColumn.getData_type(), 1);
            transformations.add(transformationRight);
            //update DbBaseDataType end by yi.zhou

            //设置Transformation连线
            transformationToId = transformationLeft.getId();
            int transformationFromId = transformationRight.getId();
            // 创建 transformation link
            TransformationLink transformationLink = transformationService.initTransformationLink(adapter, transformationFromId, transformationToId, count);
            transformationLinks.add(transformationLink);
            // 更新TransformationInputs
            transInputsDao.updTransInputs(transformationLink, transformationLeft);
        }

        extractSquid.setColumns(columns);
        extractSquid.setSourceColumns(referenceColumns);
        extractSquid.setTransformations(transformations);
        extractSquid.setTransformationLinks(transformationLinks);
        // 调用是否抽取方法
        if (StringUtils.isNotNull(extractSquid.getName())) {
            CheckExtractService check = new CheckExtractService(token, adapter);
//            params.put("ID", extractSquid.getId());
//            String sourceKey = adapter.query2Object(true, params, SquidModelBase.class).getKey();
            check.updateExtract(extractSquid.getName(), extractSquid.getId(), "create", null,
                    extractSquid.getKey(), token);
        }

        return 0;
    }

    private static void cleanTransformation(int extractSquidId, ITransformationInputsDao transInputsDao) throws Exception {
        Map<String, String> param = new HashMap<String, String>();
        param.put("squid_id", extractSquidId + "");
        List<Map<String, Object>> listMap = transInputsDao.getObjectForMap(Transformation.class, null, param);
        for (Map<String, Object> map : listMap) {
            Map<String, String> linkParam = new HashMap<String, String>();
            linkParam.put("from_transformation_id", map.get("ID").toString());
            List<Map<String, Object>> linkListMap = transInputsDao.getObjectForMap(TransformationLink.class, null, linkParam);
            for (Map<String, Object> linkMap : linkListMap) {
                transInputsDao.delete(Integer.parseInt(linkMap.get("ID").toString()), TransformationLink.class);
            }
        }
        for (Map<String, Object> map : listMap) {
            Map<String, String> inputsParam = new HashMap<String, String>();
            inputsParam.put("transformation_id", map.get("ID").toString());
            List<Map<String, Object>> inputsListMap = transInputsDao.getObjectForMap(TransformationInputs.class, null, inputsParam);
            for (Map<String, Object> linkMap : inputsListMap) {
                transInputsDao.delete(Integer.parseInt(linkMap.get("ID").toString()), TransformationInputs.class);
            }
//        	TransformationInputs
        }
        for (Map<String, Object> map : listMap) {
            transInputsDao.delete(Integer.parseInt(map.get("ID").toString()), Transformation.class);
        }
    }

    //生成系统名称
    public static String getSysColumnName(String key, Map<String, String> map, int index) {
        if (!map.containsKey(key)) {
            return key;
        } else {
            if (Constants.DEFAULT_EXTRACT_COLUMN_NAME.equals(key)) {
                key = "extraction_date_biz";
            } else {
                key = key + index;
                index = index + 1;
            }
            return getSysColumnName(key, map, index);
        }
    }

    /**
     * 单对象新增(直接返回对象ID)
     *
     * @param obj
     * @return
     */
    public int add2(DSBaseModel obj) {
        InfoPacket info = add(obj);
        return info == null ? -1 : info.getId();
    }

    public IRelationalDataManager getAdapter() {
        return adapter;
    }

    /**
     * @param toSquidId
     * @return
     * @author bo.dang
     * @date 2014年5月19日
     */
    public ReferenceColumnGroup createReferenceColumnGroup(int toSquidId) {
        ReferenceColumnGroup columnGroup = new ReferenceColumnGroup();
        try {
            Map<String, String> paramMap = new HashMap<String, String>();
            paramMap.put("reference_squid_id", Integer.toString(toSquidId, 10));
            List<ReferenceColumnGroup> rg = adapter.query2List(paramMap, ReferenceColumnGroup.class);
            columnGroup.setKey(StringUtils.generateGUID());
            columnGroup.setReference_squid_id(toSquidId);
            columnGroup.setRelative_order(rg == null || rg.isEmpty() ? 1 : rg.size() + 1);
            columnGroup.setId(adapter.insert2(columnGroup));
        } catch (SQLException e) {
            try {
                logger.error("[ReferenceColumnGroup-insert-sql-exception]", e);
                adapter.rollback();
            } catch (SQLException e1) {
                logger.error("[ReferenceColumnGroup-insert-sql-exception]", e);
                // TODO Auto-generated catch block
            }
        }
        return columnGroup;
    }

    /**
     * 创建ReferenceColumn
     *
     * @param column
     * @param columnGroup
     * @param toSquidId
     * @param index
     * @return
     * @author bo.dang
     * @date 2014年5月19日
     */
    public ReferenceColumn createReferenceColumn(Column column, ReferenceColumnGroup columnGroup, int toSquidId, int index) {
        // 创建引用列
        ReferenceColumn ref = new ReferenceColumn();
        ref.setColumn_id(column.getId());
        ref.setId(ref.getColumn_id());
        ref.setCollation(column.getCollation());
        ref.setData_type(column.getData_type());
        ref.setKey(StringUtils.generateGUID());
        ref.setName(column.getName());
        ref.setNullable(column.isNullable());
        ref.setRelative_order(index);
        ref.setSquid_id(toSquidId);
        ref.setType(DSObjectType.COLUMN.value());
        ref.setReference_squid_id(toSquidId);
        ref.setHost_squid_id(toSquidId);
        ref.setIs_referenced(true);
        ref.setGroup_id(columnGroup.getId());
        try {
            adapter.insert2(ref);
        } catch (SQLException e) {
            try {
                logger.error("[ReferenceColumnGroup-insert-sql-exception]", e);
                adapter.rollback();
            } catch (SQLException e1) {
                logger.error("[ReferenceColumnGroup-insert-sql-exception]", e);
                // TODO Auto-generated catch block
            }
        }
        return ref;
    }

}