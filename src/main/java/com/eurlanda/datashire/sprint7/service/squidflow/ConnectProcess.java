package com.eurlanda.datashire.sprint7.service.squidflow;

import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.SyntaxError;
import com.eurlanda.datashire.adapter.CassandraAdapter;
import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IDBAdapter;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.adapter.db.AdapterDataSourceManager;
import com.eurlanda.datashire.adapter.db.INewDBAdapter;
import com.eurlanda.datashire.dao.IReferenceColumnDao;
import com.eurlanda.datashire.dao.ISquidDao;
import com.eurlanda.datashire.dao.ISquidLinkDao;
import com.eurlanda.datashire.dao.ITransformationDao;
import com.eurlanda.datashire.dao.ITransformationInputsDao;
import com.eurlanda.datashire.dao.ITransformationLinkDao;
import com.eurlanda.datashire.dao.impl.ReferenceColumnDaoImpl;
import com.eurlanda.datashire.dao.impl.SquidDaoImpl;
import com.eurlanda.datashire.dao.impl.SquidLinkDaoImpl;
import com.eurlanda.datashire.dao.impl.TransformationDaoImpl;
import com.eurlanda.datashire.dao.impl.TransformationInputsDaoImpl;
import com.eurlanda.datashire.dao.impl.TransformationLinkDaoImpl;
import com.eurlanda.datashire.entity.*;
import com.eurlanda.datashire.entity.operation.BasicTableInfo;
import com.eurlanda.datashire.entity.operation.ColumnInfo;
import com.eurlanda.datashire.enumeration.DMLType;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.enumeration.DataBaseType;
import com.eurlanda.datashire.enumeration.MessageBubbleCode;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.enumeration.TransformationTypeEnum;
import com.eurlanda.datashire.exception.SystemErrorException;
import com.eurlanda.datashire.sprint7.packet.PushMessagePacket;
import com.eurlanda.datashire.sprint7.plug.ConnectPlug;
import com.eurlanda.datashire.sprint7.service.squidflow.subservice.MessageBubbleService;
import com.eurlanda.datashire.utility.CommonConsts;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import com.eurlanda.datashire.utility.SQLUtils;
import com.eurlanda.datashire.utility.StringUtils;
import com.eurlanda.datashire.utility.SysConf;
import com.eurlanda.datashire.validator.SquidValidationTask;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * connect业务逻辑实现层
 * 根据连接信息去获取数据源,然后回填DS_SQL_CONNECTION,DS_SOURCE_TABLE_CACHE,DS_COLUMN_CACHE
 * (如果输入的数据源已经存在,还需要从数据源获取到的表数据进行对比,以便判断哪些表是否被抽取)
 *
 * @author binlei
 */
public class ConnectProcess extends AbstractRepositoryService implements IConnectProcess {
    /**
     * 对ConnectProcess记录日志
     */
    static Logger logger = Logger.getLogger(ConnectProcess.class);

    private boolean is_openSession = true;

    private String key;

    public ConnectProcess(String token) {
        super(token);
    }

    public ConnectProcess(String token, String key) {
        super(token);
        this.key = key;
    }

    public ConnectProcess(IRelationalDataManager adapter) {
        super(adapter);
    }

    public ConnectProcess(String token, IRelationalDataManager adapter) {
        super(token, adapter);
        this.is_openSession = false;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    /**
     * 根据数据源，获取SourceTable
     *
     * @param info
     * @param out
     * @return
     * @throws Exception
     */
    public List<SourceTable> getConnect(String info, ReturnValue out) throws Exception {
        List<SourceTable> sourceTables = new ArrayList<SourceTable>();
        ConnectPlug connectPlug = new ConnectPlug(adapter);
        INewDBAdapter adaoterSource = null;
        int executeResult = 1;// sql语句执行状态,或者主键返回值
        /*
        try {
			Thread.sleep(300000);
		}catch (InterruptedException e1) {}
		return null;
		*/
        try {
            List<DbSquid> dbSourceSquids = JsonUtil.toGsonList(info,
                    DbSquid.class);
            logger.debug("[获取dbSourceSquids=]" + dbSourceSquids.size());
            Map<String, String> paramMap = new HashMap<String, String>();
            if (is_openSession) {
                adapter.openSession();
            }
            for (int i = 0; i < dbSourceSquids.size(); i++) {
                boolean updateResult = adapter.update2(dbSourceSquids.get(i));
                if (!updateResult) {
                    break;
                }
                //筛选条件
                String filter = dbSourceSquids.get(i).getFilter();
                // 获取数据源
                DBConnectionInfo dbs = new DBConnectionInfo();
                this.setConnectionInfo(dbs, dbSourceSquids.get(i));
                adaoterSource = AdapterDataSourceManager.getAdapter(dbs);
                //IDBAdapter adaoterSource = this.getAdapter(dbSourceSquids.get(i));
                // 获取db_squid的id
                int db_squid_id = dbSourceSquids.get(i).getId();
                logger.debug("[获取到的db_squid_id=]" + db_squid_id);
                // 根据id去查询缓存表所有的表
                long startTime1 = System.currentTimeMillis();
                List<DBSourceTable> tables = connectPlug
                        .getDbSourceTableById(db_squid_id);
                long endTime1 = System.currentTimeMillis();
                logger.info("[查询缓存表的时间]" + (endTime1 - startTime1) + "ms");
                logger.debug("[获取缓存表长度=]" + tables.size());
                // 切换数据库,根据dbSquid查询数据源的表(此时没有source_squid_id)
//				List<DBSourceTable> soureceTables = this.getTableNames(dbSourceSquids.get(i),
//						adaoterSource, out,filter);
                long startTime2 = System.currentTimeMillis();
                List<BasicTableInfo> tableList = null;
                if (dbs.getDbType() == DataBaseType.HANA) {
                    tableList = adaoterSource.getAllTables(SQLUtils.sqlserverFilter(filter), dbs.getDbName());
                } else {
                    tableList = adaoterSource.getAllTables(SQLUtils.sqlserverFilter(filter));
                }
                long endTime2 = System.currentTimeMillis();
                logger.info("[获取数据源表结构时间]" + (endTime2 - startTime2) + "ms");
                //BasicTableInfo到DBSourceTable的转换(全部换成新的adpter以及对象的替换需要时间,暂时采取这种,后续调整)
                long startTime3 = System.currentTimeMillis();
                List<DBSourceTable> soureceTables = this.convertTableName(tableList, dbSourceSquids.get(i));
                long endTime3 = System.currentTimeMillis();
                logger.info("[BasicTableInfo------→DBSourceTable转换时间]" + (endTime3 - startTime3) + "ms");
                logger.debug("[从数据源获取到的表个数为=]" + soureceTables.size());
                // 比较从缓存表和从数据源拿出来的表名,并且根据表名,将数据从数据库中删除
                long startTime4 = System.currentTimeMillis();
                List<DBSourceTable> deleteTables = this.compareTables(
                        soureceTables, tables);
                logger.debug("[需要删除的表的个数=]" + deleteTables.size());
                for (DBSourceTable dbSourceTables : deleteTables) {
                    logger.debug("[需要删除的表名=]" + dbSourceTables.getTable_name());
                    paramMap.clear();
                    paramMap.put("table_name", dbSourceTables.getTable_name());
                    paramMap.put("source_squid_id", String.valueOf(db_squid_id));
                    executeResult = adapter.delete(paramMap,
                            DBSourceTable.class);
                }
                long endTime4 = System.currentTimeMillis();
                logger.info("[删除数据表所需时间]" + (endTime4 - startTime4) + "ms");
                // 比较从缓存表和从数据源拿出来的表名,并且根据表名,数据插入数据库
                long startTime5 = System.currentTimeMillis();
                List<DBSourceTable> addTables = this.compareTables(tables, soureceTables);
                logger.debug("[需要增加的表的个数=]" + addTables.size());
                for (DBSourceTable dbSourceTables : addTables) {
                    executeResult = adapter.insert2(dbSourceTables);
                }
                long endTime5 = System.currentTimeMillis();
                logger.info("[新增数据表所需时间]" + (endTime5 - startTime5) + "ms");
                // 根据db_squid_id查询到所有的表
                List<DBSourceTable> allTables = connectPlug.getDbSourceTableById(db_squid_id);
                logger.debug("[操作执行完后的表个数=]" + allTables.size());
                logger.debug("[执行结果=]" + executeResult);
                long startTime6 = System.currentTimeMillis();
                for (int j = 0; j < allTables.size(); j++) {
                    if (executeResult > 0)// 上述的数据操作成功
                    {
                        // 根据表的tableName去数据源查询列
//						List<SourceColumn> sourceColumns = this.getColumn(adaoterSource,
//								allTables.get(j).getTable_name(), out,
//								allTables.get(j).getId());
                        List<ColumnInfo> columnList = adaoterSource.getTableColumns(allTables.get(j).getTable_name(), dbs.getDbName());
                        List<SourceColumn> sourceColumns = this.converColumn(columnList, allTables.get(j).getId(), out);
                        //根据表的tableName去缓存表查询列
                        List<SourceColumn> sourceColumnCache = connectPlug.getColumnById(true, allTables.get(j).getId());
                        logger.debug("[根据表的tableName去缓存表查询列的长度]" + sourceColumnCache.size());
                        //比较同字段名列的记录，将缓存表中有而数据源中没有的列删除
                        List<SourceColumn> deleteColumns = this.compareColumns(sourceColumns, sourceColumnCache);
                        for (SourceColumn deleteColumn : deleteColumns) {
                            logger.debug("[需要删除的列名=]" + deleteColumn.getName());
                            paramMap.clear();
                            paramMap.put("name", deleteColumn.getName());
                            paramMap.put("source_table_id", String.valueOf(deleteColumn.getSource_table_id()));
                            executeResult = adapter.delete(paramMap, SourceColumn.class);
                            if (executeResult < 1) {
                                break;
                            }
                        }
                        if (executeResult < 1) {
                            out.setMessageCode(MessageCode.SQL_ERROR);
                            break;
                        }
                        //增加数据源中有而缓存表中没有的列
                        List<SourceColumn> addColumns = this.compareColumns(sourceColumnCache, sourceColumns);
                        for (SourceColumn addColumn : addColumns) {
                            logger.debug("[需要增加的列名=]" + addColumn.getName());
                            executeResult = adapter.insert2(addColumn);
                            if (executeResult < 1) {
                                break;
                            }
                        }
                        if (executeResult < 1) {
                            out.setMessageCode(MessageCode.SQL_ERROR);
                            break;
                        }
                        //根据source_table_id查询所有的列
                        List<SourceColumn> allColumns = connectPlug.getColumnById(true, allTables.get(j).getId());
                        logger.debug("[最终的列长度==]" + allColumns.size());
                        //更新最新缓存列和数据源列同名的数据(暂时未做)
                        // 封装返回数据
                        this.setSourceTable(allTables.get(j),
                                allColumns, sourceTables);
                    } else {//执行失败
                        out.setMessageCode(MessageCode.SQL_ERROR);
                        break;
                    }
                }
                long endTime6 = System.currentTimeMillis();
                logger.info("[对所有列操作所需时间]" + (endTime6 - startTime6) + "ms");
                long startTime7 = System.currentTimeMillis();
                CommonConsts.addValidationTask(new SquidValidationTask(token, adapter, MessageBubbleService.setMessageBubble(db_squid_id, db_squid_id, dbSourceSquids.get(i).getName(), MessageBubbleCode.ERROR_SQL_SYNTAX_FILTER.value())));
                CommonConsts.addValidationTask(new SquidValidationTask(token, adapter, MessageBubbleService.setMessageBubble(db_squid_id, db_squid_id, dbSourceSquids.get(i).getName(), MessageBubbleCode.ERROR_SQUID_CONNECTION_NO_HOST.value())));
                CommonConsts.addValidationTask(new SquidValidationTask(token, adapter, MessageBubbleService.setMessageBubble(db_squid_id, db_squid_id, dbSourceSquids.get(i).getName(), MessageBubbleCode.ERROR_SQUID_CONNECTION_NO_RDBMSTYPE.value())));
                CommonConsts.addValidationTask(new SquidValidationTask(token, adapter, MessageBubbleService.setMessageBubble(db_squid_id, db_squid_id, dbSourceSquids.get(i).getName(), MessageBubbleCode.ERROR_SQUID_CONNECTION_NO_DATABASE_NAME.value())));
                long endTime7 = System.currentTimeMillis();
                logger.info("[消息泡所需时间]" + (endTime7 - startTime7) + "ms");
            }
        } catch (SystemErrorException e) {
            logger.error("ConnectProcess  error", e);
            out.setMessageCode(MessageCode.ERR_DATABASE_CONNECT);
            try {
                adapter.rollback();
            } catch (SQLException e1) {
                logger.error("rollback err!", e1);
                out.setMessageCode(MessageCode.ERR_ROLLBACK);
            }
        } catch (Exception e) {
            logger.error("getConnect is error", e);
            out.setMessageCode(MessageCode.SQL_ERROR);
            try {
                adapter.rollback();
            } catch (SQLException e1) {
                logger.error("rollback err!", e1);
                out.setMessageCode(MessageCode.ERR_ROLLBACK);
            }
        } finally {
            if (is_openSession) {
                adapter.closeSession();
            }
            if (adaoterSource != null) adaoterSource.close();
        }
        return sourceTables;
    }

    /**
     * 根据数据源，获取SourceTable
     *
     * @param info
     * @param out
     * @return
     * @throws Exception
     */
    public synchronized List<SourceTable> getConnect(String info, int type, ReturnValue out) throws Exception {
        List<SourceTable> sourceTables = new ArrayList<SourceTable>();
        ConnectPlug connectPlug = new ConnectPlug(adapter);
        INewDBAdapter adaoterSource = null;
        int executeResult = 1;// sql语句执行状态,或者主键返回值
        Map<String, String> tempMap = new HashMap<>();
        List<Integer> cancelSquidIds=new ArrayList<>();
        try {
            List<DbSquid> dbSourceSquids = JsonUtil.toGsonList(info,
                    DbSquid.class);
            logger.debug("[获取dbSourceSquids=]" + dbSourceSquids.size());
            Map<String, String> paramMap = new HashMap<>();
            if (is_openSession) {
                adapter.openSession();
            }
            for (int i = 0; i < dbSourceSquids.size(); i++) {
                List<Integer> squidIdList=new ArrayList<>();
                List<Object> paramsObject=new ArrayList<>();
                String sql=" select k.TO_SQUID_ID as TOSQUID from ds_squid_link k where k.FROM_SQUID_ID =?";
                paramsObject.clear();
                paramsObject.add(dbSourceSquids.get(i).getId());
                //查询下游所有的抽取squid
                List<Map<String, Object>> squidIds=adapter.query2List(true,sql,paramsObject);
                if(squidIds!=null && squidIds.size()>0){
                    for(Map<String,Object> map:squidIds){
                        squidIdList.add(Integer.parseInt(map.get("TOSQUID").toString()));
                    }
                }
                if(squidIdList!=null && squidIdList.size()>0){
                    List<Object> updateSquidIds=new ArrayList<>();
                    updateSquidIds.addAll(squidIdList);
                    String updateSql="update ds_squid  set DESIGN_STATUS=1 where id in ";
                    String ids = JsonUtil.toGsonString(updateSquidIds);
                    ids=ids.substring(1,ids.length()-1);
                    updateSql+="("+ids+")";
                    adapter.execute(updateSql);
                }
                cancelSquidIds.addAll(squidIdList);
                boolean updateResult = adapter.update2(dbSourceSquids.get(i));
                if (!updateResult) {
                    break;
                }
                //筛选条件
                String filter = dbSourceSquids.get(i).getFilter();
                String oldFileter = filter;
                // 获取数据源
                DBConnectionInfo dbs = new DBConnectionInfo();
                if (type == DataBaseType.HIVE.value()) {
                    dbSourceSquids.get(i).setHost(SysConf.getValue("hive.host"));
                    dbSourceSquids.get(i).setPort(Integer.parseInt(SysConf.getValue("hive.port")));
                }
                this.setConnectionInfo(dbs, dbSourceSquids.get(i));
                adaoterSource = AdapterDataSourceManager.getAdapter(dbs);
                // 获取db_squid的id
                int db_squid_id = dbSourceSquids.get(i).getId();

                ISquidLinkDao squidLinkDao = new SquidLinkDaoImpl(adapter);
                List<SquidLink> squidLinkList = squidLinkDao.getSquidLinkListByFromSquid(db_squid_id);
                logger.debug("[获取到的db_squid_id=]" + db_squid_id);
                // 根据id去查询缓存表所有的表
                long startTime1 = System.currentTimeMillis();

                // 获取工作流中存在的sourceTable
                List<DBSourceTable> tables = connectPlug.getDbSourceTablesForExtracted(true, db_squid_id);
                // 删除不参与工作流的SourceTable
                connectPlug.deleteDbSourceTableBySquid(db_squid_id);
                //重新获取删除后的sourceTable
                tables = connectPlug
                        .getDbSourceTablesForExtracted(true, db_squid_id);
                List<DBSourceTable> tables2 = new ArrayList<DBSourceTable>();
                long endTime1 = System.currentTimeMillis();
                logger.info("[查询缓存表的时间]" + (endTime1 - startTime1) + "ms");
                logger.debug("[获取缓存表长度=]" + tables.size());
                // 切换数据库,根据dbSquid查询数据源的表(此时没有source_squid_id)
//				List<DBSourceTable> soureceTables = this.getTableNames(dbSourceSquids.get(i),
//						adaoterSource, out,filter);
                long startTime2 = System.currentTimeMillis();
                List<BasicTableInfo> tableList = new ArrayList<BasicTableInfo>();
                List<BasicTableInfo> tableList2 = new ArrayList<BasicTableInfo>();

                if (dbs.getDbType() == DataBaseType.HANA) {
                    //支持多条件过滤
                    for (String filters : filter.split(",")) {
                        tableList.addAll(adaoterSource.getAllTables(SQLUtils.sqlserverFilter(filters), dbs.getDbName()));
                    }
                } else if (dbs.getDbType() == DataBaseType.HIVE) {
                    for (String filters : filter.split(",")) {
                        tableList.addAll(adaoterSource.getAllTables(filters, dbs.getDbName()));
                    }
                } else {
                    //支持多条件过滤
                    for (String filters : filter.split(",")) {
                        tableList.addAll(adaoterSource.getAllTables(SQLUtils.sqlserverFilter(filters)));
                    }
                    //tableList=adaoterSource.getAllTables(SQLUtils.sqlserverFilter(filter.replace(filter,"")));
                    for (int j = 0; j < tableList.size(); j++) {
                        tableList2.add(tableList.get(j));
                    }
                    tableList.clear();
                    if (tableList2 != null) {
                        tableList.addAll(tableList2);
                    }
                }
                if (tableList.size() > 0 || StringUtils.isNotNull(filter)) {
                    for (int l = 0; l < tables.size(); l++) {
                        if (tables.get(l).isIs_extracted()) {
                            tables2.add(tables.get(l));
                        }
                    }
                    // 删除不参与工作流的SourceTable
                    tables.clear();
                    connectPlug.deleteDbSourceTablesBySquid(db_squid_id);
                }

                long endTime2 = System.currentTimeMillis();
                logger.info("[获取数据源表结构时间]" + (endTime2 - startTime2) + "ms");
                //BasicTableInfo到DBSourceTable的转换(全部换成新的adpter以及对象的替换需要时间,暂时采取这种,后续调整)
                long startTime3 = System.currentTimeMillis();
                List<DBSourceTable> soureceTables = this.convertTableName(tableList, dbSourceSquids.get(i));
                for (int k = 0; k < tables2.size(); k++) {
                    if (!(soureceTables.contains(tables2.get(k)))) {
                        soureceTables.add(tables2.get(k));
                    }
                }
                long endTime3 = System.currentTimeMillis();
                logger.info("[BasicTableInfo------→DBSourceTable转换时间]" + (endTime3 - startTime3) + "ms");
                logger.debug("[从数据源获取到的表个数为=]" + soureceTables.size());
                // 比较从缓存表和从数据源拿出来的表名,并且根据表名,数据插入数据库
                long startTime5 = System.currentTimeMillis();
                List<DBSourceTable> addTables = this.compareTables(tables, soureceTables);
                logger.debug("[需要增加的表的个数=]" + addTables.size());
                for (DBSourceTable dbSourceTables : addTables) {
                    adapter.insert2(dbSourceTables);
                    executeResult++;
                }
                long endTime5 = System.currentTimeMillis();
                logger.info("[新增数据表所需时间]" + (endTime5 - startTime5) + "ms");
                // 根据db_squid_id查询到所有的表
                List<DBSourceTable> allTables = connectPlug.getDbSourceTableById(db_squid_id);
                logger.debug("[操作执行完后的表个数=]" + allTables.size());
                logger.debug("[执行结果=]" + executeResult);
                long startTime6 = System.currentTimeMillis();
                long tempTimes = System.currentTimeMillis();
                System.out.println("您链接的数据库中要创建的表有" + allTables.size() + "个;");
                int tempCount = 0;
                int curIndex = 0;
                int cnt = allTables.size();
                // 没有匹配到表
                if (allTables.size() == 0) {
                    Map<String, Object> map = new HashMap<String, Object>();
                    map.put("Count", cnt);
                    map.put("CurrentIndex", curIndex);
                    map.put("CurrentSourceTables", sourceTables);
                    map.put("ErrorSquidIdList", cancelSquidIds);
                    if (type == DataBaseType.HIVE.value()) {
                        PushMessagePacket.pushMap(map, DSObjectType.HIVE, "1051", "0006",
                                key, token, out.getMessageCode().value());
                    } else {
                        PushMessagePacket.pushMap(map, DSObjectType.DBSOURCE, "0001", "0102",
                                key, token, out.getMessageCode().value());
                    }
                    return null;
                }

                for (int j = 0; j < allTables.size(); j++) {
                    if (executeResult > 0)// 上述的数据操作成功
                    {
                        tempCount++;
                        // 根据表的tableName去数据源查询列
                        // todo 重构获取column的方式，统一获取，内存中按照表名分组
                        List<ColumnInfo> columnList = adaoterSource.getTableColumns(allTables.get(j).getTable_name(), dbs.getDbName());
                        List<SourceColumn> sourceColumns = this.converColumn(columnList, allTables.get(j).getId(), out);
                        //根据表的tableName去缓存表查询列
                        List<SourceColumn> sourceColumnCache = connectPlug.getColumnById(true, allTables.get(j).getId());
                        logger.debug("[根据表的tableName去缓存表查询列的长度]" + sourceColumnCache.size());
                        if (sourceColumnCache != null && sourceColumnCache.size() > 0) {
                            //比较同字段名列的记录，将缓存表中有而数据源中没有的列删除
                            List<SourceColumn> deleteColumns = this.compareColumns(sourceColumns, sourceColumnCache);
                            for (SourceColumn deleteColumn : deleteColumns) {
                                logger.debug("[需要删除的列名=]" + deleteColumn.getName());
                                paramMap.clear();
                                paramMap.put("name", deleteColumn.getName());
                                paramMap.put("source_table_id", String.valueOf(deleteColumn.getSource_table_id()));
                                executeResult = adapter.delete(paramMap, SourceColumn.class);
                                if (executeResult < 1) {
                                    break;
                                }

                                //删除下游
                                deleteSourceColumnByDbSquid(adapter, deleteColumn, squidLinkList);
                            }
                            if (executeResult < 1) {
                                out.setMessageCode(MessageCode.SQL_ERROR);
                                break;
                            }
                            //增加数据源中有而缓存表中没有的列
                            List<SourceColumn> addColumns = this.compareColumns(sourceColumnCache, sourceColumns);
                            tempMap = new HashMap<String, String>();
                            for (SourceColumn addColumn : addColumns) {
                                logger.debug("[需要增加的列名=]" + addColumn.getName());
                                if (tempMap.containsKey(addColumn.getName().toLowerCase())) {
                                    String newColumnName = getSysColumnName(addColumn.getName().toLowerCase(), tempMap, 1);
                                    addColumn.setName(newColumnName);
                                }
                                tempMap.put(addColumn.getName().toLowerCase(), addColumn.getName().toLowerCase());
                                executeResult = adapter.insert2(addColumn);
                                if (executeResult < 1) {
                                    break;
                                }
                                addSourceColumnByDbSquid(adapter, addColumn, squidLinkList, dbSourceSquids.get(i));
                            }
                            if (executeResult < 1) {
                                out.setMessageCode(MessageCode.SQL_ERROR);
                                break;
                            }
                        } else {
                            if (sourceColumns != null && sourceColumns.size() > 0) {
                                tempMap = new HashMap<String, String>();
                                for (SourceColumn sourceColumn : sourceColumns) {
                                    logger.debug("[需要增加的列名=]" + sourceColumn.getName());
                                    if (tempMap.containsKey(sourceColumn.getName().toLowerCase())) {
                                        String newColumnName = getSysColumnName(sourceColumn.getName().toLowerCase(), tempMap, 1);
                                        sourceColumn.setName(newColumnName);
                                    }
                                    tempMap.put(sourceColumn.getName().toLowerCase(), sourceColumn.getName().toLowerCase());
                                    executeResult = adapter.insert2(sourceColumn);
                                }
                            }
                        }
                        //根据source_table_id查询所有的列
                        List<SourceColumn> allColumns = connectPlug.getColumnById(true, allTables.get(j).getId());
                        logger.debug("[最终的列长度==]" + allColumns.size());
                        //更新最新缓存列和数据源列同名的数据(暂时未做)
                        // 封装返回数据
                        this.setSourceTable(allTables.get(j),
                                allColumns, sourceTables);

                        if (tempCount % 30 == 0 || tempCount == allTables.size()) {
                            System.out.println("第" + curIndex + "次执行时间为：" + (System.currentTimeMillis() - tempTimes));
                            Map<String, Object> map = new HashMap<String, Object>();
                            map.put("Count", cnt);
                            map.put("CurrentIndex", curIndex);
                            map.put("CurrentSourceTables", sourceTables);
                            map.put("ErrorSquidIdList", cancelSquidIds);
                            if (type == DataBaseType.HIVE.value()) {
                                PushMessagePacket.pushMap(map, DSObjectType.HIVE, "1051", "0006",
                                        key, token, out.getMessageCode().value());
                            } else {
                                PushMessagePacket.pushMap(map, DSObjectType.DBSOURCE, "0001", "0102",
                                        key, token, out.getMessageCode().value());
                            }
                            sourceTables.clear();
                            tempTimes = System.currentTimeMillis();
                            curIndex++;
                        }
                    } else {//执行失败
                        out.setMessageCode(MessageCode.SQL_ERROR);
                        break;
                    }
                }
                long endTime6 = System.currentTimeMillis();
                logger.info("[对所有列操作所需时间]" + (endTime6 - startTime6) + "ms");
                long startTime7 = System.currentTimeMillis();
                long endTime7 = System.currentTimeMillis();
                logger.info("[消息泡所需时间]" + (endTime7 - startTime7) + "ms");
            }
        } catch (SystemErrorException e) {
            logger.error(e);
            // TODO: handle exception
            out.setMessageCode(MessageCode.ERR_DATABASE_CONNECT);
            try {
                adapter.rollback();
            } catch (SQLException e1) {
                logger.error("rollback err!", e1);
                out.setMessageCode(MessageCode.ERR_ROLLBACK);
            }
            Map<String, Object> map = new HashMap<String, Object>();
            map.put("Count", 0);
            map.put("CurrentIndex", 0);
            map.put("CurrentSourceTables", sourceTables);
            if (type == DataBaseType.HIVE.value()) {
                out.setMessageCode(MessageCode.HIVE_DATABASENAME_NOTEXIST);
                PushMessagePacket.pushMap(map, DSObjectType.HIVE, "1051", "0006",
                        key, token, out.getMessageCode().value());
            } else {
                PushMessagePacket.pushMap(map, DSObjectType.DBSOURCE, "0001", "0102",
                        key, token, out.getMessageCode().value());
            }
        } catch (MetaException e) {
            logger.error(e);
            // TODO: handle exception
            out.setMessageCode(MessageCode.ERR_DATABASE_CONNECT);
            try {
                adapter.rollback();
            } catch (SQLException e1) {
                logger.error("rollback err!", e1);
                out.setMessageCode(MessageCode.ERR_ROLLBACK);
            }
            Map<String, Object> map = new HashMap<String, Object>();
            map.put("Count", 0);
            map.put("CurrentIndex", 0);
            map.put("CurrentSourceTables", sourceTables);
            if (type == DataBaseType.HIVE.value()) {
                out.setMessageCode(MessageCode.HIVE_DATABASENAME_NOTEXIST);
                PushMessagePacket.pushMap(map, DSObjectType.HIVE, "1051", "0006",
                        key, token, out.getMessageCode().value());
            } else {
                PushMessagePacket.pushMap(map, DSObjectType.DBSOURCE, "0001", "0102",
                        key, token, out.getMessageCode().value());
            }
        } catch (Exception e) {
            logger.error("getConnect is error", e);
            out.setMessageCode(MessageCode.SQL_ERROR);
            try {
                adapter.rollback();
            } catch (SQLException e1) {
                logger.error("rollback err!", e1);
                out.setMessageCode(MessageCode.ERR_ROLLBACK);
            }
            Map<String, Object> map = new HashMap<String, Object>();
            map.put("Count", 0);
            map.put("CurrentIndex", 0);
            map.put("CurrentSourceTables", sourceTables);
            if (type == DataBaseType.HIVE.value()) {
                if (e instanceof SQLException) {
                    SQLException exception = (SQLException) e;
                    if (exception.getSQLState().equals("08S01")) {
                        out.setMessageCode(MessageCode.HIVE_DATABASENAME_NOTEXIST);
                    }
                }
                if (e instanceof NoSuchObjectException) {
                    out.setMessageCode(MessageCode.HIVE_DATABASENAME_NOTEXIST);
                }
                PushMessagePacket.pushMap(map, DSObjectType.HIVE, "1051", "0006",
                        key, token, out.getMessageCode().value());
            } else {
                PushMessagePacket.pushMap(map, DSObjectType.DBSOURCE, "0001", "0102",
                        key, token, out.getMessageCode().value());
            }
        } finally {
            if (is_openSession) {
                adapter.closeSession();
            }
            if (adaoterSource != null) adaoterSource.close();
        }
        return null;
        //return sourceTables;
    }

    /**
     * 获取Cassandra数据源
     *
     * @param info
     * @param type
     * @param out
     * @return
     * @throws Exception
     */
    public synchronized List<SourceTable> getCassandraConnect(String info, int type, ReturnValue out) throws Exception {
        List<SourceTable> sourceTables = new ArrayList<SourceTable>();
        ConnectPlug connectPlug = new ConnectPlug(adapter);
        INewDBAdapter adaoterSource = null;
        int executeResult = 1;// sql语句执行状态,或者主键返回值
        Map<String, String> tempMap = new HashMap<>();
        List<Integer> cancelSquidIds=new ArrayList<>();
        try {
            List<CassandraConnectionSquid> dbSourceSquids = JsonUtil.toGsonList(info,
                    CassandraConnectionSquid.class);
            logger.debug("[获取dbSourceSquids=]" + dbSourceSquids.size());
            Map<String, String> paramMap = new HashMap<String, String>();
            if (is_openSession) {
                adapter.openSession();
            }
            for (int i = 0; i < dbSourceSquids.size(); i++) {
                List<Integer> squidIdList=new ArrayList<>();
                List<Object> paramsObject=new ArrayList<>();
                String sql=" select k.TO_SQUID_ID as TOSQUID from ds_squid_link k where k.FROM_SQUID_ID =?";
                paramsObject.clear();
                paramsObject.add(dbSourceSquids.get(i).getId());
                //查询下游所有的抽取squid
                List<Map<String, Object>> squidIds=adapter.query2List(true,sql,paramsObject);
                if(squidIds!=null && squidIds.size()>0){
                    for(Map<String,Object> map:squidIds){
                        squidIdList.add(Integer.parseInt(map.get("TOSQUID").toString()));
                    }
                }
                if(squidIdList!=null && squidIdList.size()>0){
                    List<Object> updateSquidIds=new ArrayList<>();
                    updateSquidIds.addAll(squidIdList);

                    String updateSql="update ds_squid  set DESIGN_STATUS=1 where id in ";
                    String ids = JsonUtil.toGsonString(updateSquidIds);
                    ids=ids.substring(1,ids.length()-1);
                    updateSql+="("+ids+")";
                    int updateSquidStatus=adapter.execute(updateSql);
                }
                cancelSquidIds.addAll(squidIdList);





                boolean updateResult = adapter.update2(dbSourceSquids.get(i));
                if (!updateResult) {
                    break;
                }
                //获取所有的表
                List<BasicTableInfo> tableList = new ArrayList<>();
                DBConnectionInfo dbs = CassandraAdapter.getCassandraConnection(dbSourceSquids.get(i));
                adaoterSource = AdapterDataSourceManager.getAdapter(dbs);
                tableList.addAll(adaoterSource.getAllTables(dbSourceSquids.get(i).getFilter()));
					/*for(int j=0;j<hosts.length;j++){
						try {
							DBConnectionInfo dbs = CassandraAdapter.getCassandraConnection(dbSourceSquids.get(i));
							dbs.setHost(hosts[j]);
							adaoterSource = AdapterDataSourceManager.getAdapter(dbs);
							tableList.addAll(adaoterSource.getAllTables(dbSourceSquids.get(i).getFilter()));
						} catch (Exception e){
							//当单个连接时，客户端给出相关的错误提示,否则不给出提示
							if(hosts.length==1){
								throw e;
							}
						} finally {
							if(adaoterSource!=null) {
								adaoterSource.close();
							}
						}
					}*/

                // 获取db_squid的id
                int db_squid_id = dbSourceSquids.get(i).getId();
                ISquidLinkDao squidLinkDao = new SquidLinkDaoImpl(adapter);
                List<SquidLink> squidLinkList = squidLinkDao.getSquidLinkListByFromSquid(db_squid_id);
                logger.debug("[获取到的db_squid_id=]" + db_squid_id);
                //删除所有的source_table
                connectPlug.deleteDbSourceTablesBySquid(db_squid_id);
                // 获取工作流中存在的sourceTable
                List<DBSourceTable> tables = connectPlug.getDbSourceTablesForExtracted(true, db_squid_id);
                logger.debug("[获取缓存表长度=]" + tables.size());
                List<DBSourceTable> addTables = this.convertTableName2(tableList, dbSourceSquids.get(i).getId());
                logger.debug("[需要增加的表的个数=]" + addTables.size());
                for (DBSourceTable dbSourceTables : addTables) {
                    adapter.insert2(dbSourceTables);
                    executeResult++;
                }
                // 根据db_squid_id查询到所有的表
                List<DBSourceTable> allTables = connectPlug.getDbSourceTableById(db_squid_id);
                logger.debug("[操作执行完后的表个数=]" + allTables.size());
                logger.debug("[执行结果=]" + executeResult);
                long startTime6 = System.currentTimeMillis();
                long tempTimes = System.currentTimeMillis();
                System.out.println("您链接的数据库中要创建的表有" + allTables.size() + "个;");
                int tempCount = 0;
                int curIndex = 0;
                int cnt = allTables.size();
                // 没有匹配到表
                if (allTables.size() == 0) {
                    Map<String, Object> map = new HashMap<String, Object>();
                    map.put("Count", cnt);
                    map.put("CurrentIndex", curIndex);
                    map.put("CurrentSourceTables", sourceTables);
                    map.put("ErrorSquidIdList", cancelSquidIds);
                    PushMessagePacket.pushMap(map, DSObjectType.CASSANDRA, "1061", "0006",
                            key, token, out.getMessageCode().value());

                    return null;
                }
                for (int j = 0; j < allTables.size(); j++) {
                    if (executeResult > 0)// 上述的数据操作成功
                    {
                        tempCount++;
                        // 根据表的tableName去数据源查询列
                        // todo 重构获取column的方式，统一获取，内存中按照表名分组
                        List<ColumnInfo> columnList = CassandraAdapter.getTableColumns(tableList, allTables.get(j));
                        List<SourceColumn> sourceColumns = this.converColumn(columnList, allTables.get(j).getId(), out);
                        //根据表的tableName去缓存表查询列
                        List<SourceColumn> sourceColumnCache = connectPlug.getColumnById(true, allTables.get(j).getId());
                        logger.debug("[根据表的tableName去缓存表查询列的长度]" + sourceColumnCache.size());
                        if (sourceColumnCache != null && sourceColumnCache.size() > 0) {
                            //比较同字段名列的记录，将缓存表中有而数据源中没有的列删除
                            List<SourceColumn> deleteColumns = this.compareColumns(sourceColumns, sourceColumnCache);
                            for (SourceColumn deleteColumn : deleteColumns) {
                                logger.debug("[需要删除的列名=]" + deleteColumn.getName());
                                paramMap.clear();
                                paramMap.put("name", deleteColumn.getName());
                                paramMap.put("source_table_id", String.valueOf(deleteColumn.getSource_table_id()));
                                executeResult = adapter.delete(paramMap, SourceColumn.class);
                                if (executeResult < 1) {
                                    break;
                                }

                                //删除下游
                                deleteSourceColumnByDbSquid(adapter, deleteColumn, squidLinkList);
                            }
                            if (executeResult < 1) {
                                out.setMessageCode(MessageCode.SQL_ERROR);
                                break;
                            }
                            //增加数据源中有而缓存表中没有的列
                            List<SourceColumn> addColumns = this.compareColumns(sourceColumnCache, sourceColumns);
                            tempMap = new HashMap<String, String>();
                            for (SourceColumn addColumn : addColumns) {
                                logger.debug("[需要增加的列名=]" + addColumn.getName());
                                if (tempMap.containsKey(addColumn.getName().toLowerCase())) {
                                    String newColumnName = getSysColumnName(addColumn.getName().toLowerCase(), tempMap, 1);
                                    addColumn.setName(newColumnName);
                                }
                                tempMap.put(addColumn.getName().toLowerCase(), addColumn.getName().toLowerCase());
                                executeResult = adapter.insert2(addColumn);
                                if (executeResult < 1) {
                                    break;
                                }
                                addSourceColumnByDbSquid(adapter, addColumn, squidLinkList, dbSourceSquids.get(i));
                            }
                            if (executeResult < 1) {
                                out.setMessageCode(MessageCode.SQL_ERROR);
                                break;
                            }
                        } else {
                            if (sourceColumns != null && sourceColumns.size() > 0) {
                                tempMap = new HashMap<String, String>();
                                for (SourceColumn sourceColumn : sourceColumns) {
                                    logger.debug("[需要增加的列名=]" + sourceColumn.getName());
                                    if (tempMap.containsKey(sourceColumn.getName().toLowerCase())) {
                                        String newColumnName = getSysColumnName(sourceColumn.getName().toLowerCase(), tempMap, 1);
                                        sourceColumn.setName(newColumnName);
                                    }
                                    tempMap.put(sourceColumn.getName().toLowerCase(), sourceColumn.getName().toLowerCase());
                                    executeResult = adapter.insert2(sourceColumn);
                                }
                            }
                        }
                        //根据source_table_id查询所有的列
                        List<SourceColumn> allColumns = connectPlug.getColumnById(true, allTables.get(j).getId());
                        logger.debug("[最终的列长度==]" + allColumns.size());
                        //更新最新缓存列和数据源列同名的数据(暂时未做)
                        // 封装返回数据
                        this.setSourceTable(allTables.get(j),
                                allColumns, sourceTables);

                        if (tempCount % 30 == 0 || tempCount == allTables.size()) {
                            System.out.println("第" + curIndex + "次执行时间为：" + (System.currentTimeMillis() - tempTimes));
                            Map<String, Object> map = new HashMap<String, Object>();
                            map.put("Count", cnt);
                            map.put("CurrentIndex", curIndex);
                            map.put("CurrentSourceTables", sourceTables);
                            map.put("ErrorSquidIdList", cancelSquidIds);
                            PushMessagePacket.pushMap(map, DSObjectType.CASSANDRA, "1061", "0006",
                                    key, token, out.getMessageCode().value());
                            sourceTables.clear();
                            tempTimes = System.currentTimeMillis();
                            curIndex++;
                        }
                    } else {//执行失败
                        out.setMessageCode(MessageCode.SQL_ERROR);
                        break;
                    }
                }
                long endTime6 = System.currentTimeMillis();
                logger.info("[对所有列操作所需时间]" + (endTime6 - startTime6) + "ms");
                long startTime7 = System.currentTimeMillis();
                long endTime7 = System.currentTimeMillis();
                logger.info("[消息泡所需时间]" + (endTime7 - startTime7) + "ms");
            }
        } catch (SystemErrorException e) {
            logger.error(e);
            // TODO: handle exception
            out.setMessageCode(MessageCode.ERR_DATABASE_CONNECT);
            try {
                adapter.rollback();
            } catch (SQLException e1) {
                logger.error("rollback err!", e1);
                out.setMessageCode(MessageCode.ERR_ROLLBACK);
            }
            Map<String, Object> map = new HashMap<String, Object>();
            map.put("Count", 0);
            map.put("CurrentIndex", 0);
            map.put("CurrentSourceTables", sourceTables);
            PushMessagePacket.pushMap(map, DSObjectType.CASSANDRA, "1061", "0006",
                    key, token, out.getMessageCode().value());
        } catch (Exception e) {
            logger.error("getConnect is error", e);
            out.setMessageCode(MessageCode.SQL_ERROR);
            if (e instanceof AuthenticationException) {
                out.setMessageCode(MessageCode.CASSANDRA_NOT_AUTH);
            }
            if (e instanceof NoHostAvailableException) {
                out.setMessageCode(MessageCode.CASSANDRA_IPORPORT_ISNOTAVAILABLE);
            }
            if (e instanceof InvalidQueryException || e instanceof SyntaxError) {
                out.setMessageCode(MessageCode.CASSANDRA_KEYSPACEORCLUSTERIS_NOT_EXIST);
            }
            try {
                adapter.rollback();
            } catch (SQLException e1) {
                logger.error("rollback err!", e1);
                out.setMessageCode(MessageCode.ERR_ROLLBACK);
            }
            Map<String, Object> map = new HashMap<String, Object>();
            map.put("Count", 0);
            map.put("CurrentIndex", 0);
            map.put("CurrentSourceTables", sourceTables);
            PushMessagePacket.pushMap(map, DSObjectType.CASSANDRA, "1061", "0006",
                    key, token, out.getMessageCode().value());
        } finally {
            if (adapter != null) {
                adapter.closeSession();
            }
            if (adaoterSource != null) adaoterSource.close();
        }
        return null;
    }

    /**
     * 删除sourceTable，并且同步下游
     *
     * @param adapter
     * @param deleteColumn
     * @param squidLinkList
     * @throws Exception
     */
    public void deleteSourceColumnByDbSquid(IRelationalDataManager adapter,
                                            SourceColumn deleteColumn, List<SquidLink> squidLinkList) throws Exception {
        IReferenceColumnDao refColDao = new ReferenceColumnDaoImpl(adapter);
        ITransformationDao transDao = new TransformationDaoImpl(adapter);
        ITransformationInputsDao inputsDao = new TransformationInputsDaoImpl(adapter);
        ITransformationLinkDao linkDao = new TransformationLinkDaoImpl(adapter);
        if (squidLinkList != null && squidLinkList.size() > 0) {
            for (SquidLink squidLink : squidLinkList) {
                ReferenceColumn refCol = refColDao.getReferenceColumnById(squidLink.getTo_squid_id(),
                        deleteColumn.getId());
                //同步下游
                if (refCol != null && refCol.getColumn_id() > 0) {
                    Transformation trans = transDao.getTransformationById(squidLink.getTo_squid_id(), deleteColumn.getId());
                    if (trans != null) {
                        // 删除所有相关的link，在删除之前重置link的source_trans
                        List<TransformationLink> links = linkDao.getTransLinkByFromTrans(trans.getId());
                        if (links != null && links.size() > 0) {
                            for (TransformationLink transformationLink : links) {
                                inputsDao.resetTransformationInput(
                                        transformationLink.getFrom_transformation_id(),
                                        transformationLink.getTo_transformation_id(), null);
                            }
                            linkDao.delTransLinkByTranId(trans.getId());
                        }
                        //删除inputs集合
                        inputsDao.delTransInputsByTransId(trans.getId());
                        //删除Transformation
                        transDao.delete(trans.getId(), Transformation.class);
                    }
                    refColDao.delReferenceColumnByColumnId(refCol.getColumn_id(), squidLink.getTo_squid_id());
                    RepositoryServiceHelper helper = new RepositoryServiceHelper(token, adapter);
                    helper.synchronousDeleteRefColumn(adapter, refCol, DMLType.DELETE.value(), null);
                }
            }
        }
    }

    /**
     * 添加sourceTable及同步下游
     *
     * @param adapter
     * @param addColumn
     * @param squidLinkList
     * @param formSquid
     * @throws Exception
     */
    public void addSourceColumnByDbSquid(IRelationalDataManager adapter,
                                         SourceColumn addColumn, List<SquidLink> squidLinkList, Squid formSquid) throws Exception {
        ISquidDao squidDao = new SquidDaoImpl(adapter);
        IReferenceColumnDao refColDao = new ReferenceColumnDaoImpl(adapter);

        if (squidLinkList != null && squidLinkList.size() > 0) {
            for (SquidLink squidLink : squidLinkList) {
                Squid squid = squidDao.getSquidForCond(squidLink.getTo_squid_id(), Squid.class);
                Class c = SquidTypeEnum.classOfValue(squid.getSquid_type());
                Map<String, Object> squidMap = squidDao.getMapForCond(squid.getId(), c);
                boolean flag = false;
                if (squidMap.containsKey("SOURCE_TABLE_ID")
                        && StringUtils.isNotNull(squidMap.get("SOURCE_TABLE_ID"))) {
                    int source_id = Integer.parseInt(squidMap.get("SOURCE_TABLE_ID") + "");
                    flag = source_id == addColumn.getSource_table_id() ? true : false;
                }
                //同步下游
                if (flag) {
                    if (squid == null || squid.getSquid_type() == SquidTypeEnum.REPORT.value()
                            || squid.getSquid_type() == SquidTypeEnum.GISMAP.value()
                            || squid.getSquid_type() == SquidTypeEnum.DESTWS.value()
                            || squid.getSquid_type() == SquidTypeEnum.EXCEPTION.value()) {
                        continue;
                    }
                    ReferenceColumnGroup group = refColDao.getRefColumnGroupBySquidId(squid.getId(), formSquid.getId());
                    if (group == null) {
                        int order = 1;
                        List<ReferenceColumnGroup> groupList = refColDao.getRefColumnGroupListBySquidId(squid.getId());
                        if (groupList != null && groupList.size() > 0) {
                            order = groupList.size() + 1;
                        }
                        group = new ReferenceColumnGroup();
                        group.setKey(StringUtils.generateGUID());
                        group.setReference_squid_id(squid.getId());
                        group.setRelative_order(order);
                        group.setId(refColDao.insert2(group));
                    }
                    TransformationService service = new TransformationService(token);
                    ReferenceColumn refColumn = service.initReference(adapter,
                            addColumn, addColumn.getId(), addColumn.getRelative_order(),
                            formSquid, squid.getId(), group);
                    Transformation refTrans = service.initTransformation(adapter,
                            squid.getId(), refColumn.getColumn_id(), TransformationTypeEnum.VIRTUAL
                                    .value(), refColumn.getData_type(), 1);
                    RepositoryServiceHelper helper = new RepositoryServiceHelper(token, adapter);
                    helper.synchronousInsertRefColumn(adapter, refColumn, DMLType.INSERT.value());
                }
            }
        }
    }

    /**
     * 更新sourceTable
     * 2014-12-1
     *
     * @return
     * @author Akachi
     * @E-Mail zsts@hotmail.com
     */
    public int modifyDBSourceTable(List<SourceTable> sourceTables, int sourceId) {
        DataAdapterFactory adapterFactory = null;
        IRelationalDataManager adapter = null;
        int executeResult = 0;
        try {
            // 实例化相关的数据库处理类
            adapterFactory = DataAdapterFactory.newInstance();
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            //赋值
            List<DBSourceTable> soureceTables = this.convertTableName(sourceTables, sourceId);
            logger.debug("[需要增加的表的个数=]" + soureceTables.size());
            for (DBSourceTable dbSourceTable : soureceTables) {
                executeResult = adapter.update2(dbSourceTable) ? 1 : 0;
            }
            logger.debug("[执行结果=]" + executeResult);
        } catch (Exception e) {
            e.printStackTrace();
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!", e1);
            }
        } finally {
            adapter.closeSession();
        }
        return executeResult;
    }

    public int createDBSourceTable(List<SourceTable> sourceTables, int sourceId) {
        DataAdapterFactory adapterFactory = null;
        IRelationalDataManager adapter = null;
        int executeResult = 0;
        try {
            // 实例化相关的数据库处理类
            adapterFactory = DataAdapterFactory.newInstance();
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            //赋值
            List<DBSourceTable> soureceTables = this.convertTableName(sourceTables, sourceId);
            logger.debug("[需要增加的表的个数=]" + soureceTables.size());
            for (DBSourceTable dbSourceTable : soureceTables) {
                executeResult = adapter.insert2(dbSourceTable);
            }
            logger.debug("[执行结果=]" + executeResult);
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!", e1);
            }
        } finally {
            adapter.closeSession();
        }
        return executeResult;
    }

    /**
     * DS_SOURCE_TABLE表存储
     *
     * @param sourceTables
     * @param sourceId
     */
    public int connectionDBSourceTable(List<SourceTable> sourceTables, int sourceId) {
        DataAdapterFactory adapterFactory = null;
        IRelationalDataManager adapter = null;
        int source_squid_id = sourceId;
        int executeResult = 0;
        Map<String, String> params = new HashMap<String, String>(); // 查询参数
        try {
            // 实例化相关的数据库处理类
            adapterFactory = DataAdapterFactory.newInstance();
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            //根据source_squid_id 查询DS_SOURCE_TABLE表
            params.clear();
            params.put("source_squid_id", String.valueOf(source_squid_id));
            List<DBSourceTable> tables = adapter.query2List(true, params, DBSourceTable.class);
            List<DBSourceTable> soureceTables = this.convertTableName(sourceTables, sourceId);
            List<DBSourceTable> deleteTables = this.compareTables(soureceTables, tables);
            for (DBSourceTable dbSourceTables : deleteTables) {
                logger.debug("[需要删除的表名=]" + dbSourceTables.getTable_name());
                params.clear();
                params.put("table_name", dbSourceTables.getTable_name());
                params.put("source_squid_id", String.valueOf(source_squid_id));
                executeResult = adapter.delete(params,
                        DBSourceTable.class);
            }
            // 比较从缓存表和从数据源拿出来的表名,并且根据表名,数据插入数据库
            List<DBSourceTable> addTables = this.compareTables(tables, soureceTables);
            logger.debug("[需要增加的表的个数=]" + addTables.size());
            for (DBSourceTable dbSourceTables : addTables) {
                executeResult = adapter.insert2(dbSourceTables);
            }
            logger.debug("[执行结果=]" + executeResult);

        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!", e1);
            }
        } finally {
            adapter.closeSession();
        }
        return executeResult;
    }

    /**
     * 校验tableName和source_squid_id是否存在
     *
     * @param tableName
     * @param source_squid_id
     * @return
     * @throws Exception
     */
    public boolean connectMore(String tableName, int source_squid_id)
            throws Exception {
        boolean flag = false;
        DataAdapterFactory adapterFactory = null;
        IRelationalDataManager adapter = null;
        // 实例化相关的数据库处理类
        try {
            adapterFactory = DataAdapterFactory.newInstance();
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            DBSourceTable dbSourceTable = adapter.query2Object(true,
                    "SELECT * FROM DS_SOURCE_TABLE  WHERE source_squid_id="
                            + source_squid_id + "  and  table_name='"
                            + tableName + "'", null, DBSourceTable.class);
            if (null != dbSourceTable) {
                flag = true;
            }
        } catch (Exception e) {
            // TODO: handle exception
            throw new Exception("创建微博/网页sourcetableList错误");
        } finally {
            adapter.closeSession();
        }
        return flag;
    }

    /**
     * DS_SOURCE_TABLE表存储
     *
     * @param dbSourceTable
     * @throws Exception
     */
    public int createDBSourceTable(DBSourceTable dbSourceTable, ReturnValue out, String type) throws Exception {
        DataAdapterFactory adapterFactory = null;
        IRelationalDataManager adapter = null;
        List<SourceColumn> sourceColumns = null;
        int executeResult = 0;
        try {
            // 实例化相关的数据库处理类
            adapterFactory = DataAdapterFactory.newInstance();
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            executeResult = adapter.insert2(dbSourceTable);
            if (executeResult > 0)//更新成功
            {
                if ("weibo".equals(type)) {
                    sourceColumns = this.getWeiboColumnList();
                } else if ("web".equals(type)) {
                    sourceColumns = this.getWebColumnList();
                }
                executeResult = this.createDBSourceColumn(sourceColumns, adapter, out, executeResult);
            }
        } catch (Exception e) {
            // TODO: handle exception
            out.setMessageCode(MessageCode.ERR_DS_SOURCE_TABLE);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!", e1);
            }
            throw new Exception("创建微博/网页sourcetableList错误");
        } finally {
            adapter.closeSession();
        }
        return executeResult;
    }

    /**
     * DS_SOURCE_COLUMN表存储
     *
     * @param sourceColumn
     * @param adapter
     * @return
     */
    public int createDBSourceColumn(List<SourceColumn> sourceColumn, IRelationalDataManager adapter, ReturnValue out, int source_table_id) {
        int executeResult = 0;
        try {
            for (SourceColumn column : sourceColumn) {
                column.setSource_table_id(source_table_id);
                executeResult = adapter.insert2(column);
                if (executeResult < 1) {
                    out.setMessageCode(MessageCode.INSERT_ERROR);
                    break;
                }
            }
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
        }
        return executeResult;
    }

    /**
     * 新增
     *
     * @param dbSourceSquids
     * @param out
     * @return
     */
    public List<SourceTable> getConnect2(List<DbSquid> dbSourceSquids, IRelationalDataManager adapter2, ReturnValue out) {
        List<SourceTable> sourceTables = new ArrayList<SourceTable>();
        ConnectPlug connectPlug = new ConnectPlug(adapter2);
        int executeResult = 1;// sql语句执行状态,或者主键返回值
        try {
            logger.debug("[获取dbSourceSquids=]" + dbSourceSquids.size());
            Map<String, String> paramMap = new HashMap<String, String>();
            adapter.openSession();
            for (int i = 0; i < dbSourceSquids.size(); i++) {
                //筛选条件
                String filter = dbSourceSquids.get(i).getFilter();
                // 获取数据源
                IDBAdapter adaoterSource = this.getAdapter(dbSourceSquids.get(i));
                // 获取db_squid的id
                int db_squid_id = dbSourceSquids.get(i).getId();
                logger.debug("[获取到的db_squid_id=]" + db_squid_id);
                // 根据id去查询缓存表所有的表
                List<DBSourceTable> tables = connectPlug
                        .getDbSourceTableById(db_squid_id);
                logger.debug("[获取缓存表长度=]" + tables.size());
                // 切换数据库,根据dbSquid查询数据源的表(此时没有source_squid_id)
                List<DBSourceTable> soureceTables = this.getTableNames(dbSourceSquids.get(i),
                        adaoterSource, out, filter);
                // 比较从缓存表和从数据源拿出来的表名,并且根据表名,将数据从数据库中删除
                List<DBSourceTable> deleteTables = this.compareTables(
                        soureceTables, tables);
                logger.debug("[需要删除的表的个数=]" + deleteTables.size());
                for (DBSourceTable dbSourceTables : deleteTables) {
                    logger.debug("[需要删除的表名=]" + dbSourceTables.getTable_name());
                    paramMap.clear();
                    paramMap.put("table_name", dbSourceTables.getTable_name());
                    paramMap.put("source_squid_id", String.valueOf(db_squid_id));
                    executeResult = adapter2.delete(paramMap,
                            DBSourceTable.class);
                }
                // 比较从缓存表和从数据源拿出来的表名,并且根据表名,数据插入数据库
                List<DBSourceTable> addTables = this.compareTables(tables, soureceTables);
                logger.debug("[需要增加的表的个数=]" + addTables.size());
                for (DBSourceTable dbSourceTables : addTables) {
                    dbSourceTables.setSource_squid_id(db_squid_id);
                    executeResult = adapter2.insert2(dbSourceTables);
                }
                // 根据db_squid_id查询到所有的表
                List<DBSourceTable> allTables = connectPlug.getDbSourceTableById(db_squid_id);
                logger.debug("[操作执行完后的表个数=]" + allTables.size());
                logger.debug("[执行结果=]" + executeResult);
                for (int j = 0; j < allTables.size(); j++) {
                    if (executeResult > 0) {// 上述的数据操作成功
                        // 根据表的tableName去数据源查询列
                        List<SourceColumn> sourceColumns = this.getColumn(adaoterSource,
                                allTables.get(j).getTable_name(), out,
                                allTables.get(j).getId());
                        //根据表的tableName去缓存表查询列
                        List<SourceColumn> sourceColumnCache = connectPlug.getColumnById(true, allTables.get(j).getId());
                        if (sourceColumnCache != null && sourceColumnCache.size() > 0) {
                            for (SourceColumn addColumn : sourceColumns) {
                                logger.debug("[需要增加的列名=]" + addColumn.getName());
                                executeResult = adapter2.insert2(addColumn);
                                if (executeResult < 1) {
                                    break;
                                }
                            }
                        } else {
                            logger.debug("[根据表的tableName去缓存表查询列的长度]" + sourceColumnCache.size());
                            //比较同字段名列的记录，将缓存表中有而数据源中没有的列删除
                            List<SourceColumn> deleteColumns = this.compareColumns(sourceColumns, sourceColumnCache);
                            for (SourceColumn deleteColumn : deleteColumns) {
                                logger.debug("[需要删除的列名=]" + deleteColumn.getName());
                                paramMap.clear();
                                paramMap.put("name", deleteColumn.getName());
                                paramMap.put("source_table_id", String.valueOf(deleteColumn.getSource_table_id()));
                                executeResult = adapter2.delete(paramMap, SourceColumn.class);
                                if (executeResult < 1) {
                                    break;
                                }
                            }
                            if (executeResult < 1) {
                                out.setMessageCode(MessageCode.SQL_ERROR);
                                break;
                            }
                            //增加数据源中有而缓存表中没有的列
                            List<SourceColumn> addColumns = this.compareColumns(sourceColumnCache, sourceColumns);
                            for (SourceColumn addColumn : addColumns) {
                                logger.debug("[需要增加的列名=]" + addColumn.getName());
                                executeResult = adapter2.insert2(addColumn);
                                if (executeResult < 1) {
                                    break;
                                }
                            }
                            if (executeResult < 1) {
                                out.setMessageCode(MessageCode.SQL_ERROR);
                                break;
                            }
                        }
                        //根据source_table_id查询所有的列
                        List<SourceColumn> allColumns = connectPlug.getColumnById(true, allTables.get(j).getId());
                        logger.debug("[最终的列长度==]" + allColumns.size());
                        //更新最新缓存列和数据源列同名的数据(暂时未做)
                        // 封装返回数据
                        this.setSourceTable(allTables.get(j),
                                allColumns, sourceTables);
                    } else {//执行失败
                        out.setMessageCode(MessageCode.SQL_ERROR);
                        break;
                    }
                }
            }
        } catch (Exception e) {
            logger.error("getConnect is error", e);
            try {
                adapter.rollback();
                out.setMessageCode(MessageCode.SQL_ERROR);
            } catch (SQLException e1) {
                logger.error("rollback err!", e1);
            }
        } finally {
            adapter.closeSession();
        }
        return sourceTables;
    }

    /**
     * 根据dbsquid获取adpter
     *
     * @param dbSquid
     * @return
     */
    public IDBAdapter getAdapter(DbSquid dbSquid) {
        IDBAdapter adaoterSource = null;
        try {
            adaoterSource = adapterFactory.makeAdapter(dbSquid);
        } catch (Exception e) {
            logger.error("getAdapter err!", e);
        }
        return adaoterSource;
    }

    /**
     * 根据adapter获取数据源的表数据
     *
     * @param dbSquid
     * @return
     */
    public List<DBSourceTable> getTableNames(DbSquid dbSquid, IDBAdapter adaoterSource, ReturnValue out, String filter) {
        List<DBSourceTable> sourceTables = new ArrayList<DBSourceTable>();
        SourceTable[] sourceTable = null;
        try {
            // 获取数据源中的表
            sourceTable = adaoterSource.getTables(out, filter);
            // 将数组转换为list集合
            for (int i = 0; i < sourceTable.length; i++) {
                DBSourceTable table = new DBSourceTable();
                table.setTable_name(sourceTable[i].getTableName());
                table.setSource_squid_id(dbSquid.getId());
                sourceTables.add(table);
            }
        } catch (Exception e) {
            logger.error("getTableNames err!", e);
            out.setMessageCode(MessageCode.SQL_ERROR);
        }
        return sourceTables;
    }

    /**
     * 根据dbsquid,表名去查询列的信息
     *
     * @param tableName
     * @param out
     * @return
     */
    public List<SourceColumn> getColumn(IDBAdapter adaoterSource, String tableName,
                                        ReturnValue out, int sourece_table_id) {
        List<SourceColumn> sourceColumns = new ArrayList<SourceColumn>();
        List<ColumnInfo> columnInfos = null;
        try {
            columnInfos = adaoterSource.getColums(tableName, out);
            for (int i = 0; i < columnInfos.size(); i++) {
                //logger.debug(columnInfos.get(i));
                SourceColumn sourceColumn = new SourceColumn();
                sourceColumn.setName(columnInfos.get(i).getName());
                sourceColumn.setData_type(columnInfos.get(i).getSystemDatatype().value());
                sourceColumn.setNullable(columnInfos.get(i).isNullable());
                sourceColumn.setLength(columnInfos.get(i).getLength());
                sourceColumn.setPrecision(columnInfos.get(i).getPrecision());
                sourceColumn.setSource_table_id(sourece_table_id);
                sourceColumns.add(sourceColumn);
            }
        } catch (Exception e) {
            logger.error("getColumn err!", e);
            out.setMessageCode(MessageCode.SQL_ERROR);
        }
        return sourceColumns;
    }

    /**
     * 对DBSourceTable表的是否抽取字段赋值
     *
     * @param dbSourceTables
     * @param out
     * @return
     */
    public int updateDBSourceTable(List<DBSourceTable> dbSourceTables,
                                   ReturnValue out, ConnectPlug connectPlug) {
        int cnt = 0;
        try {
            for (int i = 0; i < dbSourceTables.size(); i++) {
                String table_name = dbSourceTables.get(i).getTable_name();
                int source_squid_id = dbSourceTables.get(i)
                        .getSource_squid_id();
                cnt = connectPlug.updateDBSourceTable(table_name,
                        source_squid_id);
            }
        } catch (Exception e) {
            logger.error("getDBSourceTable err!", e);
            out.setMessageCode(MessageCode.SQL_ERROR);
        }
        return cnt;
    }

    /**
     * 封装返回的数据
     */
    public void setSourceTable(DBSourceTable dbSourceTable,
                               List<SourceColumn> sourceColumns, List<SourceTable> sourceTables) {
        try {
            SourceTable sourceTable = new SourceTable();
            sourceTable.setId(dbSourceTable.getId());
            sourceTable.setTableName(dbSourceTable.getTable_name());
            sourceTable.setIs_extracted(dbSourceTable.isIs_extracted());
            // 对列进行赋值
            Map<String, Column> map = new HashMap<String, Column>();
            for (int i = 0; i < sourceColumns.size(); i++) {
                Column column = new Column();
                column.setId(sourceColumns.get(i).getId());
                // 类型
                column.setData_type(sourceColumns.get(i).getData_type());
                // 精度
                column.setPrecision(sourceColumns.get(i).getPrecision());
                //小数点
                column.setScale(sourceColumns.get(i).getScale());
                // 长度
                column.setLength(sourceColumns.get(i).getLength());
                // 排序
                column.setCollation(0);
                // 是否为空
                column.setNullable(sourceColumns.get(i).isNullable());
                // 列名
                column.setName(sourceColumns.get(i).getName());
                //是否主键
                column.setIsPK(sourceColumns.get(i).isIspk());
                //是否唯一约束
                column.setIsUnique(sourceColumns.get(i).isIsunique());
                map.put(sourceColumns.get(i).getName(), column);
            }
            sourceTable.setColumnInfo(map);
            sourceTables.add(sourceTable);
        } catch (Exception e) {
            logger.error("setSourceTable err!", e);
        }

    }

    /**
     * 根据source_squid_id获取DBSourceTable表信息
     *
     * @param id
     * @param out
     * @return
     */
    public List<DBSourceTable> getDBSourceTable(int id, ReturnValue out) {
        ConnectPlug connectPlug = new ConnectPlug(adapter);
        List<DBSourceTable> dbSourceTables = new ArrayList<DBSourceTable>();
        try {
            dbSourceTables = connectPlug.getDbSourceTable(false, id);
        } catch (Exception e) {
            out.setMessageCode(MessageCode.SQL_ERROR);
            logger.error("getDBSourceTable err!", e);
        } finally {
            adapter.closeSession();
        }
        return dbSourceTables;
    }

    /**
     * 存储DS_FILEFOLDER_CONNECTION
     *
     * @param out
     * @return
     */
    public boolean updateFileFolder(FileFolderSquid fileFolderConnection, ReturnValue out) {
        DataAdapterFactory adapterFactory = null;
        IRelationalDataManager adapter = null;
        boolean updateFlag = false;
        try {
            // 实例化相关的数据库处理类
            adapterFactory = DataAdapterFactory.newInstance();
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            if (null != fileFolderConnection) {
                updateFlag = adapter.update2(fileFolderConnection);
                if (!updateFlag)// 更新失败
                {
                    out.setMessageCode(MessageCode.UPDATE_ERROR);
                }
                // 推送消息泡
//                CommonConsts.addValidationTask(new SquidValidationTask(token, adapter, MessageBubbleService.setMessageBubble(fileFolderConnection.getId(), fileFolderConnection.getId(), fileFolderConnection.getName(), MessageBubbleCode.ERROR_SQUID_CONNECTION_NO_HOST.value())));
//                CommonConsts.addValidationTask(new SquidValidationTask(token, adapter, MessageBubbleService.setMessageBubble(fileFolderConnection.getId(), fileFolderConnection.getId(), fileFolderConnection.getName(), MessageBubbleCode.ERROR_SQUID_CONNECTION_NO_FILE_PATH.value())));
//                CommonConsts.addValidationTask(new SquidValidationTask(token, adapter, MessageBubbleService.setMessageBubble(fileFolderConnection.getId(), fileFolderConnection.getId(), fileFolderConnection.getName(), MessageBubbleCode.ERROR_SQL_SYNTAX_FILTER.value())));
            }
        } catch (Exception e) {
            // TODO: handle exception
            logger.error("updateFileFolder is error", e);
            out.setMessageCode(MessageCode.UPDATE_ERROR);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!", e1);
            }
        } finally {
            adapter.closeSession();
        }
        return updateFlag;
    }

    /**
     * 存储DS_FTP_CONNECTION
     *
     * @param out
     * @return
     */
    public boolean updateFtp(FtpSquid ftpSquid, ReturnValue out) {
        DataAdapterFactory adapterFactory = null;
        IRelationalDataManager adapter = null;
        boolean updateFlag = false;
        try {
            // 实例化相关的数据库处理类
            adapterFactory = DataAdapterFactory.newInstance();
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            if (null != ftpSquid) {
                updateFlag = adapter.update2(ftpSquid);
                if (!updateFlag)// 更新失败
                {
                    out.setMessageCode(MessageCode.UPDATE_ERROR);
                }
                CommonConsts.addValidationTask(new SquidValidationTask(token, adapter, MessageBubbleService.setMessageBubble(ftpSquid.getId(), ftpSquid.getId(), ftpSquid.getName(), MessageBubbleCode.ERROR_SQUID_CONNECTION_NO_HOST.value())));
                CommonConsts.addValidationTask(new SquidValidationTask(token, adapter, MessageBubbleService.setMessageBubble(ftpSquid.getId(), ftpSquid.getId(), ftpSquid.getName(), MessageBubbleCode.ERROR_SQUID_CONNECTION_NO_FILE_PATH.value())));
                CommonConsts.addValidationTask(new SquidValidationTask(token, adapter, MessageBubbleService.setMessageBubble(ftpSquid.getId(), ftpSquid.getId(), ftpSquid.getName(), MessageBubbleCode.ERROR_SQL_SYNTAX_FILTER.value())));
            }
        } catch (Exception e) {
            // TODO: handle exception
            logger.error("updateFtp is error", e);
            out.setMessageCode(MessageCode.UPDATE_ERROR);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!", e1);
            }
        } finally {
            adapter.closeSession();
        }
        return updateFlag;
    }

    /**
     * 存储WebService_CONNECTION
     *
     * @param out
     * @return
     */
    public boolean updateWebService(WebserviceSquid ws, ReturnValue out) {
        DataAdapterFactory adapterFactory = null;
        IRelationalDataManager adapter = null;
        boolean updateFlag = false;
        try {
            // 实例化相关的数据库处理类
            adapterFactory = DataAdapterFactory.newInstance();
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            if (null != ws) {
                updateFlag = adapter.update2(ws);
                if (!updateFlag)// 更新失败
                {
                    out.setMessageCode(MessageCode.UPDATE_ERROR);
                }
                CommonConsts.addValidationTask(new SquidValidationTask(token, adapter, MessageBubbleService.setMessageBubble(ws.getId(), ws.getId(), ws.getName(), MessageBubbleCode.ERROR_SQUID_CONNECTION_NO_HOST.value())));
                CommonConsts.addValidationTask(new SquidValidationTask(token, adapter, MessageBubbleService.setMessageBubble(ws.getId(), ws.getId(), ws.getName(), MessageBubbleCode.ERROR_SQUID_CONNECTION_NO_FILE_PATH.value())));
                CommonConsts.addValidationTask(new SquidValidationTask(token, adapter, MessageBubbleService.setMessageBubble(ws.getId(), ws.getId(), ws.getName(), MessageBubbleCode.ERROR_SQL_SYNTAX_FILTER.value())));
            }
        } catch (Exception e) {
            // TODO: handle exception
            logger.error("updateFtp is error", e);
            out.setMessageCode(MessageCode.UPDATE_ERROR);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!", e1);
            }
        } finally {
            adapter.closeSession();
        }
        return updateFlag;
    }

    /**
     * 存储DS_HDFS_CONNECTION
     *
     * @param out
     * @return
     */
    public boolean updateHdfs(HdfsSquid hdfsSquid, ReturnValue out) {
        DataAdapterFactory adapterFactory = null;
        IRelationalDataManager adapter = null;
        boolean updateFlag = false;
        try {
            // 实例化相关的数据库处理类
            adapterFactory = DataAdapterFactory.newInstance();
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            if (null != hdfsSquid) {
                if(hdfsSquid.getSquid_type() == SquidTypeEnum.TRAINNINGFILESQUID.value()){
                    hdfsSquid.setHost(SysConf.getValue("train_file_host"));
                }else if(hdfsSquid.getSquid_type() == SquidTypeEnum.SOURCECLOUDFILE.value()){
                    hdfsSquid.setHost(SysConf.getValue("hdfs_host"));
                }
                updateFlag = adapter.update2(hdfsSquid);
                if (!updateFlag)// 更新失败
                {
                    out.setMessageCode(MessageCode.UPDATE_ERROR);
                }
                CommonConsts.addValidationTask(new SquidValidationTask(token, adapter, MessageBubbleService.setMessageBubble(hdfsSquid.getId(), hdfsSquid.getId(), hdfsSquid.getName(), MessageBubbleCode.ERROR_SQUID_CONNECTION_NO_HOST.value())));
                CommonConsts.addValidationTask(new SquidValidationTask(token, adapter, MessageBubbleService.setMessageBubble(hdfsSquid.getId(), hdfsSquid.getId(), hdfsSquid.getName(), MessageBubbleCode.ERROR_SQUID_CONNECTION_NO_FILE_PATH.value())));
                CommonConsts.addValidationTask(new SquidValidationTask(token, adapter, MessageBubbleService.setMessageBubble(hdfsSquid.getId(), hdfsSquid.getId(), hdfsSquid.getName(), MessageBubbleCode.ERROR_SQL_SYNTAX_FILTER.value())));
            }
        } catch (Exception e) {
            // TODO: handle exception
            logger.error("updateFtp is error", e);
            out.setMessageCode(MessageCode.UPDATE_ERROR);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!", e1);
            }
        } finally {
            adapter.closeSession();
        }
        return updateFlag;
    }

    /**
     * 存储DS_WEIBO_CONNECTION
     *
     * @param out
     * @return
     */
    public boolean updateWeibo(WeiboSquid weiboSquid, ReturnValue out) {
        DataAdapterFactory adapterFactory = null;
        IRelationalDataManager adapter = null;
        boolean updateFlag = false;
        try {
            // 实例化相关的数据库处理类
            adapterFactory = DataAdapterFactory.newInstance();
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            if (null != weiboSquid) {
                updateFlag = adapter.update2(weiboSquid);
                if (!updateFlag)// 更新失败
                {
                    out.setMessageCode(MessageCode.UPDATE_ERROR);
                }
                CommonConsts.addValidationTask(new SquidValidationTask(token, adapter, MessageBubbleService.setMessageBubble(weiboSquid.getId(), weiboSquid.getId(), null, MessageBubbleCode.ERROR_WEIBO_CONNECTION_NO_SERVICE_PROVIDER.value())));
                CommonConsts.addValidationTask(new SquidValidationTask(token, adapter, MessageBubbleService.setMessageBubble(weiboSquid.getId(), weiboSquid.getId(), null, MessageBubbleCode.ERROR_SQL_SYNTAX_FILTER.value())));
            }
        } catch (Exception e) {
            // TODO: handle exception
            logger.error("updateWeibo is error", e);
            out.setMessageCode(MessageCode.UPDATE_ERROR);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!", e1);
            }
        } finally {
            adapter.closeSession();
        }
        return updateFlag;
    }

    /**
     * 存储DS_WEB_CONNECTION
     *
     * @param out
     * @return
     */
    public boolean updateWeb(WebSquid webSquid, ReturnValue out) {
        DataAdapterFactory adapterFactory = null;
        IRelationalDataManager adapter = null;
        boolean updateFlag = false;
        try {
            // 实例化相关的数据库处理类
            adapterFactory = DataAdapterFactory.newInstance();
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            if (null != webSquid) {
                updateFlag = adapter.update2(webSquid);
                if (!updateFlag)// 更新失败
                {
                    out.setMessageCode(MessageCode.UPDATE_ERROR);
                }
                CommonConsts.addValidationTask(new SquidValidationTask(token, adapter, MessageBubbleService.setMessageBubble(webSquid.getId(), webSquid.getId(), webSquid.getName(), MessageBubbleCode.WARN_WEB_CONNECTION_NO_URL_LIST.value())));
                CommonConsts.addValidationTask(new SquidValidationTask(token, adapter, MessageBubbleService.setMessageBubble(webSquid.getId(), webSquid.getId(), webSquid.getName(), MessageBubbleCode.ERROR_SQL_SYNTAX_FILTER.value())));
            }
        } catch (Exception e) {
            // TODO: handle exception
            logger.error("updateWeb is error", e);
            out.setMessageCode(MessageCode.UPDATE_ERROR);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!", e1);
            }
        } finally {
            adapter.closeSession();
        }
        return updateFlag;
    }

    /**
     * 根据source_table_id获取SourceColumn表记录
     *
     * @param id
     * @param out
     * @return
     */
    public List<SourceColumn> getSourceColumn(int id, ReturnValue out) {
        List<SourceColumn> sourceColumns = new ArrayList<SourceColumn>();
        ConnectPlug connectPlug = new ConnectPlug(adapter);
        try {
            sourceColumns = connectPlug.getColumnById(false, id);
        } catch (Exception e) {
            out.setMessageCode(MessageCode.SQL_ERROR);
            logger.error("getSourceColumn err!", e);
        } finally {
            //adapter.closeSession();
        }
        return sourceColumns;
    }

    /**
     * 根据tableName,source_squid_id查询列
     *
     * @param out
     * @return
     */
    public List<SourceColumn> getSourceColumn2(String tableName, int source_squid_id, ReturnValue out) {
        List<SourceColumn> sourceColumns = new ArrayList<SourceColumn>();
        ConnectPlug connectPlug = new ConnectPlug(adapter);
        try {
            sourceColumns = connectPlug.getColumn(tableName, source_squid_id);
        } catch (Exception e) {
            out.setMessageCode(MessageCode.SQL_ERROR);
            logger.error("getSourceColumn2 err!", e);
        } finally {
            adapter.closeSession();
        }
        return sourceColumns;
    }

    /**
     * 抽取list2中有，而list1中没有的元素
     *
     * @param list1
     * @param list2
     * @return
     */
    public List<DBSourceTable> compareTables(List<DBSourceTable> list1,
                                             List<DBSourceTable> list2) {
        List<DBSourceTable> list3 = new ArrayList<DBSourceTable>();
        HashMap<String, DBSourceTable> hashold = new HashMap<String, DBSourceTable>();
        HashMap<String, DBSourceTable> hashnew = new HashMap<String, DBSourceTable>();
        if (list1 != null) {
            for (int i = 0; i < list1.size(); i++) {
                hashold.put(list1.get(i).getTable_name(), list1.get(i));
            }
        }
        if (list2 != null) {
            for (int j = 0; j < list2.size(); j++) {
                //如果list2中的元素不在list1中
                if (!hashold.containsKey(list2.get(j).getTable_name())) {
                    hashnew.put(list2.get(j).getTable_name(), list2.get(j));
                }
            }
        }
        for (Map.Entry<String, DBSourceTable> entry : hashnew.entrySet()) {
            if (entry == null) {
                continue;
            }
            DBSourceTable value = entry.getValue();
            //System.out.println("key=" + key + " value=" + value);
            list3.add(value);
        }
        return list3;
    }

    /**
     * 抽取list2中有，而list1中没有的元素
     *
     * @param list1
     * @param list2
     * @return
     */
    public List<SourceColumn> compareColumns(List<SourceColumn> list1,
                                             List<SourceColumn> list2) {
        List<SourceColumn> list3 = new ArrayList<SourceColumn>();
        HashMap<String, SourceColumn> hashold = new HashMap<String, SourceColumn>();
        HashMap<String, SourceColumn> hashnew = new HashMap<String, SourceColumn>();
        if (list1 != null && list1.size() > 0) {
            for (int i = 0; i < list1.size(); i++) {
                hashold.put(list1.get(i).getName(), list1.get(i));
            }
        }
        if (list2 != null && list2.size() > 0) {
            for (int j = 0; j < list2.size(); j++) {
                //如果list2中的元素不在list1中
                if (!hashold.containsKey(list2.get(j).getName())) {
                    hashnew.put(list2.get(j).getName(), list2.get(j));
                } else {
                    SourceColumn oldCol = hashold.get(list2.get(j).getName());
                    SourceColumn newCol = list2.get(j);
                    if (oldCol.getData_type() != newCol.getData_type()) {
                        hashnew.put(list2.get(j).getName(), list2.get(j));
                    }
                }
            }
        }
        for (Map.Entry<String, SourceColumn> entry : hashnew.entrySet()) {
            if (entry == null) continue;
            SourceColumn value = entry.getValue();
            //System.out.println("key=" + key + " value=" + value);
            list3.add(value);
        }
        return list3;
    }

    /**
     * 同步SourceColumn
     *
     * @param adapter
     * @param list1
     * @param list2
     */
    public void synchronousColumns(IRelationalDataManager adapter,
                                   List<SourceColumn> list1, List<SourceColumn> list2) {

    }

    /**
     * 连接对象设置值
     *
     * @param dbs
     * @param dbSquid
     */
    private void setConnectionInfo(DBConnectionInfo dbs, DbSquid dbSquid) {
        dbs.setHost(dbSquid.getHost());
        dbs.setPort(dbSquid.getPort());
        dbs.setUserName(dbSquid.getUser_name());
        dbs.setPassword(dbSquid.getPassword());
        dbs.setDbName(dbSquid.getDb_name());
        dbs.setDbType(DataBaseType.parse(dbSquid.getDb_type()));
    }

    /**
     * 对象转换
     *
     * @param tableList
     * @return
     */
    private List<DBSourceTable> convertTableName(List<BasicTableInfo> tableList, DbSquid dbSquid) {
        List<DBSourceTable> sourceTables = new ArrayList<DBSourceTable>();
        for (BasicTableInfo basicTableInfo : tableList) {
            DBSourceTable dbSourceTable = new DBSourceTable();
            if (StringUtils.isNotNull(basicTableInfo.getScheme())) {
                dbSourceTable.setTable_name(basicTableInfo.getScheme() + "." + basicTableInfo.getTableName());
            } else {
                dbSourceTable.setTable_name(basicTableInfo.getTableName());
            }
            dbSourceTable.setSource_squid_id(dbSquid.getId());
            sourceTables.add(dbSourceTable);
        }
        return sourceTables;
    }

    /**
     * 对象转换
     *
     * @param tableList
     * @param squidId
     * @return
     */
    public List<DBSourceTable> convertTableName2(List<BasicTableInfo> tableList, int squidId) {
        List<DBSourceTable> sourceTables = new ArrayList<DBSourceTable>();
        for (BasicTableInfo basicTableInfo : tableList) {
            DBSourceTable dbSourceTable = new DBSourceTable();
            if (StringUtils.isNotNull(basicTableInfo.getScheme())) {
                dbSourceTable.setTable_name(basicTableInfo.getScheme() + "." + basicTableInfo.getTableName());
            } else {
                dbSourceTable.setTable_name(basicTableInfo.getTableName());
            }
			/*if(basicTableInfo.getDbType()==DataBaseType.CASSANDRA){
				dbSourceTable.setKeyspace(basicTableInfo.getKeyspace());
				dbSourceTable.setCluster(basicTableInfo.getCluster());
			}*/
            dbSourceTable.setSource_squid_id(squidId);
            sourceTables.add(dbSourceTable);
        }
        return sourceTables;
    }

    /**
     * 对象转换
     *
     * @return
     */
    private List<DBSourceTable> convertTableName(List<SourceTable> sourceTables, int sourceId) {
        List<DBSourceTable> dbSourceTables = new ArrayList<DBSourceTable>();
        for (SourceTable sourceTable : sourceTables) {
            DBSourceTable dbSourceTable = new DBSourceTable();
            dbSourceTable.setId(sourceTable.getId());
            dbSourceTable.setTable_name(sourceTable.getTableName());
            dbSourceTable.setSource_squid_id(sourceId);
            sourceTable.setSource_squid_id(sourceId);
            dbSourceTable.setUrl(sourceTable.getUrl());
            dbSourceTable.setIs_extracted(sourceTable.isIs_extracted());
//			dbSourceTable.setUrl_params(sourceTable.getUrl_params());
//			dbSourceTable.setHeader_params(sourceTable.getHeader_params());
            dbSourceTable.setIs_directory(sourceTable.isIs_directory());
            dbSourceTable.setHeader_xml(sourceTable.getHeader_xml());
            dbSourceTable.setParams_xml(sourceTable.getParams_xml());
            dbSourceTable.setPost_params(sourceTable.getPost_params());
            dbSourceTable.setEncoded(sourceTable.getEncoded());
            dbSourceTable.setTemplet_xml(sourceTable.getTemplet_xml());
            dbSourceTable.setMethod(sourceTable.getMethod());
            dbSourceTables.add(dbSourceTable);
        }
        return dbSourceTables;
    }

    /**
     * 列的转换
     *
     * @param columnList
     * @param sourece_table_id
     * @param out
     * @return
     */
    public List<SourceColumn> converColumn(List<ColumnInfo> columnList,
                                           int sourece_table_id, ReturnValue out) {
        List<SourceColumn> sourceColumns = new ArrayList<SourceColumn>();
        try {
            for (int i = 0; i < columnList.size(); i++) {
                // logger.debug(columnInfos.get(i));
                SourceColumn sourceColumn = new SourceColumn();
                sourceColumn.setName(columnList.get(i).getName());
                sourceColumn.setData_type(columnList.get(i).getDbBaseDatatype().value());
                sourceColumn.setNullable(columnList.get(i).isNullable());
                if (null != columnList.get(i).getLength()) {
                    sourceColumn.setLength(columnList.get(i).getLength());
                }
                if (null != columnList.get(i).getPrecision()) {
                    sourceColumn.setPrecision(columnList.get(i).getPrecision());
                }
                if (null != columnList.get(i).getScale()) {
                    sourceColumn.setScale(columnList.get(i).getScale());
                }
                sourceColumn.setIspk(columnList.get(i).isPrimary());
                sourceColumn.setIsunique(columnList.get(i).isUniqueness());
                sourceColumn.setSource_table_id(sourece_table_id);
                sourceColumns.add(sourceColumn);
            }
        } catch (Exception e) {
            logger.error("getColumn err!", e);
            out.setMessageCode(MessageCode.SQL_ERROR);
        }
        return sourceColumns;
    }

    /**
     * 根据source_squid_id查询集合
     *
     * @param sourceId
     * @return
     */
    public List<DBSourceTable> getDBSourceTable(int sourceId) {
        List<DBSourceTable> tables = new ArrayList<DBSourceTable>();
        DataAdapterFactory adapterFactory = null;
        IRelationalDataManager adapter = null;
        int source_squid_id = sourceId;
        Map<String, String> params = new HashMap<String, String>(); // 查询参数
        try {
            // 实例化相关的数据库处理类
            adapterFactory = DataAdapterFactory.newInstance();
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            //根据source_squid_id 查询DS_SOURCE_TABLE表
            params.clear();
            params.put("source_squid_id", String.valueOf(source_squid_id));
            tables = adapter.query2List(true, params, DBSourceTable.class);
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!", e1);
            }
        } finally {
            adapter.closeSession();
        }
        return tables;
    }

    /**
     * 对微博的SourceColumn进行赋值
     */
    public List<SourceColumn> getWeiboColumnList() {
        List<SourceColumn> list = new ArrayList<SourceColumn>();
        String name[] = {"user_id", "repost_count", "comment_count", "favoured_count", "supported_count", "created_date", "origina", "is_organization", "content", "matched_keyword", "user_name", "sex", "birth_date", "signup_date", "education", "province", "city", "bio", "fetch_date"};
        int dataType[] = {9, 2, 2, 2, 2, 13, 9, 4, 9, 9, 9, 9, 9, 13, 9, 9, 9, 9, 13};
        int length[] = {30, 0, 0, 0, 0, 0, 45, 0, 280, 50, 45, 2, 20, 0, 50, 50, 50, 500, 0};
        int precision[] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        for (int i = 0; i < 19; i++) {
            SourceColumn sourceColumn = new SourceColumn();
            sourceColumn.setName(name[i]);
            sourceColumn.setData_type(dataType[i]);
            sourceColumn.setNullable(true);
            sourceColumn.setLength(length[i]);
            sourceColumn.setPrecision(precision[i]);
            sourceColumn.setRelative_order(i + 1);
            list.add(sourceColumn);
        }
        return list;
    }

    /**
     * 对网页的SourceColumn进行赋值
     */
    public List<SourceColumn> getWebColumnList() {
        List<SourceColumn> list = new ArrayList<SourceColumn>();
        String name[] = {"id", "url", "title", "domain", "key_words", "update_date", "source_content", "fetch_date"};
        int dataType[] = {9, 9, 9, 9, 9, 13, 9, 13};
        int length[] = {500, 500, 1000, 500, 1000, 0, -1, 0};
        int precision[] = {0, 0, 0, 0, 0, 0, 0, 0};
        for (int i = 0; i < 8; i++) {
            SourceColumn sourceColumn = new SourceColumn();
            sourceColumn.setName(name[i]);
            sourceColumn.setData_type(dataType[i]);
            sourceColumn.setNullable(true);
            sourceColumn.setLength(length[i]);
            sourceColumn.setPrecision(precision[i]);
            sourceColumn.setRelative_order(i + 1);
            list.add(sourceColumn);
        }
        return list;
    }

}