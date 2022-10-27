package com.eurlanda.datashire.destCassandraService;

import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.SyntaxError;
import com.eurlanda.datashire.adapter.CassandraAdapter;
import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.adapter.datatype.DataTypeConvertor;
import com.eurlanda.datashire.adapter.db.AdapterDataSourceManager;
import com.eurlanda.datashire.adapter.db.INewDBAdapter;
import com.eurlanda.datashire.dao.ISquidDao;
import com.eurlanda.datashire.dao.ISquidLinkDao;
import com.eurlanda.datashire.dao.impl.SquidDaoImpl;
import com.eurlanda.datashire.dao.impl.SquidLinkDaoImpl;
import com.eurlanda.datashire.entity.CassandraConnectionSquid;
import com.eurlanda.datashire.entity.Column;
import com.eurlanda.datashire.entity.DBConnectionInfo;
import com.eurlanda.datashire.entity.SquidLink;
import com.eurlanda.datashire.entity.dest.DestCassandraColumn;
import com.eurlanda.datashire.entity.dest.DestCassandraSquid;
import com.eurlanda.datashire.entity.operation.BasicTableInfo;
import com.eurlanda.datashire.entity.operation.ColumnInfo;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.enumeration.DataBaseType;
import com.eurlanda.datashire.enumeration.datatype.DbBaseDatatype;
import com.eurlanda.datashire.enumeration.datatype.SystemDatatype;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import com.eurlanda.datashire.utility.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Eurlanda on 2017/5/15.
 */
@Service
public class DestCassandraService {
    private static Log log = LogFactory.getLog(DestCassandraService.class);
    private String token;
    private String key;

    /**
     * 创建Cassandra落地squid
     *
     * @param info
     * @return
     */
    public String createDestCassandraSquid(String info) {
        ReturnValue out = new ReturnValue();
        Map<String, Object> resultMap = new HashMap<>();
        IRelationalDataManager adapter = null;
        try {
            Map<String, Object> paramsMap = JsonUtil.toHashMap(info);
            DestCassandraSquid destCassandraSquid = JsonUtil.toGsonBean(paramsMap.get("DestCassandraSquid") + "", DestCassandraSquid.class);
            if (null != destCassandraSquid) {
                adapter = DataAdapterFactory.getDefaultDataManager();
                adapter.openSession();
                int newSquidId = adapter.insert2(destCassandraSquid);
                resultMap.put("newSquidId", newSquidId);
            } else {
                out.setMessageCode(MessageCode.NODATA);
            }
        } catch (Exception e) {
            try {
                adapter.rollback();
            } catch (SQLException e1) {
                //回滚失败
                log.fatal("rollback err", e1);
            }
            log.error("创建DestCassandraSquid异常", e);
            out.setMessageCode(MessageCode.INSERT_ERROR);
        } finally {
            if (adapter != null) {
                adapter.closeSession();
            }
        }
        return JsonUtil.toJsonString(resultMap, DSObjectType.DEST_CASSANDRA, out.getMessageCode());
    }

    /**
     * 更新
     *
     * @param info
     * @return
     */
    public String updateDestCassandraSquid(String info) {
        ReturnValue out = new ReturnValue();
        IRelationalDataManager adapter = null;
        try {
            Map<String, Object> paramsMap = JsonUtil.toHashMap(info);
            DestCassandraSquid destCassandraSquid = JsonUtil.toGsonBean(paramsMap.get("DestCassandraSquid") + "", DestCassandraSquid.class);
            if (null != destCassandraSquid) {
                adapter = DataAdapterFactory.getDefaultDataManager();
                adapter.openSession();
                boolean flag = adapter.update2(destCassandraSquid);
                if (!flag) {
                    out.setMessageCode(MessageCode.UPDATE_ERROR);
                }
            } else {
                out.setMessageCode(MessageCode.NODATA);
            }
        } catch (Exception e) {
            try {
                adapter.rollback();
            } catch (SQLException e1) {
                //回滚失败
                log.fatal("rollback err", e1);
            }
            log.error("更新DestCassandraSquid异常", e);
            out.setMessageCode(MessageCode.UPDATE_ERROR);
        } finally {
            if (adapter != null) {
                adapter.closeSession();
            }
        }
        return JsonUtil.toJsonString(new HashMap<String, Object>(), DSObjectType.DEST_CASSANDRA, out.getMessageCode());
    }

    /**
     * 获取表中的列的信息(CassandraAdapter中有getAllTable方法，参数filter为填写的tablename)
     *
     * @param info
     * @return
     */
    public String getDestCassandraColumn(String info) {
        IRelationalDataManager adapter = null;
        INewDBAdapter cassandraAdapter = null;
        ReturnValue out = new ReturnValue();
        Map<String, Object> returnMap = new HashMap<>();
        try {
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            Map<String, Object> paramMap = JsonUtil.toHashMap(info);
            DestCassandraSquid cassandraSquid = JsonUtil.toGsonBean(paramMap.get("DestCassandraSquid") + "", DestCassandraSquid.class);
            boolean refresh = Boolean.parseBoolean(paramMap.get("IsReferesh") + "");
            Map<String, Object> sqlMap = new HashMap<>();
            sqlMap.put("squid_id", cassandraSquid.getId());
            List<DestCassandraColumn> cassandraColumns = adapter.query2List2(true, sqlMap, DestCassandraColumn.class);
            if (!refresh) {
                if (cassandraColumns == null || cassandraColumns.size() == 0) {
                    refresh = true;
                }
            }
            if (StringUtils.isNull(cassandraSquid.getTable_name()) || cassandraSquid.getDest_squid_id() == 0) {
                returnMap.put("DestCassandraColumns", cassandraColumns);
                return JsonUtil.toJsonString(returnMap, DSObjectType.DEST_CASSANDRA, out.getMessageCode());
            }
            if (refresh) {
                //先删除原来的destCassandraColumn数据
                String delSql = "delete from ds_dest_cassandra_column where squid_id=" + cassandraSquid.getId();
                adapter.execute(delSql);
                //获取squid信息，得到tableName
                String tableName = cassandraSquid.getTable_name();
                int dest_squid_id = cassandraSquid.getDest_squid_id();
                ISquidDao squidDao = new SquidDaoImpl(adapter);
                CassandraConnectionSquid connectionSquid = squidDao.getSquidForCond(dest_squid_id, CassandraConnectionSquid.class);
                if (connectionSquid != null) {
                    DBConnectionInfo dbs = CassandraAdapter.getCassandraConnection(connectionSquid);
                    cassandraAdapter = AdapterDataSourceManager.getAdapter(dbs);
                    cassandraColumns = new ArrayList<>();
                    List<BasicTableInfo> tableInfos = cassandraAdapter.getAllTables(tableName);
                    if (tableInfos != null&&tableInfos.size()>0) {
                        for (int i = 0; i < tableInfos.size(); i++) {
                            BasicTableInfo tableInfo = tableInfos.get(i);
                            List<ColumnInfo> columnInfos = tableInfo.getColumnList();
                            for (int j = 0; j < columnInfos.size(); j++) {
                                ColumnInfo columnInfo = columnInfos.get(j);
                                DestCassandraColumn cassandraColumn = new DestCassandraColumn();
                                cassandraColumn.setField_name(columnInfo.getName());
                                cassandraColumn.setColumn_order(j + 1);
                                cassandraColumn.setIs_dest_column(1);
                                cassandraColumn.setIs_partition_column(columnInfo.getPartition());
                                cassandraColumn.setSquid_id(cassandraSquid.getId());
                                cassandraColumn.setLength(columnInfo.getLength()==null ? 0 : columnInfo.getLength());
                                cassandraColumn.setPrecision(columnInfo.getPrecision()==null ? 0 : columnInfo.getPrecision());
                                cassandraColumn.setScale(columnInfo.getScale() == null ? 0 : columnInfo.getScale()) ;
                                cassandraColumn.setIs_primary_column(columnInfo.isPrimary() ? 1 : 0);
                                cassandraColumn.setData_type(columnInfo.getDbBaseDatatype().value());
                                cassandraColumn.setId(adapter.insert2(cassandraColumn));
                                cassandraColumns.add(cassandraColumn);
                            }
                        }
                    }else{
                        out.setMessageCode(MessageCode.CASSANDRA_DATASOURCE_OR_TABLE_NO_EXIST);
                    }
                }
            } else {
                if (cassandraColumns != null) {
                    sqlMap = new HashMap<>();
                    for (DestCassandraColumn column : cassandraColumns) {
                        if (column.getColumn_id() != 0) {
                            sqlMap.put("id", column.getColumn_id());
                            Column upColumn = adapter.query2Object(true, sqlMap, Column.class);
                            if (upColumn != null) {
                                column.setColumn(upColumn);
                            }
                        }
                    }
                }
            }
            returnMap.put("DestCassandraColumns", cassandraColumns);
            return JsonUtil.toJsonString(returnMap, DSObjectType.DEST_CASSANDRA, out.getMessageCode());
        } catch (Exception e) {
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
            e.printStackTrace();
            try {
                adapter.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
        } finally {
            if (cassandraAdapter != null) {
                cassandraAdapter.close();
            }
            if (adapter != null) {
                adapter.closeSession();
            }
        }
        return JsonUtil.toJsonString(returnMap, DSObjectType.DEST_CASSANDRA, out.getMessageCode());
    }

    /**
     * 更新column  id
     *
     * @param info
     * @return
     */
    public String updateDestCassandraColumn(String info) {
        ReturnValue out = new ReturnValue();
        IRelationalDataManager adapter = null;
        try {
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            List<DestCassandraColumn> cassandraColumns = JsonUtil.toGsonList(JsonUtil.toHashMap(info).get("DestCassandraColumns") + "", DestCassandraColumn.class);
            if (cassandraColumns != null) {
                for (DestCassandraColumn cassandraColumn : cassandraColumns) {
                    Map<String, Object> paramMap = new HashMap<>();
                    paramMap.put("id",cassandraColumn.getColumn_id());
                    List<Column> columns = adapter.query2List2(true, paramMap, Column.class);
                    ColumnInfo columnInfo=new ColumnInfo();
                    columnInfo.setName(cassandraColumn.getField_name());
                    columnInfo.setLength(cassandraColumn.getLength());
                    columnInfo.setScale(cassandraColumn.getScale());
                    columnInfo.setPartition(cassandraColumn.getPrecision());
                    columnInfo.setDbBaseDatatype(DbBaseDatatype.parse(cassandraColumn.getData_type()));
                    SystemDatatype systemDatatype = DataTypeConvertor.getSystemDatatypeByColumnInfo(DataBaseType.CASSANDRA.value(),columnInfo);
                    for (Column column :columns){
                        if(column.getData_type()==systemDatatype.value()){
                            adapter.update2(cassandraColumn);
                        }else{
                            out.setMessageCode(MessageCode.ERR_COLUMN_INPUT_TYPE);
                        }
                    }
                    //adapter.update2(cassandraColumn);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            if (adapter != null) {
                try {
                    adapter.rollback();
                } catch (SQLException e1) {
                    e1.printStackTrace();
                }
            }
            out.setMessageCode(MessageCode.UPDATE_ERROR);
        } finally {
            if (adapter != null) {
                adapter.closeSession();
            }
        }
        return JsonUtil.toJsonString(new HashMap<>(), DSObjectType.DEST_CASSANDRA, out.getMessageCode());
    }

    /**
     * 创建squidlink（只需要创建squidlink，无需做其他操作）
     *
     * @param info
     * @return
     */
    public String createDestCassandraLink(String info) {
        ReturnValue out = new ReturnValue();
        IRelationalDataManager adapter = null;
        Map<String, Object> returnMap = new HashMap<>();
        try {
            Map<String, Object> paramMap = JsonUtil.toHashMap(info);
            SquidLink squidLink = JsonUtil.toGsonBean(paramMap.get("SquidLink") + "", SquidLink.class);
            if (squidLink != null) {
                adapter = DataAdapterFactory.getDefaultDataManager();
                adapter.openSession();
                int squidLinkId = adapter.insert2(squidLink);
                returnMap.put("SquidLinkId", squidLinkId);
            } else {
                out.setMessageCode(MessageCode.ERR_SQUIDlINK_LIST_NULL);
            }
        } catch (Exception e) {
            e.printStackTrace();
            if (adapter != null) {
                try {
                    adapter.rollback();
                } catch (SQLException e1) {
                    e1.printStackTrace();
                }
            }
            out.setMessageCode(MessageCode.SQL_ERROR);
        } finally {
            if (adapter != null) {
                adapter.closeSession();
            }
        }
        return JsonUtil.toJsonString(returnMap, DSObjectType.DEST_CASSANDRA, out.getMessageCode());
    }

    /**
     * 删除squidlink（需要将CassandraColumn中的column_id设置为0）
     *
     * @param info
     * @return
     */
    public String deleteDestCassandraSquidLink(String info) {
        ReturnValue out = new ReturnValue();
        IRelationalDataManager adapter = null;
        try {
            Map<String, Object> paramMap = JsonUtil.toHashMap(info);
            SquidLink squidLink = JsonUtil.toGsonBean(paramMap.get("SquidLink") + "", SquidLink.class);
            if (squidLink != null) {
                adapter = DataAdapterFactory.getDefaultDataManager();
                adapter.openSession();
                ISquidLinkDao squidLinkDao = new SquidLinkDaoImpl(adapter);
                //删除squidLink
                squidLinkDao.delete(squidLink.getId(), SquidLink.class);
                //将cassandra Column中的column_id置为空
                String sql = "update ds_dest_cassandra_column set column_id=0 where squid_id = " + squidLink.getTo_squid_id();
                adapter.execute(sql);
            }
        } catch (Exception e) {
            e.printStackTrace();
            if (adapter != null) {
                try {
                    adapter.rollback();
                } catch (SQLException e1) {
                    e1.printStackTrace();
                }
            }
            out.setMessageCode(MessageCode.SQL_ERROR);
        } finally {
            if (adapter != null) {
                adapter.closeSession();
            }
        }
        return JsonUtil.toJsonString(new HashMap<>(), DSObjectType.DEST_CASSANDRA, out.getMessageCode());
    }
}

