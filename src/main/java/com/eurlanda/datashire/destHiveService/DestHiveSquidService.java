package com.eurlanda.datashire.destHiveService;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.adapter.datatype.DataTypeConvertor;
import com.eurlanda.datashire.adapter.db.AdapterDataSourceManager;
import com.eurlanda.datashire.adapter.db.HiveAdapter;
import com.eurlanda.datashire.adapter.db.INewDBAdapter;
import com.eurlanda.datashire.dao.ISquidLinkDao;
import com.eurlanda.datashire.dao.impl.SquidLinkDaoImpl;
import com.eurlanda.datashire.entity.Column;
import com.eurlanda.datashire.entity.DBConnectionInfo;
import com.eurlanda.datashire.entity.SquidLink;
import com.eurlanda.datashire.entity.dest.DestHiveColumn;
import com.eurlanda.datashire.entity.dest.DestHiveSquid;
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
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Eurlanda on 2017/5/3.
 */
@Service
public class DestHiveSquidService {
    private static Log log = LogFactory.getLog(DestHiveSquidService.class);
    private String token;
    private String key;

    /**
     * 创建hive落地squid
     *
     * @param info
     * @return
     */
    public String createDestHiveSquid(String info) {
        ReturnValue out = new ReturnValue();
        Map<String, Object> resultMap = new HashMap<>();
        IRelationalDataManager adapter = null;
        try {
            Map<String, Object> paramsMap = JsonUtil.toHashMap(info);
            DestHiveSquid destHiveSquid = JsonUtil.toGsonBean(paramsMap.get("DestHiveSquid").toString(), DestHiveSquid.class);
            if (null != destHiveSquid) {
                adapter = DataAdapterFactory.getDefaultDataManager();
                adapter.openSession();
                int newSquidId = adapter.insert2(destHiveSquid);
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
            log.error("创建DestHiveSquid异常", e);
            out.setMessageCode(MessageCode.INSERT_ERROR);
        } finally {
            if (adapter != null) {
                adapter.closeSession();
            }
        }
        return JsonUtil.toJsonString(resultMap, DSObjectType.DEST_HIVE, out.getMessageCode());
    }

    /**
     * 更新
     *
     * @param info
     * @return
     */
    public String updateDestHiveSquid(String info) {
        ReturnValue out = new ReturnValue();
        IRelationalDataManager adapter = null;
        try {
            Map<String, Object> paramsMap = JsonUtil.toHashMap(info);
            DestHiveSquid destHiveSquid = JsonUtil.toGsonBean(paramsMap.get("DestHiveSquid").toString(), DestHiveSquid.class);
            if (null != destHiveSquid) {
                adapter = DataAdapterFactory.getDefaultDataManager();
                adapter.openSession();
                Boolean flag = adapter.update2(destHiveSquid);
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
            log.error("更新DestHiveSquid异常", e);
            out.setMessageCode(MessageCode.UPDATE_ERROR);
        } finally {
            if (adapter != null) {
                adapter.closeSession();
            }
        }
        return JsonUtil.toJsonString(new HashMap<String, Object>(), DSObjectType.DEST_HIVE, out.getMessageCode());
    }

    /**
     * 获取表中的列的信息
     *
     * @param info
     * @return
     */
    public String getDestHiveSquidColumn(String info) {
        IRelationalDataManager adapter = null;
        INewDBAdapter hiveAdapter = null;
        ReturnValue out = new ReturnValue();
        Map<String, Object> returnMap = new HashMap<>();
        try {
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            Map<String, Object> paramMap = JsonUtil.toHashMap(info);
            DestHiveSquid hiveSquid = JsonUtil.toGsonBean(paramMap.get("DestHiveSquid") + "", DestHiveSquid.class);
            boolean refresh = Boolean.parseBoolean(paramMap.get("Refresh") + "");
            Map<String, Object> sqlMap = new HashMap<>();
            sqlMap.put("squid_id", hiveSquid.getId());
            List<DestHiveColumn> hiveColumns = adapter.query2List2(true, sqlMap, DestHiveColumn.class);
            if (!refresh) {
                if (hiveColumns == null || hiveColumns.size() == 0) {
                    refresh = true;
                }
            }
            if(StringUtils.isNull(hiveSquid.getDb_name()) || StringUtils.isNull(hiveSquid.getTable_name())){
                returnMap.put("DestHiveSquidColumn",hiveColumns);
                return JsonUtil.toJsonString(returnMap, DSObjectType.DEST_HIVE, out.getMessageCode());
            }
            if (refresh) {
                //先删除原来的estHiveColumn数据
                String delSql = "delete from ds_dest_hive_column where squid_id=" + hiveSquid.getId();
                adapter.execute(delSql);
                //获取squid信息，得到tableName
                String tableName = hiveSquid.getTable_name();
                DBConnectionInfo dbs = HiveAdapter.getHiveConnection();
                dbs.setDbName(hiveSquid.getDb_name());
                hiveAdapter = AdapterDataSourceManager.getAdapter(dbs);
                List<ColumnInfo> columnInfoList = hiveAdapter.getTableColumns(tableName, hiveSquid.getDb_name());
                hiveColumns = new ArrayList<>();
                if (columnInfoList != null) {
                    for (int i = 0; i < columnInfoList.size(); i++) {
                        ColumnInfo columnInfo = columnInfoList.get(i);
                        DestHiveColumn hiveColumn = new DestHiveColumn();
                        hiveColumn.setField_name(columnInfo.getName());
                        hiveColumn.setColumn_order(i + 1);
                        hiveColumn.setIs_dest_column(1);
                        hiveColumn.setIs_partition_column(columnInfo.getPartition());
                        hiveColumn.setSquid_id(hiveSquid.getId());
                        hiveColumn.setLength(columnInfo.getLength());
                        hiveColumn.setPrecision(columnInfo.getPrecision());
                        hiveColumn.setScale(columnInfo.getScale());
                        //SystemDatatype dataType = DataTypeConvertor.getSystemDatatypeByColumnInfo(DataBaseType.HIVE.value(),columnInfo);
                        //hiveColumn.setData_type(dataType.value());
                        hiveColumn.setData_type(columnInfo.getDbBaseDatatype().value());
                        hiveColumn.setId(adapter.insert2(hiveColumn));
                        hiveColumns.add(hiveColumn);
                    }
                }
            } else {
                if (hiveColumns != null) {
                    sqlMap = new HashMap<>();
                    for (DestHiveColumn column : hiveColumns) {
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
            returnMap.put("DestHiveSquidColumn", hiveColumns);
            return JsonUtil.toJsonString(returnMap, DSObjectType.DEST_HIVE, out.getMessageCode());
        } catch (Exception e) {
            out.setMessageCode(MessageCode.SQL_ERROR);
            e.printStackTrace();
            try {
                adapter.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
            if (e instanceof NoSuchObjectException) {
               out.setMessageCode(MessageCode.HIVE_DATABASE_OR_TABLE_NO_EXIST);
            }
        } finally {
            if (hiveAdapter != null) {
                hiveAdapter.close();
            }
            if (adapter != null) {
                adapter.closeSession();
            }
        }
        return JsonUtil.toJsonString(returnMap, DSObjectType.DEST_HIVE, out.getMessageCode());
    }

    /**
     * 更新column  id
     *
     * @param info
     * @return
     */
    public String updateDestHiveSquidColumn(String info) {
        ReturnValue out = new ReturnValue();
        IRelationalDataManager adapter = null;
        try {
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            List<DestHiveColumn> hiveColumns = JsonUtil.toGsonList(JsonUtil.toHashMap(info).get("UpdateDestHiveSquidColumns") + "", DestHiveColumn.class);
            if (hiveColumns != null) {
                for (DestHiveColumn hiveColumn : hiveColumns) {
                    Map<String, Object> paramMap = new HashMap<>();
                    paramMap.put("id",hiveColumn.getColumn_id());
                    List<Column> columns = adapter.query2List2(true, paramMap, Column.class);
                    ColumnInfo columnInfo=new ColumnInfo();
                    columnInfo.setName(hiveColumn.getField_name());
                    columnInfo.setLength(hiveColumn.getLength());
                    columnInfo.setScale(hiveColumn.getScale());
                    columnInfo.setPartition(hiveColumn.getPrecision());
                    columnInfo.setDbBaseDatatype(DbBaseDatatype.parse(hiveColumn.getData_type()));
                    SystemDatatype systemDatatype = DataTypeConvertor.getSystemDatatypeByColumnInfo(DataBaseType.HIVE.value(),columnInfo);
                    for (Column column :columns){
                        if(column.getData_type()==systemDatatype.value()){
                            adapter.update2(hiveColumn);
                        }else{
                            out.setMessageCode(MessageCode.ERR_COLUMN_INPUT_TYPE);
                        }
                    }
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
        return JsonUtil.toJsonString(new HashMap<>(), DSObjectType.DEST_HIVE, out.getMessageCode());
    }

    /**
     * 创建squidlink（只需要创建squidlink，无需做其他操作）
     *
     * @param info
     * @return
     */
    public String createDestHiveLink(String info) {
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
        return JsonUtil.toJsonString(returnMap, DSObjectType.DEST_HIVE, out.getMessageCode());
    }

    /**
     * 删除squidlink（需要将hiveColumn中的column_id设置为0）
     *
     * @param info
     * @return
     */
    public String deleteDestSquidLink(String info) {
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
                //将hiveColumn中的column_id置为空
                String sql = "update ds_dest_hive_column set column_id=0 where squid_id = " + squidLink.getTo_squid_id();
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
        return JsonUtil.toJsonString(new HashMap<>(), DSObjectType.DEST_HIVE, out.getMessageCode());
    }
}
