package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.entity.DBSourceTable;
import com.eurlanda.datashire.entity.HBaseConnectionSquid;
import com.eurlanda.datashire.entity.SourceTable;
import com.eurlanda.datashire.enumeration.DataBaseType;
import com.eurlanda.datashire.sprint7.packet.InfoNewMessagePacket;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.TableName;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by yhc on 2/22/2016.
 */
@Service
public class HbaseSquidService {
    private static Log log = LogFactory.getLog(HbaseSquidService.class);

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
     * 创建Hbase connection SquidModelBase
     * @param info
     * @return
     */
    public String createHbaseConnectionSquid(String info){
        ReturnValue out = new ReturnValue();
        Map<String, Object> outputMap = new HashMap<>();
        IRelationalDataManager adapter = null;
        try {

            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            //获取数据Map
            Map<String, Object> paramsMap = JsonUtil.toHashMap(info);
            HBaseConnectionSquid
                    newHbaseConnectionSquid = JsonUtil.toGsonBean(String.valueOf(paramsMap.get("HbaseConnectionSquid")), HBaseConnectionSquid.class);
            if (newHbaseConnectionSquid == null){
                out.setMessageCode(MessageCode.NODATA);
            } else {
                int newSquidId = adapter.insert2(newHbaseConnectionSquid);
                outputMap.put("newSquidId", newSquidId);
            }
        } catch (Exception e){
            log.error("Create HbaseConnectionSquid error", e);
            out.setMessageCode(MessageCode.INSERT_ERROR);
            try {
                adapter.rollback();
            } catch (SQLException sqlE){
                log.fatal("rollback error", sqlE);
            }
        } finally {
            adapter.closeSession();
        }

        InfoNewMessagePacket infoPacket = new InfoNewMessagePacket(outputMap, out.getMessageCode().value());
        return infoPacket.toJsonString();
    }

    /**
     * 更新Hbase connection squid
     * @param info
     * @return
     */
    public String updateHbaseConnectionSquid(String info){
        ReturnValue out = new ReturnValue();
        DataAdapterFactory adapterFactory = null;
        IRelationalDataManager adapter = null;
        try {
            adapterFactory = DataAdapterFactory.newInstance();
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            Map<String, Object> paramsMap = JsonUtil.toHashMap(info);
            HBaseConnectionSquid
                    currentSquid = JsonUtil.toGsonBean(String.valueOf(paramsMap.get("HbaseConnectionSquid")), HBaseConnectionSquid.class);
            currentSquid.setDb_type(DataBaseType.HBASE_PHOENIX.value());
            if (currentSquid != null){
                adapter.update2(currentSquid);
            } else {
                out.setMessageCode(MessageCode.NODATA);
            }
        } catch (Exception e){
            log.error("Updated Hbase Connection SquidModelBase error.", e);
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
     * 根据 Hbase connection squid 获取数据库表
     * @param info
     * @return
     */
    public String connectHbaseConnectionSquid(String info) throws Exception{
        ReturnValue out = new ReturnValue();
        Map<String, Object> outputMap = new HashMap<>();
        IRelationalDataManager adapter = null;
        DataAdapterFactory adaFactory = null;
        TableName[] hBaseTableNames=null;   //存放所有的表信息
        List<SourceTable> returnSourceTableList = new ArrayList<>(); //返回客户端的数据
        List<Object> params = new ArrayList<>(); // 查询参数
        List<Integer> cancelSquidIds=new ArrayList<>();
        try {
            //获取数据Map
            Map<String, Object> paramsMap = JsonUtil.toHashMap(info);
            HBaseConnectionSquid
                    hBaseConnectionSquid = JsonUtil.toGsonBean(String.valueOf(paramsMap.get("HbaseConnectionSquid")), HBaseConnectionSquid.class);
            HbaseService hbaseService = new HbaseService();
            hBaseTableNames = hbaseService.getHbaseTablesByHbaseConnectionSquid(
                    hBaseConnectionSquid);
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();

            //查询下游所有的抽取squid
            String sql=" select k.TO_SQUID_ID as TOSQUID from ds_squid_link k where k.FROM_SQUID_ID =?";
            params.clear();
            params.add(hBaseConnectionSquid.getId());
            List<Map<String, Object>> squidIds=adapter.query2List(true,sql,params);
            if(squidIds!=null && squidIds.size()>0){
                for(Map<String,Object> map:squidIds){
                    cancelSquidIds.add(Integer.parseInt(map.get("TOSQUID").toString()));
                }
            }
            //修改这squid状态。
            if(cancelSquidIds!=null && cancelSquidIds.size()>0){
                List<Object> updateSquidIds=new ArrayList<>();
                String updateSql="update ds_squid  set DESIGN_STATUS=1 where id in ";
                updateSquidIds.addAll(cancelSquidIds);
                String ids = JsonUtil.toGsonString(updateSquidIds);
                ids=ids.substring(1,ids.length()-1);
                updateSql+="("+ids+")";
                adapter.execute(updateSql);
            }
            outputMap.put("ErrorSquidIdList", cancelSquidIds);
            Map<String, String> deleteSourceTableParams= new HashMap<String, String>();
            deleteSourceTableParams.put("source_squid_id", hBaseConnectionSquid.getId()+"");
            deleteSourceTableParams.put("is_extracted", "N");
            adapter.delete(deleteSourceTableParams, DBSourceTable.class);
            deleteSourceTableParams.clear();
            deleteSourceTableParams.put("source_squid_id",hBaseConnectionSquid.getId()+"");
            List<DBSourceTable> existTables =adapter.query2List(true,deleteSourceTableParams,DBSourceTable.class);
            if (hBaseTableNames!=null && hBaseTableNames.length>0){
                for (TableName hbaseTable : hBaseTableNames) {
                    DBSourceTable existTable = null;
                    for (DBSourceTable sourceTable : existTables){
                        if (sourceTable.getTable_name().equals(hbaseTable.getNameAsString()))
                        {
                            existTable = sourceTable;
                            break;
                        }
                    }

                    //如果没有查出同名的表，就重新入库，如果有就直接返回给客户端
                    if (existTable == null) {
                        existTable = new DBSourceTable();
                        existTable.setTable_name(hbaseTable.getNameAsString());
                        existTable.setSource_squid_id(hBaseConnectionSquid.getId());
                        existTable.setIs_extracted(false);
                        existTable.setId(adapter.insert2(existTable)); //入库返回ID
                    }

                    //create sourcetable object for return to client
                    SourceTable returnSourceTable = new SourceTable();
                    returnSourceTable.setTableName(existTable.getTable_name());
                    returnSourceTable.setSource_squid_id(existTable.getSource_squid_id());
                    returnSourceTable.setIs_extracted(existTable.isIs_extracted());
                    returnSourceTable.setId(existTable.getId());
                    returnSourceTableList.add(returnSourceTable);

                }
            }
        }
        catch (IOException ioe){
            log.error("连接 hbase connection squid 异常", ioe);
            out.setMessageCode(MessageCode.COLUMNLIST_NULL);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                log.fatal("rollback err!", e1);
            }
        } catch (Exception ex){
            log.error("获取 hbase connection squid元数据异常", ex);
            out.setMessageCode(MessageCode.COLUMNLIST_NULL);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                log.fatal("rollback err!", e1);
            }
        } finally {
            adapter.closeSession();
            //当获取的表名长度不为0,但是返回的SourceTableList长度为0时
            if(returnSourceTableList.size()==0 && hBaseTableNames!=null && hBaseTableNames.length>0){
                out.setMessageCode(MessageCode.ERR_DATABASE_CONNECT);
            } else if(hBaseTableNames==null){
                out.setMessageCode(MessageCode.ERR_DATABASE_CONNECT);
            }
            else {
                outputMap.put("SourceTables", returnSourceTableList);
            }
        }
        InfoNewMessagePacket infoPacket = new InfoNewMessagePacket(outputMap, out.getMessageCode().value());
        return infoPacket.toJsonString();

    }

    /**
     * 预览Hbase数据库的数据
     * @param info
     * @return
     */
    public String previewHbaseSourceTableData(String info){
        Map<String, Object> outputMap = new HashMap<>();
        ReturnValue out = new ReturnValue();
        try{
            Map<String, Object> paramsMap = JsonUtil.toHashMap(info);
            HBaseConnectionSquid hBaseConnectionSquid = JsonUtil.toGsonBean(String.valueOf(paramsMap.get("HbaseConnectionSquid")), HBaseConnectionSquid.class);
            int sourceTableId = JsonUtil.toGsonBean(String.valueOf(paramsMap.get("SourceTableId")), int.class);
            SourceTable currentTable = null;
            for (SourceTable table : hBaseConnectionSquid.getSourceTableList()){
                if (table.getId() == sourceTableId)
                    currentTable = table;
            }
            if (currentTable == null)
                throw new Exception("必要数据缺失，严重错误，无法查询source table");
            HbaseService service = new HbaseService();
            String returnValue = service.getHbasePreviewData(100, hBaseConnectionSquid, currentTable.getTableName());
            outputMap.put("DataList", returnValue);


        } catch (Exception e) {
            log.error("获取 hbase connection squid元数据异常", e);
            out.setMessageCode(MessageCode.ERR_DATABASE_CONNECT);
        } finally {
            InfoNewMessagePacket infoPacket = new InfoNewMessagePacket(outputMap, out.getMessageCode().value());
            return infoPacket.toJsonString();
        }
    }

}
