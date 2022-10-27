package com.eurlanda.datashire.sprint7.service.squidflow.dest;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.entity.Column;
import com.eurlanda.datashire.entity.SquidLink;
import com.eurlanda.datashire.entity.dest.DestHDFSColumn;
import com.eurlanda.datashire.entity.dest.DestHDFSSquid;
import com.eurlanda.datashire.sprint7.packet.InfoNewMessagePacket;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import com.eurlanda.datashire.utility.SysConf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Created by dzp on 2016/4/26.
 */
@Service
public class DestHDFSSquidService {
    private static Log log = LogFactory.getLog(DestEsSquidService.class);
    private String token;//令牌根据令牌得到相应的连接信息
    private String key;// key值

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }


    /**
     * 创建HDFS落地squid
     * @param info  "RepositoryId:int SquidFlowId:int DestHDFSSquid:DestHDFSSquid"
     * @return  "newSquidId:int"
     */
    public String createDestHDFSSquid(String info){
        ReturnValue out = new ReturnValue();
        Map<String, Object> output = new HashMap<>();
        DataAdapterFactory adapterFactory = null;
        IRelationalDataManager adapter =null;
        try {
            adapter = DataAdapterFactory.getDefaultDataManager();
            Map<String, Object> paramsMap = JsonUtil.toHashMap(info);
            adapter.openSession();
            DestHDFSSquid destHDFSSquid = JsonUtil.toGsonBean(paramsMap.get("DestHDFSSquid")
                    .toString(),DestHDFSSquid.class);
            if (destHDFSSquid!=null){
                if (SysConf.getValue("hdfs_host").equals(destHDFSSquid.getHost())) {
                    destHDFSSquid.setHost(SysConf.getValue("hdfsIpAndPort"));
                }
                int newSquidId = adapter.insert2(destHDFSSquid);
                output.put("newSquidId",newSquidId);
            }else{
                out.setMessageCode(MessageCode.NODATA);
            }
        }catch (Exception e){
            log.error("CreateDestHDFSSquid",e);
            out.setMessageCode(MessageCode.INSERT_ERROR);
            try{
                adapter.rollback();
            }catch (SQLException e1){  // 数据库回滚失败（程序不能处理该异常）！
                log.fatal("rollback err",e1);
            }
        }finally {
            adapter.closeSession();
        }
        InfoNewMessagePacket infoNewMessagePacket = new InfoNewMessagePacket<>(output,out.getMessageCode().value());
        return infoNewMessagePacket.toJsonString();
    }



    /**
     * 更新HDFS落地squid
     * @param info "DestHDFSSquid:DestHDFSSquid"
     */
    public String updateDestHDFSSquid (String info){
        IRelationalDataManager adapter = null;
        ReturnValue out = new ReturnValue();
        Map<String, Object> paramsMap = JsonUtil.toHashMap(info);
        try{
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            DestHDFSSquid destHDFSSquid = JsonUtil.toGsonBean(paramsMap.get("DestHDFSSquid")
                    .toString(),DestHDFSSquid.class);
            if (destHDFSSquid!=null){
                if (SysConf.getValue("hdfs_host").equals(destHDFSSquid.getHost())) {
                    destHDFSSquid.setHost(SysConf.getValue("hdfsIpAndPort"));
                }
                adapter.update2(destHDFSSquid);
            }else{
                out.setMessageCode(MessageCode.NODATA);
            }
        }catch (Exception e){
            log.error("UpdateDestHDFSSquid is err",e);
            out.setMessageCode(MessageCode.UPDATE_ERROR);
            try{
                adapter.rollback();
            }catch (SQLException e1){  // 数据库回滚失败（程序不能处理该异常）！
                log.fatal("rollback err !",e1);
            }
        }
        finally {
            adapter.closeSession();
        }
        InfoNewMessagePacket infoNewMessagePacket = new InfoNewMessagePacket<>(new HashMap<>(),out.getMessageCode().value());
        return infoNewMessagePacket.toJsonString();
    }

    /**
     * 创建HDFS 落地squid link, 创建link的时候，HDFSSquid需要添加HDFScolumn
     * @param info SquidLink
     * @return SquidLinkId
     */
    public  String createDestHDFSSquidLink(String info){
        ReturnValue out = new ReturnValue();
        IRelationalDataManager adapter = null;
        Map<String, Object> output = new HashMap<>();
        Map<String, Object> paramsMap = JsonUtil.toHashMap(info);
        SquidLink squidLink = JsonUtil.toGsonBean(paramsMap.get("SquidLink").toString(),SquidLink.class);
        squidLink.setKey(UUID.randomUUID().toString());
        try{
            adapter =DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            squidLink.setId(adapter.insert2(squidLink));
            // 添加 hdfsColumn
            // 1. 获取上游squid column
            Map<String, String> params = new HashMap<>();
            params.put("squid_id", squidLink.getFrom_squid_id() + "");
            List<Column> columns = adapter.query2List(true, params, Column.class);
            if(columns != null) {
                // 2. 生成hdfsColumn
                for(Column column : columns) {
                    DestHDFSColumn HdfsColumn =HdfsColumnService.getHDFSColumnByColumn(column, squidLink.getTo_squid_id());
                    // 3. 插入数据库
                    adapter.insert2(HdfsColumn);
                }
            }

            output.put("SquidLinkId", squidLink.getId());
        } catch (Exception e) {
            log.error("createDestHDFSSquidLink error", e);
            out.setMessageCode(MessageCode.SQL_ERROR);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                log.fatal("rollback err!", e1);
            }
        } finally {
            if(adapter != null) {
                adapter.closeSession();
            }
        }

        InfoNewMessagePacket infoNewMessagePacket = new InfoNewMessagePacket<>(output, out.getMessageCode().value());
        return  infoNewMessagePacket.toJsonString();
        }
    }


