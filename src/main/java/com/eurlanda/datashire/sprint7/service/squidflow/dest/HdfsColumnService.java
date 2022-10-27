package com.eurlanda.datashire.sprint7.service.squidflow.dest;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.dao.dest.IHdfsColumnDao;
import com.eurlanda.datashire.dao.dest.impl.HdfsColumnDaoImpl;
import com.eurlanda.datashire.entity.Column;
import com.eurlanda.datashire.entity.dest.DestHDFSColumn;
import com.eurlanda.datashire.entity.dest.DestHDFSSquid;
import com.eurlanda.datashire.sprint7.packet.InfoNewMessagePacket;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by dzp on 2016/4/27.
 */

@Service
public class HdfsColumnService {
    private static Log log = LogFactory.getLog(HdfsColumnService.class);
    private String token;
    private String key;

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
     * 得到HDFSColumn列
     * "RepositoryId:int SquidFlowId:int DestHDFSSquid:DestHDFSSquid"
     * @param info
     * @return HDFSColumns:List<DestHDFSColumn>
     */
    public String getDestHDFSColumn(String info) {
        ReturnValue out = new ReturnValue();
        IRelationalDataManager adapter = null;
        Map<String, Object> output = new HashMap<>();
        Map<String, Object> paramsMap = JsonUtil.toHashMap(info);
        try {
            adapter = DataAdapterFactory.getDefaultDataManager();
            DestHDFSSquid DestHdfsSquid = JsonUtil.toGsonBean(paramsMap.get("DestHDFSSquid").toString(),DestHDFSSquid.class);
            int destHdfsSquidId = DestHdfsSquid.getId();
            List<Object> list = new ArrayList<>();
            if (destHdfsSquidId!=0){
                adapter.openSession();
                IHdfsColumnDao columnDao = new HdfsColumnDaoImpl(adapter);
                List<DestHDFSColumn> HDFSColumns = columnDao.getHdfsColumnBySquidId(true ,destHdfsSquidId);
                //将得到的列进行排序
                if (HDFSColumns.size()>0&&HDFSColumns!=null){
                    Collections.sort(HDFSColumns,new Comparator<DestHDFSColumn>(){
                        public int compare(DestHDFSColumn arg0, DestHDFSColumn arg1) {
                            return arg0.getColumn_order() - arg1.getColumn_order();
                        }
                    });
                    for(DestHDFSColumn u : HDFSColumns){
                        list.add(u);
                    }
                }
                output.put("HDFSColumns",list);
            }
            else{
                out.setMessageCode(MessageCode.NODATA);
            }
        } catch (Exception e) {
            log.error("createDestEsSquid is error", e);
            out.setMessageCode(MessageCode.INSERT_ERROR);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                log.fatal("rollback err!", e1);
            }
        } finally {
            adapter.closeSession();
        }
        InfoNewMessagePacket infoNewMessagePacket = new InfoNewMessagePacket<>(output,out.getMessageCode().value());
        return  infoNewMessagePacket.toJsonString();
    }

    /**
     * "UpdateDestHDFSColumns:List<DestHDFSColumn>"
     * @param info
     */
    public String updateDestHDFSColumns (String info) {
        ReturnValue out = new ReturnValue();
        DataAdapterFactory adapterFactory = null;
        IRelationalDataManager adapter = null;
        try {
            adapterFactory = DataAdapterFactory.newInstance();
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            Map<String, Object> paramsMap = JsonUtil.toHashMap(info);
            List<DestHDFSColumn> esColumns  = JsonUtil.toGsonList(String.valueOf(paramsMap.get("UpdateDestHDFSColumns")),
                    DestHDFSColumn.class);
            if (null != esColumns) {
                for(DestHDFSColumn ec : esColumns) {
                    adapter.update2(ec);
                }
            }else{
                out.setMessageCode(MessageCode.NODATA);
            }
        } catch (Exception e) {
            log.error("update UpdateDestHDFSColumns is error", e);
            out.setMessageCode(MessageCode.UPDATE_ERROR);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                log.fatal("rollback err!", e1);
            }
        } finally {
            adapter.closeSession();
        }
        InfoNewMessagePacket infoNewMessagePacket = new InfoNewMessagePacket<>(new HashMap<>(), out.getMessageCode().value());
        return  infoNewMessagePacket.toJsonString();
    }


    /**
     * 通过column 生成HDFSColumn落地
     * @param column HDFS squid上游squid的column
     * @param squidId HDFS squid id
     * @return
     */
    public static DestHDFSColumn getHDFSColumnByColumn(Column column, int squidId) {
        DestHDFSColumn HdfsColumn = new DestHDFSColumn();
        HdfsColumn.setColumn(column);
        HdfsColumn.setColumn_id(column.getId());
        HdfsColumn.setSquid_id(squidId);
        HdfsColumn.setField_name(column.getName());
        HdfsColumn.setIs_dest_column(1);
        HdfsColumn.setField_name(column.getName());
        HdfsColumn.setColumn_order(column.getRelative_order());
        return HdfsColumn;
    }
}
