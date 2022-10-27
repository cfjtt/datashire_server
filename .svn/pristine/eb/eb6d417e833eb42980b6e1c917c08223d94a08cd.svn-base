package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.entity.HBaseConnectionSquid;
import com.eurlanda.datashire.utility.ReturnValue;
import com.eurlanda.datashire.utility.SysConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by yhc on 2/23/2016.
 * 对Hbase数据库进行操作
 */
public class HbaseService {
    private static Logger logger = Logger.getLogger(HbaseService.class);

    /**
     * 通过Hbase连接Squid获取Hbase中所有的表
     *
     * @param currentSquid
     * @return
     */
    public TableName[] getHbaseTablesByHbaseConnectionSquid(HBaseConnectionSquid currentSquid) throws Exception {
        ReturnValue out = new ReturnValue();
        //设置Hbase连接信息
        Configuration hBaseConfig = HBaseConfiguration.create();
        String hBaseUrl = currentSquid.getUrl();
        String hBaseQuorum = hBaseUrl.split(":")[0];
        String hBasePort = hBaseUrl.split(":")[1];

        hBaseConfig.set("hbase.zookeeper.quorum", hBaseQuorum);
        hBaseConfig.set("hbase.zookeeper.property.clientPort", hBasePort);
        SysConf sysConf = new SysConf();
        hBaseConfig.setInt("hbase.rpc.timeout", Integer.parseInt(sysConf.getValue("hbase.rpc.timeout")));  //rpc超时时间
        hBaseConfig.setInt("zookeeper.session.timeout", Integer.parseInt(sysConf.getValue("zookeeper.session.timeout")));
        hBaseConfig.setInt("hbase.client.retries.number", Integer.parseInt(sysConf.getValue("hbase.client.retries.number"))); //重连次数
        hBaseConfig.setInt("ipc.socket.timeout", Integer.parseInt(sysConf.getValue("ipc.socket.timeout"))); //建立socket连接的超时时间
        hBaseConfig.setInt("hbase.client.pause", Integer.parseInt(sysConf.getValue("hbase.client.pause")));  //重试的休眠时间
        hBaseConfig.setInt("zookeeper.recovery.retry", Integer.parseInt(sysConf.getValue("zookeeper.recovery.retry")));
        hBaseConfig.setInt("zookeeper.recovery.retry.intervalmill", Integer.parseInt(sysConf.getValue("zookeeper.recovery.retry.intervalmill")));
        hBaseConfig.setInt("hbase.client.operation.timeout", 5000);
        TableName[] tableNames = null;
        Connection hbaseConnection = null;
        try {
            hbaseConnection = ConnectionFactory.createConnection(hBaseConfig);
            if (currentSquid.getFilter().isEmpty()) {
                tableNames = hbaseConnection.getAdmin().listTableNames(); //获取所有表名
            } else {
                //模糊查询，只要出现了就匹配成功
                String filter = currentSquid.getFilter();
                filter = filter.replaceAll("\\*", ".*").replaceAll("\\?", ".").replaceAll("%", ".*");
                String[] filters = filter.split(",");
                List<TableName[]> tableList = new ArrayList<>();
                for (String str : filters) {
                    tableList.add(hbaseConnection.getAdmin().listTableNames(str, false)); //获取所有表名
                }

                Set<TableName> returnNames = new HashSet<>();
                for (TableName[] tableName : tableList) {
                    for (TableName table : tableName) {
                        if(returnNames.size()==0){
                            returnNames.add(table);
                        }else {
                            if (!returnNames.contains(table)) {
                                returnNames.add(table);
                            }
                        }

                        //这样会报错。returnNames循环检查的时候发现，集合有变化。报ConcurrentModificationException异常。
                       /* if (returnNames.size() == 0) {
                            returnNames.add(table);
                        } else {
                            for (TableName returnName : returnNames) {
                                if (!table.getNameAsString().equals(returnName.getNameAsString())) {
                                    returnNames.add(table);
                                }
                            }
                        }*/
                    }
                }
                tableNames = new TableName[returnNames.size()];
                returnNames.toArray(tableNames);
            }
        } catch (IOException ex) {
            ex.printStackTrace();
            throw ex;
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            hbaseConnection.close();
            return tableNames;
        }

    }

    /**
     * 获取Hbase库中指定数量的列与列族名称(重复名称会被筛选掉)
     *
     * @param topNumber 指定的行数
     * @return
     */
    public List<Cell> getHbaseColumnAndColumnFamilyNames(int topNumber, HBaseConnectionSquid hBaseConnectionSquid, String tableName) throws Exception {
        Configuration hBaseConfig = HBaseConfiguration.create();
        String hBaseUrl = hBaseConnectionSquid.getUrl();
        String hBaseQuorum = hBaseUrl.split(":")[0];
        String hBasePort = hBaseUrl.split(":")[1];

        hBaseConfig.set("hbase.zookeeper.quorum", hBaseQuorum);
        hBaseConfig.set("hbase.zookeeper.property.clientPort", hBasePort);

        Connection hbaseConnection = null;
        List<String> columnNames = new ArrayList<>();
        List<Cell> htableResult = new ArrayList<>();

        hbaseConnection = ConnectionFactory.createConnection(hBaseConfig);
        Table htable = hbaseConnection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        Filter topNFilter = new PageFilter(topNumber);
        scan.setFilter(topNFilter);
        ResultScanner results = htable.getScanner(scan);
        int i = 0;
        for (Result r : results) {
            i++;
            if (i > topNumber)
                break;
            for (Cell cell : r.rawCells()) {
                String columnName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()) + ":" + Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
                if (columnNames.contains(columnName))
                    continue;
                columnNames.add(columnName);
                htableResult.add(cell);
            }

        }
        hbaseConnection.close();
        return htableResult;
    }

    /**
     * 通过行数获取预览数据
     *
     * @param rowNumber
     * @return
     */
    public String getHbasePreviewData(int rowNumber, HBaseConnectionSquid hBaseConnectionSquid, String tableName) throws Exception {
        Configuration hBaseConfig = HBaseConfiguration.create();
        String hBaseUrl = hBaseConnectionSquid.getUrl();
        String hBaseQuorum = hBaseUrl.split(":")[0];
        String hBasePort = hBaseUrl.split(":")[1];

        hBaseConfig.set("hbase.zookeeper.quorum", hBaseQuorum);
        hBaseConfig.set("hbase.zookeeper.property.clientPort", hBasePort);

        Connection hbaseConnection = null;
        hbaseConnection = ConnectionFactory.createConnection(hBaseConfig);

        Table htable = hbaseConnection.getTable(TableName.valueOf(tableName));

        Scan scan = new Scan();
        ResultScanner results = htable.getScanner(scan);
        int i = 0;
        String returnValueString = "";
        for (Result r : results) {
            i++;
            if (i > rowNumber)
                break;
            //String rowString = "";
            StringBuffer rowString = new StringBuffer("");
            boolean getedRowKey = false;
            Cell[] rawCells = r.rawCells();
            if (rawCells != null && rawCells.length > 0) {
                for (Cell cell : r.rawCells()) {
                    if (!getedRowKey) {
                        rowString.append("rowkey:" + Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
                        getedRowKey = true;
                    }
                    rowString.append(" || " + Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()) + ":"
                            + Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()) + " " + Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));

                }
            }
            rowString.append("\r\n");
            returnValueString += rowString.toString();
        }
        return returnValueString;
    }
}

