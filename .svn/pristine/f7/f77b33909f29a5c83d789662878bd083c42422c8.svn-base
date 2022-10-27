package com.eurlanda.datashire.server.service;

import com.eurlanda.datashire.entity.Column;
import com.eurlanda.datashire.entity.DbSquid;
import com.eurlanda.datashire.entity.SourceColumn;
import com.eurlanda.datashire.entity.SourceTable;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.enumeration.DataBaseType;
import com.eurlanda.datashire.exception.SystemErrorException;
import com.eurlanda.datashire.server.dao.SourceColumnDao;
import com.eurlanda.datashire.server.dao.SourceTableDao;
import com.eurlanda.datashire.server.dao.SquidDao;
import com.eurlanda.datashire.server.model.Base.SquidModelBase;
import com.eurlanda.datashire.server.utils.Constants;
import com.eurlanda.datashire.server.utils.dbsource.AdapterDataSourceManager;
import com.eurlanda.datashire.server.utils.dbsource.INewDBAdapter;
import com.eurlanda.datashire.server.utils.entity.DBConnectionInfo;
import com.eurlanda.datashire.server.utils.entity.operation.ColumnInfo;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by eurlanda - new 2 on 2017/7/11.
 */
@Service
public class SourceColumnService {
    private static final Logger logger = LoggerFactory.getLogger(SourceColumnService.class);
    @Autowired
    private SourceColumnDao sourceColumnDao;
    @Autowired
    private SourceTableDao sourceTableDao;
    @Autowired
    private SquidDao squidDao;

    /**
     * 获取SourceColumn
     * @param dbSquid
     * @param sourceTable
     * @param type
     * @return
     * @throws Exception
     */
    public String getSourceColumn(DbSquid dbSquid, SourceTable sourceTable, int type) throws Exception{
        ReturnValue out = new ReturnValue();
        Map<String, String> tempMap = new HashMap<>();
        Map<String,Object> resultMap=new HashMap<>();
        List<Integer> resultSquids=new ArrayList<>();
        INewDBAdapter adaoterSource = null;
        try {
            if(dbSquid!=null&&sourceTable!=null){
                // 获取数据源
                DBConnectionInfo dbs = new DBConnectionInfo();
                dbs.setHost(dbSquid.getHost());
                dbs.setPort(dbSquid.getPort());
                dbs.setUserName(dbSquid.getUser_name());
                dbs.setPassword(dbSquid.getPassword());
                dbs.setDbName(dbSquid.getDb_name());
                dbs.setDbType(DataBaseType.parse(dbSquid.getDb_type()));
                adaoterSource = AdapterDataSourceManager.getAdapter(dbs);
                //获取数据源列
                List<ColumnInfo> columnList = adaoterSource.getTableColumns(sourceTable.getTableName(), dbs.getDbName());
                //转换列
                List<SourceColumn> sourceColumnList= this.converColumn(columnList, sourceTable.getId(), out);
                //获取缓存表的列
                List<SourceColumn> sourceColumns =sourceColumnDao.getColumnByTableId(sourceTable.getId());
                int num=0;
                if(sourceColumnList.size()==sourceColumns.size()){
                    for (SourceColumn sourceColumn:sourceColumnList){
                        for (SourceColumn sourceColumn1:sourceColumns){
                            //判断数据源的列与缓存表的列是否一样.
                            if(sourceColumn.getName().equals(sourceColumn1.getName())&&sourceColumn.getData_type()==sourceColumn1.getData_type()
                                    && sourceColumn.getPrecision()==sourceColumn1.getPrecision() && sourceColumn.getScale()==sourceColumn1.getScale()
                                    && sourceColumn.isNullable()==sourceColumn1.isNullable() && sourceColumn.getLength()==sourceColumn1.getLength()
                                    && sourceColumn.getRelative_order()==sourceColumn1.getRelative_order() && sourceColumn.isIspk()==sourceColumn1.isIspk()
                                    && sourceColumn.isIsunique()==sourceColumn1.isIsunique()){
                                num++;
                                break;
                            }
                        }
                    }
                }
                //不一样就重新添加
                if(num!=sourceColumns.size()||sourceColumns.size()==0) {
                    if (sourceColumnList != null && sourceColumnList.size() != 0) {
                        tempMap = new HashMap<String, String>();
                        for (SourceColumn sourceColumn : sourceColumnList) {
                            logger.debug("[需要增加的列名=]" + sourceColumn.getName());
                            if (tempMap.containsKey(sourceColumn.getName().toLowerCase())) {
                                //生成系统名称
                                String newColumnName = getSysColumnName(sourceColumn.getName().toLowerCase(), tempMap, 1);
                                sourceColumn.setName(newColumnName);
                            }
                            tempMap.put(sourceColumn.getName().toLowerCase(), sourceColumn.getName().toLowerCase());
                        }
                        List<Integer> sourceColumnId=new ArrayList<>();
                        //要删除的sourceColumn
                        for (SourceColumn sourceColumn :sourceColumns){
                            sourceColumnId.add(sourceColumn.getId());
                        }
                        if(sourceColumnId!=null&&sourceColumnId.size()>0){
                            sourceColumnDao.deleteSourceColumnByIds(sourceColumnId);
                            sourceColumns.clear();
                        }
                        logger.debug("[需要删除列的个数=]" + sourceColumnId.size());
                        //批量添加
                        sourceColumnDao.insertSourceColumn(sourceColumnList);
                        logger.debug("[需要添加列的个数=]" + sourceColumnList.size());
                    }
                }
                //重新连接后判断下游数据和源数据是否相同，不相同就作废squid
                if(sourceTable.isIs_extracted()&&num!=sourceColumns.size()){
                    int i=0;
                    //根据sourceTableId查squid
                    List<SquidModelBase> squids=squidDao.selectBySourceTable(sourceTable.getId());
                    if(squids!=null&&squids.size()>0){
                        for (SquidModelBase squidModelBase :squids){
                            //有不同的就作废
                            resultSquids.add(squidModelBase.getId());
                        }
                    }
                    //根据id把对应的抽取状态改为0
                    sourceTableDao.updateIsExtractedById(sourceTable.getId());
                    sourceColumns.clear();
                }
                //修改squid的状态
                if(resultSquids!=null&&resultSquids.size()>0){
                    squidDao.updateSquStatusBySquIds(resultSquids);
                }
                if(sourceColumns==null||sourceColumns.size()==0){
                    sourceColumns.addAll(sourceColumnList);
                }
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
                    map.put(sourceColumns.get(i).getName(),column);
                }
                resultMap.put("MapColumnInfo",map);
                resultMap.put("ErrorSquidIdList",resultSquids);
                return JsonUtil.toJsonString(resultMap, DSObjectType.DBSOURCE, out.getMessageCode());
            }else{
                out.setMessageCode(MessageCode.DESERIALIZATION_FAILED);
                return JsonUtil.toJsonString(resultMap, null, out.getMessageCode());
            }
        }catch (SystemErrorException e) {
            logger.error("getConnect is error",e);
            out.setMessageCode(MessageCode.ERR_DATABASE_CONNECT);
            return JsonUtil.toJsonString(resultMap, DSObjectType.DBSOURCE, out.getMessageCode());
        }catch (Exception e){
            logger.error("getConnect is error",e);
            out.setMessageCode(MessageCode.SQL_ERROR);
            return JsonUtil.toJsonString(resultMap, DSObjectType.DBSOURCE, out.getMessageCode());
        } finally {
            if (adaoterSource != null) {
                adaoterSource.close();
            }
        }
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
     * 生成系统名称
     */
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
}
