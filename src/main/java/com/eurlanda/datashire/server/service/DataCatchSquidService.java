package com.eurlanda.datashire.server.service;

import com.eurlanda.datashire.entity.DataSquid;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.enumeration.datatype.SystemDatatype;
import com.eurlanda.datashire.server.dao.DataCatchSquidDao;
import com.eurlanda.datashire.server.dao.SquidLinkDao;
import com.eurlanda.datashire.server.exception.ErrorMessageException;
import com.eurlanda.datashire.server.model.Column;
import com.eurlanda.datashire.server.model.DataCatchSquid;
import com.eurlanda.datashire.server.model.SquidLink;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;

/**
 * Created by eurlanda - new 2 on 2017/6/28.
 */
@Service
public class DataCatchSquidService {
    private static final Logger logger = LoggerFactory.getLogger(DataCatchSquidService.class);
    @Autowired
    private DataCatchSquidDao dataCatchSquidDao;
    @Autowired
    private SquidLinkDao squidLinkDao;
    @Autowired
    private ColumnService columnService;

    /**
     * 创建DataViewSquid
     * @param info
     * @return
     * @throws Exception
     */
    @Transactional(rollbackFor = Exception.class)
    public Map<String, Object> createDataCatchSquid(String info) throws Exception {
        Map<String, Object> resultMap = new HashMap<String, Object>();
        DataCatchSquid dataCatchSquid = JsonUtil.toGsonBean(JsonUtil.toHashMap(info).get("Squid") + "", DataCatchSquid.class);
        SquidLink squidLink= JsonUtil.toGsonBean(JsonUtil.toHashMap(info).get("SquidLink") + "", SquidLink.class);
        DataSquid dataSquid=JsonUtil.toGsonBean(JsonUtil.toHashMap(info).get("SourceSquid") + "", DataSquid.class);
        try {
            if(dataCatchSquid!=null){
                dataCatchSquidDao.insertDataViewSquid(dataCatchSquid);
            }else{
                throw new ErrorMessageException(MessageCode.DESERIALIZATION_FAILED.value());
            }
            if(squidLink!=null){
                squidLink.setTo_squid_id(dataCatchSquid.getId());
                squidLinkDao.insert(squidLink);
            }
            List<Column> columnList = new ArrayList<Column>();
            //列的信息
            Map<String, Integer> columnsMap = new LinkedHashMap<>();
            if(dataSquid!=null) {
                if (SquidTypeEnum.PLS.value() == dataSquid.getSquid_type() && SquidTypeEnum.DATAVIEW.value() == dataCatchSquid.getSquid_type()) {
                    columnsMap.put("XWeight", SystemDatatype.CSN.value());
                    columnsMap.put("YWeight", SystemDatatype.CSN.value());
                    columnsMap.put("XScore", SystemDatatype.CSN.value());
                    columnsMap.put("YScore", SystemDatatype.CSN.value());
                    columnsMap.put("XLoading", SystemDatatype.CSN.value());
                    columnsMap.put("YLoading", SystemDatatype.CSN.value());
                    columnsMap.put("XRotation", SystemDatatype.CSN.value());
                    columnsMap.put("YRotation", SystemDatatype.CSN.value());
                }
                if (SquidTypeEnum.NORMALIZER.value() == dataSquid.getSquid_type()) {
                    columnsMap.put("Normalized", SystemDatatype.CSN.value());
                }
                if (SquidTypeEnum.LOGREG.value() == dataSquid.getSquid_type() || SquidTypeEnum.RIDGEREG.value() == dataSquid.getSquid_type()
                        || SquidTypeEnum.LINEREG.value() == dataSquid.getSquid_type() || SquidTypeEnum.RANDOMFORESTREGRESSION.value() == dataSquid.getSquid_type() ||
                        SquidTypeEnum.PLS.value() == dataSquid.getSquid_type() || SquidTypeEnum.LASSO.value() == dataSquid.getSquid_type()
                        || SquidTypeEnum.DECISIONTREEREGRESSION.value() == dataSquid.getSquid_type() || SquidTypeEnum.DECISIONTREECLASSIFICATION.value() == dataSquid.getSquid_type()
                        || SquidTypeEnum.NAIVEBAYES.value()==dataSquid.getSquid_type() || SquidTypeEnum.SVM.value()==dataSquid.getSquid_type()) {
                    if (SquidTypeEnum.COEFFICIENT.value() == dataCatchSquid.getSquid_type()) {
                        columnsMap.put("i", SystemDatatype.BIGINT.value());
                        columnsMap.put("j", SystemDatatype.BIGINT.value());
                        columnsMap.put("value", SystemDatatype.DOUBLE.value());
                        columnsMap.put("key", SystemDatatype.NVARCHAR.value());
                        columnsMap.put("version", SystemDatatype.INT.value());
                    }
                }
            }
            int index = 0;
            for (Map.Entry<String, Integer> entry : columnsMap.entrySet()) {
                index++;
                Column newColumn = new Column();
                newColumn.setCollation(0);
                newColumn.setData_type(entry.getValue());
                newColumn.setName(entry.getKey());
                newColumn.setNullable(false);
                newColumn.setUnique(false);
                newColumn.setCdc(0);
                newColumn.setRelative_order(index);
                newColumn.setSquid_id(dataCatchSquid.getId());
                newColumn.setLength(256);
                columnList.add(newColumn);
            }
            //批量添加column
            columnService.insertColumn(columnList);
            resultMap.put("SquidId", dataCatchSquid.getId());
            resultMap.put("SquidLinkId", squidLink.getId());
            resultMap.put("ColumnList", columnList);
        }catch (Exception e){
            e.printStackTrace();
            throw e;
        }
        return resultMap;
    }

    /**
     * 更新DataViewSquid
     * @param dataCatchSquid
     * @throws Exception
     */
    @Transactional(rollbackFor = Exception.class)
    public void updateCatchSquidSquid(DataCatchSquid dataCatchSquid) throws Exception{
        try {
            if(dataCatchSquid!=null){
                dataCatchSquidDao.updateDataViewSquid(dataCatchSquid);
            }else{
                throw new ErrorMessageException(MessageCode.DESERIALIZATION_FAILED.value());
            }
        }catch (Exception e){
            e.printStackTrace();
            throw e;
        }
    }
}
