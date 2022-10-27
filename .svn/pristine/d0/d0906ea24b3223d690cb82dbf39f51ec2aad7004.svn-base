package com.eurlanda.datashire.server.service;

import cn.com.jsoft.jframe.utils.ValidateUtils;
import com.eurlanda.datashire.enumeration.AggregationType;
import com.eurlanda.datashire.enumeration.datatype.SystemDatatype;
import com.eurlanda.datashire.server.dao.ColumnDao;
import com.eurlanda.datashire.server.dao.SquidDao;
import com.eurlanda.datashire.server.model.*;
import com.eurlanda.datashire.server.model.Base.SquidModelBase;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.enumeration.TransformationTypeEnum;
import com.eurlanda.datashire.server.dao.TransformationDao;
import com.eurlanda.datashire.server.dao.TransformationInputsDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;

/**
 * Created by eurlanda - new 2 on 2017/6/30.
 */
@Service
@Transactional
public class TransformationInputsService {
    private static final Logger LOGGER = LoggerFactory.getLogger(TransformationInputsService.class);
    @Autowired
    private TransformationInputsDao transformationInputsDao;
    @Autowired
    private TransformationDao transformationDao;
    @Autowired
    private SquidDao squidDao;
    @Autowired
    private ColumnDao columnDao;
    @Autowired
    private ReferenceColumnService referenceColumnService;





    public List<TransformationInputs> initTransInputs1(Transformation trans,
                                                      int type,
                                                      int source_trans_id,
                                                      String sourceTransName,
                                                      int order, int source_input_index) throws Exception {
        List<TransformationInputs> inputsList=new ArrayList<>();
        //当source_trans_id 小于0时，说明是虚拟的trans或者直接拖拽的trans input_order是-1，9999.说明不需要创建
        if(source_trans_id<0){
            if(TransformationTypeEnum.isNotTransInputs(trans.getTranstype())){
                //根据transType 找到对应的inputs
               List<Map<String,Object>> inputsMap=SquidLinkService.getInputscache().get(trans.getTranstype()+"");
                    for(Map<String,Object> map:inputsMap){
                        TransformationInputs inputs=new TransformationInputs();
                        String description=map.get("description").toString();
                        String relative_order=map.get("inputOrder").toString();
                    }
            }
        }
        return inputsList;
    }


    /**
     * 根据 ds_tran_inputs_definiton 记录生成 input
     *
     * @param
     * @param其他类型的时候传入0，进行数据库匹配
     * @param source_trans_id
     * @param sourceTransName
     * @param order
     * @param source_input_index
     * @return
     * @throws Exception
     */
    public List<TransformationInputs> initTransInputs(Transformation transformation,
                                                     int type,
                                                     int source_trans_id,
                                                     String sourceTransName,
                                                     int order, int source_input_index) throws Exception {
        Map<String,Object> mapSql =new HashMap<>();
        mapSql.put("transformationId",transformation.getId());
        if (source_trans_id > 0) {
            mapSql.put("source_trans_id",source_trans_id);
            //dataMiningSquid 中input_order是9999 , 会影响复制
        } else if (source_trans_id == 0) {
            mapSql.put("source_trans_id",0);
        } else {
            mapSql.put("source_trans_id",source_trans_id);
        }
        List<Map<String, Object>> mapList=transformationDao.findTransformationType(mapSql);
        List<TransformationInputs> newInputs = null;

        /*********************************
         * 这个参数是用来记录是否已经接受了外来input data type类型用的
         * 当短线的时候，外来input data type在某些时候与input data type不一样，需要赋值，但是只需要赋值一次就可以了
         */
        boolean isSetedInputDataType = false;


        if (null != mapList && mapList.size() > 0) {
            newInputs = new ArrayList<>();
            for (Map<String, Object> map : mapList) {

                if (transformation.getTranstype() == TransformationTypeEnum.PREDICT.value() &&
                        (Integer.parseInt(map.get("input_order") + "") == 1)) {
                    continue;
                }
                //当transinputs为-1，当transformation不需要入参时，不创建transInputs
                if (Integer.parseInt(map.get("input_order") + "") == -1) {
                    continue;
                }

                TransformationInputs transformationInput = new TransformationInputs();
                transformationInput.setSource_transform_id(0);
                transformationInput.setTransformationId(transformation.getId());
                if (order >= 0) {
                    transformationInput.setRelative_order(order);
                } else {
                    transformationInput.setRelative_order(Integer.parseInt(map.get("input_order") + ""));
                }
                transformationInput.setDescription(map.get("description") + "");
                if (type != 0 && !isSetedInputDataType) {
                    transformationInput.setInput_Data_Type(type);
                    isSetedInputDataType = true; /***** 只赋值断线的第一个参数，其它的还是按照数据库中的赋值 ****/
                } else {

                    transformationInput.setInput_Data_Type(Integer.parseInt(map.get("input_data_type") + ""));
                }
                if (source_trans_id > 0) {
                    transformationInput.setSource_transform_id(source_trans_id);
                    transformationInput.setSourceTransformationName(sourceTransName);
                    if (source_input_index == -1) {
                        source_input_index = 0;
                    }
                    transformationInput.setSource_tran_output_index(source_input_index);
                }
                if (transformation.getTranstype() == TransformationTypeEnum.CONCATENATE.value()
                        || transformation.getTranstype() == TransformationTypeEnum.CHOICE.value()
                        || transformation.getTranstype() == TransformationTypeEnum.CSVASSEMBLE.value()
                        || transformation.getTranstype() == TransformationTypeEnum.NUMASSEMBLE.value()) {
                    if (transformationInput.getSource_transform_id() == 0) {
                        continue;
                    }
                }
                transformationInputsDao.insertSelective(transformationInput);
                transformationInput.setTransformationId(transformation.getId());
                newInputs.add(transformationInput);
            }
        }
        return newInputs;
    }

    /**
     * 判断link的链接是否符合要求，如果符合更新trans_inputs
     *
     * @param link
     * @param trans
     * @return
     * @throws Exception
     */
    public boolean updTransInputs(TransformationLink link, Transformation trans) throws Exception {
        List<TransformationInputs> newInputs = new ArrayList<TransformationInputs>();
        boolean flag = false;
        boolean isUpdate = false;
        if (null != trans) {
            //得到当前trans中input个数
            if (trans.getInputs() != null) {
                newInputs = trans.getInputs();
            }
            //得到trans的最大链接数
            Map<String, Object> map = transformationDao.getTransParamsByTransId(trans.getId());
            //验证，是否还有空余的链接数
            int cnt = 0;
            int input_data_type = 0;
            if (map != null && map.containsKey("CNT") &&
                    ValidateUtils.isNumeric(map.get("CNT") + "")) {
                cnt = Integer.parseInt(map.get("CNT") + "") + 1;
                input_data_type = Integer.parseInt(map.get("INPUT_DATA_TYPE") + "");
                if (cnt >= newInputs.size()) {
                    isUpdate = true;
                }
            }
            //不能连线就直接返回
            if (!isUpdate) {
                return false;
            }
            if (trans.getTranstype() == TransformationTypeEnum.CHOICE.value()) {
                input_data_type = trans.getOutput_data_type();
            }
            SquidModelBase squidModelBase = squidDao.selectByPrimaryKey(trans.getSquid_id());
            if (squidModelBase == null) {
                return false;
            }

            //验证通过后，进行trans的type比较，如果符合就更新。
            if (isUpdate) {
                Transformation fromTrans = transformationDao.selectByPrimaryKey(link.getFrom_transformation_id());
                Column column = null;
                if (trans.getColumn_id() > 0) {
                    column = columnDao.selectByPrimaryKey(trans.getColumn_id());
                }
                if (fromTrans != null) {
                    //无限个入参
                    if (cnt > 9999) {
                        flag = this.compareTransType(fromTrans.getOutput_data_type(),
                                input_data_type,
                                trans.getTranstype(),
                                squidModelBase.getSquid_type(),
                                newInputs.size(),
                                column);
                        if (flag) {
                            int inputCnt = 0;
                            //判断需要生成的order的值

                            if (newInputs != null && newInputs.size() > 0) {
//                                Collections.sort(newInputs);
                                inputCnt = newInputs.get(newInputs.size() - 1).getRelative_order() + 1;
                            } else {
                                inputCnt = 0;
                            }
                            String sourceName = trans.getName();
                            if (trans.getTranstype() == TransformationTypeEnum.VIRTUAL.value()) {
                                sourceName = referenceColumnService.getRefColumnNameForTrans(trans.getSquid_id(), trans.getColumn_id());
                            }
                            if ((trans.getTranstype() == TransformationTypeEnum.CHOICE.value()) && inputCnt == 0) {
                                trans.setInputs(this.initTransInputs(trans, fromTrans.getOutput_data_type(), fromTrans.getId(), sourceName, inputCnt, link.getSource_input_index()));
                                trans.setOutput_data_type(fromTrans.getOutput_data_type());
                                transformationDao.updateByPrimaryKey(trans);
                            } else {
                                trans.setInputs(this.initTransInputs(trans, input_data_type, fromTrans.getId(), sourceName, inputCnt, link.getSource_input_index()));
                            }
                        }
                    } else if (newInputs != null && newInputs.size() > 0) {
                        for (TransformationInputs transformationInputs : newInputs) {
                            //数据类型比较
                            if (transformationInputs.getSource_transform_id() == 0) {
                                flag = this.compareTransType(fromTrans.getOutput_data_type(),
                                        transformationInputs.getInput_Data_Type(),
                                        trans.getTranstype(),
                                        squidModelBase.getSquid_type(),
                                        newInputs.size(),
                                        column);
                                if (flag) {
                                    transformationInputs.setSource_transform_id(fromTrans.getId());
                                    int source_input_index = link.getSource_input_index();

                                    if (source_input_index > 0) {
                                        LOGGER.info("source_input_index:" + source_input_index);
                                    }
                                    if (source_input_index == -1) {
                                        source_input_index = 0;
                                    }

                                    if (squidModelBase.getSquid_type() == SquidTypeEnum.QUANTIFY.value()) {
                                        transformationInputs.setInput_Data_Type(fromTrans.getOutput_data_type());
                                    }
                                    transformationInputs.setSource_tran_output_index(source_input_index);
                                    flag = transformationInputsDao.updateByPrimaryKey(transformationInputs)==1;

									/*if ((trans.getTranstype()==TransformationTypeEnum.PUT.value()
										||trans.getTranstype()==TransformationTypeEnum.REMOVE.value())
											&&transformationInputs.getRelative_order()==0){
										trans.setOutput_data_type(fromTrans.getOutput_data_type());
										transInputsDao.update(trans);
									}*/
                                    break;
                                }
                            }
                        }
                        trans.setInputs(newInputs);
                    }
                }
            }
        } else {
            Transformation to_trans = transformationDao.selectByPrimaryKey(link.getTo_transformation_id());
            //System.out.println("a"+to_trans.getOutput_data_type());
            if (to_trans != null && to_trans.getId() > 0) {
                List<TransformationInputs> list = transformationInputsDao.selectByTransId(to_trans.getId());
                if (list != null) {
                    to_trans.setInputs(list);
                }
                flag = updTransInputs(link, to_trans);
            }
        }
        return flag;
    }

    /**
     * 数据类型比较
     *
     * @param fromType  上游Trans input 类型
     * @param toType    下游Trans的  output 类型
     * @param transType 上游Trans的 类型
     * @param squidType 当前squid的 类型
     * @param inputCnt  当前trans的input个数
     * @return
     */
    private boolean compareTransType(int fromType, int toType, int transType, int squidType, int inputCnt,Column column) {
        int fromVal = 0;
        int toVal = 0;
        fromVal = getCompareTypeValue(fromType);
        //CHOICE、ROWNUMBE 首次链接 匹配全部类型
        if (SquidTypeEnum.isExtractBySquidType(squidType)) {
            return true;
        } else if (fromType == SystemDatatype.OBJECT.value() || fromType == 99) {
            return false;
        } else if (column != null && column.getAggregation_type() == AggregationType.COUNT.value()) {
            return true;
        } else if (transType == TransformationTypeEnum.ROWNUMBER.value()) {
            return true;
        } else if (transType == TransformationTypeEnum.PREDICT.value()) {
            return true;
        } else if (transType == TransformationTypeEnum.INVERSEQUANTIFY.value()
                || transType == TransformationTypeEnum.NUMERICCAST.value()) {
            toVal = 1;
            return fromVal == toVal ? true : false;
        } else if (transType == TransformationTypeEnum.CHOICE.value()) {
            if (toType == SystemDatatype.OBJECT.value() || toType == 99) {
                return true;
            }
            return fromType == toType ? true : false;
        } else if (transType == TransformationTypeEnum.NUMASSEMBLE.value()) {
            if (fromType == SystemDatatype.CSN.value()) {
                return true;
            } else {
                toVal = 1;
                return fromVal == toVal ? true : false;
            }
        } else if (transType == TransformationTypeEnum.TRAIN.value()
                && squidType == SquidTypeEnum.QUANTIFY.value()) {
            return true; // QUANTIFY 量化Squid 并且trans的类型为PREDICT、TRAIN  匹配全部类型
        } else if (transType == TransformationTypeEnum.TRAIN.value()
                && squidType == SquidTypeEnum.DISCRETIZE.value()) {
            toVal = 1; //当Trans类型为PREDICT、TRAIN时 并且当前squid为 特殊DataMiningSquid（DISCRETIZE）匹配 整数类型
            if (fromVal == toVal) {
                return true;
            }
        } else if (toType == SystemDatatype.OBJECT.value()) {
            return true; /* Object 类型可以被所有类型连接直接返回true YHC */
        } else if (fromType == SystemDatatype.CSV.value() && ((toType == SystemDatatype.NCHAR.value()) || (toType == SystemDatatype.NVARCHAR.value()))) {
            //当上游为cvs，下游为nvhar/nvarchar
            return true;
        } else if (((fromType == SystemDatatype.NVARCHAR.value()) || (fromType == SystemDatatype.NCHAR.value())) && (toType == SystemDatatype.CSV.value()) && (column.getAggregation_type() == AggregationType.STRING_SUM.value())) {
            return true;
        }
        toVal = getCompareTypeValue(toType);
        return fromVal == toVal ? true : false;
    }

    //数据转唤分组
    private int getCompareTypeValue(int dataType) {
        int val = 7;
        SystemDatatype tempType = SystemDatatype.parse(dataType);
        switch (tempType) {
            case BIGINT:
            case DECIMAL:
            case FLOAT:
            case DOUBLE:
            case INT:
            case SMALLINT:
            case TINYINT:
            case BIT:
                val = 1;
                break;
            case BINARY:
            case VARBINARY:
                val = 2;
                break;
            case DATETIME:
                val = 3;
                break;
            case NCHAR:
            case NVARCHAR:
                //case CSN:
                val = 5;
                break;
            case OBJECT:
                val = 6;
                break;
            case CSN:
                val = 7;
                break;
            case MAP:
                val = 9;
                break;
            case ARRAY:
                val = 10;
                break;
            case UNKNOWN:
                val = 8;
                break;
        }
        return val;
    }
}
