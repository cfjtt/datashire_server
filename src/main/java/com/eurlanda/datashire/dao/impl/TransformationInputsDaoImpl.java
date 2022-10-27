package com.eurlanda.datashire.dao.impl;

import cn.com.jsoft.jframe.utils.ValidateUtils;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.dao.IColumnDao;
import com.eurlanda.datashire.dao.IReferenceColumnDao;
import com.eurlanda.datashire.dao.ISquidDao;
import com.eurlanda.datashire.dao.ITransformationDao;
import com.eurlanda.datashire.dao.ITransformationInputsDao;
import com.eurlanda.datashire.entity.Column;
import com.eurlanda.datashire.entity.Squid;
import com.eurlanda.datashire.entity.Transformation;
import com.eurlanda.datashire.entity.TransformationInputs;
import com.eurlanda.datashire.entity.TransformationLink;
import com.eurlanda.datashire.enumeration.AggregationType;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.enumeration.TransformationTypeEnum;
import com.eurlanda.datashire.enumeration.datatype.SystemDatatype;
import com.eurlanda.datashire.utility.DatabaseException;

import java.sql.SQLException;
import java.util.*;

public class TransformationInputsDaoImpl extends BaseDaoImpl
        implements ITransformationInputsDao {

    public TransformationInputsDaoImpl() {
    }

    public TransformationInputsDaoImpl(IRelationalDataManager adapter) {
        this.adapter = adapter;
    }

    public TransformationInputs getTransInputsBySourceTransId(int transId, int sourceTransId) {
        Map<String, Object> paramMap = new HashMap<String, Object>();
        paramMap.put("transformation_id", transId);
        paramMap.put("source_transform_id", sourceTransId);
        return adapter.query2Object(true, paramMap, TransformationInputs.class);
    }

    @Override
    public List<TransformationInputs> getTransInputListByTransId(int transId) {
        Map<String, Object> paramMap = new HashMap<String, Object>();
        paramMap.put("transformation_id", transId);
        List<TransformationInputs> transInputList = adapter.query2List2(true, paramMap,
                TransformationInputs.class);
        return transInputList;
    }

    @Override
    public boolean delTransInputsByTransId(int transId) throws SQLException {
        Map<String, String> paramMap = new HashMap<String, String>();
        paramMap.put("transformation_id", transId + "");
        return adapter.delete(paramMap, TransformationInputs.class) >= 0 ? true : false;
    }

    @Override
    public void resetTransformationInput(int from_transformation_id, int to_transformation_id, Map<String, Integer> indexMap) throws SQLException {
        Map<String, String> paramMap = new HashMap<String, String>();
        TransformationInputs transformationInput = this.getTransInputsBySourceTransId(to_transformation_id, from_transformation_id);
        if (null != transformationInput) {
            boolean isUpdate = false;
            StringBuffer sql = new StringBuffer();
            sql.append("select max(dtid.input_order) as CNT from ds_tran_input_definition dtid ");
            sql.append("inner join ds_transformation_type dtt on dtid.code=dtt.code ");
            sql.append("inner join ds_transformation dt on dtt.id=dt.transformation_type_id ");
            sql.append(" where dtid.input_order not in (-1) and dt.id=" + to_transformation_id);
            Map<String, Object> map = adapter.query2Object(true,
                    sql.toString(), null);
            //验证，是否还有空余的链接数
            int cnt = 0;
            if (map != null && map.containsKey("CNT") &&
                    ValidateUtils.isNumeric(map.get("CNT") + "")) {
                cnt = Integer.parseInt(map.get("CNT") + "") + 1;
                if (cnt < 9999) {
                    isUpdate = true;
                }
            }
            if (indexMap != null) {
                indexMap.put("index", transformationInput.getSource_tran_output_index());
            }
            if (isUpdate) {
                transformationInput.setIn_condition("");
                transformationInput.setSource_tran_output_index(0);
                transformationInput.setSource_transform_id(0);
                adapter.update2(transformationInput);
            } else {
                paramMap.clear();
                paramMap.put("id", String.valueOf(transformationInput.getId()));
                adapter.delete(paramMap, TransformationInputs.class);
            }

            paramMap.clear();
            paramMap.put("id", String.valueOf(to_transformation_id));
            Transformation toTrans = adapter.query2Object2(true, paramMap, Transformation.class);
            if (toTrans != null) {
                int typeid = toTrans.getTranstype();
                if (typeid == TransformationTypeEnum.CHOICE.value()) {
                    String sqlstr = "select * from ds_transformation_link " +
                            "where from_transformation_id=" + transformationInput.getTransformationId();
                    List<TransformationLink> links = adapter.query2List(true, sqlstr, null, TransformationLink.class);
                    if (links == null || links.size() == 0) {
                        paramMap.clear();
                        paramMap.put("id", String.valueOf(transformationInput.getTransformationId()));
                        Transformation trans = adapter.query2Object2(true, paramMap, Transformation.class);
                        trans.setOutput_data_type(SystemDatatype.OBJECT.value());
                        //System.out.println(SystemDatatype.OBJECT.value());
                        adapter.update2(trans);
                    }
                }
            }
        }
    }

    @Override
    public List<TransformationInputs> getTransInputsForColumnByTransId(
            int transId) throws SQLException {
        if (transId == 0) {
            return null;
        }
        List<TransformationInputs> list = null;
        Map<String, String> params = new HashMap<String, String>();
        params.clear();
        params.put("TRANSFORMATION_ID", transId + "");
        return adapter.query2List(true, params, TransformationInputs.class);
    }

    /**
     * 根据 ds_tran_inputs_definiton 记录生成 input
     *
     * @param dataType 虚拟Transformation时，复制column的datatType， 其他类型的时候传入0，进行数据库匹配
     * @return
     * @throws Exception
     */
    public List<TransformationInputs> initTransInputs(Transformation trans, int dataType) throws Exception {
        return initTransInputs(trans, dataType, -1, "", -1, 0);
    }

    /**
     * 根据 ds_tran_inputs_definiton 记录生成 input   复制时使用
     *
     * @param dataType 虚拟Transformation时，复制column的datatType， 其他类型的时候传入0，进行数据库匹配
     * @return
     * @throws Exception
     */
    public List<TransformationInputs> initTransInputs1(Transformation trans, int dataType) throws Exception {
        return initTransInputs(trans, dataType, 0, "", -1, 0);
    }

    /**
     * 根据 ds_tran_inputs_definiton 记录生成 input
     *
     * @param trans
     * @param dataType           虚拟Transformation时，复制column的datatType， 其他类型的时候传入0，进行数据库匹配
     * @param source_trans_id
     * @param sourceTransName
     * @param order
     * @param source_input_index
     * @return
     * @throws Exception
     */
    public List<TransformationInputs> initTransInputs(Transformation trans,
                                                      int dataType,
                                                      int source_trans_id,
                                                      String sourceTransName,
                                                      int order, int source_input_index) throws Exception {
        String sql = "select dtid.input_data_type, dtid.description, dtid.input_order from ds_tran_input_definition dtid " +
                "inner join ds_transformation_type dtt on dtid.code=dtt.code " +
                "inner join ds_transformation dt on dtt.id=dt.transformation_type_id " +
                " where dt.id=" + trans.getId();
        if (source_trans_id > 0) {
            sql += " and dtid.input_order not in (-1)";
            //dataMiningSquid 中input_order是9999 , 会影响复制
        } else if (source_trans_id == 0) {
            sql += "";
        } else {
            sql += " and dtid.input_order not in (-1,9999)";
        }
        sql += " order by dtid.input_order";
        List<Map<String, Object>> mapList = adapter.query2List(true,
                sql, null);
        List<TransformationInputs> newInputs = null;


        /*********************************
         * 这个参数是用来记录是否已经接受了外来input data type类型用的
         * 当短线的时候，外来input data type在某些时候与input data type不一样，需要赋值，但是只需要赋值一次就可以了
         */
        boolean isSetedInputDataType = false;


        if (null != mapList && mapList.size() > 0) {
            newInputs = new ArrayList<>();
            for (Map<String, Object> map : mapList) {

                if (trans.getTranstype() == TransformationTypeEnum.PREDICT.value() &&
                        (Integer.parseInt(map.get("INPUT_ORDER") + "") == 1)) {
                    continue;
                }
                //当transinputs为-1，当transformation不需要入参时，不创建transInputs
                if (Integer.parseInt(map.get("INPUT_ORDER") + "") == -1) {
                    continue;
                }

                TransformationInputs transformationInput = new TransformationInputs();
                transformationInput.setTransformationId(trans.getId());
                if (order >= 0) {
                    transformationInput.setRelative_order(order);
                } else {
                    transformationInput.setRelative_order(Integer.parseInt(map.get("INPUT_ORDER") + ""));
                }
                transformationInput.setDescription(map.get("DESCRIPTION") + "");
                if (dataType != 0 && !isSetedInputDataType) {
                    transformationInput.setInput_Data_Type(dataType);
                    isSetedInputDataType = true; /***** 只赋值断线的第一个参数，其它的还是按照数据库中的赋值 ****/
                } else {

                    transformationInput.setInput_Data_Type(Integer.parseInt(map.get("INPUT_DATA_TYPE") + ""));
                }
                if (source_trans_id > 0) {
                    transformationInput.setSource_transform_id(source_trans_id);
                    transformationInput.setSourceTransformationName(sourceTransName);
                    if (source_input_index == -1) {
                        source_input_index = 0;
                    }
                    transformationInput.setSource_tran_output_index(source_input_index);
                }
                if (trans.getTranstype() == TransformationTypeEnum.CONCATENATE.value()
                        || trans.getTranstype() == TransformationTypeEnum.CHOICE.value()
                        || trans.getTranstype() == TransformationTypeEnum.CSVASSEMBLE.value()
                        || trans.getTranstype() == TransformationTypeEnum.NUMASSEMBLE.value()) {
                    if (transformationInput.getSource_transform_id() == 0) {
                        continue;
                    }
                }
                transformationInput.setId(adapter.insert2(transformationInput, false)); /*入库*/
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
        ITransformationInputsDao transInputsDao = new TransformationInputsDaoImpl(adapter);
        ITransformationDao transDao = new TransformationDaoImpl(adapter);
        IReferenceColumnDao refColumnDao = new ReferenceColumnDaoImpl(adapter);
        ISquidDao squidDao = new SquidDaoImpl(adapter);
        IColumnDao columnDao = new ColumnDaoImpl(adapter);
        if (null != trans) {
            //得到当前trans中input个数
            if (trans.getInputs() != null) {
                newInputs = trans.getInputs();
            }
            Squid squid = squidDao.getObjectById(trans.getSquid_id(), Squid.class);
            //得到trans的最大链接数
            Map<String, Object> map = transDao.getTransParamsByTransId(trans.getId());
            //验证，是否还有空余的链接数
            int cnt = 0;
            int input_data_type = 0;
            if (map != null && map.containsKey("CNT") &&
                    ValidateUtils.isNumeric(map.get("CNT") + "")) {
                cnt = Integer.parseInt(map.get("CNT") + "") + 1;
                //PLS最大链接数2个
                if(squid.getSquid_type()==SquidTypeEnum.PLS.value()){
                    cnt = Integer.parseInt(map.get("CNT") + "") + 2;
                }
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
            if (squid == null) {
                return false;
            }

            //验证通过后，进行trans的type比较，如果符合就更新。
            if (isUpdate) {
                Transformation fromTrans = transDao.getObjectById(link.getFrom_transformation_id(), Transformation.class);
                Column column = null;
                if (trans.getColumn_id() > 0) {
                    column = columnDao.getObjectById(trans.getColumn_id(), Column.class);
                }
                if (fromTrans != null) {
                    //无限个入参
                    if (cnt > 9999) {
                        flag = this.CompareTransType(fromTrans.getOutput_data_type(),
                                input_data_type,
                                trans.getTranstype(),
                                squid.getSquid_type(),
                                newInputs.size(),
                                column);
                        if (flag) {
                            int inputCnt = 0;
                            //判断需要生成的order的值

                            if (newInputs != null && newInputs.size() > 0) {
                                //自定义排序，按照relative_order大小进行排序
                                Collections.sort(newInputs, new Comparator<TransformationInputs>() {
                                    @Override
                                    public int compare(TransformationInputs o1, TransformationInputs o2) {
                                        return o1.getRelative_order()-o2.getRelative_order() > 0 ? 0 : 1;
                                    }
                                });
                                inputCnt = newInputs.get(newInputs.size() - 1).getRelative_order() + 1;
                            } else {
                                inputCnt = 0;
                            }
                            String sourceName = trans.getName();
                            if (trans.getTranstype() == TransformationTypeEnum.VIRTUAL.value()) {
                                sourceName = refColumnDao.getRefColumnNameForTrans(trans.getSquid_id(), trans.getColumn_id());
                            }

                            if ((trans.getTranstype() == TransformationTypeEnum.CHOICE.value()) && inputCnt == 0) {
                                trans.setInputs(transInputsDao.initTransInputs(trans, fromTrans.getOutput_data_type(), fromTrans.getId(), sourceName, inputCnt, link.getSource_input_index()));
                                trans.setOutput_data_type(fromTrans.getOutput_data_type());
                                transInputsDao.update(trans);
                            } else  if(trans.getTranstype() == TransformationTypeEnum.NULLPERCENTAGE.value()){   //如果NUllPercentage Transformation上游是什么类型，inputs的类型就是什么类型
                                trans.setInputs(transInputsDao.initTransInputs(trans, fromTrans.getOutput_data_type(), fromTrans.getId(), sourceName, inputCnt, link.getSource_input_index()));
                            }else {
                                trans.setInputs(transInputsDao.initTransInputs(trans, input_data_type, fromTrans.getId(), sourceName, inputCnt, link.getSource_input_index()));
                            }
                        }
                    } else if (newInputs != null && newInputs.size() > 0) {
                        for (TransformationInputs transformationInputs : newInputs) {
                            //数据类型比较
                            if (transformationInputs.getSource_transform_id() == 0) {
                                flag = this.CompareTransType(fromTrans.getOutput_data_type(),
                                        transformationInputs.getInput_Data_Type(),
                                        trans.getTranstype(),
                                        squid.getSquid_type(),
                                        newInputs.size(),
                                        column);
                                if (flag) {
                                    //NVL 两个transInpts都需要修改它的dataType
                                    List<TransformationInputs> NVLinputsList=new ArrayList<>();
                                    transformationInputs.setSource_transform_id(fromTrans.getId());
                                    int source_input_index = link.getSource_input_index();

                                    if (source_input_index > 0) {
                                        logger.info("source_input_index:" + source_input_index);
                                    }
                                    if (source_input_index == -1) {
                                        source_input_index = 0;
                                    }

                                    if (squid.getSquid_type() == SquidTypeEnum.QUANTIFY.value()) {
                                        transformationInputs.setInput_Data_Type(fromTrans.getOutput_data_type());
                                    }
                                    transformationInputs.setSource_tran_output_index(source_input_index);
                                    if (trans.getTranstype() == TransformationTypeEnum.NVL.value()) {
                                        NVLinputsList.addAll(newInputs);
                                        transformationInputs.setInput_Data_Type(fromTrans.getOutput_data_type());
                                    }
                                    flag = transInputsDao.update(transformationInputs);
                                    if(NVLinputsList!=null && NVLinputsList.size()>0){
                                        //去掉连接的transInputs，剩下的Inputs 只需要修改它的InputDataType
                                        NVLinputsList.remove(transformationInputs);
                                        for(TransformationInputs tranInputs : NVLinputsList){
                                            tranInputs.setInput_Data_Type(fromTrans.getOutput_data_type());
                                            flag = transInputsDao.update(tranInputs);
                                        }
                                        //修改trans的outputDataType
                                        trans.setOutput_data_type(fromTrans.getOutput_data_type());
                                        transInputsDao.update(trans);
                                    }

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
            Transformation to_trans = transDao.getObjectById(link.getTo_transformation_id(), Transformation.class);
            //System.out.println("a"+to_trans.getOutput_data_type());
            if (to_trans != null && to_trans.getId() > 0) {
                List<TransformationInputs> list = transInputsDao.getTransInputsForColumnByTransId(to_trans.getId());
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
    private boolean CompareTransType(int fromType, int toType, int transType, int squidType, int inputCnt, Column column) {
        //System.out.println("cnt"+inputCnt);
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
        } else if (transType == TransformationTypeEnum.CHOICE.value() || transType == TransformationTypeEnum.NVL.value()) {
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

    /**
     * 通过Squid id获取以Trans Id为Key的inputs集合
     *
     * @param squidId
     * @return
     * @throws Exception
     */
    @Override
    public Map<Integer, List<TransformationInputs>> getTransInputsBySquidId(
            int squidId) throws Exception {
        if (squidId == 0) {
            return null;
        }
        Map<Integer, List<TransformationInputs>> rsMap = new HashMap<>();
        Map<String, String> params = new HashMap<>();
        //先获取Trans集合
        params.put("SQUID_ID", squidId + "");
        List<Transformation> transList = adapter.query2List(true, params, Transformation.class);
        //获取inputs集合并且组装
        for (Transformation item : transList) {
            params.clear();
            params.put("TRANSFORMATION_ID", item.getId() + "");
            List<TransformationInputs> inputs = adapter.query2List(true, params, TransformationInputs.class);
            rsMap.put(item.getId(), inputs);
        }
        return rsMap;
    }

    /**
     * 复制input
     *
     * @param transMap
     * @param oldToTransId
     * @return
     * @throws DatabaseException
     * @throws SQLException
     */
    @Override
    public boolean copyTransInputByLink(int newToTransId, Map<Integer, Integer> transMap,
                                        int oldToTransId) throws DatabaseException, SQLException {
        boolean isCopy = true;
        String sql = "delete from ds_tran_inputs where transformation_id=" + newToTransId;
        adapter.execute(sql);
        sql = "select * from ds_tran_inputs where transformation_id=" + oldToTransId;
        List<TransformationInputs> lists = adapter.query2List(true, sql, null, TransformationInputs.class);
        if (lists != null && lists.size() > 0) {
            for (TransformationInputs input : lists) {
                int sourceTransformation = input.getSource_transform_id();
                if (sourceTransformation > 0 && transMap.containsKey(sourceTransformation) && transMap.get(sourceTransformation) != null) {
                    input.setSource_transform_id(transMap.get(sourceTransformation));
                } else {
                    input.setSource_transform_id(0);
                }
                if (input.getSource_transform_id() == 0) {
                    sql = "select transformation_type_id as type from ds_transformation where id=" + input.getTransformationId();
                    Map<String, Object> typeMap = adapter.query2Object(true, sql, null);
                    if (typeMap != null) {
                        if (typeMap.get("TYPE") != null &&
                                (Integer.parseInt(typeMap.get("TYPE") + "") == TransformationTypeEnum.CONCATENATE.value()
                                        || Integer.parseInt(typeMap.get("TYPE") + "") == TransformationTypeEnum.CHOICE.value()
                                        || Integer.parseInt(typeMap.get("TYPE") + "") == TransformationTypeEnum.CSVASSEMBLE.value()
                                        || Integer.parseInt(typeMap.get("TYPE") + "") == TransformationTypeEnum.NUMASSEMBLE.value())) {
                            continue;
                        }
                    }
                }
                input.setTransformationId(newToTransId);
                input.setId(0);
                adapter.insert2(input, false);
            }
        }
        return isCopy;
    }


    /**
     * 更新所选择的trans input
     *
     * @param currentInput
     * @return
     * @throws Exception
     */
    @Override
    public boolean updTransSelect(TransformationInputs currentInput,Transformation toTrans) throws Exception {
        ITransformationInputsDao transInputsDao = new TransformationInputsDaoImpl(adapter);
        boolean flag = false;
        if (currentInput != null) {
            List<TransformationInputs> transformationInputsList=new ArrayList<>();
            flag = transInputsDao.update(currentInput);
            if(toTrans.getTranstype()==TransformationTypeEnum.NVL.value()){
                List<TransformationInputs> inputss=toTrans.getInputs();
                transformationInputsList.addAll(inputss);
            }
            if(transformationInputsList!=null && transformationInputsList.size()>0){
                transformationInputsList.remove(currentInput);
                for(TransformationInputs transInpts:transformationInputsList){
                    transInpts.setInput_Data_Type(currentInput.getInput_Data_Type());
                    flag = transInputsDao.update(transInpts);
                }
                toTrans.setOutput_data_type(currentInput.getInput_Data_Type());
                transInputsDao.update(toTrans);
            }
        }
        return flag;
    }
}
