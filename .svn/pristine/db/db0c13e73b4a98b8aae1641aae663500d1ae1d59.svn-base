package com.eurlanda.datashire.server.dao;


import com.eurlanda.datashire.server.model.TransformationInputs;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;

@Repository
@Transactional
public interface TransformationInputsDao {
    int deleteByPrimaryKey(Integer id);

    int insert(List<TransformationInputs> records);

    int insertSelective(TransformationInputs record);

    TransformationInputs selectByPrimaryKey(Integer id);

    int updateByPrimaryKeySelective(TransformationInputs record);

    int updateByPrimaryKey(TransformationInputs record);

    int deleteByTransId(List<Integer> list);

    List<TransformationInputs> selectByTransId(int transId);

    int updateTransInputSourceTransId(List<Integer> transLinkIds);

    List<TransformationInputs> selectTransInputByLinkIds(Map<String,Object> linkAndTranIds);

    int updateTransInputs(List<Integer> ids);

    int deleteByInputIds(List<Integer> inputIds);


    //int deleteInputsByTransId(List<Integer> transId);
    //根据transId删Inputs
    int deleteInputsByTransId(List<Map<String,Integer>> transId);

    int updateInputSourceByTransIdAndSource(List<Map<String,Integer>> list);

    //根据transformationId查input个数
    List<Map<String,Object>> findTransInputCount(List<Map<String,Integer>> ids);

    List<Integer> selectTransInputByTransTypeAndInputIds_delete(List<Integer> tranInputIds);

    List<Integer> selectTransInputByTransTypeAndInputIds_update(List<Integer> tranInputIds);
    //根据squidId删除transformation
    int deleteTransInputBySquidId(List<Integer> list);

    List<Integer> selectTransInputByTransIds(List<Integer> tranIds);

    List<Integer> selectInputByLinkIdsAndTransIds(Map<String,Object> linkAndTranIds);

    int updateTransInputsByTransId(List<Integer> transId);


    List<Map<String,Object>> findInputsDefintionByTransType();

    List<TransformationInputs> selectTransInputByTrans(List<Integer> tranIds);

    int updateInpusDataType(Map<String,Object> map);

}