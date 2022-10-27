package com.eurlanda.datashire.server.dao;

import com.eurlanda.datashire.server.model.Column;
import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

@Repository
public interface ColumnDao {
    int deleteByPrimaryKey(Integer id);

    int insert(List<Column> columnList);

    int insertSelective(Column record);

    Column selectByPrimaryKey(Integer id);

    int updateByPrimaryKeySelective(Column record);

    int updateByPrimaryKey(Column record);

    int selectColumnBySquIdAndName(Column column);

    /**
     * 批量删除column
     * @param list
     * @return
     */
    int deleteBatch(List<Integer> list);

    List<Map<String,Object>> findNextSquidByColumnId(int columnId);

    List<Integer> findTransByColumnId(Map<String,Object> map);

    List<Map<String,Object>> findLeftTransAndLinkAndColumn(List<Integer> transIds);

    List<Map<String,Object>> findTransformationById(List<Integer> ids);

    List<Map<String,Object>> findRightTransAndLinkAndRefColumn(List<Integer> transIds);

    int deleteDestHdfsColumn(List<Integer> list);
    int deleteDestImpalaColumn(List<Integer> list);
    int deleteESColumn(List<Integer> list);
    int updateCassandraColumn(List<Integer> list);
    int updateHiveColumn(List<Integer> list);
    int updateUserDefinedColumn(List<Integer> list);
    int updateStatisticColumn(List<Integer> list);
    int deleteTransLink(List<Integer> list);
    Integer findSquidByColumnId(Integer columnId);
    List<Integer> findTransInputByTrans(List<Integer> transId);

    List<Integer> selectColumnBySquIds(Map<String,Object> map);

    int deleteColumnByIds(List<Integer> columnIds);



    List<Column> findByFromSquidId(int fromsquidId);
    //根据squidId删除Column
    int deleteColumnBySquidId(List<Integer> squidIds);

    List<Integer> selectColumnBySquidIds(Map<String,Object> map);

    List<Integer> selectColumnBySquidId(int squidId);


    List<Column> selectColumnBySquid_Id(int squidId);

    List<Integer> selectColumnIdBySquidId(Map<String, Object> map);

    Integer updateDataTypeByColumnId(Column column);

    Integer selectCountNoGroupByColumnColumn(@Param("column") Column column);

    /**
     * 查询一条非分组列
     * @param squidId
     * @return
     */
    Column selectPivotColumnPaged(@Param("squidId") Integer squidId);



    /**
     * 查询分组列
     */
    List<Column> selectGroupByColumns(@Param("squidId") Integer squidId);
}