package com.eurlanda.datashire.server.dao;

import com.eurlanda.datashire.entity.SourceColumn;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

/**
 * Created by eurlanda - new 2 on 2017/7/11.
 */
@Repository
public interface SourceColumnDao {

    //根据sourceTableId查询Column
    List<SourceColumn> getColumnById(int sourceTableId);

    //根据name和source_table_id删SourceColumn
    int deleteSourceColumn(Map<String,String> map);
    //批量添加
    int insertSourceColumn(List<SourceColumn> sourceColumnList);
    //根据tableId获取SourceColumn集合
    List<SourceColumn> getColumnByTableId(int tableId);
    //根据id删
    int deleteSourceColumnByIds(List<Integer> ids);

    int deleteSourceColumnByTableId(int tableId);
}
