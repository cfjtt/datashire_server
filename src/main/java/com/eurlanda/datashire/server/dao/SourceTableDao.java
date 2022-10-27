package com.eurlanda.datashire.server.dao;

import com.eurlanda.datashire.entity.DBSourceTable;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Created by eurlanda - new 2 on 2017/7/10.
 */
@Repository
public interface SourceTableDao {
    //根据squidid查询DBSourceTable
    List<DBSourceTable> getDbSourceTablesForExtracted(int squidId);
    //根据id删除SourceTable
    int deleteDbSourceTableById(List<Integer> ids);

    //删除某dbSquid下的所有未抽取的SourceTable
    int deleteDbSourceTablesBySquid(int squidId);
    //添加DBSourceTable
    int insertDBSourceTable(List<DBSourceTable> dbSourceTableList);

    List<DBSourceTable> getDbSourceTableById(int squidId);

    //根据Id修改是否抽取的状态
    int updateIsExtractedById(int id);


    //根据source_table_id 查询对象
    DBSourceTable getDbSourceBySourceTableId(int source_table_id);


    Integer updateIsExtractedByIds(List<Integer> ids);

}
