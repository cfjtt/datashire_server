package com.eurlanda.datashire.server.dao.impl;

import com.eurlanda.datashire.entity.SquidJoin;
import com.eurlanda.datashire.server.dao.SquidDao;
import com.eurlanda.datashire.server.model.Base.DataSquidBaseModel;
import com.eurlanda.datashire.server.model.Base.SquidModelBase;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.Map;

/**
 * Created by zhudebin on 16/2/1.
 */
public class SquidDaoImpl implements SquidDao {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Override
    public int deleteByPrimaryKey(Integer id) {
        return 0;
    }

    @Override
    public int insert(SquidModelBase record) {
        return 0;
    }

    @Override
    public int insertSelective(SquidModelBase record) {
        return 0;
    }

    @Override
    public SquidModelBase selectByPrimaryKey(Integer id) {
        return null;
    }

    @Override
    public int updateByPrimaryKeySelective(SquidModelBase record) {
        return 0;
    }

    @Override
    public int updateByPrimaryKey(SquidModelBase record) {
        return 0;
    }

    @Override public void save(SquidModelBase squidModelBase) {
//        jdbcTemplate
    }

    @Override public void update(SquidModelBase squidModelBase) {

    }

    @Override public List<SquidModelBase> findAll() {
        return null;
    }

    @Override
    public Map<String, Object> findSquidBySquidId(int squidId) {
        return null;
    }

    @Override
    public DataSquidBaseModel selectExceSquByFromSquIds(Map<String, Object> map) {
        return null;
    }

    @Override
    public int updateSquStatus(Integer squIds) {
        return 0;
    }



    @Override
    public List<DataSquidBaseModel> selectSquBySquIds(List<Integer> squidIds) {
        return null;
    }

    @Override
    public int deleteEsColumnBySquId(Integer id) {
        return 0;
    }

    @Override
    public int deleteHDFSColumnBySquId(Integer squId) {
        return 0;
    }

    @Override
    public int deleteImpalaColumnBySquId(Integer squId) {
        return 0;
    }

    @Override
    public int updateHiveColumnBySquIds(Integer squId) {
        return 0;
    }


    @Override
    public int updateCASSSColumnBySquIds(Integer squId) {
        return 0;
    }

    @Override
    public List<Integer> selectExceptionSquSquIds(Map<String, Object> map) {
        return null;
    }

    @Override
    public int updateSquStatusBySquIds(List<Integer> squidIds) {
        return 0;
    }

    @Override
    public List<SquidJoin> selectJoinBySquid(Integer squid) {
        return null;
    }

    @Override
    public List<Map<String, Object>> selectExceViewSquByTypeAndSquIds(Map<String, Object> map) {
        return null;
    }

    @Override
    public Integer selectSquidTypeById(Integer id) {
        return null;
    }

    @Override
    public List<Integer> selectSourceTableIdBySquids(List<Integer> squidIds) {
        return null;
    }


    @Override
    public List<Map<String, Object>> findNextSquidByFromSquid(int fromSquid) {
        return null;
    }

    @Override
    public Map<String,Object> findSquidType(int squidType){
        return null;
    }

    @Override
    public List<Integer> selectFromSquidBySquLinkIds(List<Integer> squidLinkIds) {
        return null;
    }

    @Override
    public List<Integer> selectToSquidBySquLinkIds(List<Integer> squidLinkIds) {
        return null;
    }

    @Override
    public List<SquidModelBase> selectSquBySquLinkIds(List<Integer> squidLinkIds) {
        return null;
    }

    @Override
    public List<Map<String,Object>> selectSquBySquTypeAndSquIds(Map<String, Object> map) {
        return null;
    }

    @Override
    public int deleteSquid(List<Integer> list){return 0;}
    @Override
    public int deleteSourceTableColumnBySquidId(List<Integer> list){return 0;}
    @Override
    public int deleteSourceTableBySquidId(List<Integer> list){return 0;}
    @Override
    public int deleteEsColumn(List<Integer> list){return 0;}
    @Override
    public int deleteHDFSColumn(List<Integer> list){return 0;}
    @Override
    public int deleteImpalaColumn(List<Integer> list){return 0;}
    @Override
    public int deleteJoin(List<Integer> list){return 0;}
    @Override
    public int deleteUserdefinedDatamapColumn(List<Integer> list){return 0;}
    @Override
    public int deleteUserdefinedParametersColumn(List<Integer> list){return 0;}
    @Override
    public int deleteDestHiveColumn(List<Integer> list){return 0;}
    @Override
    public int deleteDestCassandraColumn(List<Integer> list){return 0;}
    @Override
    public int deleteStatisticsDatamapColumn(List<Integer> list){return 0;}
    @Override
    public int deleteStatisticsParametersColumn(List<Integer> list){return 0;}
    @Override
    public List<SquidModelBase> selectBySourceTable(int tableId){return null;}

    @Override
    public Map<String, Object> selectProjectBySquidId(int squidId) {
        return null;
    }

    @Override
    public List<Integer> selectSquidTypesByIds(List<Integer> ids) {
        return null;
    }

    @Override
    public String getSquidNameByColumnId(int column) {
        return null;
    }

    @Override
    public int getSquidTypeById(int squid) {
        return 0;
    }

    @Override
    public int updateSquidByLastValue(Map<String, Object> map) {
        return 0;
    }
}
