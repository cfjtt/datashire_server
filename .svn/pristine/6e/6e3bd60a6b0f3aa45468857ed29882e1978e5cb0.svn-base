package com.eurlanda.datashire.destHiveService;

import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.dao.impl.BaseDaoImpl;
import com.eurlanda.datashire.entity.dest.DestHiveColumn;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by eurlanda - new 2 on 2017/5/3.
 */
public class DestHiveColumnDaoImpl extends BaseDaoImpl implements IDestHiveColumnDao{
    public DestHiveColumnDaoImpl(IRelationalDataManager adapter){
        this.adapter = adapter;
    }
    public List<DestHiveColumn> getDestHiveColumnBySquidId(int squid_id){
        Map<String, Object> map = new HashMap<>();
        map.put("squid_id",squid_id);
        List<DestHiveColumn> list=adapter.query2List2(true, map, DestHiveColumn.class);
        return adapter.query2List2(true, map, DestHiveColumn.class);
    }
}
