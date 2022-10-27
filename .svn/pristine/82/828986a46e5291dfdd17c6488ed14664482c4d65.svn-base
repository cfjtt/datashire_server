package com.eurlanda.datashire.dao.impl;

import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.dao.ISquidIndexesDao;
import com.eurlanda.datashire.entity.SquidIndexes;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SquidIndexesDaoImpl extends BaseDaoImpl implements ISquidIndexesDao{

	public SquidIndexesDaoImpl(){
	}
	
	public SquidIndexesDaoImpl(IRelationalDataManager adapter){
		this.adapter = adapter;
	}
	
	@Override
	public List<SquidIndexes> getSquidIndexesBySquidId(int squidId) {
		 Map<String, Object> paramsMap = new HashMap<String, Object>();
        paramsMap.put("squid_id", squidId);
        List<SquidIndexes> squidIndexsList = adapter.query2List2(true,
                paramsMap, SquidIndexes.class);
        return squidIndexsList;
	}
}
