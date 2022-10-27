package com.eurlanda.datashire.dao.impl;

import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.dao.IUrlDao;
import com.eurlanda.datashire.entity.Url;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UrlDaoImpl	extends BaseDaoImpl implements IUrlDao{

	public UrlDaoImpl(){
	}
	
	public UrlDaoImpl(IRelationalDataManager adapter){
		this.adapter = adapter;
	}
	
	@Override
	public List<Url> getUrlsBySquidId(int squidId) {
		Map<String, Object> params = new HashMap<String, Object>(); // 查询参数
    	params.put("squid_id", squidId);
    	return adapter.query2List2(true, params, Url.class);
	}
}
