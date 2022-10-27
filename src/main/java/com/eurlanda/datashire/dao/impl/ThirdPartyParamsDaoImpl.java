package com.eurlanda.datashire.dao.impl;

import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.dao.IThirdPartyParamsDao;
import com.eurlanda.datashire.entity.ThirdPartyParams;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ThirdPartyParamsDaoImpl extends BaseDaoImpl implements IThirdPartyParamsDao {
	public ThirdPartyParamsDaoImpl(IRelationalDataManager adapter){
		this.adapter=adapter;
	}
	@Override
	public List<ThirdPartyParams> findThirdPartyParamsByWSEID(int id) {
		Map<String,Object> param = new HashMap<String,Object>();
		param.put("squid_id", id);
		List<ThirdPartyParams> listThirdPartyParams = adapter.query2List2(true,param, ThirdPartyParams.class);
		return listThirdPartyParams;
	}
	
	@Override
	public int modifyThirdPartyParamsForSquid(int squidId, 
			Map<Integer, Integer> squidMap, Map<String, Integer> columnMap) throws SQLException{
		List<ThirdPartyParams> list = this.findThirdPartyParamsByWSEID(squidId);
		if (list!=null&&list.size()>0){
			for (ThirdPartyParams thirdPartyParams : list) {
				int refSquidId = thirdPartyParams.getRef_squid_id();
				int columnId = thirdPartyParams.getColumn_id();
				if (squidMap.containsKey(refSquidId)
						&&columnMap.containsKey(refSquidId+"_"+columnId)){
					thirdPartyParams.setRef_squid_id(squidMap.get(refSquidId));
					thirdPartyParams.setColumn_id(columnMap.get(refSquidId+"_"+columnId));
					adapter.update2(thirdPartyParams);
				}
			}
		}
		return 0;
	}
}
