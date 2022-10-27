package com.eurlanda.datashire.dao;

import com.eurlanda.datashire.entity.ThirdPartyParams;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface IThirdPartyParamsDao extends IBaseDao{
	/**
	 * 查询第三方参数按WebServiceId
	 * 2014-12-9
	 * @author Akachi
	 * @E-Mail zsts@hotmail.com
	 * @param id WebServiceId
	 * @return
	 */
	public List<ThirdPartyParams> findThirdPartyParamsByWSEID(int id);
	
	/**
	 * 修改第三方参数引用squid和引用column信息
	 * @param squidId
	 * @return
	 */
	public int modifyThirdPartyParamsForSquid(int squidId, 
			Map<Integer, Integer> squidMap, Map<String, Integer> columnMap) throws SQLException;
}
