package com.eurlanda.datashire.dao.impl;

import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.dao.ISquidJoinDao;
import com.eurlanda.datashire.entity.ReferenceColumnGroup;
import com.eurlanda.datashire.entity.SquidJoin;
import com.eurlanda.datashire.enumeration.JoinType;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SquidJoinDaoImpl extends BaseDaoImpl implements ISquidJoinDao {

	public SquidJoinDaoImpl(){
	}
	
	public SquidJoinDaoImpl(IRelationalDataManager adapter){
		this.adapter = adapter;
	}
	
	@Override
	public SquidJoin getSquidJoinByParams(int target_squid_id,
			int joined_squid_id) {
		Map<String, Object> paramMap = new HashMap<String, Object>();
		paramMap.put("target_squid_id", target_squid_id);
		paramMap.put("joined_squid_id", joined_squid_id);
		SquidJoin squidJoin = adapter.query2Object(true, paramMap, SquidJoin.class);
		return squidJoin;
	}

	@Override
	public boolean delSquidJoinByParams(int target_squid_id, int joined_squid_id) throws SQLException {
		Map<String, String> paramMap = new HashMap<String, String>();
		paramMap.put("target_squid_id", target_squid_id+"");
		paramMap.put("joined_squid_id", joined_squid_id+"");
		return adapter.delete(paramMap, SquidJoin.class)>=0?true:false;
	}
	
	@Override
	public SquidJoin getUpSquidJoinByJoin(SquidJoin squidJoin) throws SQLException{
		SquidJoin join = null;
		if (squidJoin.getJoinType()==JoinType.BaseTable.value()){
			String sql = "select * from ds_join where joined_squid_id="+squidJoin.getJoined_squid_id()+" and id!="+squidJoin.getId()+" order by prior_join_id,id";
			List<SquidJoin> joins = adapter.query2List(true, sql, null, SquidJoin.class);
			if (joins!=null&&joins.size()>0){
				int order = 1;
				for (SquidJoin squidJoin2 : joins) {
					squidJoin2.setPrior_join_id(order);
					if (order==1){
						squidJoin2.setJoin_Condition("");
						squidJoin2.setJoinType(JoinType.BaseTable.value());
						join = squidJoin2;
					}
					adapter.update2(squidJoin2);
					sql = "select distinct t1.* from ds_reference_column_group t1 inner join ds_reference_column t2 on t1.id=t2.group_id " +
							" where t2.reference_squid_id="+squidJoin2.getJoined_squid_id()+" and host_squid_id="+squidJoin2.getTarget_squid_id()+" limit 0,1";
					ReferenceColumnGroup group = adapter.query2Object(true, sql, null, ReferenceColumnGroup.class);
					if (group!=null){
						group.setRelative_order(order);
						adapter.update2(group);
					}
					order++;
				}
			}
		}
		return join;
	}

	@Override
	public List<SquidJoin> getSquidJoinListByJoinedId(int joinedId) {
		Map<String, Object> paramMap = new HashMap<String, Object>();
		paramMap.put("joined_squid_id", joinedId);
		return adapter.query2List2(true, paramMap, SquidJoin.class);
	}
}
