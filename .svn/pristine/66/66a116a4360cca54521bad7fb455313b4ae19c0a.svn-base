package com.eurlanda.datashire.dao;

import com.eurlanda.datashire.entity.SquidJoin;

import java.sql.SQLException;
import java.util.List;

/**
 * SquidJoin数据操作类
 * @author yi.zhou
 *
 */
public interface ISquidJoinDao extends IBaseDao {
	/**
	 * 根据上游squidId和下游squidId获取Join对象
	 * @param target_squid_id 上游squidId
	 * @param joined_squid_id 下游squidId
	 * @return
	 */
	public SquidJoin getSquidJoinByParams(int target_squid_id, int joined_squid_id);
	
	/**
	 * 删除squidJoin
	 * @param target_squid_id 上游squidId
	 * @param joined_squid_id 下游squidId
	 * @return
	 */
	public boolean delSquidJoinByParams(int target_squid_id, int joined_squid_id) throws SQLException ;
	
	/**
	 * 更新某squid下的squidJoin的类型，同时重新排序
	 * @param squidJoin
	 * @return
	 * @throws SQLException
	 */
	public SquidJoin getUpSquidJoinByJoin(SquidJoin squidJoin) throws SQLException;
	
	/**
	 * 获取某squid下的所有的join列表
	 * @param joinedId
	 * @return
	 */
	public List<SquidJoin> getSquidJoinListByJoinedId(int joinedId);
}
