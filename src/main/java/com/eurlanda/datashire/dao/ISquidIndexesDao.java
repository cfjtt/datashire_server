package com.eurlanda.datashire.dao;

import com.eurlanda.datashire.entity.SquidIndexes;

import java.util.List;

public interface ISquidIndexesDao extends IBaseDao {
	/**
	 * 获取squid下的indexes的集合
	 * @param squidId
	 * @return
	 */
	public List<SquidIndexes> getSquidIndexesBySquidId(int squidId);
}
