package com.eurlanda.datashire.dao;

import com.eurlanda.datashire.entity.Url;

import java.util.List;

public interface IUrlDao {
	/**
	 * 获取squid下的url列表
	 * @param squidId
	 * @return
	 */
	public List<Url> getUrlsBySquidId(int squidId);
}
