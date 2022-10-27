package com.eurlanda.datashire.entity;

import java.util.List;

/**
 * 搜索repository接口，返回的查询集合
 * @author yi.zhou
 * @date   2014/10/13
 */
public class FindResult {
	private List<PathItem> pathItems;

	public List<PathItem> getPathItems() {
		return pathItems;
	}

	public void setPathItems(List<PathItem> pathItems) {
		this.pathItems = pathItems;
	}
	
	
}
