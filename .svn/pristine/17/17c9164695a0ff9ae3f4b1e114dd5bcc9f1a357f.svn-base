package com.eurlanda.datashire.entity;


/**
 * 搜索Repository的入参对象
 * @author yi.zhou
 * @date 2014/10/13
 *
 */
public class FindModel {
	private String condition; //匹配条件
	private boolean ignoreCase; //忽略大小写
	private boolean wholeWord; //全字匹配
	private int objectType; //查找类型
	private PathItem sourceItem; //查找源节点(repository/project/squidFlow/squid)
	
	public FindModel(){
	}
	
	public FindModel(String condition, boolean ignoreCase, boolean wholeWord,
			int objectType, PathItem sourceItem) {
		super();
		this.condition = condition;
		this.ignoreCase = ignoreCase;
		this.wholeWord = wholeWord;
		this.objectType = objectType;
		this.sourceItem = sourceItem;
	}
	public String getCondition() {
		return condition;
	}
	public void setCondition(String condition) {
		this.condition = condition;
	}
	public boolean isIgnoreCase() {
		return ignoreCase;
	}
	public void setIgnoreCase(boolean ignoreCase) {
		this.ignoreCase = ignoreCase;
	}
	public boolean isWholeWord() {
		return wholeWord;
	}
	public void setWholeWord(boolean wholeWord) {
		this.wholeWord = wholeWord;
	}
	public int getObjectType() {
		return objectType;
	}
	public void setObjectType(int objectType) {
		this.objectType = objectType;
	}
	public PathItem getSourceItem() {
		return sourceItem;
	}
	public void setSourceItem(PathItem sourceItem) {
		this.sourceItem = sourceItem;
	}
}
