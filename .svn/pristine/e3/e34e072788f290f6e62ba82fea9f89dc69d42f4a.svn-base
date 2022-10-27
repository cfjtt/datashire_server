package com.eurlanda.datashire.entity.operation;

import com.eurlanda.datashire.enumeration.DataStatusEnum;
import com.eurlanda.datashire.enumeration.MatchTypeEnum;

/**
 * 查询条件实体类
 * @author Fumin
 *
 */
public class WhereCondition extends DataEntity {

	private MatchTypeEnum matchType;
	
	/**
	 * 构造器
	 */
	public WhereCondition(){
		
	}
	
	/** 查询条件 */
	public WhereCondition(String attributeName, MatchTypeEnum matchType, Object value){
		super.setAttributeName(attributeName);
		this.matchType = matchType;
		super.setValue(value);
	}
	
	/**
	 * 构造器
	 * @param attributeName
	 * @param value
	 * @param dataType
	 * @param matchType
	 */
	public WhereCondition(String attributeName, DataStatusEnum dataType, Object value, MatchTypeEnum matchType){
		super();
		this.matchType = matchType;
	}

	public MatchTypeEnum getMatchType() {
		return matchType;
	}

	public void setMatchType(MatchTypeEnum matchType) {
		this.matchType = matchType;
	}
}
