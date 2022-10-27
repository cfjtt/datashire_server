package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;

import java.sql.Types;

/**
 * 
 * @author Cheryl 
 *
 */

@MultitableMapping(pk="", name = "DS_JOIN", desc = "")
public class SquidJoin extends DSBaseModel{

	@ColumnMpping(name="target_squid_id", desc="目标序列的squid连接", nullable=false, precision=0, type=Types.INTEGER, valueReg=">=1")
	private int target_squid_id;

	@ColumnMpping(name="joined_squid_id", desc="加入到之前的Squid", nullable=false, precision=0, type=Types.INTEGER, valueReg=">=1")
	private int joined_squid_id;
	
	@ColumnMpping(name="prior_join_id", desc="第一个为空", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private int prior_join_id;
	
	@ColumnMpping(name="join_type_id", desc="join类型", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private int joinType;
	
	@ColumnMpping(name="join_condition", desc="连接条件", nullable=true, precision=500, type=Types.VARCHAR, valueReg="")
	private String join_Condition;
	
	public SquidJoin() {
		super();
	}


	/**
	 * 全参构造
	 * @param id
	 * @param targetSquidId
	 * @param joinedSquidId
	 * @param priorJoinId
	 * @param joinTypeId
	 * @param joinCondition
	 */
	public SquidJoin(int id, int target_squid_id, int joined_squid_id,
			int prior_join_id, int joinType, String join_Condition,
			String guid) {
		super();
		this.id = id;
		this.target_squid_id = target_squid_id;
		this.joined_squid_id = joined_squid_id;
		this.prior_join_id = prior_join_id;
		this.joinType = joinType;
		this.join_Condition = join_Condition;
		this.key = guid;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public int getTarget_squid_id() {
		return target_squid_id;
	}

	public void setTarget_squid_id(int target_squid_id) {
		this.target_squid_id = target_squid_id;
	}

	public int getJoined_squid_id() {
		return joined_squid_id;
	}

	public void setJoined_squid_id(int joined_squid_id) {
		this.joined_squid_id = joined_squid_id;
	}

	public int getPrior_join_id() {
		return prior_join_id;
	}

	public void setPrior_join_id(int prior_join_id) {
		this.prior_join_id = prior_join_id;
	}

	public int getJoinType() {
		return joinType;
	}

	public void setJoinType(int joinType) {
		this.joinType = joinType;
	}

	public String getJoin_Condition() {
		return join_Condition;
	}

	public void setJoin_Condition(String join_Condition) {
		this.join_Condition = join_Condition;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String guid) {
		this.key = guid;
	}

	@Override
	public String toString() {
		return "Join [target_squid_id=" + target_squid_id
				+ ", joined_squid_id=" + joined_squid_id + ", prior_join_id="
				+ prior_join_id + ", joinType=" + joinType
				+ ", join_Condition=" + join_Condition + ", id=" + id
				+ ", key=" + key + "]";
	}
	
}