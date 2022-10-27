package com.eurlanda.datashire.sprint7.plug;

import com.eurlanda.datashire.entity.operation.WhereCondition;
import com.eurlanda.datashire.enumeration.DataStatusEnum;
import com.eurlanda.datashire.enumeration.MatchTypeEnum;
import com.eurlanda.datashire.enumeration.SystemTableEnum;

import java.util.ArrayList;
import java.util.List;

public class Dispose {
	/**
	 * 根据ID删除SquidFlow语句
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param squidFlowId
	 *@param condList
	 *@param tableNames
	 */
	public void getSquidFlowLoadById(int squidFlowId, List<List<WhereCondition>> condList, List<String> tableNames) {
		List<WhereCondition> whereCondList;
		WhereCondition condition;
		whereCondList = new ArrayList<WhereCondition>();
		condition = new WhereCondition();
		condition.setAttributeName("ID");
		condition.setDataType(DataStatusEnum.INT);
		condition.setMatchType(MatchTypeEnum.EQ);
		condition.setValue(squidFlowId);
		whereCondList.add(condition);
		// 拼接条件
		condList.add(whereCondList);
		tableNames.add(SystemTableEnum.DS_SQUID_FLOW.toString());
	}
	
	/**
	 * 根据ID删除Squid条件
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param id
	 *@param condList
	 *@param tableNames
	 */
	public void getSquidLoadById(int id, List<List<WhereCondition>> condList, List<String> tableNames) {
		List<WhereCondition> whereCondList;
		WhereCondition condition;
		// 拼接Squid删除语句
		whereCondList = new ArrayList<WhereCondition>();
		condition = new WhereCondition();
		condition.setAttributeName("ID");
		condition.setDataType(DataStatusEnum.INT);
		condition.setMatchType(MatchTypeEnum.EQ);
		condition.setValue(id);
		whereCondList.add(condition);
		// 装载删除Squid
		condList.add(whereCondList);
		tableNames.add(SystemTableEnum.DS_SQUID.toString());
	}
	
	/**
	 * 根据SquidId删除Column语句
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param condList
	 *@param tableNames
	 *@param id
	 */
	public void getColumnLoadBysquidId(List<List<WhereCondition>> condList, List<String> tableNames, int id) {
		List<WhereCondition> whereCondList;
		WhereCondition condition;
		// column
		whereCondList = new ArrayList<WhereCondition>();
		condition = new WhereCondition();
		condition.setAttributeName("SQUID_ID");
		condition.setDataType(DataStatusEnum.INT);
		condition.setMatchType(MatchTypeEnum.EQ);
		condition.setValue(id);
		whereCondList.add(condition);
		// 拼接条件
		condList.add(whereCondList);
		tableNames.add(SystemTableEnum.DS_COLUMN.toString());
	}
	
	/**
	 * 根据SquidID删除TRANSFORMATION语句
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param condList
	 *@param tableNames
	 *@param id
	 */
	public void getTransformationLoadBySquid(List<List<WhereCondition>> condList, List<String> tableNames, int id) {
		List<WhereCondition> whereCondList;
		WhereCondition condition;
		whereCondList = new ArrayList<WhereCondition>();
		condition = new WhereCondition();
		condition.setAttributeName("SQUID_ID");
		condition.setDataType(DataStatusEnum.INT);
		condition.setMatchType(MatchTypeEnum.EQ);
		condition.setValue(id);
		whereCondList.add(condition);
		// 拼接条件
		condList.add(whereCondList);
		tableNames.add(SystemTableEnum.DS_TRANSFORMATION.toString());
	}
	
	/**
	 * 根据from_transformation_id删除TRANSFORMATION_LINK语句
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param condList
	 *@param tableNames
	 *@param id
	 */
	public void getTransformationLinkLoadByformId(List<List<WhereCondition>> condList, List<String> tableNames, int id) {
		List<WhereCondition> whereCondList;
		WhereCondition condition;
		whereCondList = new ArrayList<WhereCondition>();
		condition = new WhereCondition();
		condition.setAttributeName("FROM_TRANSFORMATION_ID");
		condition.setDataType(DataStatusEnum.INT);
		condition.setMatchType(MatchTypeEnum.EQ);
		condition.setValue(id);
		whereCondList.add(condition);
		// 装载删除TRANSFORMATION_LINK(from)
		condList.add(whereCondList);
		tableNames.add(SystemTableEnum.DS_TRANSFORMATION_LINK.toString());
	}
	
	/**
	 * 根据to_transformation_id删除TRANSFORMATION_LINK
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param condList
	 *@param tableNames
	 *@param id
	 */
	public void getTransformationLinkLoadByToId(List<List<WhereCondition>> condList, List<String> tableNames, int id) {
		List<WhereCondition> whereCondList;
		WhereCondition condition;
		whereCondList = new ArrayList<WhereCondition>();
		condition = new WhereCondition();
		condition.setAttributeName("TO_TRANSFORMATION_ID");
		condition.setDataType(DataStatusEnum.INT);
		condition.setMatchType(MatchTypeEnum.EQ);
		condition.setValue(id);
		whereCondList.add(condition);
		// 装载删除TRANSFORMATION_LINK(to)
		condList.add(whereCondList);
		tableNames.add(SystemTableEnum.DS_TRANSFORMATION_LINK.toString());
	}
	
	/**
	 * 根据from_squid_id删除DS_SQUID_LINK语句
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param id
	 *@param condList
	 *@param tableNames
	 */
	public void getSquidLinkLoadByfromId(int id, List<List<WhereCondition>> condList, List<String> tableNames) {
		List<WhereCondition> whereCondList;
		WhereCondition condition;
		// SquidLink
		// 条件
		whereCondList = new ArrayList<WhereCondition>();
		condition = new WhereCondition();
		condition.setAttributeName("FROM_SQUID_ID");
		condition.setDataType(DataStatusEnum.INT);
		condition.setMatchType(MatchTypeEnum.EQ);
		condition.setValue(id);
		whereCondList.add(condition);
		// 装载删除SquidLink
		condList.add(whereCondList);
		tableNames.add(SystemTableEnum.DS_SQUID_LINK.toString());
	}
	
	/**
	 * 根据to_squid_id删除DS_SQUID_LINK
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param id
	 *@param condList
	 *@param tableNames
	 */
	public void getSquidLinkLoadBytoSquidId(int id, List<List<WhereCondition>> condList, List<String> tableNames) {
		List<WhereCondition> whereCondList;
		WhereCondition condition;
		// 条件
		whereCondList = new ArrayList<WhereCondition>();
		condition = new WhereCondition();
		condition.setAttributeName("to_squid_id");
		condition.setDataType(DataStatusEnum.INT);
		condition.setMatchType(MatchTypeEnum.EQ);
		condition.setValue(id);
		whereCondList.add(condition);
		// 装载删除SquidLink
		condList.add(whereCondList);
		tableNames.add(SystemTableEnum.DS_SQUID_LINK.toString());
	}
	
	/**
	 * 根据squid_flow_id删除Squid语句
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param squidFlowId
	 *@param condList
	 *@param tableNames
	 */
	public void getSquidLoadBySquidFlowId(int squidFlowId, List<List<WhereCondition>> condList, List<String> tableNames) {
		List<WhereCondition> whereCondList;
		WhereCondition condition;
		whereCondList = new ArrayList<WhereCondition>();
		condition = new WhereCondition();
		condition.setAttributeName("squid_flow_id");
		condition.setDataType(DataStatusEnum.INT);
		condition.setMatchType(MatchTypeEnum.EQ);
		condition.setValue(squidFlowId);
		whereCondList.add(condition);
		// 拼接条件
		condList.add(whereCondList);
		tableNames.add(SystemTableEnum.DS_SQUID.toString());
	}
	
	/**
	 * 根据squidflowid删除squidLink
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param squidFlowId
	 *@param condList
	 *@param tableNames
	 */
	public void getSquidLinkLoadBySquidFlowId(int squidFlowId, List<List<WhereCondition>> condList,
			List<String> tableNames) {
		List<WhereCondition> whereCondList;
		WhereCondition condition;
		whereCondList = new ArrayList<WhereCondition>();
		condition = new WhereCondition();
		condition.setAttributeName("squid_flow_id");
		condition.setDataType(DataStatusEnum.INT);
		condition.setMatchType(MatchTypeEnum.EQ);
		condition.setValue(squidFlowId);
		whereCondList.add(condition);
		// 拼接条件
		condList.add(whereCondList);
		tableNames.add(SystemTableEnum.DS_SQUID_LINK.toString());
	}
}
