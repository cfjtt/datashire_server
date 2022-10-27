package com.eurlanda.datashire.sprint7.plug;

import com.eurlanda.datashire.entity.operation.WhereCondition;
import com.eurlanda.datashire.enumeration.DataStatusEnum;
import com.eurlanda.datashire.enumeration.MatchTypeEnum;
import com.eurlanda.datashire.enumeration.SystemTableEnum;

import java.util.ArrayList;
import java.util.List;

/**
 * 
 * 
 * <p>
 * Title : 
 * </p>
 * <p>
 * Description: 
 * </p>
 * <p>
 * Author :赵春花 2013-9-10
 * </p>
 * <p>
 * update :赵春花 2013-9-10
 * </p>
 * <p>
 * Department :  JAVA后端研发部
 * </p>
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司
 * </p>
 */
public class JointCondition {
	
	/**
	 * 根据SquidID删除Squid语句拼接
	 * 
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
	 * 根据SquidID删除column
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
	 * 根据SquidID删除Transformantion
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
	 * 
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
		condition.setAttributeName("from_transformation_id");
		condition.setDataType(DataStatusEnum.INT);
		condition.setMatchType(MatchTypeEnum.EQ);
		condition.setValue(id);
		whereCondList.add(condition);
		// 装载删除TRANSFORMATION_LINK(from)
		condList.add(whereCondList);
		tableNames.add(SystemTableEnum.DS_TRANSFORMATION_LINK.toString());
	}
	
	/**
	 * 
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
		condition.setAttributeName("to_transformation_id");
		condition.setDataType(DataStatusEnum.INT);
		condition.setMatchType(MatchTypeEnum.EQ);
		condition.setValue(id);
		whereCondList.add(condition);
		// 装载删除TRANSFORMATION_LINK(to)
		condList.add(whereCondList);
		tableNames.add(SystemTableEnum.DS_TRANSFORMATION_LINK.toString());
	}
	
	/**
	 * 
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
		condition.setAttributeName("from_squid_id");
		condition.setDataType(DataStatusEnum.INT);
		condition.setMatchType(MatchTypeEnum.EQ);
		condition.setValue(id);
		whereCondList.add(condition);
		// 装载删除SquidLink
		condList.add(whereCondList);
		tableNames.add(SystemTableEnum.DS_SQUID_LINK.toString());
	}

	/**
	 * 
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
}
