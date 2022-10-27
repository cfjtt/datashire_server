package com.eurlanda.datashire.sprint7.service.squidflow.subservice;

import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.entity.*;
import com.eurlanda.datashire.entity.operation.StageSquidAndSquidLink;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.enumeration.JoinType;
import com.eurlanda.datashire.enumeration.MessageBubbleCode;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.enumeration.TransformationTypeEnum;
import com.eurlanda.datashire.enumeration.datatype.SystemDatatype;
import com.eurlanda.datashire.sprint7.packet.PushMessagePacket;
import com.eurlanda.datashire.sprint7.service.squidflow.AbstractRepositoryService;
import com.eurlanda.datashire.utility.DebugUtil;
import com.eurlanda.datashire.utility.StringUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author dang.lu 2013.11.12
 *
 */
public class StageSquidServicesub extends AbstractRepositoryService implements IStageSquidService{
	
	public StageSquidServicesub(String token) {
		super(token);
	}
	public StageSquidServicesub(IRelationalDataManager adapter){
		super(adapter);
	}
	public StageSquidServicesub(String token, IRelationalDataManager adapter){
		super(token, adapter);
	}
	
	/**
	 * 从extract右键link到stage(该stage可能新创建，或者已经存在上游extract)
	 * 
	 * @param squidAndSquidLink
	 * @return
	 */
	public void link2StageSquid(StageSquidAndSquidLink squidAndSquidLink) {
		// 获得StageSquidID
		StageSquid stageSquid = squidAndSquidLink.getStageSquid();
		SquidLink squidLink = squidAndSquidLink.getSquidLink();
		squidLink.setType(DSObjectType.SQUIDLINK.value());

		int sourceSquidId = squidLink.getFrom_squid_id();
		int toSquidId = squidLink.getTo_squid_id();
		// Transformation面板中的目标列在创建squidLink或者join之后，导入了源squid的所有列，应该为空，由用户根据需要来导入。
		List<Column> toColumns = new ArrayList<Column>();
		Column newColumn = null;
		Map<String, String> paramMap = new HashMap<String, String>(1);
		
		try {
			adapter.openSession();
			if(squidLink.getId()<=0){ //创建link （ extract->stage ）
				if(StringUtils.isNull(squidLink.getKey())){
					squidLink.setKey(StringUtils.generateGUID());
				}
				squidLink.setId(adapter.insert2(squidLink));
			}
			
			// 创建join
			SquidJoin join = new SquidJoin();
			join.setJoined_squid_id(toSquidId);
			join.setTarget_squid_id(sourceSquidId);
			paramMap.put("joined_squid_id", Integer.toString(toSquidId, 10));
			List<SquidJoin> joinList = adapter.query2List(true, paramMap, SquidJoin.class);
			if (joinList == null || joinList.isEmpty()) {
				joinList = new ArrayList<SquidJoin>(1);
				join.setPrior_join_id(1);
				join.setJoinType(JoinType.BaseTable.value());
			}else{
				join.setPrior_join_id(joinList.size()+1);
				join.setJoinType(JoinType.InnerJoin.value());
				join.setJoin_Condition("");
			}
			join.setKey(StringUtils.generateGUID());
			join.setId(adapter.insert2(join));
			joinList.add(join);
			stageSquid.setJoins(joinList);
			
			paramMap.clear();
			paramMap.put("squid_id", Integer.toString(sourceSquidId, 10));
			List<Column> sourceColumns = adapter.query2List(true, paramMap, Column.class);
			if (sourceColumns == null || sourceColumns.isEmpty()) {
				logger.warn("columns of extract squid is null! sourceSquidId="+sourceSquidId);
				return;
			}
			
			paramMap.clear();
			paramMap.put("squid_id", Integer.toString(toSquidId, 10));
			List<Column> columns = adapter.query2List(true, paramMap, Column.class);
			int columnSize = 0;
			if(columns!=null && !columns.isEmpty()){
				columnSize = columns.size(); // 已存在目标列的个数
			}
			
			paramMap.clear();
			paramMap.put("reference_squid_id", Integer.toString(toSquidId, 10));
			List<ReferenceColumnGroup> rg = adapter.query2List(paramMap, ReferenceColumnGroup.class);
			ReferenceColumnGroup columnGroup = new ReferenceColumnGroup();
			columnGroup.setKey(StringUtils.generateGUID());
			columnGroup.setReference_squid_id(toSquidId);
			columnGroup.setRelative_order(rg==null||rg.isEmpty()?1:rg.size()+1);
			columnGroup.setId(adapter.insert2(columnGroup));
			Column column = null;
			for (int i=0; i<sourceColumns.size(); i++) {
				column = sourceColumns.get(i);
				// 创建引用列
				ReferenceColumn ref = new ReferenceColumn();
				ref.setColumn_id(column.getId());
				ref.setId(ref.getColumn_id());
				ref.setCollation(column.getCollation());
				ref.setData_type(column.getData_type());
				ref.setKey(StringUtils.generateGUID());
				ref.setName(column.getName());
				ref.setNullable(column.isNullable());
				ref.setRelative_order(column.getRelative_order());
				ref.setSquid_id(sourceSquidId);
				ref.setType(DSObjectType.COLUMN.value());
				ref.setReference_squid_id(toSquidId);
				ref.setHost_squid_id(sourceSquidId);
				ref.setIs_referenced(true);
				ref.setGroup_id(columnGroup.getId());
				adapter.insert2(ref);
			}

			List<MessageBubble> mbList = new ArrayList<MessageBubble>(2);
			if(columnSize<=0){
				// 自动增加以下 column id 类型为int 型，从0 每次加 1 自增
				newColumn = new Column();
				newColumn.setCollation(0);
				newColumn.setData_type(SystemDatatype.INT.value());
				newColumn.setLength(0);
				newColumn.setName("id");
				newColumn.setNullable(true);
				newColumn.setPrecision(0);
				newColumn.setRelative_order(0);
				newColumn.setSquid_id(toSquidId);
				newColumn.setKey(StringUtils.generateGUID());
				newColumn.setCollation(0);
				newColumn.setId(adapter.insert2(newColumn));
				toColumns.add(newColumn);
				// 创建新增列对应的虚拟变换
				Transformation newTrans = new Transformation();
				newTrans.setColumn_id(newColumn.getId());
				newTrans.setKey(StringUtils.generateGUID());
				newTrans.setSquid_id(toSquidId);
				newTrans.setTranstype(TransformationTypeEnum.VIRTUAL.value());
				newTrans.setId(adapter.insert2(newTrans));
				// 目标列（左边列）没有参与变换
				mbList.add(new MessageBubble(stageSquid.getKey(), newColumn.getKey(), MessageBubbleCode.ERROR_COLUMN_NO_TRANSFORMATION.value(), false));
			}else{
				toColumns.addAll(columns);
			}
			//消除stagesquid的消息泡
			mbList.add(new MessageBubble(stageSquid.getKey(), stageSquid.getKey(), MessageBubbleCode.WARN_SQUID_NO_LINK.value(), true));
			PushMessagePacket.push(mbList, token);
			
			stageSquid.setColumns(toColumns);
			// 获取所有相关数据（join、源/目标column、虚拟/真实transformation、transformation link）
			setStageSquidData(token, true, adapter, stageSquid);
		} catch (Exception e) {
			logger.error("创建stage squid异常", e);
			try {
				adapter.rollback();
			} catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
				logger.fatal("rollback err!", e1);
			}
		} finally {
			adapter.closeSession();
		}
	}
	
	/** 从datasquid的目标列选择若干column放到stagesquid上 */
	public void drag2StageSquid(StageSquidAndSquidLink squidAndSquidLink) {
		StageSquid stageSquid = squidAndSquidLink.getStageSquid();
		List<Column> columns = stageSquid.getColumns();
		SquidLink squidLink = squidAndSquidLink.getSquidLink();

		if(columns==null || columns.isEmpty()){
			logger.warn("may be no column selected, current squid id = "+stageSquid.getId());
			return;
		}
		
		try {
			Squid squid = getOne(Squid.class, squidLink.getFrom_squid_id());
			if (squid == null) { // 来源squid为空 异常处理
				logger.error("cannot find from data squid by id = "+squidLink.getFrom_squid_id()+", current squid id = "+stageSquid.getId());
				return;
			}
			
			adapter.openSession();
			// 没相同LINK存在的情况下创建
			if (squidLink.getId()<=0) {
				squidLink.setId(adapter.insert2(squidLink));
			}
			squidLink.setType(DSObjectType.SQUIDLINK.value());
			
			// 创建join
			List<SquidJoin> joinList = new ArrayList<SquidJoin>(1);
			SquidJoin join = new SquidJoin();
			join.setJoined_squid_id(squidLink.getFrom_squid_id());
			join.setTarget_squid_id(squidLink.getTo_squid_id());
			join.setPrior_join_id(1);
			join.setJoinType(JoinType.BaseTable.value());
			join.setKey(StringUtils.generateGUID());
			join.setId(adapter.insert2(join));
			joinList.add(join);
			stageSquid.setJoins(joinList);
			
			// 创建引用列组
			ReferenceColumnGroup columnGroup = new ReferenceColumnGroup();
			columnGroup.setKey(StringUtils.generateGUID());
			columnGroup.setReference_squid_id(stageSquid.getId());
			columnGroup.setRelative_order(1);
			columnGroup.setId(adapter.insert2(columnGroup));
			
			for (int i=0; i<columns.size(); i++) {
				Column column = columns.get(i);
				column.setRelative_order(i+1);
				
				ReferenceColumn ref = new ReferenceColumn();
				ref.setColumn_id(column.getId());
				ref.setId(ref.getColumn_id());
				ref.setCollation(0);
				ref.setData_type(column.getData_type());
				ref.setKey(StringUtils.generateGUID());
				ref.setName(column.getName());
				ref.setNullable(column.isNullable());
				ref.setRelative_order(i+1);
				ref.setSquid_id(squid.getId());
				ref.setReference_squid_id(stageSquid.getId());
				ref.setHost_squid_id(squid.getId());
				ref.setIs_referenced(true);
				ref.setGroup_id(columnGroup.getId());
				ref.setId(adapter.insert2(ref));  // 创建引用列
				column.setId(adapter.insert2(column)); // 创建目标列
				
				// 创建目标列的虚拟变换
				Transformation trans = new Transformation();
				trans.setColumn_id(column.getId());
				trans.setKey(StringUtils.generateGUID());
				trans.setLocation_x(0);
				trans.setLocation_y(i * 25 + 25 / 2);
				trans.setSquid_id(stageSquid.getId());
				trans.setTranstype(TransformationTypeEnum.VIRTUAL.value());
				trans.setId(adapter.insert2(trans));
			}
			setStageSquidData(token, true, adapter, stageSquid);
		} catch (Exception e) {
			logger.error("创建stage squid异常", e);
			try {
				adapter.rollback();
			} catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
				logger.fatal("rollback err!", e1);
			}
		} finally {
			adapter.closeSession();
		}
		return;
	}
	
	/** 获取StageSquid详细信息（如果不在同一个事务里，adapter连接打开、关闭需要调用函数自己处理） */
	public  void setStageSquidData(String token, boolean inSession, IRelationalDataManager adapter, StageSquid stageSquid) {
		stageSquid.setType(DSObjectType.STAGE.value());
		if(stageSquid!=null){
			logger.debug("(stage) squid detail, id="+stageSquid.getId()+", name="+stageSquid.getName()+", table_name="+stageSquid.getTable_name()+", type="+SquidTypeEnum.parse(stageSquid.getSquid_type()));
		}
		Map<String, String> paramMap = new HashMap<String, String>(1);
		Map<String, Object> paramMap2 = new HashMap<String, Object>(5);
		List<ReferenceColumn> sourceColumnlist = null;
		List<Integer> sourceColumnId = new ArrayList<Integer>();
		List<Integer> sourceSquidId = new ArrayList<Integer>();
		List<Integer> transId = new ArrayList<Integer>();
		//boolean host_column_deleted = false;
		try {
			if(!inSession){
				adapter.openSession();
			}
			
			// 所有join
			if(stageSquid.getJoins()==null||stageSquid.getJoins().isEmpty()){
				paramMap.clear();
				paramMap.put("joined_squid_id", Integer.toString(stageSquid.getId(), 10));
				stageSquid.setJoins(adapter.query2List(true, paramMap, SquidJoin.class));
			}
			
			// 所有目标列
			if(stageSquid.getColumns()==null || stageSquid.getColumns().isEmpty()){
				paramMap.clear();
				paramMap.put("squid_id", Integer.toString(stageSquid.getId(), 10));
				stageSquid.setColumns(adapter.query2List(true, paramMap, Column.class));
			}
			
			// 所有引用列
			//paramMap.clear();
			//paramMap.put("reference_squid_id", Integer.toString(stageSquid.getId(), 10));
			sourceColumnlist = adapter.query2List(true, "select nvl(c.id,r.column_id) id,r.host_squid_id squid_id,r.*, nvl2(c.id, 'N','Y') host_column_deleted,g.relative_order group_order from"
					+ " DS_COLUMN c right join DS_REFERENCE_COLUMN r on r.column_id=c.id"
					+ " left join DS_REFERENCE_COLUMN_GROUP g on r.group_id=g.id"
					+ " where reference_squid_id="+stageSquid.getId(), null, ReferenceColumn.class);
			stageSquid.setSourceColumns(sourceColumnlist);
			
			// 所有上游squid
			if(sourceColumnlist!=null && !sourceColumnlist.isEmpty()){
				for(int i=0; i<sourceColumnlist.size(); i++){
					if(sourceColumnlist.get(i).getColumn_id()>0&&!sourceColumnId.contains(sourceColumnlist.get(i).getColumn_id())){
						sourceColumnId.add(sourceColumnlist.get(i).getColumn_id());
					}
					if(sourceColumnlist.get(i).getSquid_id()>0&&!sourceSquidId.contains(sourceColumnlist.get(i).getSquid_id())){
						sourceSquidId.add(sourceColumnlist.get(i).getSquid_id());
					}
//					if(sourceColumnlist.get(i).isHost_column_deleted()){
//						//logger.warn("host_column_deleted: "+sourceColumnlist.get(i));
//						//host_column_deleted = true; // 只要有一个上游列被删除就表示有问题
//						sourceColumnlist.get(i).setId(sourceColumnlist.get(i).getColumn_id());
//					}
				}
			}
			
			// 所有transformation（包括虚拟、真实变换）
			paramMap.clear();
			paramMap.put("squid_id", Integer.toString(stageSquid.getId(), 10));
			List<Transformation> transformations = adapter.query2List(true, paramMap, Transformation.class);
			if(transformations!=null && !transformations.isEmpty()){
				for(int i=0; i<transformations.size(); i++){
					transId.add(transformations.get(i).getId());
				}
			}
			
			// 所有上游引用列的虚拟变换
			if(sourceColumnId!=null && !sourceColumnId.isEmpty()){
				paramMap2.put("column_id", sourceColumnId);
				paramMap2.put("squid_id", sourceSquidId);
				paramMap2.put("transformation_type_id", TransformationTypeEnum.VIRTUAL.value());
				StringUtils.addAll(transformations, adapter.query2List2(true, paramMap2, Transformation.class));
			}
			stageSquid.setTransformations(transformations);
			
			// 所有transformation link
			List<TransformationLink> transformationLinks = null;
			if(transId!=null && !transId.isEmpty()){
				paramMap2.clear();
				paramMap2.put("TO_TRANSFORMATION_ID", transId);
				transformationLinks = adapter.query2List2(true, paramMap2, TransformationLink.class);
				stageSquid.setTransformationLinks(transformationLinks);
			}
			
			// 上游的column被删除
//			PushMessagePacket.push(
//					new MessageBubble(stageSquid.getKey(), stageSquid.getKey(), MessageBubbleCode.ERROR_REFERENCE_COLUMN_DELETED.value(), !host_column_deleted), 
//					token);
			
			if(DebugUtil.isDebugenabled())logger.debug(DebugUtil.squidDetail(stageSquid));
		} catch (Exception e) {
			logger.error("getStageSquidData-datas", e);
		} finally {
			if(!inSession){
				adapter.closeSession();
			}
		}
	}

}