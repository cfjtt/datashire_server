package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.dao.IReferenceColumnDao;
import com.eurlanda.datashire.dao.ISquidJoinDao;
import com.eurlanda.datashire.dao.ISquidLinkDao;
import com.eurlanda.datashire.dao.ITransformationDao;
import com.eurlanda.datashire.dao.ITransformationInputsDao;
import com.eurlanda.datashire.dao.ITransformationLinkDao;
import com.eurlanda.datashire.dao.impl.ReferenceColumnDaoImpl;
import com.eurlanda.datashire.dao.impl.SquidJoinDaoImpl;
import com.eurlanda.datashire.dao.impl.SquidLinkDaoImpl;
import com.eurlanda.datashire.dao.impl.TransformationDaoImpl;
import com.eurlanda.datashire.dao.impl.TransformationInputsDaoImpl;
import com.eurlanda.datashire.dao.impl.TransformationLinkDaoImpl;
import com.eurlanda.datashire.entity.ReferenceColumn;
import com.eurlanda.datashire.entity.ReferenceColumnGroup;
import com.eurlanda.datashire.entity.SquidJoin;
import com.eurlanda.datashire.entity.SquidLink;
import com.eurlanda.datashire.entity.Transformation;
import com.eurlanda.datashire.entity.TransformationInputs;
import com.eurlanda.datashire.entity.TransformationLink;
import com.eurlanda.datashire.enumeration.JoinType;
import com.eurlanda.datashire.utility.DatabaseException;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * transformation业务处理类
 * 
 * @author Administrator
 * 
 */
public class TransformationGroupProcess{
	/**
	 * 记录TransformationGroupProcess日志
	 */
	static Logger logger = Logger.getLogger(TransformationGroupProcess.class);

	// 异常处理机制
	ReturnValue out = new ReturnValue();

	private String token;//令牌根据令牌得到相应的连接信息

	public TransformationGroupProcess(String token) {
		this.token = token;
	}
	
	public Map<String, Object> deleteTransGroup(String info, ReturnValue out) {
		Map<String, Object> outputMap = new HashMap<String, Object>();
		IRelationalDataManager adapter3 = null;
		Integer groupId = 0;
		try {
			Map<String, Object> map = JsonUtil.toHashMap(info);
			groupId = map.get("GroupID") == null ? 0 : Integer.parseInt(map.get("GroupID") + "");// 入参转换为SquidJoin对象
			if (groupId > 0) {
				adapter3 = DataAdapterFactory.getDefaultDataManager();
				adapter3.openSession();
				// 初始化数据库接口
				IReferenceColumnDao refColumnDao = new ReferenceColumnDaoImpl(adapter3);
				ITransformationDao transDao = new TransformationDaoImpl(adapter3);
				ITransformationLinkDao transLinkDao = new TransformationLinkDaoImpl(adapter3);
				ITransformationInputsDao transInputsDao = new TransformationInputsDaoImpl(adapter3);
				ISquidJoinDao squidJoinDao = new SquidJoinDaoImpl(adapter3);
				ISquidLinkDao squidLinkDao = new SquidLinkDaoImpl(adapter3);
				
				// 初始化输出对象
				List<Integer> outputTransList = new ArrayList<Integer>();
				List<Integer> outputTransLinkList = new ArrayList<Integer>();
				List<Integer> outputSourceList = new ArrayList<Integer>();
				// 记录处理过的joinId
				List<Integer> joinList = new ArrayList<Integer>();
				// 根据group_id查询Columns
				List<ReferenceColumn> referenceColumnus = refColumnDao.getRefColumnListByGroupId(groupId);
				if (referenceColumnus != null && referenceColumnus.size() > 0) {
					for (ReferenceColumn referenceColumn : referenceColumnus) {
						// 根据column里面的信息查询 trans,transLink,join
						Transformation transformation = transDao.getTransformationById(referenceColumn.getReference_squid_id(), 
								referenceColumn.getColumn_id());
						if (transformation != null) {
							List<TransformationLink> fromTransLinks = transLinkDao.getTransLinkByTransId(transformation.getId());
							if (fromTransLinks != null && fromTransLinks.size() > 0) {
								for (TransformationLink transformationLink : fromTransLinks) {
									transLinkDao.delete(transformationLink.getId(), TransformationLink.class);
									outputTransLinkList.add(transformationLink.getId());
									//重置或者删除对应的inputs
									transInputsDao.resetTransformationInput(transformationLink.getFrom_transformation_id(), 
											transformationLink.getTo_transformation_id(), null);
								}
							}
							//删除本身trans的input集合
							transInputsDao.delTransInputsByTransId(transformation.getId());
							transDao.delete(transformation.getId(), Transformation.class);
							outputTransList.add(transformation.getId());
						}
						refColumnDao.delReferenceColumnByColumnId(referenceColumn.getColumn_id(), 
								referenceColumn.getReference_squid_id());
						outputSourceList.add(referenceColumn.getColumn_id());
					}
					SquidJoin squidJoin = squidJoinDao.getSquidJoinByParams(referenceColumnus.get(0).getHost_squid_id(), 
							referenceColumnus.get(0).getReference_squid_id());
					if (squidJoin!=null&&!joinList.contains(squidJoin.getId())) {
						boolean delJoined = squidJoinDao.delete(squidJoin.getId(), SquidJoin.class)>=0?true:false;
						if (delJoined){
							outputMap.put("DelSquidJoinId", squidJoin.getId());
							// 删除squidJoin的JoinType为BASETABLE时 执行更新squidJoin
							SquidJoin upSquidJoin = squidJoinDao.getUpSquidJoinByJoin(squidJoin);
							outputMap.put("updSquidJoin", upSquidJoin);
						}
						joinList.add(squidJoin.getId());
					}
					SquidLink squidLink = squidLinkDao.getSquidLinkByParams(referenceColumnus.get(0).getHost_squid_id(), 
							referenceColumnus.get(0).getReference_squid_id());
					if (squidLink!=null&&squidLink.getId()>0){
						squidLinkDao.delete(squidLink.getId(), SquidLink.class);
						outputMap.put("DelSquidLinkId", squidLink.getId());
					}
					outputMap.put("delVTransformIds", outputTransList);
					outputMap.put("delTransLinkIds", outputTransLinkList);
					outputMap.put("delSourceColumnIds", outputSourceList);
				}
			}
			return outputMap;
		} catch (Exception e) {
			logger.error("[更新deleteTransGroup=========================================exception]", e);
			try {
				if (adapter3!=null) adapter3.rollback();
			} catch (SQLException e1) {
				logger.error("rollback err!", e1);
			}
			logger.error(MessageFormat.format("删除deleteTransGroup异常  id={0}", groupId), e);
			out.setMessageCode(MessageCode.SQL_ERROR);
		} finally{
			if (adapter3!=null) adapter3.closeSession();
		}
		return null;
	}

	public Map<String, Object> updateTransGroupOrder(String info,
			ReturnValue out) {
		Map<String, Object> outputMap = new HashMap<String, Object>();
		IRelationalDataManager adapter3 = null;
		try {
			Map<String, Integer> groupMap = new HashMap<String, Integer>();
			List<SquidJoin> upSquidJoin = new ArrayList<SquidJoin>();
			List<String> otherColumn = new ArrayList<String>();
			otherColumn.add("column_id");
			// 实例化相关的数据库处理类
			adapter3 = DataAdapterFactory.getDefaultDataManager();
			adapter3.openSession();
			//初始化数据处理类
			IReferenceColumnDao refColumnDao = new ReferenceColumnDaoImpl(adapter3);
			ISquidJoinDao squidDao = new SquidJoinDaoImpl(adapter3);
			
			Map<String, Object> rsMap = JsonUtil.toHashMap(info);// 入参转换为SquidJoin对象
			List<ReferenceColumn> referenceColumns = JsonUtil.toGsonList(
					rsMap.get("SourceColumns")+"", ReferenceColumn.class);
			int count = 0;
			if (referenceColumns != null && referenceColumns.size()>0) {
				for (ReferenceColumn referenceColumn : referenceColumns) {
					if (!groupMap.containsKey(referenceColumn.getGroup_id()+"")) {
						// 没处理过的groupId添加到集合里面，下次循环就不执行了
						groupMap.put(referenceColumn.getGroup_id() + "",
								referenceColumn.getGroup_id());
						ReferenceColumnGroup group = refColumnDao.getObjectById(referenceColumn.getGroup_id(), 
								ReferenceColumnGroup.class);
						group.setRelative_order(referenceColumn.getGroup_order());
						refColumnDao.update(group);
						SquidJoin squidJoin = squidDao.getSquidJoinByParams(referenceColumn.getHost_squid_id(), 
								referenceColumn.getReference_squid_id());
						squidJoin.setPrior_join_id(referenceColumn.getGroup_order());
						if (count==0){
							squidJoin.setJoinType(JoinType.BaseTable.value());
						}else if(count>0&&squidJoin.getJoinType()==JoinType.BaseTable.value()){
							squidJoin.setJoinType(JoinType.InnerJoin.value());
						}
						squidDao.update(squidJoin);
						upSquidJoin.add(squidJoin);
						count++;
					}
				}
				//outputList.add(output);
				outputMap.put("updJoins", upSquidJoin);
			}
			return outputMap;
		} catch (Exception e) {
			logger.error("[更新updateTransGroupOrder=========================================exception]", e);
			try {
				adapter3.rollback();
			} catch (SQLException e1) {
				logger.error("rollback err!", e1);
			}
			logger.error(MessageFormat.format("updateTransGroupOrder异常", 0), e);
			out.setMessageCode(MessageCode.SQL_ERROR);
		} finally {
			adapter3.closeSession();
		}
		return null;
	}

	/**
	 * 删除TransformationLink，同时更改引用列is_referenced='N'
	 * @param TransformationLink_id
	 * @return
	 */
	public boolean deleteTransformationLink(IRelationalDataManager adapter3,
			int TransformationLink_id, ReturnValue out){
		Map<String, String> paramMap = new HashMap<String, String>();
		try{
			// 1. 更改引用列is_referenced='N'
			paramMap.put("id", String.valueOf(TransformationLink_id));
			List<TransformationLink> transformationLinks=adapter3.query2List(true, paramMap, TransformationLink.class);
			if(transformationLinks!=null && transformationLinks.size()>0){
				int from_transformation_id = transformationLinks.get(0).getFrom_transformation_id();
				updateReferenceColumn(adapter3 ,from_transformation_id, false);
			}
			// 2. 删除TransformationLink
			paramMap.put("id", Integer.toString(TransformationLink_id, 10));
			int cnt = adapter3.delete(paramMap, TransformationLink.class);
			if (cnt>0){
				//删除对于的inputs
				paramMap.clear();
				paramMap.put("transformation_id", transformationLinks.get(0).getTo_transformation_id()+"");
				paramMap.put("source_transform_id", transformationLinks.get(0).getFrom_transformation_id()+"");
				adapter3.delete(paramMap, TransformationInputs.class);
			}
			logger.debug(cnt+"[id="+TransformationLink_id+"] TransformationLink(s) removed!");
		} catch (Exception e) {
			logger.error("[删除deleteTransLink=========================================exception]", e);
			try {
				adapter3.rollback();
			} catch (SQLException e1) {
				logger.error("rollback err!", e1);
			}
			out.setMessageCode(MessageCode.SQL_ERROR);
			return false;
		} 
		return true;
	}

	/**
	 * 更新引用列
	 * 
	 * @param from_transformation_id
	 * @param is_referenced
	 * @return
	 * @throws SQLException
	 */
	public boolean updateReferenceColumn(IRelationalDataManager adapter3, int from_transformation_id,
			boolean is_referenced) throws DatabaseException {
		adapter3.openSession();
		String sql = "UPDATE DS_REFERENCE_COLUMN SET is_referenced='"
				+ (is_referenced ? 'Y' : 'N') + "'";
		sql += " WHERE column_id IN (SELECT column_id FROM DS_TRANSFORMATION WHERE ID="
				+ from_transformation_id + " AND column_id IS NOT NULL)";
		return adapter3.execute(sql) > 0 ? true : false;
	}
}
