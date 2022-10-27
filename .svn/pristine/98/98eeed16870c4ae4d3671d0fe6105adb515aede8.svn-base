package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.dao.IColumnDao;
import com.eurlanda.datashire.dao.IReferenceColumnDao;
import com.eurlanda.datashire.dao.ISquidDao;
import com.eurlanda.datashire.dao.ISquidJoinDao;
import com.eurlanda.datashire.dao.ISquidLinkDao;
import com.eurlanda.datashire.dao.impl.ColumnDaoImpl;
import com.eurlanda.datashire.dao.impl.ReferenceColumnDaoImpl;
import com.eurlanda.datashire.dao.impl.SquidDaoImpl;
import com.eurlanda.datashire.dao.impl.SquidJoinDaoImpl;
import com.eurlanda.datashire.dao.impl.SquidLinkDaoImpl;
import com.eurlanda.datashire.entity.Column;
import com.eurlanda.datashire.entity.ReferenceColumn;
import com.eurlanda.datashire.entity.ReferenceColumnGroup;
import com.eurlanda.datashire.entity.Squid;
import com.eurlanda.datashire.entity.SquidJoin;
import com.eurlanda.datashire.entity.SquidLink;
import com.eurlanda.datashire.entity.StageSquid;
import com.eurlanda.datashire.entity.Transformation;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.enumeration.MessageBubbleCode;
import com.eurlanda.datashire.enumeration.TransformationTypeEnum;
import com.eurlanda.datashire.server.utils.TokenUtil;
import com.eurlanda.datashire.sprint7.service.squidflow.subservice.MessageBubbleService;
import com.eurlanda.datashire.utility.CommonConsts;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import com.eurlanda.datashire.utility.StringUtils;
import com.eurlanda.datashire.validator.SquidValidationTask;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 对join操作的业务处理类，在datashireService中调用此方法 继承SupportPlug获取adapter连接池对象
 * 
 * @version 1.0
 * @author yi.zhou
 * @created 2014-04-04
 */
public class SquidJoinProcess {
	/**
	 * 记录SquidJoinProcess日志
	 */
	static Logger logger = Logger.getLogger(SquidJoinProcess.class);

	// 异常处理机制
	ReturnValue out = new ReturnValue();

	private String token;// 令牌根据令牌得到相应的连接信息

	public SquidJoinProcess(String token) {
		this.token = token;
	}

	public Map<String, Object> createJoin(String info, ReturnValue out) {
		int newJoinId = 0;// join表是否创建成功
		Map<String, Object> outputMap = new HashMap<String, Object>();
		Map<String, Object> link2TransMap = new HashMap<String, Object>();
		IRelationalDataManager adapter3 = null;
		try {
			// 实例化相关的数据库处理类
			adapter3 = DataAdapterFactory.getDefaultDataManager();
			adapter3.openSession();

			ISquidJoinDao squidJoinDao = new SquidJoinDaoImpl(adapter3);
			ISquidDao squidDao = new SquidDaoImpl(adapter3);
			SquidJoin squidJoin = JsonUtil.object2HashMap(info, SquidJoin.class);// 入参转换为SquidJoin对象
			if (squidJoin != null) {
				logger.debug("解析后的JoinInput为===" + squidJoin);
				// 对join表进行新增
				// 开启连接池，并且设置为自动提交
				newJoinId = squidJoinDao.insert2(squidJoin);
				SquidJoin newSquidJoin = null;
				SquidLink newSquidLink = null;
				StageSquid stageSquid = null;
				if (newJoinId != 0) {
					// 把插入成功的join信息根据key查询出来
					newSquidJoin = squidJoinDao.getObjectById(newJoinId, SquidJoin.class);
					logger.debug("=====创建的joinOut=====" + newSquidJoin.getId());
					// 获取StageSquid // 前端传送的jsoned_squid_id小于0的时候，不执行
					if (newSquidJoin != null && newSquidJoin.getJoined_squid_id() > 0) {
						stageSquid = squidDao.getObjectById(
								newSquidJoin.getJoined_squid_id(),
								StageSquid.class);
					}
					// 创建SquidLink // 前端传送的target_id小于0的时候,不创建squidlink
					if (newSquidJoin != null && newSquidJoin.getTarget_squid_id() > 0) { //
						// 对squidLink类的转换 及创建
						newSquidLink = this.joinTOLink(adapter3, newSquidJoin);
						logger.debug("=====创建的squidlink====="
								+ newSquidLink.getId());
						// 成功创建link并且stageSquid不为空，创建transformation
						if (newSquidLink != null && stageSquid != null) {
							link2TransMap = link2Transformation(newSquidLink,
									adapter3, stageSquid);
							outputMap.putAll(link2TransMap);
						} else {
							outputMap.put("newColumns", new ArrayList<Column>());
							outputMap.put("newSourceColumns", new ArrayList<ReferenceColumn>());
							outputMap.put("newVTransforms", new ArrayList<Transformation>());
						}
					}
				}
				outputMap.put("newSquidJoinId", newSquidJoin.getId());
				outputMap.put("newSquidLink", newSquidLink);
			} else {
				out.setMessageCode(MessageCode.NODATA);
				return null;
			}
		} catch (Exception e) {
			logger.error("createSquidJoin is error", e);
			try {
				adapter3.rollback();
			} catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
				logger.fatal("rollback err!", e1);
			}
			out.setMessageCode(MessageCode.ERR_CREATEJOIN);
		} finally {
			adapter3.closeSession();
		}
		return outputMap;
	}

	/**
	 * join对象到squidLink对象的转换
	 * 
	 * @param join
	 * @param out
	 * @return SquidLink
	 * @author lei.bin
	 * @throws SQLException
	 */
	private SquidLink joinTOLink(IRelationalDataManager adapter3, SquidJoin join)
			throws Exception {
		ISquidDao squidDao = new SquidDaoImpl(adapter3);
		ISquidLinkDao squidLinkDao = new SquidLinkDaoImpl(adapter3);
		SquidLink squidLink = new SquidLink();
		squidLink.setKey(StringUtils.generateGUID());
		// 根据TargetSquidId查询squid表，得到squid_flow_id
		Squid squid = squidDao.getObjectById(join.getTarget_squid_id(),
				Squid.class);
		squidLink.setSquid_flow_id(squid.getSquidflow_id());
		squidLink.setFrom_squid_id(join.getTarget_squid_id());
		squidLink.setTo_squid_id(join.getJoined_squid_id());
		squidLink.setLine_type(1);
		int newLinkId = squidLinkDao.insert2(squidLink);
		if (newLinkId != 0) {
			SquidLink newSquidLink = squidLinkDao.getObjectById(newLinkId,
					SquidLink.class);
			if (newSquidLink != null) {
				return newSquidLink;
			}
		}
		return null;
	}

	/**
	 * 创建link时,创建transformation的信息
	 * 
	 * @param squidAndSquidLink
	 * @return
	 * @throws Exception
	 */
	private Map<String, Object> link2Transformation(SquidLink squidLink,
			IRelationalDataManager adapter2, StageSquid stageSquidOut)
			throws Exception {
		ISquidDao squidDao = new SquidDaoImpl(adapter2);
		IColumnDao columnDao = new ColumnDaoImpl(adapter2);
		IReferenceColumnDao refColumnDao = new ReferenceColumnDaoImpl(adapter2);
		squidLink.setType(DSObjectType.SQUIDLINK.value());

		// 返回值
		Map<String, Object> output = new HashMap<String, Object>();
		int sourceSquidId = squidLink.getFrom_squid_id();
		int toSquidId = squidLink.getTo_squid_id();
		// Transformation面板中的目标列在创建squidLink或者join之后，导入了源squid的所有列，应该为空，由用户根据需要来导入。
		List<Column> toColumns = new ArrayList<Column>();
		List<ReferenceColumn> outColumns = new ArrayList<ReferenceColumn>();
		List<Transformation> outTransformations = new ArrayList<Transformation>();
		// 根据squidlink的to_id查询stagesquid对象
		Squid stageSquid = squidDao.getObjectById(toSquidId, Squid.class);
		stageSquidOut.setKey(stageSquid.getKey());
		// 获取所有的上游的column集合
		List<Column> sourceColumns = columnDao
				.getColumnListBySquidId(sourceSquidId);
		// 获取所有的下游的column集合
		List<Column> columns = columnDao.getColumnListBySquidId(toSquidId);
		int columnSize = 0;
		if (columns != null && !columns.isEmpty()) {
			columnSize = columns.size(); // 已存在目标列的个数
		}

		TransformationService transformationService = new TransformationService(
				TokenUtil.getToken());
		if (sourceColumns != null && sourceColumns.size() > 0) {
			List<ReferenceColumnGroup> rg = refColumnDao
					.getRefColumnGroupListBySquidId(toSquidId);
			ReferenceColumnGroup columnGroup = new ReferenceColumnGroup();
			columnGroup.setKey(StringUtils.generateGUID());
			columnGroup.setReference_squid_id(toSquidId);
			columnGroup.setRelative_order(rg == null || rg.isEmpty() ? 1 : rg
					.size() + 1);
			columnGroup.setId(refColumnDao.insert2(columnGroup));

			Squid extSquid = squidDao.getObjectById(sourceSquidId, Squid.class);
			for (int i = 0; i < sourceColumns.size(); i++) {
				Column column = sourceColumns.get(i);
				// if (extSquid.getSquid_type()==SquidTypeEnum.EXTRACT.value()
				// &&column.getName().equals("extraction_date")){
				// continue;
				// }
				// 创建引用列
				ReferenceColumn ref = transformationService.initReference(
						adapter2, column, column.getId(), i + 1, extSquid,
						toSquidId, columnGroup);
				outColumns.add(ref);
				// 创建新增列对应的虚拟变换
				Transformation newTrans = transformationService
						.initTransformation(adapter2, toSquidId,
								column.getId(),
								TransformationTypeEnum.VIRTUAL.value(),
								column.getData_type(), 1);
				// 创建后，添加到输出对象
				outTransformations.add(newTrans);
			}
		}
		stageSquidOut.setColumns(toColumns);
		// 获取所有相关数据（join、源/目标column、虚拟/真实transformation、transformation link）
		// setStageSquidData(token, true, adapter, stageSquid);
		output.put("newColumns", toColumns);
		output.put("newSourceColumns", outColumns);
		output.put("newVTransforms", outTransformations);
		return output;
	}

	/**
	 * 根据join_id删除join
	 * 
	 * @param join_id
	 * @param out
	 * @return
	 * @throws Exception
	 */
	public Map<String, Object> deleteSquidJoinDetail(
			IRelationalDataManager adapter3, SquidJoin squidJoin,
			ReturnValue out) throws Exception {
		boolean delJoinFlag = false;
		ISquidJoinDao squidJoinDao = new SquidJoinDaoImpl(adapter3);
		ISquidLinkDao squidLinkDao = new SquidLinkDaoImpl(adapter3);
		Map<String, Object> outputMap = new HashMap<String, Object>();
		LinkProcess process = new LinkProcess(TokenUtil.getToken());
		// 根据target_squid_id和joined_squid_id查出link表的id(target_squid_id为-1时,不执行删除squidlink的操作)
		if (squidJoin.getTarget_squid_id() > 0) {
			SquidLink squidLink = squidLinkDao.getSquidLinkByParams(
					squidJoin.getTarget_squid_id(),
					squidJoin.getJoined_squid_id());
			// 删除link
			// SquidLinkServicesub squidLinkService=new
			// SquidLinkServicesub(token, adapter2);
			if (squidLink != null) {
				outputMap.putAll(process.delColumnsAndTransByLink(adapter3, squidLink));
				delJoinFlag = squidLinkDao.delete(squidLink.getId(), SquidLink.class) >= 0 ? true : false;
				if (delJoinFlag){
					outputMap.put("DelSquidLinkId", squidLink.getId());
					// 孤立Squid消息泡
		            CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(), adapter3,
		            		MessageBubbleService.setMessageBubble(squidLink.getFrom_squid_id(), 
		            				squidLink.getFrom_squid_id(), null, 
		            				MessageBubbleCode.WARN_SQUID_NO_LINK.value())));
				}
			}
		}
		// 如果是删除join信息的话，需要删除join信息，如果是更新joinSquidName的话，不需要删除join信息
		// 删除join表
		delJoinFlag = squidJoinDao.delete(squidJoin.getId(), SquidJoin.class) >= 0 ? true : false;
		// 如果是删除join信息的话，需要判断是否需要要更新join的type信息，如果是更新joinSquidName的话，则不需要判断是否更新join的type的信息
		// 如果删除join成功，那么需要判断是否要更新squidJoin信息
		if (delJoinFlag) {
			outputMap.put("DelSquidJoinId", squidJoin.getId());
			outputMap.put("updSquidJoin", squidJoinDao.getUpSquidJoinByJoin(squidJoin));
			// Joins的消息泡
			CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(),
					adapter3, MessageBubbleService.setMessageBubble(
							squidJoin.getJoined_squid_id(), squidJoin.getId(),
							squidJoin.getJoin_Condition(),
							MessageBubbleCode.ERROR_SQL_SYNTAX_JOIN.value())));
		}
		return outputMap;
	}

	/**
	 * @param squidJoin
	 * @param out
	 * @return
	 * @author bo.dang
	 * @date 2014-4-9
	 */
	public Map<String, Object> updateSquidJoin(String info, ReturnValue out) {
		SquidJoin squidJoin = JsonUtil.object2HashMap(info, SquidJoin.class);
		IRelationalDataManager adapter3 = null;
		Map<String, Object> updateMap = new HashMap<String, Object>();
		try {
			adapter3 = DataAdapterFactory.getDefaultDataManager();
			adapter3.openSession();
			// 初始化数据接口
			ISquidJoinDao squidJoinDao = new SquidJoinDaoImpl(adapter3);
			ISquidLinkDao squidLinkDao = new SquidLinkDaoImpl(adapter3);
			ISquidDao squidDao = new SquidDaoImpl(adapter3);
			// 取得原始的squidJoin的信息
			SquidJoin tempSquidJoin = squidJoinDao.getObjectById(squidJoin.getId(), SquidJoin.class);
			if (tempSquidJoin==null||squidJoin.getId()==0){
				out.setMessageCode(MessageCode.NODATA);
				return null;
			}
			SquidLink squidLink = squidLinkDao.getSquidLinkByParams(tempSquidJoin.getTarget_squid_id(), 
					tempSquidJoin.getJoined_squid_id());
			LinkProcess squidLinkProcess = new LinkProcess(TokenUtil.getToken());
			if (squidLink!=null){
				// 删除原来的ReferenceColumn、ReferenceColumnGroup、Transformation、TransformationLink信息
				updateMap.putAll(squidLinkProcess.delColumnsAndTransByLink(adapter3, squidLink));
			}
			
			// 返回更新之后的SquidLink信息
			boolean updateSquidLinkFlag = squidJoinDao.update(squidJoin);
			SquidLink squidLinkResult = null;
			if(updateSquidLinkFlag){
				squidLink.setFrom_squid_id(squidJoin.getTarget_squid_id());
				squidLink.setTo_squid_id(squidJoin.getJoined_squid_id());
				boolean flag = squidLinkDao.update(squidLink);
				if (flag){
					squidLinkResult = squidLinkDao.getObjectById(squidLink.getId(), SquidLink.class);
				}
			}
			updateMap.put("updSquidLink", squidLinkResult);
			
			// 修改后的column、transformation信息
			SquidJoinProcess squidJoinProcess = new SquidJoinProcess(TokenUtil.getToken());
			StageSquid stageSquid = squidDao.getObjectById(squidJoin.getJoined_squid_id(), StageSquid.class);
			updateMap.putAll(squidJoinProcess.link2Transformation(squidLink, adapter3, stageSquid));
		} catch (Exception e) {
			logger.error("[删除updateSquidJoin=========================================exception]", e);
			try {
				adapter3.rollback();
			} catch (SQLException e1) {
				logger.error("rollback err!", e1);
			}
			logger.error(MessageFormat.format("删除updateSquidJoin异常  id={0}",
					squidJoin.getTarget_squid_id()), e);
			out.setMessageCode(MessageCode.SQL_ERROR);
		} finally {
			adapter3.closeSession();
		}
		// 设置新的target_squid_id
		// squidLink.setFrom_squid_id(squidJoin.getTarget_squid_id());
		return updateMap;
	}

	/**
	 * 更新join的顺序
	 * 
	 * @param info
	 * @return
	 * @author bo.dang
	 * @date 2014-4-9
	 */
	public Map<String, Object> updateJoinOrder(String info, ReturnValue out) {
		Map<String, Object> map = JsonUtil.toHashMap(info);
		List<SquidJoin> squidJoinList = JsonUtil.toGsonList(
				map.get("SquidJoins") + "", SquidJoin.class);
		Map<String, String> paramMap = new HashMap<String, String>();
		List<ReferenceColumn> refColumnTotalList = null;
		IRelationalDataManager adapter3 = null;
		if (null != squidJoinList && squidJoinList.size() > 0) {
			try {
				adapter3 = DataAdapterFactory.getDefaultDataManager();
				adapter3.openSession();
				
				// 初始化数据接口
				ISquidDao squidDao = new SquidDaoImpl(adapter3);
				ISquidJoinDao squidJoinDao = new SquidJoinDaoImpl(adapter3);
				IReferenceColumnDao refColumnDao = new ReferenceColumnDaoImpl(adapter3);
				
				refColumnTotalList = new ArrayList<ReferenceColumn>();
				for (SquidJoin squidJoin : squidJoinList) {
					paramMap.clear();
					Squid squid = squidDao.getObjectById(squidJoin.getTarget_squid_id(), 
							Squid.class);
					if (squid == null) {
						out.setMessageCode(MessageCode.NODATA);
						return null;
					}
					// 更新SquidJoin信息
					squidJoinDao.update(squidJoin);
					ReferenceColumnGroup refColumnGroup = refColumnDao.getRefColumnGroupBySquidId(squidJoin.getJoined_squid_id(), 
							squidJoin.getTarget_squid_id());
					if (refColumnGroup==null||refColumnGroup.getId()==0){
						continue;
					}
					refColumnGroup.setRelative_order(squidJoin.getPrior_join_id());
					// 更新ReferenceColumnGroup信息
					refColumnDao.update(refColumnGroup);
					List<ReferenceColumn> refColumnList = refColumnDao.getRefColumnListByGroupId(refColumnGroup.getId());
					if (refColumnList != null && refColumnList.size() > 0) {
						for (ReferenceColumn referenceColumn : refColumnList) {
							referenceColumn.setGroup_order(refColumnGroup.getRelative_order());
							referenceColumn.setGroup_name(squid.getName());
						}
						refColumnTotalList.addAll(refColumnList);
					}
					// join消息泡校验
					CommonConsts.addValidationTask(new SquidValidationTask(
							TokenUtil.getToken(), adapter3,
							MessageBubbleService.setMessageBubble(squidJoin
									.getJoined_squid_id(), squidJoin.getId(),
									squidJoin.getJoin_Condition(),
									MessageBubbleCode.ERROR_SQL_SYNTAX_JOIN
											.value())));
				}
			} catch (Exception e) {
				logger.error("[删除updateJoinOrder=========================================exception]", e);
				try {
					if (adapter3 != null) adapter3.rollback();
				} catch (SQLException e1) {
					logger.error("rollback err!", e1);
				}
				logger.error(MessageFormat.format("删除updateJoinOrder异常", 0), e);
				out.setMessageCode(MessageCode.SQL_ERROR);
				return null;
			} finally {
				if (adapter3 != null) adapter3.closeSession();
			}
		}
		Map<String, Object> infoMap = new HashMap<String, Object>();
		infoMap.put("updSourceColumns", refColumnTotalList);
		return infoMap;
	}

	/**
	 * 删除join的信息
	 * 
	 * @param info
	 * @return
	 * @author bo.dang
	 * @date 2014-4-4
	 */
	public Map<String, Object> deleteSquidJoin(String info) {
		SquidJoin squidJoin = JsonUtil.object2HashMap(info, SquidJoin.class);
		// 返回到Client的删除SquidJoin信息集合
		Map<String, Object> delSquidJoinMap = new HashMap<String, Object>();
		// 删除SquidLink
		// SquidLinkServicesub squidLinkService=new
		// SquidLinkServicesub(token,helper.getAdapter());
		IRelationalDataManager adapter3 = null;
		try {
			adapter3 = DataAdapterFactory.getDefaultDataManager();
			adapter3.openSession();
			delSquidJoinMap.putAll(deleteSquidJoinDetail(adapter3, squidJoin, out));
		} catch (Exception e) {
			logger.error("[删除deleteJoin=========================================exception]", e);
			try {
				adapter3.rollback();
			} catch (SQLException e1) {
				logger.error("rollback err!", e1);
			}
			out.setMessageCode(MessageCode.SQL_ERROR);
			return null;
		} finally {
			adapter3.closeSession();
		}
		return delSquidJoinMap;
	}

	/**
	 * 删除多个SquidJoin集合
	 * 
	 * @param info
	 * @param out
	 * @return
	 */
	public Map<String, Object> deleteJoins(String info, ReturnValue out) {
		Map<String, Object> outputMap = new HashMap<String, Object>();
		IRelationalDataManager adapter3 = null;
		List<Integer> tempSquidLinkIDs = new ArrayList<Integer>();
		List<Integer> tempSourceColumnIds = new ArrayList<Integer>();
		List<Integer> tempTransIds = new ArrayList<Integer>();
		List<Integer> tempTransLinks = new ArrayList<Integer>();
		SquidJoin tempSquidJoin = null;
		try {
			Map<String, Object> infoMap = JsonUtil.toHashMap(info);
			List<Integer> squidJoinIds = JsonUtil.toGsonList(
					infoMap.get("SquidJoinIDs") + "", Integer.class);
			adapter3 = DataAdapterFactory.getDefaultDataManager();
			adapter3.openSession();
			if (squidJoinIds != null && squidJoinIds.size() > 0) {
				ISquidJoinDao squidJoinDao = new SquidJoinDaoImpl(adapter3);
				for (Integer squidJoinId : squidJoinIds) {
					SquidJoin squidJoin = squidJoinDao.getObjectById(squidJoinId, SquidJoin.class);
					Map<String, Object> tempMap = deleteSquidJoinDetail(adapter3, squidJoin, out);
					if (tempMap != null) {
						int linkIds = Integer.parseInt(tempMap.get("DelSquidLinkId") + "");
						tempSquidLinkIDs.add(linkIds);
						Object souceColumnIds = tempMap.get("delSourceColumnIds");
						if (souceColumnIds != null) {
							tempSourceColumnIds.addAll((List<Integer>) souceColumnIds);
						}
						Object transIds = tempMap.get("delVTransformIds");
						if (transIds != null) {
							tempTransIds.addAll((List<Integer>) transIds);
						}
						Object transLinkIds = tempMap.get("delTransLinkIds");
						if (transLinkIds != null) {
							tempTransLinks.addAll((List<Integer>) transLinkIds);
						}
						Object updSquidJoin = tempMap.get("updSquidJoin");
						if (updSquidJoin != null) {
							tempSquidJoin = (SquidJoin) updSquidJoin;
						}
						CommonConsts.addValidationTask(new SquidValidationTask(
								TokenUtil.getToken(), adapter3,
								MessageBubbleService.setMessageBubble(squidJoin
										.getJoined_squid_id(), squidJoin
										.getId(),
										squidJoin.getJoin_Condition(),
										MessageBubbleCode.ERROR_SQL_SYNTAX_JOIN
												.value())));
					}
				}
				outputMap.put("DelSquidLinkIds", tempSquidLinkIDs);
				outputMap.put("delSourceColumnIds", tempSourceColumnIds);
				outputMap.put("delVTransformIds", tempTransIds);
				outputMap.put("delTransLinkIds", tempTransLinks);
				outputMap.put("updSquidJoin", tempSquidJoin);
			}
			return outputMap;
		} catch (Exception e) {
			logger.error("[删除deleteJoins=========================================exception]", e);
			try {
				adapter3.rollback();
			} catch (SQLException e1) {
				logger.error("rollback err!", e1);
			}
			out.setMessageCode(MessageCode.SQL_ERROR);
			return null;
		} finally {
			adapter3.closeSession();
		}
	}

	/**
	 * 更新Join基础信息，如：joinType,condition等 joinType由 join 变更
	 * unoin或unoinAll时，需要联动其他相关SquidJoin
	 * 
	 * @param info
	 * @param out
	 * @return
	 */
	public Map<String, Object> updateJoinForTypeCond(String info,
			ReturnValue out) {
		Map<String, Object> outputMap = new HashMap<String, Object>();
		IRelationalDataManager adapter3 = null;
		DataAdapterFactory adapterFactory = null;
		try {
			SquidJoin upSquidJoin = JsonUtil.object2HashMap(info, SquidJoin.class);
			//adapterFactory = DataAdapterFactory.newInstance();
			adapter3 = DataAdapterFactory.getDefaultDataManager();
			adapter3.openSession();
			ISquidJoinDao squidJoinDao = new SquidJoinDaoImpl(adapter3);
			if (upSquidJoin != null) {
				SquidJoin squidJoin = squidJoinDao.getObjectById(upSquidJoin.getId(), 
						SquidJoin.class);
				if (squidJoin != null
						&& ((squidJoin.getJoinType() <= 5 && upSquidJoin
								.getJoinType() >= 6) || (squidJoin
								.getJoinType() >= 6 && upSquidJoin
								.getJoinType() <= 5))) {
					/*
					 * parmsMap.clear(); parmsMap.put("join_squid_id",
					 * upSquidJoin.getJoined_squid_id()); List<SquidJoin>
					 * listJoin = adapter3.query2List2(true, parmsMap,
					 * SquidJoin.class); for (SquidJoin join : listJoin) { if
					 * (join.getJoinType()!=JoinType.BaseTable.value()){
					 * join.setJoinType(upSquidJoin.getJoinType());
					 * adapter3.update2(join); } }
					 */
				}
				squidJoinDao.update(upSquidJoin);
				CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(), adapter3,
								MessageBubbleService.setMessageBubble(
										upSquidJoin.getJoined_squid_id(),
										upSquidJoin.getId(), upSquidJoin
												.getJoin_Condition(),
										MessageBubbleCode.ERROR_SQL_SYNTAX_JOIN.value())));
				return outputMap;
			} else {
				out.setMessageCode(MessageCode.NODATA);
			}
		} catch (Exception e) {
			logger.error("[删除deleteJoins=========================================exception]", e);
			try {
				adapter3.rollback();
			} catch (SQLException e1) {
				logger.error("rollback err!", e1);
			}
			out.setMessageCode(MessageCode.SQL_ERROR);
		} finally {
			adapter3.closeSession();
		}
		return null;
	}
}
