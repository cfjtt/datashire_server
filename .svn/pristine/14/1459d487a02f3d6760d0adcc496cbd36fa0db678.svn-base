package com.eurlanda.datashire.sprint7.service.squidflow;

import cn.com.jsoft.jframe.utils.ValidateUtils;
import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.entity.*;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.enumeration.MessageBubbleCode;
import com.eurlanda.datashire.enumeration.TransformationTypeEnum;
import com.eurlanda.datashire.enumeration.datatype.SystemDatatype;
import com.eurlanda.datashire.sprint7.packet.PushMessagePacket;
import com.eurlanda.datashire.sprint7.plug.JoinPlug;
import com.eurlanda.datashire.sprint7.plug.SquidLinkPlug;
import com.eurlanda.datashire.sprint7.plug.SquidPlug;
import com.eurlanda.datashire.sprint7.plug.SupportPlug;
import com.eurlanda.datashire.sprint7.service.squidflow.subservice.StageSquidService;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import com.eurlanda.datashire.utility.StringUtils;
import com.eurlanda.datashire.validator.SQLValidator;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 对join操作的业务处理类，在datashireService中调用此方法 继承SupportPlug获取adapter连接池对象
 * 
 * @version 1.0
 * @author lei.bin
 * @created 2013-10-21
 */
public class JoinProcess extends SupportPlug implements IJoinProcess {
	/**
	 * 记录JoinProcess日志
	 */
	static Logger logger = Logger.getLogger(JoinProcess.class);

	private String key;
	
	public JoinProcess(String token) {
		super(token);
	}
	
	public JoinProcess(String token, String key) {
		super(token);
		this.key = key;
	}

	/**
	 * 根据前端传送的信息创建一个SquidJoin对象，并且返回结果
	 * 
	 * @param info
	 *            输入信息
	 * @param out
	 *            返回结果代码
	 * @return List<SpecialSquidJoin>
	 * @author lei.bin
	 */
	public List<SpecialSquidJoin> createSquidJoin(String info, ReturnValue out) {
		List<SquidJoin> JoinInput = new ArrayList<SquidJoin>();
		JoinPlug joinPlug = new JoinPlug(adapter);
		boolean createJoin = false;// join表是否创建成功
		// boolean createLink=false;//Link表是否创建成功
		List<SpecialSquidJoin> squidJoinOutput = new ArrayList<SpecialSquidJoin>();
		SpecialSquidJoin specialSquidJoinOut = new SpecialSquidJoin();
		SquidJoin joinOut = new SquidJoin();
		// SquidLink squidLinkOut=new SquidLink();
		// StageSquid stageSquidOut=new StageSquid();
		// RelationalDataManager adapter2 =
		// adapterFactory.getDataManagerByToken(token);
		try {
			JoinInput = JsonUtil.toGsonList(info, SquidJoin.class);// 入参转换为SquidJoin对象
			logger.debug("解析后的JoinInput为===" + JoinInput);
			if (null != JoinInput && JoinInput.size() > 0) {
				// SquidLinkPlug squidLinkPlug=new SquidLinkPlug(adapter);
				for (int i = 0; i < JoinInput.size(); i++) {
					// 对join表进行新增
					createJoin = joinPlug.createJoin(JoinInput.get(i), out);
					if (createJoin) {
						// 把插入成功的join信息根据key查询出来
						joinOut = joinPlug.getJoinByKey(JoinInput.get(i).getKey(), out);
						logger.debug("查询出来的joinOut==" + joinOut.getId());
						/*
						 * // 创建SquidLink // 前端传送的target_id小于0的时候,不创建squidlink
						 * if (JoinInput.get(i).getTarget_squid_id() > 0) { //
						 * 对squidLink类的转换 SquidLink squidLink = this.joinTOLink(
						 * JoinInput.get(i), out);
						 * logger.debug("=====创建squidlink====="); createLink =
						 * squidLinkPlug.createSquidLink( squidLink, out); }
						 */
					}
					/*
					 * if(createLink) { //把插入成功的squidLink信息根据from_squid_id
					 * 和to_squid_id查询出来
					 * squidLinkOut=squidLinkPlug.getSquidLinkByJoin
					 * (JoinInput.get(i), out);
					 * logger.debug("squidLinkOut===="+squidLinkOut);
					 * //stageSquidOut
					 * =joinPlug.getStageSquidData(JoinInput.get(i), out); }
					 */
					// stageSquidOut.setId(JoinInput.get(i).getJoined_squid_id());
					// StageSquidService.setStageSquidData(token, false,
					// adapter2, stageSquidOut);
					specialSquidJoinOut.setSquidJoin(joinOut);
					// 封装返回信息
					// specialSquidJoinOut.setSquidLink(squidLinkOut);
					// specialSquidJoinOut.setStageSquid(stageSquidOut);
					squidJoinOutput.add(specialSquidJoinOut);
				}
			} else {
				out.setMessageCode(MessageCode.NODATA);
				return null;
			}
		} catch (Exception e) {
			logger.error("createSquidJoin is error", e);
			out.setMessageCode(MessageCode.ERR_CREATEJOIN);
		} finally {
			adapter.commitAdapter();
		}
		return squidJoinOutput;
	}

	/**
	 * 根据前端传送的信息更新SquidJoin对象
	 * 
	 * @param info
	 *            输入信息
	 * @param out
	 *            返回结果代码
	 * @return List<SpecialSquidJoin>
	 * @author lei.bin
	 */
	public List<SpecialSquidJoin> updateSquidJoin(String info, ReturnValue out) {
		List<SquidJoin> JoinInput = new ArrayList<SquidJoin>();
		List<SpecialSquidJoin> squidJoinOutput = new ArrayList<SpecialSquidJoin>();
		SpecialSquidJoin specialSquidJoinOut = new SpecialSquidJoin();
		boolean updateJoin = false;// join表是否更新成功
		int updateSquidLink = 0;// squidLink表是否更新成功
		boolean createLink = false;// Link表是否创建成功
		JoinPlug joinPlug = new JoinPlug(adapter);
		SquidLinkPlug linkPlug = new SquidLinkPlug(adapter);
		SquidJoin joinOut = new SquidJoin();// join返回对象
		SquidLink squidLinkOut = new SquidLink();// link返回对象
		StageSquid stageSquidOut = new StageSquid();// stageSquid返回对象
		SquidLinkPlug squidLinkPlug = new SquidLinkPlug(adapter);
		SquidPlug squidPlug = new SquidPlug(adapter);
		IRelationalDataManager adapter2 = DataAdapterFactory.getDefaultDataManager();
		adapter2.openSession();
		try {
			JoinInput = JsonUtil.toGsonList(info, SquidJoin.class);// 入参转换为SquidJoin对象
			if (null != JoinInput && JoinInput.size() > 0) {
				for (int i = 0; i < JoinInput.size(); i++) {
					// 根据target_join_id和id去查询join表
					List<SquidJoin> joins = joinPlug.queryJoinsById(JoinInput.get(i), out);
					logger.debug("join的长度===" + joins.size());
					if (null != joins && joins.size() > 0) {// 有记录,则只更新join表，其余的表返回为空就可以
						updateJoin = joinPlug.updateJoin(JoinInput.get(i), out);
						if (updateJoin) {
							// 如果更新成功，组织join对象返回
							joinOut = joinPlug.getJoinById(JoinInput.get(i).getId(), out);
							logger.debug("joinOut=====" + joinOut);
						}
						// this.link2Transformation(squidLinkOut,adapter2,stageSquidOut);
						stageSquidOut.setId(JoinInput.get(i).getJoined_squid_id());
						StageSquidService.setStageSquidData(adapter2, stageSquidOut);
						// 删除link相关的所有数据.
						List<ReferenceColumn> sourceColumnlist = adapter2.query2List(true, "select nvl(c.id,r.column_id) id,r.host_squid_id squid_id,r.*, nvl2(c.id, 'N','Y') host_column_deleted,g.relative_order group_order from"
								+ " DS_COLUMN c right join DS_REFERENCE_COLUMN r on r.column_id=c.id"
								+ " left join DS_REFERENCE_COLUMN_GROUP g on r.group_id=g.id"
								+ " where reference_squid_id="+JoinInput.get(i).getTarget_squid_id(), null, ReferenceColumn.class);
						stageSquidOut.setSourceColumns(sourceColumnlist);
					} else if (joins.size() == 0) {// 没有记录,则需要更新join,和squidlink表
						// 如果根据join_key 查询出的target_id<0,那么需要新增squidlink(第一次更新)
						SquidJoin squidJoin = joinPlug.getJoinByKey(JoinInput.get(i).getKey(), out);
						if (squidJoin.getTarget_squid_id() < 0) {
							// 新增squidlink
							SquidLink squidLink = this.joinTOLink(squidPlug, JoinInput.get(i), out);
							logger.debug("=====创建squidlink=====");
							createLink = squidLinkPlug.createSquidLink(squidLink, out);
							if (createLink) {
								// 把插入成功的squidLink信息根据from_squid_id
								// 和to_squid_id查询出来
								squidLinkOut = squidLinkPlug.getSquidLinkByJoin(JoinInput.get(i), out);
								logger.debug("squidLinkOut====" + squidLinkOut);
								// 更新join表
								updateJoin = joinPlug.updateJoin(JoinInput.get(i), out);
								if (updateJoin) {
									// 如果更新成功，将join表的信息返回
									joinOut = joinPlug.getJoinById(JoinInput.get(i).getId(), out);
								}
								stageSquidOut.setId(JoinInput.get(i).getJoined_squid_id());
								stageSquidOut.setStatus(joinOut.getTarget_squid_id());//借用字段
								this.link2Transformation(squidLinkOut, adapter2, stageSquidOut);
								StageSquidService.setStageSquidData(adapter2, stageSquidOut);
							}
						} else {// 第N+1次更细

							// 先更新squidlink表
							updateSquidLink = linkPlug.updateSquidLinkBySql(JoinInput.get(i), out);
							if (updateSquidLink > 0) {
								// 如果更新成功，组织link对象返回
								squidLinkOut = linkPlug.getSquidLinkByJoin(JoinInput.get(i), out);
								// 更新join表
								updateJoin = joinPlug.updateJoin(JoinInput.get(i), out);
								if (updateJoin) {
									// 如果更新成功，将join表的信息返回
									joinOut = joinPlug.getJoinById(JoinInput.get(i).getId(), out);
								}
								// 组织stageSquid信息返回
								stageSquidOut.setId(JoinInput.get(i).getJoined_squid_id());
								stageSquidOut.setStatus(JoinInput.get(i).getTarget_squid_id());//借用字段
								this.link2Transformation(squidLinkOut,adapter2,stageSquidOut);
								StageSquidService.setStageSquidData(adapter2, stageSquidOut);
							}
						}
					}
					// 返回记录
					specialSquidJoinOut.setSquidJoin(joinOut);
					specialSquidJoinOut.setSquidLink(squidLinkOut);
					specialSquidJoinOut.setStageSquid(stageSquidOut);
					squidJoinOutput.add(specialSquidJoinOut);
				}
			}
		} catch (Exception e) {
			logger.error("updateSquidJoin is error", e);
			out.setMessageCode(MessageCode.ERR_UPDATEJOIN);
		} finally {
			adapter.commitAdapter();
			adapter2.closeSession();
		}
		return squidJoinOutput;
	}

	/**
	 * 对join的condition条件进行校验
	 * 
	 * @param sql
	 *            将前端传入的condition语句进行拼接后的sql语句
	 * @param out
	 * @return
	 */
	/*
	 * public int checkCondition(String sql, ReturnValue out) { int status =
	 * 1;// 初始化状态，校验成功 SQLParser p = new SQLParser(); try { //引用jar包封装好的校验方法
	 * StatementNode s = p.parseStatement(sql); } catch (StandardException e) {
	 * logger.error("checkCondition is error", e); status = 0;//校验不通过 } return
	 * status; }
	 */

	/**
	 * join对象到squidLink对象的转换
	 * 
	 * @param join
	 * @param out
	 * @return SquidLink
	 * @author lei.bin
	 */
	private SquidLink joinTOLink(SquidPlug squidPlug, SquidJoin join, ReturnValue out) {
		SquidLink squidLink = new SquidLink();
		squidLink.setKey(StringUtils.generateGUID());
		logger.debug("重新生成的guid========" + squidLink.getKey());
		// 根据TargetSquidId查询squid表，得到squid_flow_id
		Squid squid = squidPlug.getSquidById(join.getTarget_squid_id(), out);
		squidLink.setSquid_flow_id(squid.getSquidflow_id());
		squidLink.setFrom_squid_id(join.getTarget_squid_id());
		squidLink.setTo_squid_id(join.getJoined_squid_id());
		squidLink.setLine_type(1);
		return squidLink;
	}

	/**
	 * 创建link时,创建transformation的信息
	 * 
	 * @param squidAndSquidLink
	 * @return
	 */
	public void link2Transformation(SquidLink squidLink, IRelationalDataManager adapter2, StageSquid stageSquidOut) {
		squidLink.setType(DSObjectType.SQUIDLINK.value());

		int sourceSquidId = squidLink.getFrom_squid_id();
		int toSquidId = squidLink.getTo_squid_id();
		// Transformation面板中的目标列在创建squidLink或者join之后，导入了源squid的所有列，应该为空，由用户根据需要来导入。
		List<Column> toColumns = new ArrayList<Column>();
		Column newColumn = null;
		Map<String, String> paramMap = new HashMap<String, String>(1);

		try {
			adapter2.openSession();
			// 根据squidlink的to_id查询stagesquid对象
			paramMap.clear();
			paramMap.put("id", String.valueOf(toSquidId));
			List<Squid> stageSquid = adapter2.query2List(true, paramMap, Squid.class);
			logger.debug("stageSquid的长度为==" + stageSquid.size());
			stageSquidOut.setKey(stageSquid.get(0).getKey());
			logger.debug("stageSquid的key为==" + stageSquid.get(0).getKey());
			paramMap.clear();
			paramMap.put("squid_id", Integer.toString(sourceSquidId, 10));
			List<Column> sourceColumns = adapter2.query2List(true, paramMap, Column.class);
			if (sourceColumns == null || sourceColumns.isEmpty()) {
				logger.warn("columns of extract squid is null! sourceSquidId=" + sourceSquidId);
				return;
			}

			paramMap.clear();
			paramMap.put("squid_id", Integer.toString(toSquidId, 10));
			List<Column> columns = adapter2.query2List(true, paramMap, Column.class);
			int columnSize = 0;
			if (columns != null && !columns.isEmpty()) {
				columnSize = columns.size(); // 已存在目标列的个数
			}

			paramMap.clear();
			paramMap.put("reference_squid_id", Integer.toString(toSquidId, 10));
			List<ReferenceColumnGroup> rg = adapter2.query2List(true, paramMap, ReferenceColumnGroup.class);
			ReferenceColumnGroup columnGroup = new ReferenceColumnGroup();
			columnGroup.setKey(StringUtils.generateGUID());
			columnGroup.setReference_squid_id(toSquidId);
			columnGroup.setRelative_order(rg == null || rg.isEmpty() ? 1 : rg.size() + 1);
			columnGroup.setId(adapter2.insert2(columnGroup));

			for (int i = 0; i < sourceColumns.size(); i++) {
				Column column = sourceColumns.get(i);
				// 创建引用列
				ReferenceColumn ref = new ReferenceColumn();
				ref.setColumn_id(column.getId());
				ref.setId(ref.getColumn_id());
				ref.setCollation(column.getCollation());
				ref.setData_type(column.getData_type());
				ref.setKey(StringUtils.generateGUID());
				ref.setName(column.getName());
				ref.setNullable(column.isNullable());
				ref.setRelative_order(i + 1);
				ref.setSquid_id(sourceSquidId);
				ref.setType(DSObjectType.COLUMN.value());
				ref.setReference_squid_id(toSquidId);
				ref.setHost_squid_id(sourceSquidId);
				ref.setIs_referenced(true);
				ref.setGroup_id(columnGroup.getId());
				adapter2.insert2(ref);
			}

			List<MessageBubble> mbList = new ArrayList<MessageBubble>(2);
			if (columnSize <= 0) {
				// 自动增加以下 column id 类型为int 型，从0 每次加 1 自增
				newColumn = new Column();
				newColumn.setCollation(0);
				newColumn.setData_type(SystemDatatype.BIGINT.value());
				newColumn.setLength(0);
				newColumn.setName("id");
				newColumn.setNullable(true);
				newColumn.setPrecision(0);
				newColumn.setRelative_order(1);
				newColumn.setSquid_id(toSquidId);
				newColumn.setKey(StringUtils.generateGUID());
				newColumn.setId(adapter2.insert2(newColumn));
				toColumns.add(newColumn);
				// 创建新增列对应的虚拟变换
				Transformation newTrans = new Transformation();
				newTrans.setColumn_id(newColumn.getId());
				newTrans.setKey(StringUtils.generateGUID());
				newTrans.setSquid_id(toSquidId);
				newTrans.setTranstype(TransformationTypeEnum.VIRTUAL.value());
				newTrans.setOutput_data_type(newColumn.getData_type());
				newTrans.setId(adapter2.insert2(newTrans));
				// 目标列（左边列）没有参与变换
				mbList.add(new MessageBubble(stageSquid.get(0).getKey(), newColumn.getKey(), MessageBubbleCode.ERROR_COLUMN_NO_TRANSFORMATION.value(), false));
			} else {
				toColumns.addAll(columns);
			}
			// 消除stagesquid的消息泡
			mbList.add(new MessageBubble(stageSquid.get(0).getKey(), stageSquid.get(0).getKey(), MessageBubbleCode.WARN_SQUID_NO_LINK.value(), true));
			PushMessagePacket.push(mbList, token);

			stageSquidOut.setColumns(toColumns);
			// 获取所有相关数据（join、源/目标column、虚拟/真实transformation、transformation link）
			// setStageSquidData(token, true, adapter, stageSquid);
		} catch (Exception e) {
			logger.error("创建stage squid异常", e);
			try {
				adapter2.rollback();
			} catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
				logger.fatal("rollback err!", e1);
			}
		} finally {
			adapter2.closeSession();
		}
	}

	@Override
	public StageSquid getStageSquidData(SquidJoin join, ReturnValue out) {
		// TODO Auto-generated method stub
		return null;
	}
	
	/**
	 * 校验SquidJoin的 condition
	 * @param info
	 * @param out
	 * @return
	 */
	public Map<String, Object> getSquidJoinValidator(String info, ReturnValue out){
		try {
			List<SquidJoin> joins = JsonUtil.toGsonList(JsonUtil.toHashMap(info).get("Joins")+"", SquidJoin.class);
			if (joins!=null&&joins.size()>0){
				String sql = "select 1 from t1 where 1=1 ";
				for (SquidJoin squidJoin : joins) {
					if (!ValidateUtils.isEmpty(squidJoin.getJoin_Condition())){
						sql = sql + " and " + squidJoin.getJoin_Condition();
					}
					boolean flag = SQLValidator.isValidSQL(sql);
					Map<String, Object> map = new HashMap<String, Object>();
					map.put("Status", flag);
					map.put("Id", squidJoin.getId());
					PushMessagePacket.pushMap(map, DSObjectType.SQUIDJOIN, "1000", "0700", 
							key, token);
				}
			}else{
				out.setMessageCode(MessageCode.NODATA);
				return null;
			}
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
			out.setMessageCode(MessageCode.SQL_ERROR);
		}
		return null;
	}
}
