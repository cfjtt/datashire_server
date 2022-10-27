package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.dao.ISquidFlowStatusDao;
import com.eurlanda.datashire.dao.impl.SquidFlowStatusDaoImpl;
import com.eurlanda.datashire.entity.SquidFlow;
import com.eurlanda.datashire.entity.SquidFlowStatus;
import com.eurlanda.datashire.entity.operation.DataEntity;
import com.eurlanda.datashire.entity.operation.WhereCondition;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.enumeration.DataStatusEnum;
import com.eurlanda.datashire.enumeration.MatchTypeEnum;
import com.eurlanda.datashire.enumeration.SystemTableEnum;
import com.eurlanda.datashire.sprint7.packet.InfoPacket;
import com.eurlanda.datashire.sprint7.plug.GetParam;
import com.eurlanda.datashire.sprint7.plug.SquidFlowPlug;
import com.eurlanda.datashire.sprint7.plug.SupportPlug;
import com.eurlanda.datashire.utility.CalcSquidFlowStatus;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import com.mysql.jdbc.exceptions.jdbc4.MySQLIntegrityConstraintViolationException;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
/**
 * SquidFlow业务处理类
 * 
 * Description: 
 * Author :赵春花 2013-10-28
 * Department :  JAVA后端研发部
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司
 */
public class SquidFlowService extends SupportPlug implements ISquidFlowService{
	static Logger logger = Logger.getLogger(DataShirService.class);// 记录日志
	private String token; //令牌
	public SquidFlowService(String token) {
		super(token);
		this.token = token;
	}
	

	/**
	 * 创建SquidFLow对象集合
	 * 作用描述：
	 * 1、根据传入的SquidFLow对象集合创建SquidFLow对象
	 * 2、创建成功——根据SquidFLow集合对象里的Key查询相对应的ID组装返回infopacket对象集合返回
	 * 验证：
	 * 1、SquidFLow对象集合不能为空且必须有数据
	 * 2、SquidFLow对象集合里的每个Key必须有值
	 * 修改说明：
	 *@param squidFlows squidFlow对象集合
	 *@param out 异常返回
	 *@return
	 */
	public List<InfoPacket> createSquidFLows(List<SquidFlow> squidFlows,ReturnValue out){
		logger.debug(String.format("createSquidFLows-SquidFLowList.size()=%s", squidFlows.size()));
		SquidFlowPlug squidFlowPlug = new SquidFlowPlug(adapter);
		boolean create = false;
		//定义接收返回结果集
		List<InfoPacket> infoPackets = new ArrayList<InfoPacket>();
		try {
			create = squidFlowPlug.createSquidFLows(squidFlows, out);
			//新增成功
			if(create){
				//根据Key查询出新增的ID
				for (SquidFlow squidFlow : squidFlows) {
					//获得Key
					String key = squidFlow.getKey();
					//根据Key获得ID
					int id = squidFlowPlug.getSquidFlowId(key, out);
					InfoPacket infoPacket = new InfoPacket();
					infoPacket.setId(id);
					infoPacket.setKey(key);
					infoPacket.setType(DSObjectType.SQUID_FLOW);
					infoPackets.add(infoPacket);
				}
			}
			logger.debug(String.format("createSquidFLows-return=%s",infoPackets ));
		} catch (Exception e) {
			out.setMessageCode(MessageCode.ERR_ARRAYS);
		}finally{
			//释放连接
			adapter.commitAdapter();
		}
		
		return infoPackets;
		
	}
	
	/**
	 * 修改SquidFlow
	 * 作用描述：
	 * 修改squidFlows对象集合
	 * 调用方法前确保squidFlows对象集合不为空，并且squidFlows对象集合里的每个squidFlows对象的Id不为空
	 * 修改说明：
	 *@param squidFlows squidFlow集合对象
	 *@param out 异常返回
	 *@return
	 */
	public List<InfoPacket> updateSquidFlows(List<SquidFlow> squidFlows,ReturnValue out){
		logger.debug(String.format("updateSquidFlows-squidFlowList.size()=%s", squidFlows.size()));
		List<InfoPacket> infoPackets = new ArrayList<InfoPacket>();
		boolean create = false;
		try {
			//封装批量新增数据集
			List<List<DataEntity>>  paramList = new ArrayList<List<DataEntity>>();
			//封装条件集合
			List<List<WhereCondition>>  whereCondList = new ArrayList<List<WhereCondition>>();
			//调用转换类
			GetParam getParam = new GetParam();
			for (SquidFlow squiFlow : squidFlows) {
				//参数
				List<DataEntity> dataEntitys =  new ArrayList<DataEntity>();
				getParam.getSquidFlow(squiFlow, dataEntitys);
				paramList.add(dataEntitys);
				//条件
				List<WhereCondition> whereConditions = new ArrayList<WhereCondition>();
				WhereCondition whereCondition = new WhereCondition();
				whereCondition.setAttributeName("ID");
				whereCondition.setDataType(DataStatusEnum.INT);
				whereCondition.setMatchType(MatchTypeEnum.EQ);
				whereCondition.setValue(squiFlow.getId());
				whereConditions.add(whereCondition);
				whereCondList.add(whereConditions);
				
			}
			//调用批量修改
			create = adapter.updatBatch(SystemTableEnum.DS_SQUID_FLOW.toString(), paramList, whereCondList, out);
			logger.debug(String.format("updateSquidFlows-return=%s",create ));
			
			//查询返回结果
			if(create){
				SquidFlowPlug  squidFlowPlug = new SquidFlowPlug(adapter);
				//定义接收返回结果集
				infoPackets = new ArrayList<InfoPacket>();
				//根据Key查询出新增的ID
				for (SquidFlow  squidFlow : squidFlows) {
					//获得Key
					String key = squidFlow.getKey();
					int id = squidFlowPlug.getSquidFlowId(key, out);
					InfoPacket infoPacket = new InfoPacket();
					infoPacket.setCode(1);
					infoPacket.setId(id);
					infoPacket.setType(DSObjectType.SQUID_FLOW);
					infoPacket.setKey(key);
					infoPackets.add(infoPacket);
				}
			}
		} catch (Exception e) {
			logger.error("updateSquidFlows-return=%s",e);
			out.setMessageCode(MessageCode.ERR_ARRAYS);
		}finally{
			//释放
			adapter.commitAdapter();
		}
		
		return infoPackets;
	}
	
	/**
	 * 根据ProjectID查询SquidFlow集合
	 * 作用描述：
	 * 根据ProjectId查询相应下的SquidFlow信息
	 * 修改说明：
	 *@param id projectID
	 *@param out 异常返回
	 */
	public List<SquidFlow> querySquidFlows(int projectId,int repositoryId, ReturnValue out) {
		logger.debug(String.format("querySquidFlows-id=%s", projectId));
		DataAdapterFactory adapterFactory=DataAdapterFactory.newInstance();
		IRelationalDataManager adapter=DataAdapterFactory.getDefaultDataManager();
		SquidFlowPlug squidFlowPlug = new SquidFlowPlug(adapter);
		List<SquidFlow> squidFlows = null;
		try {
			adapter.openSession();
			squidFlows = squidFlowPlug.getSquidFlows(projectId, out,adapter);
			//获取所有squidflow的状态
			LockSquidFlowProcess lockSquidFlowProcess=new LockSquidFlowProcess();
			List<SquidFlowStatus> squidFlowStatus=lockSquidFlowProcess.getSquidFlowStatus(repositoryId, projectId, out,adapter);
			//根据squidflow的id匹配得到状态
			for(int i=0;i<squidFlows.size();i++)
			{
				for(int j=0;j<squidFlowStatus.size();j++)
				{
					if(squidFlows.get(i).getId()==squidFlowStatus.get(j).getSquid_flow_id())
					{
						squidFlows.get(i).setSquid_flow_status(squidFlowStatus.get(j).getSquid_flow_status());
						break;
					}
				}
			}
		} catch (Exception e) {
			logger.error("querySquidFlows-return=%s" + e);
		} finally {
			adapter.closeSession();
		}
		return squidFlows;
	}
	
	/**
	 * 更新的squidFlow的名称
	 * @param info
	 * @param out
	 * @return
	 */
	public Map<String, Object> updSquidFlow(String info, ReturnValue out){
		Map<String, Object> outputMap = new HashMap<String, Object>();
		IRelationalDataManager adapter3 = DataAdapterFactory.getDefaultDataManager();
		adapter3.openSession();
		boolean flag = false;
		try {
			Map<String, Object> parmsMap = JsonUtil.toHashMap(info);
			SquidFlow flow = JsonUtil.toGsonBean((parmsMap.get("SquidFlow")+""), SquidFlow.class);
			int repository_id=Integer.parseInt(parmsMap.get("RepositoryId").toString());
			//切换到系统数据库
			ISquidFlowStatusDao squidFlowStatusDao = new SquidFlowStatusDaoImpl(adapter3);
			List<SquidFlowStatus> statusList = squidFlowStatusDao.getSquidFlowStatusBySquidFlow(repository_id, flow.getId());
			Map<String, Object> map = CalcSquidFlowStatus.calcStatus(statusList);
			if(Integer.parseInt(map.get("schedule").toString())>0
					||Integer.parseInt(map.get("checkout").toString())>0){
				out.setMessageCode(MessageCode.WARN_UPDATESQUIDFLOW);
				return null;
			}else{
				if (flow!=null){
					SquidFlow squidFlow = squidFlowStatusDao.getObjectById(flow.getId(), SquidFlow.class);

					if (squidFlow!=null){
						flow.setModification_date(new Timestamp(new Date().getTime()).toString());
						flag = squidFlowStatusDao.update(flow);
						if (flag){
							return outputMap;
						}else{
							out.setMessageCode(MessageCode.SQL_ERROR);
							return null;
						}
					}else{
						out.setMessageCode(MessageCode.ERR_DS_SQUID_FLOW_NULL);
						return null;
					}
				} 
			}
		} catch (Exception e) {
			logger.error("[获取updSquidFlowName=========================================exception]", e);
			try {
				adapter3.rollback();
			} catch (SQLException e1) {
				logger.error("rollback err!", e1);
			}
			out.setMessageCode(MessageCode.SQL_ERROR);
			if(e instanceof MySQLIntegrityConstraintViolationException){
				out.setMessageCode(MessageCode.ERR_SQUIDFLOW_EXISTS_NAME);
			}
		} finally {
			adapter3.closeSession();
		}
		return null;
	}
	
	/**
	 * 更新SquidFlow的编译状态
	 * @param squidFlowId
	 * @author bo.dang
	 * @date 2014年6月20日
	 */
    public Boolean updateSquidFlowCompilationStatus(int squidFlowId, ReturnValue out){
          String sql = "update DS_SQUID_FLOW set compilation_status = 1 where id = " + squidFlowId;
          Boolean updateFlag = false;
          try {
                if(adapter.executeSQL(sql, out)>0){
                    updateFlag = true;
                }
            } catch (Exception e) {
                logger.error("[获取updSquidFlowName=========================================exception]", e);
                try {
                    adapter.rollback();
                } catch (SQLException e1) {
                    logger.error("rollback err!", e1);
                }
                out.setMessageCode(MessageCode.SQL_ERROR);
            } finally {
                adapter.commitAdapter();
            }
          return updateFlag;
    }
}

