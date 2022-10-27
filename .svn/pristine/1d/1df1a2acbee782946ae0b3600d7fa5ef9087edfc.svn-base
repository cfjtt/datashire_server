package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.dao.ISquidFlowStatusDao;
import com.eurlanda.datashire.dao.impl.SquidFlowStatusDaoImpl;
import com.eurlanda.datashire.entity.SquidFlowStatus;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.server.utils.SocketUtil;
import com.eurlanda.datashire.server.utils.TokenUtil;
import com.eurlanda.datashire.sprint7.packet.PushMessagePacket;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
/**
 * SquidFlow加锁解锁业务处理
 * @author lei.bin
 *
 */
public class LockSquidFlowProcess {
	private static Logger logger = Logger.getLogger(LockSquidFlowProcess.class);// 记录日志
	private String token;
	public LockSquidFlowProcess()
	{
		
	}
	public LockSquidFlowProcess(String token)
	{
		this.token=token;
	}
	  /**
     * 对squidFlow进行加锁
     * @return
	 * @throws Exception 
     */
    public void getLockOnSquidFlow(int squid_flow_id,int project_id,int repository_id,int squid_flow_status,ReturnValue out,IRelationalDataManager adapter) throws Exception
    {
		IRelationalDataManager newAdapter=null;
		boolean closeSession=false;
		try {
			// 实例化相关的数据库处理类
			if(null!=adapter)
			{
			   newAdapter=adapter;
			}else
			{
				newAdapter = DataAdapterFactory.getDefaultDataManager();
				newAdapter.openSession();
				closeSession=true;
			}
			ISquidFlowStatusDao squidFlowStatusDao=new SquidFlowStatusDaoImpl(newAdapter);
			SquidFlowStatus status = squidFlowStatusDao.getOneSquidFlowStatus(repository_id, squid_flow_id);
			if(null!=status && 0!=status.getId())//&&(status.getSquid_flow_status()==0)
			{
				status.setSquid_flow_status(squid_flow_status);
			    boolean updateflag=	squidFlowStatusDao.update(status);
			}else
			{
				SquidFlowStatus flowStatus=new SquidFlowStatus();
				flowStatus.setProject_id(project_id);
				flowStatus.setRepository_id(repository_id);
				flowStatus.setSquid_flow_id(squid_flow_id);
				flowStatus.setSquid_flow_status(squid_flow_status);
				flowStatus.setOwner_client_token(TokenUtil.getToken());
				int executeResult=squidFlowStatusDao.insert2(flowStatus);
			}
			this.sendAllClientWithSelf(repository_id,squid_flow_id,squid_flow_status);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
			out.setMessageCode(MessageCode.ERR_ISSQUIDFLOWLOCKED);
			try {
				newAdapter.rollback();
			} catch (SQLException e1) {
				logger.error("rollback err!", e1);
			}
			throw new Exception("加锁仓库id："+repository_id+", squidflowId: "+squid_flow_id+"异常");
		}finally {
			if(closeSession)
			{
				newAdapter.closeSession();
			}
		}
		
    }
    /**
     * 根据repositoryId和projectId获取所有squidflowstatus集合
     * @return
     * @throws Exception 
     */
    public List<SquidFlowStatus> getSquidFlowStatus(int repositoryId,int projectId,ReturnValue out,IRelationalDataManager adapter) throws Exception
    {
		List<SquidFlowStatus> squidFlowStatus=null;
		try {
			ISquidFlowStatusDao squidFlowStatusDao=new SquidFlowStatusDaoImpl(adapter);
			squidFlowStatus= squidFlowStatusDao.getSquidFlowStatusByProject(repositoryId, projectId);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
			out.setMessageCode(MessageCode.ERR_GETSQUIDFLOWSTATUS);
			throw new Exception("获取仓库id："+repositoryId+", projectid ："+projectId+"错误");
		}
		return squidFlowStatus;
    }
    /**
     * 根据repositoryId和squidflowId获取所有squidflowstatus集合
     * @return
     * @throws Exception 
     */
    public List<SquidFlowStatus> getSquidFlowStatus2(int repositoryId,int squidflowId,ReturnValue out,IRelationalDataManager adapter) throws Exception
    {
		List<SquidFlowStatus> squidFlowStatus=new ArrayList<SquidFlowStatus>();
		try {
			ISquidFlowStatusDao squidFlowStatusDao=new SquidFlowStatusDaoImpl(adapter);
			squidFlowStatus= squidFlowStatusDao.getSquidFlowStatusBySquidFlow(repositoryId, squidflowId);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
			out.setMessageCode(MessageCode.ERR_GETSQUIDFLOWSTATUS);
			try {
				adapter.rollback();
			} catch (SQLException e1) {
				logger.error("rollback err!", e1);
			}
			throw new Exception("获取仓库id："+repositoryId+", squidflowId ："+squidflowId+"错误");
		}
		return squidFlowStatus;
    }
	/**
	 * 询问squidFlow是否加锁
	 * 
	 * @return
	 */
	public Map<String, Object> getStatus(String info, ReturnValue out) {
		IRelationalDataManager adapter = DataAdapterFactory.getDefaultDataManager();
		Map<String, Object> outputMap = new HashMap<String, Object>();
		int  status = 0;
		try {
			Map<String, Object> map = JsonUtil.toHashMap(info);
			int repositoryId = Integer.parseInt(map.get("RepositoryId")
					.toString());
			int squidFlowId = Integer.parseInt(map.get("SquidFlowId")
					.toString());
			adapter.openSession();
			ISquidFlowStatusDao squidFlowStatusDao=new SquidFlowStatusDaoImpl(adapter);
			List<SquidFlowStatus> statusList = squidFlowStatusDao.getSquidFlowStatusBySquidFlow(repositoryId, squidFlowId);
			if (null != statusList && statusList.size() > 0 ) {
				status =statusList.get(0).getSquid_flow_status() ;
			}
		} catch (Exception e) {
			// TODO: handle exception
			out.setMessageCode(MessageCode.ERR_ISSQUIDFLOWLOCKED);
			try {
				adapter.rollback();
			} catch (SQLException e1) {
				logger.error("rollback err!", e1);
			}
		} finally {
			adapter.closeSession();
		}
		outputMap.put("Status", status);
		return outputMap;
	}
	/**
     * 解锁squidFlow(分为管理员主动解锁，和关闭窗口解锁)
     * @return
     */
    public void unLockSquidFlow(String info,ReturnValue out)
    {
		IRelationalDataManager adapter = DataAdapterFactory.getDefaultDataManager();
		int executResult=0;
		try {
			Map<String, Object> map = JsonUtil.toHashMap(info);
			int repositoryId = Integer.parseInt(map.get("RepositoryId")
					.toString());
			int squidFlowId = Integer.parseInt(map.get("SquidFlowId")
					.toString());
			adapter.openSession();
			ISquidFlowStatusDao squidFlowStatusDao=new SquidFlowStatusDaoImpl(adapter);
			executResult=squidFlowStatusDao.updateSquidFlowStatus(repositoryId, squidFlowId);
			if(executResult<=0)
			{
				logger.error("====================管理员解锁仓库id："+repositoryId+"  squidflowId:"+squidFlowId+"失败======================");
				out.setMessageCode(MessageCode.ERR_UNLOCKSQUIDFLOW);
			}else
			{
				//给所有客户端推送解锁的消息
				/*outputMap.put("IsLocked", 0);
				outputMap.put("RepositoryId", repositoryId);
				outputMap.put("SquidFlowId", squidFlowId);*/
				/*MessagePacket packet=new MessagePacket();
				packet.setCommandId("9001");
				packet.setChildCommandId("9002");
				packet.setData("123".getBytes());*/
				//packet.setGuid();
				//ChannelService.sendToAllClients(packet, token);
				//除开本token的客户端
				//调用push推送方法
				this.sendAllClient(repositoryId,squidFlowId,0);
			}
			
		} catch (Exception e) {
			// TODO: handle exception
		    e.printStackTrace();
			out.setMessageCode(MessageCode.ERR_ISSQUIDFLOWLOCKED);
			try {
				adapter.rollback();
			} catch (SQLException e1) {
				logger.error("rollback err!", e1);
			}
		} finally {
			adapter.closeSession();
		}
    }
    /**
     * 向除开本身的客户端推送加解锁的信息
     * @throws SQLException
     */
    public void sendAllClient(int repositoryId,int squidFlowId,int lockType) throws SQLException {
    	Map<String, Object> outputMap = new HashMap<String, Object>();
    	outputMap.put("LockedStatus", lockType);
		outputMap.put("RepositoryId", repositoryId);
		outputMap.put("SquidFlowId", squidFlowId);
		for (String oneToken: SocketUtil.TOKEN2CHANNEL.keySet()) {
			if (org.apache.commons.lang.StringUtils.isNotBlank(TokenUtil.getToken())
					&& !oneToken.equals(TokenUtil.getToken())) {
				PushMessagePacket.pushMap(outputMap, DSObjectType.SQUID_FLOW, "1017", "9002", com.eurlanda.datashire.utility.StringUtils.generateGUID(), oneToken);
				logger.info(">>>>当前客户端为" + token + "<<<<<<===================向客户端   " + oneToken + "  发送消息=====================");
			} else {
				logger.info("===================客户端  " + token + "自己加解锁,客户端总数为: " + SocketUtil.TOKEN2CHANNEL.size() + " =====================");
			}
		}
    }

	/**
	 * 向所有的客户端推送消息(包括自己)
	 * @param repositoryId
	 * @param squidFlowId
	 * @param lockType
	 */
    public void sendAllClientWithSelf(int repositoryId,int squidFlowId,int lockType){
		Map<String, Object> outputMap = new HashMap<String, Object>();
		outputMap.put("LockedStatus", lockType);
		outputMap.put("RepositoryId", repositoryId);
		outputMap.put("SquidFlowId", squidFlowId);
		for (String oneToken: SocketUtil.TOKEN2CHANNEL.keySet()) {
			if (org.apache.commons.lang.StringUtils.isNotBlank(TokenUtil.getToken())) {
				PushMessagePacket.pushMap(outputMap, DSObjectType.SQUID_FLOW, "1017", "9002", com.eurlanda.datashire.utility.StringUtils.generateGUID(), oneToken);
				logger.info(">>>>当前客户端为" + token + "<<<<<<===================向客户端   " + oneToken + "  发送消息=====================");
			}
		}
	}
}
