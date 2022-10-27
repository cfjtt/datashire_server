package com.eurlanda.datashire.sprint7.service.squidflow.subservice;

import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.entity.Project;
import com.eurlanda.datashire.entity.SquidFlow;
import com.eurlanda.datashire.sprint7.service.squidflow.AbstractRepositoryService;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
/**
 * 删除project处理类
 * @author lei.bin
 *
 */
public class ProjectServicesub extends AbstractRepositoryService implements IProjectService {
	Logger logger = Logger.getLogger(ProjectServicesub.class);// 记录日志
	public ProjectServicesub(String token) {
		super(token);
	}
	public ProjectServicesub(IRelationalDataManager adapter){
		super(adapter);
	}
	public ProjectServicesub(String token, IRelationalDataManager adapter){
		super(token, adapter);
	}
	/**
	 * 根据projectid删除project以及关联的子类
	 * @param id
	 * @param out
	 * @return
	 */
	public boolean deleteProject(int projectId,int repositoryId, ReturnValue out) {
		boolean delete = true;
		Map<String, String> paramMap = new HashMap<String, String>();
		boolean refreshFlag=false;//squidflow状态表是否有非normal数据
	/*	Map<String, Object> statusMap=null;
		List<SquidFlowStatus> statusList=null;
		*/
		try {
		/*	if (0 != repositoryId) {
				// 根据repositoryId和projectId去系统表查询squidflow的状态
				LockSquidFlowProcess lockSquidFlowProcess = new LockSquidFlowProcess();
				statusList = lockSquidFlowProcess.getSquidFlowStatus(repositoryId, projectId, out);
				statusMap = CalcSquidFlowStatus.calcStatus(statusList);
				if (Integer.parseInt(statusMap.get("schedule").toString()) > 0
						|| Integer.parseInt(statusMap.get("checkout").toString()) > 0) {
					refreshFlag = true;
				}
			}*/
			// 根据id查询出squidflow的集合
			paramMap.put("project_id", String.valueOf(projectId));
			List<SquidFlow> squidFlows = adapter.query2List(true, paramMap,SquidFlow.class);
			if (null != squidFlows && squidFlows.size() > 0) {
				SquidFlowServicesub flowService = new SquidFlowServicesub(token, adapter);
				for (SquidFlow squidFlow : squidFlows) {
				/*	if (refreshFlag)// squidflow状态表有数据
					{
						for (SquidFlowStatus squidFlowStatus : statusList) {
							// 如果squidflowid一致,并且状态为非normal(非正常状态)
							if (squidFlow.getId() == squidFlowStatus.getSquid_flow_id()&& squidFlowStatus.getSquid_flow_status() != 0) {
								continue;
							} else {
								delete = flowService.deleteSquidFlow(squidFlow.getId(), repositoryId, out);
							}
						}
					} else {*/
						delete = flowService.deleteSquidFlow(squidFlow.getId(),repositoryId, out);
						if(60002==out.getMessageCode().value())
						{
							refreshFlag=true;
						}
					/*}*/
				}
			}
			if (delete && refreshFlag) {
				out.setMessageCode(MessageCode.WARN_DELETEPROJECT);
				return true;
			} else if (delete && !refreshFlag) {
				// 删除project
				paramMap.clear();
				paramMap.put("id", String.valueOf(projectId));
				return adapter.delete(paramMap, Project.class) > 0 ? true: false;
			}
		} catch (Exception e) {
			logger.error("[删除deleteProject=========================================exception]", e);
			try {
				adapter.rollback();
			} catch (SQLException e1) {
				logger.error("rollback err!", e1);
			}
			out.setMessageCode(MessageCode.SQL_ERROR);
			return false;
		}
		return delete;
	}
}
