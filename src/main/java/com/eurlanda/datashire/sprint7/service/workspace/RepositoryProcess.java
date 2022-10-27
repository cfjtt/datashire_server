package com.eurlanda.datashire.sprint7.service.workspace;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.dao.IProjectDao;
import com.eurlanda.datashire.dao.IRepositoryDao;
import com.eurlanda.datashire.dao.ISquidFlowDao;
import com.eurlanda.datashire.dao.ISquidFlowStatusDao;
import com.eurlanda.datashire.dao.ISquidLinkDao;
import com.eurlanda.datashire.dao.IVariableDao;
import com.eurlanda.datashire.dao.impl.ProjectDaoImpl;
import com.eurlanda.datashire.dao.impl.RepositoryDaoImpl;
import com.eurlanda.datashire.dao.impl.SquidFlowDaoImpl;
import com.eurlanda.datashire.dao.impl.SquidFlowStatusDaoImpl;
import com.eurlanda.datashire.dao.impl.SquidLinkDaoImpl;
import com.eurlanda.datashire.dao.impl.VariableDaoImpl;
import com.eurlanda.datashire.entity.DSVariable;
import com.eurlanda.datashire.entity.FindModel;
import com.eurlanda.datashire.entity.FindResult;
import com.eurlanda.datashire.entity.Project;
import com.eurlanda.datashire.entity.Repository;
import com.eurlanda.datashire.entity.Squid;
import com.eurlanda.datashire.entity.SquidFlow;
import com.eurlanda.datashire.entity.SquidFlowStatus;
import com.eurlanda.datashire.entity.SquidLink;
import com.eurlanda.datashire.server.utils.TokenUtil;
import com.eurlanda.datashire.sprint7.packet.InfoPacket;
import com.eurlanda.datashire.sprint7.service.squidflow.DataShirServiceplug;
import com.eurlanda.datashire.sprint7.service.squidflow.LockSquidFlowProcess;
import com.eurlanda.datashire.utility.CalcSquidFlowStatus;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import com.eurlanda.datashire.utility.StringUtils;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 
 *Repository操作类
 *Repository预处理类：对数据进行验证验证数据的完整性和正确性；并调用下层业务处理方法
 * Title : 
 * Description: 
 * Author :赵春花 2013-9-17
 * update :赵春花 2013-9-17
 * Department :  JAVA后端研发部
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司
 */
public class RepositoryProcess  {
	static Logger logger = Logger.getLogger(RepositoryProcess.class);// 记录日志
	private RepositoryService repositoryService ;
	private DataShirServiceplug dataShirBusiness;
	private String token;
	private String key;
	public RepositoryProcess(String token) {
		repositoryService = new RepositoryService(token);
		dataShirBusiness = new DataShirServiceplug(token);
	}
	
	public RepositoryProcess(String token, String key){
		this.token = token;
		this.key = key;
	}
	
	
	/**
	 * 创建Repository集合对象处理类
	 * 作用描述：
	 * 根据server层传入Json数据序列化成集合Repository对象
	 * 并验证Repository信息是否正确
	 * 修改说明：
	 *@param info
	 *@param out
	 *@return
	 */
	public List<InfoPacket> createRepositorys(String info,ReturnValue out){
		List<InfoPacket> infoPackets = new ArrayList<InfoPacket>(1);
		try {
			//序列化JSONArray成Repository对象集合
			List<Repository> repositories =  JsonUtil.toGsonList(info, Repository.class);
			//验证repositorie集合对象
			if(repositories==null||repositories.size()==0){
				//repositories对象集合不能为空size不能等于0
				//异常返回
				out.setMessageCode(MessageCode.ERR_PROJECT_LIST_NULL);
				logger.debug(String.format("createRepositorys-projects-=%s", false));
				return infoPackets;
			}else{
				//验证数据是否完整
				for (Repository repository : repositories) {
					//验证Repository类型是否正确
					if(repository.getRepository_type()==0){
						out.setMessageCode(MessageCode.ERR_RESITORY_DBTYPE);
						//repository.setType(1); //TODO
						return infoPackets;
					}
					//验证key是否有值
					if(repository.getKey() == null || repository.getKey().equals("")){
						out.setMessageCode(MessageCode.ERR_GUID_NULL);
						return infoPackets;
					}
					//数据库名是否为空
					if(StringUtils.isNull(repository.getRepository_db_name())){
						out.setMessageCode(MessageCode.ERR_RESITORY_DB);
						//repository.setRepository_db_name(repository.getName()); //TODO
						//if(StringUtils.isNull(repository.getName()))
						return infoPackets;
					}
					//验证Repository状态是否正确
					if(repository.getStatus() == 0){
						out.setMessageCode(MessageCode.ERR_REPOSITORY_STATUS);
						//repository.setStatus(1); //TODO
						return infoPackets;
					}
					//验证Repository的TeamId是否正常、
					if(repository.getTeam_id() == 0){
						out.setMessageCode(MessageCode.ERR_REPOSITORY_TEAM_ID_NULL);
						return infoPackets;
					}
				}
				infoPackets = repositoryService.createRepository(repositories, out);
				logger.debug(String.format("createRepositorys-return=%s", infoPackets.toString()));
			}
		} catch (Exception e) {
			out.setMessageCode(MessageCode.DESERIALIZATION_FAILED);
			logger.error("error", e);
		}
		return infoPackets;
	}
	
	/**
	 * 注册Repository
	 * 作用描述：
	 * 序列化Repository对象掉用业务
	 * 修改说明：
	 *@param info
	 *@param out
	 *@return
	 */
	public boolean createRegisterRepositorys(String info,ReturnValue out){
		logger.debug(String.format("createRegisterRepository-info=%s", info));
		boolean create = false;
		try {
			Repository repository = JsonUtil.toGsonBean(info, Repository.class);
			//验证：
			//路径不能为空
			if(repository.getRepository_path()== null){
				out.setMessageCode(MessageCode.ERR_REPOSITORY_PATH);
			}
			//数据库名不能为空
			
			create = repositoryService.createRegisterRepository(repository, out);
		} catch (Exception e) {
			out.setMessageCode(MessageCode.DESERIALIZATION_FAILED);
		}
		logger.debug(String.format("createRegisterRepository-return=%s", create));
		return create;
	}
	
	/**
	 * 
	 
	 * 作用描述：
	 
	 
	 * 修改说明：
	 
	 *@param info
	 *@param out
	 *@return
	 */
	public List<InfoPacket> updateRepositorys(String info,ReturnValue out){
		logger.debug(String.format("updateRepositorys-info=%s", info));
		List<InfoPacket> infoPackets = new ArrayList<InfoPacket>();
		//Json序列化
		List<Repository> repositorys = JsonUtil.toGsonList(info, Repository.class);
		if(repositorys == null || repositorys.size()==0){
			out.setMessageCode(MessageCode.ERR_RESITORY_DATA_ISNULL);
			return infoPackets;
		}
		infoPackets = repositoryService.updateRepositorys(repositorys, out);
		logger.debug(String.format("updateRepositorys-return=%s", infoPackets));
		return infoPackets;
		
	}

	
	/**
	 * 得到所有Repository
	 
	 * 作用描述：
	 * 序列化InfoPacket对象
	 * 验证：
	 * InfoPacket Id不能为空
	 
	 
	 * 修改说明：
	 
	 *@param info json字符串
	 *@param out 异常处理
	 *@return
	 */
	public Repository[] querylAllRepositorys(String info,ReturnValue out){
		logger.debug(String.format("queryAllProjects-info=%s", info));
		InfoPacket infoPacket = JsonUtil.toGsonBean(info, InfoPacket.class);
		Repository[] repository = repositoryService.queryRepositorys(infoPacket.getId(), out);
		logger.debug(String.format("queryAllProjects-return=%s", repository));
		return repository;
	}
	
	/**
	 * 作用描述：根据suqidFlowId 执行一个suqidFlow
	 * 修改说明：
	 * @param squidFlowId
	 * @param out
	 */
	public void executeFlow(String cond, ReturnValue out) {
		SquidFlow squidFlow =JsonUtil.toGsonBean(cond, SquidFlow.class);
		List<Squid> squidList=squidFlow.getSquidList();
		if(squidList!=null && !squidList.isEmpty() && squidList.get(0)!=null){
			dataShirBusiness.executeFlow(squidList.get(0).getSquidflow_id(), squidFlow.isPush(), out);
		}
	}
	
	/**
	 * 获取搜索对象的结果集
	 * @param info
	 * @param out
	 * @return
	 */
	public Map<String, Object> findObjectsForParams(String info, ReturnValue out){
		IRelationalDataManager adapter = null;
		Map<String, Object> outputMap = new HashMap<String, Object>();
		try {
			FindModel findModel = JsonUtil.toGsonBean(info, FindModel.class);
			if (findModel!=null){
				adapter = DataAdapterFactory.getDefaultDataManager();
				adapter.openSession();
				IRepositoryDao dao = new RepositoryDaoImpl(adapter);
				List<FindResult> rs = dao.getFindResultByParams(findModel);
				outputMap.put("FindResults", rs);
			}else{
				out.setMessageCode(MessageCode.NODATA);
			}
		} catch (Exception e) {
			if (adapter!=null){
				try {
					adapter.rollback();
				} catch (SQLException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
			out.setMessageCode(MessageCode.SQL_ERROR);
			logger.error("findObjectsForParams error !", e);
		} finally{
			if (adapter!=null){
				adapter.closeSession();
			}
		}
		return outputMap;
	}
	
	/**
	 * 打开搜索结果集中的某条记录，同时刷新project信息，当搜索为squidflow时，squidflow打开参数传空集合
	 * @param info
	 * @param out
	 * @return
	 */
	public Map<String, Object> openSubitemsOfRepository(String info, ReturnValue out){
		IRelationalDataManager adapter = null;
		Map<String, Object> outputMap = new HashMap<String, Object>();
		try {
			Map<String, Object> paramsMap = JsonUtil.toHashMap(info);
			if (paramsMap!=null){
				int repositoryId = (int)paramsMap.get("RepositoryId");
				int projectId = (int)paramsMap.get("ProjectId");
				int squidFlowId = (int)paramsMap.get("SquidFlowId");
				boolean openSquidFlow = (boolean)paramsMap.get("OpenSquidFlow");
				adapter = DataAdapterFactory.getDefaultDataManager();
				adapter.openSession();
				ISquidFlowDao squidFlowDao = new SquidFlowDaoImpl(adapter);
				ISquidLinkDao squidLinkDao = new SquidLinkDaoImpl(adapter);
				ISquidFlowStatusDao squidFlowStatusDao = new SquidFlowStatusDaoImpl(adapter);
				IVariableDao variableDao = new VariableDaoImpl(adapter);
				if (repositoryId>0){
					IProjectDao projectDao = new ProjectDaoImpl(adapter);
					List<Project> projectList = projectDao.getAllProject(repositoryId);
					outputMap.put("Projects", projectList==null?new ArrayList<Project>():projectList);
				}
				if (projectId>0){
					List<SquidFlow> squidFlowList = squidFlowDao.getAllSquidFlows(projectId);
					//获取所有squidflow的状态
					LockSquidFlowProcess lockSquidFlowProcess=new LockSquidFlowProcess();
					List<SquidFlowStatus> squidFlowStatus=lockSquidFlowProcess.getSquidFlowStatus(repositoryId, projectId, out,adapter);
					//根据squidflow的id匹配得到状态
					for(int i=0;i<squidFlowList.size();i++){
						//SquidFlow squidFlow =  squidFlows.get(i);
						//squidFlows.get(i).setName(squidFlow.getName()+"_"+squidFlow.getId());
						for(int j=0;j<squidFlowStatus.size();j++){
							if(squidFlowList.get(i).getId()==squidFlowStatus.get(j).getSquid_flow_id()){
								squidFlowList.get(i).setSquid_flow_status(squidFlowStatus.get(j).getSquid_flow_status());
								break;
							}
						}
					}
					//查询变量集合
					List<DSVariable> variables = variableDao.getDSVariableByScope(0, projectId);
					if (variables==null){
						variables = new ArrayList<DSVariable>();
					}
					outputMap.put("ProjectVariables", variables);
					outputMap.put("SquidFlows", squidFlowList==null?new ArrayList<SquidFlow>():squidFlowList);
				}
				if (openSquidFlow){
					int count = squidFlowDao.getSquidCountBySquidFlow(squidFlowId);
					logger.info("SquidFlow:"+squidFlowId+", squidCount:"+count);
					outputMap.put("Count", count);
					//添加squidLink集合
					List<SquidLink> lists = squidLinkDao.getSquidLinkListBySquidFlow(squidFlowId);
					outputMap.put("SquidLinks", lists);
					//查询变量集合
					List<DSVariable> variables = variableDao.getDSVariableByScope(1, squidFlowId);
					if (variables==null){
						variables = new ArrayList<DSVariable>();
					}
					outputMap.put("Variables", variables);
					//return JsonUtil.toString(list, DSObjectType.SQUID, new ReturnValue());
					//查询squidFlow的状态
					List<SquidFlowStatus> list= squidFlowStatusDao.getSquidFlowStatusBySquidFlow(repositoryId, squidFlowId);
					if(StringUtils.isNotNull(list) && !list.isEmpty()){
						Map<String, Object> calcMap=CalcSquidFlowStatus.calcStatus(list);
						if(Integer.parseInt(calcMap.get("checkout").toString())>0
								||Integer.parseInt(calcMap.get("schedule").toString())>0){
							//表示该squidflow已经被加锁
							out.setMessageCode(MessageCode.WARN_GETSQUIDFLOW);
						}
					}
					//调用squidFlow加锁
					boolean flag = squidFlowDao.getLockOnSquidFlow(squidFlowId, projectId, repositoryId, 1, TokenUtil.getToken());
					if (flag){
						LockSquidFlowProcess lockSquidFlowProcess = new LockSquidFlowProcess(TokenUtil.getToken());
						lockSquidFlowProcess.sendAllClient(repositoryId, squidFlowId, 1);
					}
				}
			}else{
				out.setMessageCode(MessageCode.NODATA);
			}
		} catch (Exception e) {
			if (adapter!=null){
				try {
					adapter.rollback();
				} catch (SQLException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
			out.setMessageCode(MessageCode.SQL_ERROR);
			logger.error("openSubitemsOfRepository error !", e);
		} finally{
			if (adapter!=null){
				adapter.closeSession();
			}
		}
		return outputMap;
	}
	
	/**
	 * 打开搜索结果集中的某条记录，同时刷新squidflow信息
	 * @param info
	 * @return
	 */
	public Map<String, Object> openSubitemsOfProject(String info, ReturnValue out){
		IRelationalDataManager adapter = null;
		Map<String, Object> outputMap = new HashMap<String, Object>();
		try {
			Map<String, Object> paramsMap = JsonUtil.toHashMap(info);
			if (paramsMap!=null){
				int repositoryId = (int)paramsMap.get("RepositoryId");
				int projectId = (int)paramsMap.get("ProjectId");
				int squidFlowId = (int)paramsMap.get("SquidFlowId");
				adapter = DataAdapterFactory.getDefaultDataManager();
				adapter.openSession();
				ISquidFlowDao squidFlowDao = new SquidFlowDaoImpl(adapter);
				ISquidLinkDao squidLinkDao = new SquidLinkDaoImpl(adapter);
				ISquidFlowStatusDao squidFlowStatusDao = new SquidFlowStatusDaoImpl(adapter);
				IVariableDao variableDao = new VariableDaoImpl(adapter);
				if (projectId>0){
					List<SquidFlow> squidFlowList = squidFlowDao.getAllSquidFlows(projectId);
					//获取所有squidflow的状态
					LockSquidFlowProcess lockSquidFlowProcess=new LockSquidFlowProcess();
					List<SquidFlowStatus> squidFlowStatus=lockSquidFlowProcess.getSquidFlowStatus(repositoryId, projectId, out,adapter);
					//根据squidflow的id匹配得到状态
					for(int i=0;i<squidFlowList.size();i++){
						//SquidFlow squidFlow =  squidFlows.get(i);
						//squidFlows.get(i).setName(squidFlow.getName()+"_"+squidFlow.getId());
						for(int j=0;j<squidFlowStatus.size();j++){
							if(squidFlowList.get(i).getId()==squidFlowStatus.get(j).getSquid_flow_id()){
								squidFlowList.get(i).setSquid_flow_status(squidFlowStatus.get(j).getSquid_flow_status());
								break;
							}
						}
					}
					//查询变量集合
					List<DSVariable> variables = variableDao.getDSVariableByScope(0, projectId);
					if (variables==null){
						variables = new ArrayList<DSVariable>();
					}
					outputMap.put("ProjectVariables", variables);
					outputMap.put("SquidFlows", squidFlowList==null?new ArrayList<SquidFlow>():squidFlowList);
				}
				
				//查询变量集合
				List<DSVariable> variables = variableDao.getDSVariableByScope(1, squidFlowId);
				if (variables==null){
					variables = new ArrayList<DSVariable>();
				}
				outputMap.put("Variables", variables);
				int count = squidFlowDao.getSquidCountBySquidFlow(squidFlowId);
				logger.info("SquidFlow:"+squidFlowId+", squidCount:"+count);
				outputMap.put("Count", count);
				//添加squidLink集合
				List<SquidLink> lists = squidLinkDao.getSquidLinkListBySquidFlow(squidFlowId);
				outputMap.put("SquidLinks", lists);
				//return JsonUtil.toString(list, DSObjectType.SQUID, new ReturnValue());
				//查询squidFlow的状态
				List<SquidFlowStatus> list= squidFlowStatusDao.getSquidFlowStatusBySquidFlow(repositoryId, squidFlowId);
				if(StringUtils.isNotNull(list) && !list.isEmpty()){
					Map<String, Object> calcMap=CalcSquidFlowStatus.calcStatus(list);
					if(Integer.parseInt(calcMap.get("checkout").toString())>0
							||Integer.parseInt(calcMap.get("schedule").toString())>0){
						//表示该squidflow已经被加锁
						out.setMessageCode(MessageCode.WARN_GETSQUIDFLOW);
					}
				}
				//调用squidFlow加锁
				boolean flag = squidFlowDao.getLockOnSquidFlow(squidFlowId, projectId, repositoryId, 1, TokenUtil.getToken());
				if (flag){
					LockSquidFlowProcess lockSquidFlowProcess = new LockSquidFlowProcess(TokenUtil.getToken());
					lockSquidFlowProcess.sendAllClient(repositoryId, squidFlowId, 1);
				}
			}else{
				out.setMessageCode(MessageCode.NODATA);
			}
		} catch (Exception e) {
			if (adapter!=null){
				try {
					adapter.rollback();
				} catch (SQLException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
			out.setMessageCode(MessageCode.SQL_ERROR);
			logger.error("openSubitemsOfProject error !", e);
		} finally{
			if (adapter!=null){
				adapter.closeSession();
			}
		}
		return outputMap;
	}
	
	public static void main(String[] args) {
		RepositoryProcess process = new RepositoryProcess("");
		String info = "{\"SourceItem\":{\"Id\":1,\"Name\":\"客户端测试库\",\"Type\":1002},\"Condition\":\"var1\",\"IgnoreCase\":false,\"WholeWord\":false,\"ObjectType\":1068}";
		ReturnValue out = new ReturnValue();
		Map<String, Object> outputMap = process.findObjectsForParams(info, out);
		System.out.println(outputMap);
	}
}
