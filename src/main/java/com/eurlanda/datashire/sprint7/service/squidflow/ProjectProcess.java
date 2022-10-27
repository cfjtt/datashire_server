package com.eurlanda.datashire.sprint7.service.squidflow;

import cn.com.jsoft.jframe.utils.ValidateUtils;
import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.dao.IProjectDao;
import com.eurlanda.datashire.dao.IRepositoryDao;
import com.eurlanda.datashire.dao.impl.ProjectDaoImpl;
import com.eurlanda.datashire.dao.impl.RepositoryDaoImpl;
import com.eurlanda.datashire.entity.Project;
import com.eurlanda.datashire.entity.Repository;
import com.eurlanda.datashire.sprint7.packet.InfoPacket;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import com.eurlanda.datashire.utility.StringUtils;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ProjectProcess预处理类
 * ProjectProcess预处理类：对数据进行验证验证数据的完整性和正确性；并调用下层业务处理方法
 * Title :
 * Description:
 * Author :Cheryl 2013-8-23
 * update :Cheryl2013-8-23
 * Department : JAVA后端研发部
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司 </p>
 */
public class ProjectProcess implements IProjectProcess{
	static Logger logger = Logger.getLogger(ProjectProcess.class);// 记录日志
	private ProjectService projectService;
	private String token;
	public ProjectProcess(String token) {
		projectService = new ProjectService(token);
		this.token = token;
	}
	
	/**
	 * 创建Project集合对象处理类
	 * 作用描述：
	 * 将Json包解析成Project对象集合调用DataShirBusiness的新增Project对象集合的方法
	 * 验证：
	 * 1、验证数据是否为空
	 * 2、验证Project对象集合里Project对象必要对象的值是否正确
	 * 修改说明：
	 *@param info json字符串
	 *@param out 异常处理
	 *@return
	 */
	public Map<String, Object> createProjects(String info,ReturnValue out){
		logger.debug(String.format("createProject-info=%s", info));
		Map<String, Object> outputMap = new HashMap<String, Object>();
		DataAdapterFactory adapterFactory = null;
		IRelationalDataManager adapter3 = null;
		try {
			Map<String,Object> paramMap = JsonUtil.toHashMap(info);
			int repositoryId = Integer.parseInt(paramMap.get("RepositoryId")+"");
			int userId = Integer.parseInt(paramMap.get("UserId")+"");
			if (repositoryId>0 && userId>0){
				//序列化JSONArray
				adapter3 = DataAdapterFactory.getDefaultDataManager();
				adapter3.openSession();
				
				Map<String, Object> map = adapter3.query2Object(true, 
						"select count(id) as id from DS_PROJECT where repository_id="+repositoryId, null);
				int temp = 0;
				if (map!=null){
					String mid = map.containsKey("ID")+"";
					if (ValidateUtils.isNumeric(mid)){
						temp = Integer.parseInt(map.get("ID")+"");
					}
				}
				Project project = new Project();
				project.setName(getProjectName(adapter3, "NewProject", temp,repositoryId));
				project.setKey(StringUtils.generateGUID());
				SimpleDateFormat dateFm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); //格式化当前系统日期
				String dateTime = dateFm.format(new java.sql.Date(System.currentTimeMillis()));
				project.setCreation_date(dateTime);
				project.setModification_date(dateTime);
				project.setCreator(userId);
				project.setRepository_id(repositoryId);
				project.setId(adapter3.insert2(project));
				/*List<Project> projects =  JsonUtil.toGsonList(info, Project.class);
				//验证Project集合对象
				if(projects==null||projects.size()==0){
					//Project对象集合不能为空size不能等于0
					//异常返回
					out.setMessageCode(MessageCode.ERR_PROJECT_LIST_NULL);
					logger.debug(String.format("createProject-projects-=%s", false));
					return null;
				}else{
					//数据完整性验证
					for (Project project : projects) {
						if(project.getRepository_id()==0||project.getKey()== null ){
							out.setMessageCode(MessageCode.ERR_PROJECT_DATA_INCOMPLETE);
							return null;
						}
					}
					infoPackets = projectService.createProject(projects, out);
					logger.debug(String.format("createProject-return=%s", infoPackets));
				}*/
				outputMap.put("Project", project);
				return outputMap;
			}else{
				out.setMessageCode(MessageCode.NODATA);
			}
		} catch (Exception e) {
			try {
				adapter3.rollback();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			out.setMessageCode(MessageCode.DESERIALIZATION_FAILED);
		} finally{
			adapter3.closeSession();
		}
		return null;
	}
	
	/**
	 * 修改Project对象集合
	 * 作用描述：
	 * 根据Project对象集合里Project的ID进行修改
	 * 验证：
	 * 1、Project集合对象序列化后结果是否有值
	 * 2、Project集合对象中Project对象的必要字段是否有值
	 * 修改说明：
	 *@param info json字符串
	 *@param out 异常处理
	 *@return
	 */
	public List<InfoPacket> updateProjects(String info,ReturnValue out){
		logger.debug(String.format("updateProject-info=%s", info));
		boolean update = false;
		//Json序列化
		//TODO 验证数据
		List<InfoPacket> infoPackets  = new ArrayList<InfoPacket>();
		try {
			List<Project> projects = JsonUtil.toGsonList(info, Project.class);
			if(projects == null || projects.size()==0){
				out.setMessageCode(MessageCode.ERR_PROJECT_NULL);
			}
			infoPackets = projectService.updateProjects(projects, out);
			logger.debug(String.format("updateProject-return=%s", update));
		} catch (Exception e) {
			out.setMessageCode(MessageCode.DESERIALIZATION_FAILED);
		}
		return infoPackets;
		
	}
	
	/**
	 * 查询所有的Project对象集合
	 * 作用描述：
	 * 序列化Repository对象调用ProjectService业务处理
	 * 修改说明：
	 *@param info json字符串
	 *@param out 异常处理
	 *@return
	 */
	public Map<String, Object> queryAllProjects(String info,ReturnValue out){
		Map<String, Object> outputMap = new HashMap<String, Object>();
		IRelationalDataManager adapter = DataAdapterFactory.getDefaultDataManager();
		Repository repository=null;
		try {
			Map<String, Object> map = JsonUtil.toHashMap(info);
			int repositoryId=Integer.parseInt(map.get("RepositoryId").toString());
			adapter.openSession();
			IProjectDao projectDao=new ProjectDaoImpl(adapter);
			List<Project> projects = projectDao.getAllProject(repositoryId);
			IRepositoryDao repositoryDao=new RepositoryDaoImpl(adapter);
			repository=repositoryDao.getObjectById(repositoryId, Repository.class);
			outputMap.put("Projects", projects);
		} catch (Exception e) {
			// TODO: handle exception
			out.setMessageCode(MessageCode.ERR_GETALLPROJECT);
			logger.error("获取所有的project异常", e);
		}finally
		{
			adapter.closeSession();
//			User user = CommonConsts.UserSession.get(TokenUtil.getToken());
//			if(user!=null){
//				user.setRepository(repository);
//				logger.info("[激活仓库] "+user.getUser_name()+"\t"+repository.getRepository_db_name());
//			}else{
//				logger.warn("[获取用户当前会话为空] "+token);
//			}
		}
		return outputMap;
	}
	
	/**
	 * 设置重复名称后自动创建序号
	 * @param adapter3
	 * @param squidName
	 * @param squidFlowId
	 * @return
	 */
	private String getProjectName(IRelationalDataManager adapter3, String projectName, int temp,int repositoryId){
		Map<String, Object> params = new HashMap<String, Object>();
		int cnt = temp;
		String tempName;
		while (true) {
			tempName = projectName + cnt;
			params.put("name", tempName);
			params.put("repository_id",repositoryId);
			Project squid = adapter3.query2Object(true, params, Project.class);
			if (squid==null){
				return tempName;
			}
			cnt++;
		}
	}
	
}
