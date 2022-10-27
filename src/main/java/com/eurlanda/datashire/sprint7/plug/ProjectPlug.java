package com.eurlanda.datashire.sprint7.plug;

import com.eurlanda.datashire.adapter.IDBAdapter;
import com.eurlanda.datashire.entity.Project;
import com.eurlanda.datashire.entity.Repository;
import com.eurlanda.datashire.entity.operation.DataEntity;
import com.eurlanda.datashire.entity.operation.WhereCondition;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.enumeration.DataStatusEnum;
import com.eurlanda.datashire.enumeration.MatchTypeEnum;
import com.eurlanda.datashire.enumeration.SystemTableEnum;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import org.apache.log4j.Logger;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
/**
 * Project处理类
 * project业务处理，新增Project信息，根据Id修改Project信息；根据ID查询Project信息，根据key获得ProjectID
 * <p>
 * Title : 
 * </p>
 * <p>
 * Description: 
 * </p>
 * <p>
 * Author :赵春花 2013-8-26
 * </p>
 * <p>
 * update :赵春花2013-8-26
 * </p>
 * <p>
 * Department :  JAVA后端研发部
 * </p>
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司
 * </p>
 */
public class ProjectPlug  extends Support{
	static Logger logger = Logger.getLogger(ProjectPlug.class);// 记录日志
	
	public ProjectPlug() {
	}

	public ProjectPlug(IDBAdapter adapter){
		super(adapter);
	}

	/**
	 * 创建Project对象集合
	 * <p>
	 * 作用描述：
	 * 1、根据传入的Project对象集合封装成DataEntity对象集合调用批量新增的方法新增
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param projects project对象
	 *@param out 异常处理
	 *@return
	 */
	public boolean createProjects(List<Project> projects,ReturnValue out){
		logger.debug(String.format("createProject-projectList.size()=%s", projects.size()));
		boolean create = false;
		try {
			//封装批量新增数据集
			List<List<DataEntity>>  paramList = new ArrayList<List<DataEntity>>();
			//调用转换类
			GetParam getParam = new GetParam();
			for (Project project : projects) {
				List<DataEntity> dataEntitys =  new ArrayList<DataEntity>();
				getParam.getProject(project, dataEntitys);
				// 默认系统时间
				Timestamp date = new Timestamp(System.currentTimeMillis());
				// 创建时间
				DataEntity	dataEntity = new DataEntity("CREATION_DATE", DataStatusEnum.DATE, "'"+date+"'");
				dataEntitys.add(dataEntity);
				paramList.add(dataEntitys);
			}
			//调用批量新增
			create = adapter.createBatch(SystemTableEnum.DS_PROJECT.toString(), paramList, out);
			logger.debug(String.format("createProject-return=%s",create));
		} catch (Exception e) {
			out.setMessageCode(MessageCode.ERR_ARRAYS);
		}
		return create;
		
	}
	/**
	 * 创建Project
	 * <p>
	 * 作用描述：
	 * 创建新的PROject返回结果
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param newProject project对象
	 *@param out 异常处理
	 *@return
	 */
	public boolean createProject(Project newProject, ReturnValue out) {
		logger.debug(String.format("createProject-newProject", newProject));
		if (newProject == null) {
			out.setMessageCode(MessageCode.ERR_PROJECT_NULL);
			return false;
		}
		logger.debug(String.format("createProject-squid=%s;id=%s", newProject.getKey(), newProject.getId()));
		boolean create = false;
		// 将TransformationLink转换为数据包集合
		// 封装列属性
		List<DataEntity> paramList = new ArrayList<DataEntity>();
		// 设置值
		getParam(newProject, paramList,out);
		// 执行新增
		create = adapter.insert(SystemTableEnum.DS_PROJECT.toString(), paramList, out);
		logger.debug(String.format("createProject-return=%s", create));
		return create;
	}
	
	/**
	 * 修改Project
	 * <p>
	 * 作用描述：
	 * 根据传入参数修改Project
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param project project对象
	 *@param out 异常处理
	 *@return
	 */
	public boolean updateProject(Project project, ReturnValue out) {
		if (project == null) {
			out.setMessageCode(MessageCode.ERR_PROJECT_NULL);
			return false;
		}
		logger.debug(String.format("updateProject-guid=%s", project.getKey()));
//		if (project.getId() == null || project.getId().equals("")) {
//			out.setMessageCode(MessageCode.ERR_PROJECT_ID_NULL);
//			return false;
//		}
		boolean update = false;
		// 封装更新列
		// 将SquidLink转换为数据包集合
		List<DataEntity> paramList = new ArrayList<DataEntity>();
		// 设置值
		getParam(project, paramList,out);
		// 封装条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		WhereCondition condition = new WhereCondition();
		condition.setAttributeName("ID");
		condition.setDataType(DataStatusEnum.INT);
		condition.setMatchType(MatchTypeEnum.EQ);
		condition.setValue(project.getId());
		whereCondList.add(condition);
		// 执行更新操作
		update = adapter.update(SystemTableEnum.DS_PROJECT.toString(), paramList, whereCondList, out);
		logger.debug(String.format("updateColumn-return=%s", update));
		return update;
	}
	

	/**
	 * 根据ID删除Project
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param id projectID
	 *@param out 异常处理
	 *@return
	 */
	public boolean deleteProject(int id, ReturnValue out,String token) {
		logger.debug(String.format("deleteProject-id=%d", id));
		boolean delete = true;
		SquidFlowPlug squidFlowPlug = new SquidFlowPlug(adapter);
		// 查询出squidflow的id
		String queryFlowSql = "select id from DS_SQUID_FLOW where project_id= "
				+ id;
		List<Map<String, Object>> FlowList = adapter.query(queryFlowSql, out);
		if (null != FlowList&&FlowList.size()>0) {
			logger.debug("DS_SQUID_FLOW的长度==="+FlowList.size());
			for (int i = 0; i < FlowList.size(); i++) {
				delete = squidFlowPlug.deleteSquidFlow((Integer) FlowList
						.get(i).get("ID"), out, token);
				if (!delete) {
					break;
				}
			}
		}
		if(delete)
		{
			// 封装条件
			List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
			WhereCondition condition = new WhereCondition();
			condition.setAttributeName("ID");
			condition.setDataType(DataStatusEnum.INT);
			condition.setMatchType(MatchTypeEnum.EQ);
			condition.setValue(id);
			whereCondList.add(condition);
			delete = adapter.delete(SystemTableEnum.DS_PROJECT.toString(), whereCondList, out);
			logger.debug(String.format("deleteSquid-id=%d,result=%s", id, delete));
		}	
		return delete;
	}
	/**
	 * 根据唯一标识符获得Project
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param key
	 *@param out 异常处理
	 *@return
	 */
	public Project getProject(String key, ReturnValue out) {
		logger.debug(String.format("getProject-guid=%s", key));
		// 封装查询条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		WhereCondition condition = new WhereCondition();
		condition.setAttributeName("KEY");
		condition.setDataType(DataStatusEnum.STRING);
		condition.setMatchType(MatchTypeEnum.EQ);
		if(key!=null){
			key ="'"+key+"'";
		}
		condition.setValue(key);
		whereCondList.add(condition);
		// 定义集合接收
		Map<String, Object> columnsValue = adapter.queryRecord(null, SystemTableEnum.DS_PROJECT.toString(),
				whereCondList, out);
		if (columnsValue == null) {
			// 无数据返回
			out.setMessageCode(MessageCode.NODATA);
			return null;
		}
		// 将数据库查询结果转换为SquidFlow实体
		Project projectInfo = this.getProject(columnsValue);
		out.setMessageCode(MessageCode.SUCCESS);
		logger.debug(String.format("getProject  by projectInfo_id(%s)...", projectInfo.getId()));
		return projectInfo;
	}
	
	/**
	 * 根据Key获得ID
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param key
	 *@param out
	 *@return
	 */
	public int getProjectId(String key, ReturnValue out) {
		logger.debug(String.format("getProject-guid=%s", key));
		// 封装查询条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		WhereCondition condition = new WhereCondition();
		condition.setAttributeName("KEY");
		condition.setDataType(DataStatusEnum.STRING);
		condition.setMatchType(MatchTypeEnum.EQ);
		if(key!=null){
			key ="'"+key+"'";
		}
		condition.setValue(key);
		whereCondList.add(condition);
		// 定义集合接收
		Map<String, Object> columnsValue = adapter.queryRecord(null, SystemTableEnum.DS_PROJECT.toString(),
				whereCondList, out);
		if (columnsValue == null) {
			// 无数据返回
			out.setMessageCode(MessageCode.NODATA);
			return 0;
		}
		int id =  0;
		// 将数据库查询结果转换为SquidFlow实体
		if(columnsValue.get("ID")!=null){
			id =  Integer.parseInt(columnsValue.get("ID").toString());
		}
		
		out.setMessageCode(MessageCode.SUCCESS);
		logger.debug(String.format("getProject  by projectInfo_id(%s)...", id));
		return id;
	}
	
	/**
	 * 根据key获得Project对象
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param key
	 *@param out 异常处理
	 *@return
	 */
	public Project getProjectObject(String guid, ReturnValue out) {
		boolean create=false;
		Project project = new Project();
		project.setKey(guid);
		create = createProject(project, out);
		// 创建项目
		if (create) {
			// 若创建成功，查询SquidLink
			project = getProject(guid, out);
		}
		
		return project;
	}
	
	
	/**
	 * 根据ID查询Project
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param id ProjectID
	 *@param out 异常处理
	 *@return
	 */
	public Project getProject(int id, ReturnValue out) {
		logger.debug(String.format("getProject-guid=%s", id));
		// 封装查询条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		WhereCondition condition = new WhereCondition();
		condition.setAttributeName("ID");
		condition.setDataType(DataStatusEnum.STRING);
		condition.setMatchType(MatchTypeEnum.EQ);
		condition.setValue(id);
		whereCondList.add(condition);
		// 定义集合接收
		Map<String, Object> columnsValue = adapter.queryRecord(null, SystemTableEnum.DS_PROJECT.toString(),
				whereCondList, out);
		if (columnsValue == null) {
			// 无数据返回
			out.setMessageCode(MessageCode.NODATA);
			return null;
		}
		// 将数据库查询结果转换为SquidFlow实体
		Project projectInfo = this.getProject(columnsValue);
		out.setMessageCode(MessageCode.SUCCESS);
		logger.debug(String.format("getProject not found by projectInfo_id(%s)...", projectInfo.getId()));
		return projectInfo;
	}
	
	/**
	 * 根据传入参数筛选查询Project对象
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param condition 条件
	 *@param out 异常处理
	 *@return
	 */
	public Project[] getProjects(ReturnValue out,Repository repository) {
		// 封装查询条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		WhereCondition condition = new WhereCondition();
		condition.setAttributeName("REPOSITORY_ID");
		condition.setDataType(DataStatusEnum.INT);
		condition.setMatchType(MatchTypeEnum.EQ);
		condition.setValue(repository.getId());
		whereCondList.add(condition);
		List<Map<String, Object>> columnsValue = 
				adapter.query(null, SystemTableEnum.DS_PROJECT.toString(), whereCondList, out);
		if (columnsValue == null || columnsValue.size() == 0) {
			// 异常处理
			out.setMessageCode(MessageCode.NODATA);
			return null;
		}
		Project[] projectInfos = new Project[columnsValue.size()];// 定义数组接收
		// 将数据库查询结果转换为SquidFlow实体
		int i = 0;
		for (Map<String, Object> column : columnsValue) {
			projectInfos[i] = this.getProject(column);// 接收数据($)
			projectInfos[i].setRepository_id(repository.getId());
			i++;
		}
		logger.debug(String.format("getTransformationLinks_result-size()=%s", projectInfos.length));
		return projectInfos;
	}
	/**
	 * 将查询结果Map集合数据转换成Project对象
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param columnsValue
	 *@return
	 */
	private Project getProject(Map<String, Object> columnsValue) {
		Project projectInfo = null;
		int id = (Integer) columnsValue.get("ID");
		projectInfo = new Project();
		// ID
		projectInfo.setId(id);
		// 名字
		projectInfo.setName(columnsValue.get("NAME") == null ? null : columnsValue.get("NAME").toString());
		// 描述
		projectInfo.setDescription(columnsValue.get("DESCRIPTION") == null ? null : columnsValue.get("DESCRIPTION").toString());
		// 创建时间
		projectInfo.setCreation_date(columnsValue.get("CREATION_DATE") == null ? null : columnsValue.get("CREATION_DATE").toString());

		// 修改时间
		projectInfo.setModification_date(columnsValue.get("MODIFICATION_DATE") == null ? null
				:  columnsValue.get("MODIFICATION_DATE").toString());

		// 创建者
		projectInfo.setCreator(columnsValue.get("CREATOR") == null ? 0 : Integer.parseInt(columnsValue.get("CREATOR")+""));
		// 唯一标识
		projectInfo.setKey(columnsValue.get("KEY").toString());
		// 类型
		projectInfo.setType(DSObjectType.PROJECT.value());

		return projectInfo;
	}
	
	/**
	 * 将Project组装成DataEntity对象
	 * <p>
	 * 作用描述：
	 * 组装成DataEntityd对象便于后期处理
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param newProject project对象
	 *@param paramList DataEntity对象集合
	 *@param out
	 */
	private void getParam(Project newProject, List<DataEntity> dataEntitys,ReturnValue out) {
		logger.debug(String.format("getParam-Data_assembly_Project", newProject));
		DataEntity dataEntity = null;
		// 名称
		String name = newProject.getName();
		if(name!=null){
			name = "'"+name+"'";
		}
		dataEntity = new DataEntity("NAME", DataStatusEnum.STRING,name );
		dataEntitys.add(dataEntity);

		// 默认系统时间
		Timestamp date = new Timestamp(System.currentTimeMillis());

		// 描述
		String desc =  newProject.getDescription();
		if(desc!=null){
			desc = "'"+desc+"'";
		}
		dataEntity = new DataEntity("DESCRIPTION", DataStatusEnum.STRING,desc);
		dataEntitys.add(dataEntity);

		// 创建时间
		
		dataEntity = new DataEntity("CREATION_DATE", DataStatusEnum.DATE, "'"+date+"'");
		dataEntitys.add(dataEntity);

		// 修改时间
		dataEntity = new DataEntity("MODIFICATION_DATE", DataStatusEnum.DATE,"'"+date+"'");
		dataEntitys.add(dataEntity);

		// 创建人
		int creator =  newProject.getCreator();
		dataEntity = new DataEntity("CREATOR", DataStatusEnum.INT, creator);
		dataEntitys.add(dataEntity);

		// 唯一标识
		String key =   newProject.getKey();
		if(key!=null){
			key = "'"+key+"'";
		}
		dataEntity = new DataEntity("KEY", DataStatusEnum.STRING,key);
		dataEntitys.add(dataEntity);
	}
}
