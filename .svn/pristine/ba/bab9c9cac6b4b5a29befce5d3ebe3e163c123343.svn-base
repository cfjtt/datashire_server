package com.eurlanda.datashire.sprint7.plug;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IDBAdapter;
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
 * Repository处理类
 * Repository对象业务处理，Repository对象新增业务，Repository根据ID修改业务；Repository根据ID查询业务；Repository注册业务；根据key获得RepositoryＩＤ；
 * <p>
 * Title : 
 * </p>
 * <p>
 * Description: 
 * </p>
 * <p>
 * Author :赵春花 2013-10-28
 * </p>
 * <p>
 * update :赵春花 2013-10-28
 * </p>
 * <p>
 * Department :  JAVA后端研发部
 * </p>
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司
 * </p>
 */
public class RepositoryPlug  extends Support{
static Logger logger = Logger.getLogger(RepositoryPlug.class);// 记录日志
protected DataAdapterFactory adapterFactory;
	public RepositoryPlug() {
	}

	public RepositoryPlug(IDBAdapter adapter){
		super(adapter);
		adapterFactory = DataAdapterFactory.newInstance();
	}
	
	/**
	 * 创建新的Repository
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param repository repository对象
	 *@param out 异常处理
	 *@return
	 */
	public boolean createNewRepository(Repository repository,ReturnValue out){
		boolean create = false;
		IDBAdapter adaoterSource = null;
		try {
			adaoterSource = adapterFactory.getAdapter(repository.getRepository_db_name());
			//RepositoryPlug repositoryPlug = new RepositoryPlug(adaoterSource);
			create = adaoterSource.createRepository(out);//repositoryPlug.createNewRepository(out);
		} catch (Exception e) {
			//数据库连接失败
			out.setMessageCode(MessageCode.ERR_SYSDBLINK);
		}finally{
			if(adaoterSource!=null)adaoterSource.commitAdapter();
		}
		return create;
	}
	
	/**
	 * 创建Repository对象集合
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param repositories repository集合对象
	 *@param out 异常处理
	 *@return
	 */
	public boolean createRepository(List<Repository> repositories,ReturnValue out){
		logger.debug(String.format("createRepository-repositoriesList.size()=%s", repositories.size()));
		boolean create = false;
		//封装批量新增数据集
		List<List<DataEntity>>  paramList = new ArrayList<List<DataEntity>>();
		//调用转换类
		GetParam getParam = new GetParam();
		for (Repository repository : repositories) {
			List<DataEntity> dataEntitys =  new ArrayList<DataEntity>();
			getParam.getRepository(repository, dataEntitys);
			// 默认系统时间
			Timestamp date = new Timestamp(System.currentTimeMillis());
			// 创建时间
			DataEntity	dataEntity = new DataEntity("CREATION_DATE", DataStatusEnum.DATE, "'"+date+"'");
			dataEntitys.add(dataEntity);
			paramList.add(dataEntitys);
		}
		//调用批量新增
		create = adapter.createBatch(SystemTableEnum.DS_SYS_REPOSITORY.toString(), paramList, out);
		logger.debug(String.format("createRepository-return=%s",create));
		return create;
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
	 *@param out异常处理
	 *@return
	 */
	public int getRepositoryId(String key, ReturnValue out) {
		logger.debug(String.format("getRepositoryId-guid=%s", key));
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
		Map<String, Object> columnsValue = adapter.queryRecord(null, SystemTableEnum.DS_SYS_REPOSITORY.toString(),
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
		logger.debug(String.format("getRepositoryId  by Repository_ID(%s)...", id));
		return id;
	}
	/**
	 * 根据TEAMID获得所有repository
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param id teamID
	 *@param out 异常处理
	 *@return
	 */
	public Repository[] getRepository(int id, ReturnValue out) {
		// teamID
		logger.debug(String.format("getRepository=%s", id));
		
		// 封装查询条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		WhereCondition condition = new WhereCondition();
		condition.setAttributeName("TEAM_ID");
		condition.setDataType(DataStatusEnum.INT);
		condition.setMatchType(MatchTypeEnum.EQ);
		condition.setValue(id);
		whereCondList.add(condition);

		List<Map<String, Object>> columnsValue = adapter.query(null, SystemTableEnum.DS_SYS_REPOSITORY.toString(), whereCondList,
				out);
		if (columnsValue == null || columnsValue.size() == 0) {
			// 异常处理
			out.setMessageCode(MessageCode.NODATA);
			return null;
		}
		Repository[] projectInfos = new Repository[columnsValue.size()];// 定义数组接收
		// 将数据库查询结果转换为SquidFlow实体
		int i = 0;
		for (Map<String, Object> column : columnsValue) {
			projectInfos[i] = this.getRepostory(column);// 接收数据($)
			i++;
		}
		logger.debug(String.format("getTransformationLinks_result-size()=%s", projectInfos.length));
		return projectInfos;
	}
	
	/**
	 * 将Map对象返回Repostory
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param columnsValue
	 *@return
	 */
	private Repository getRepostory(Map<String, Object> columnsValue) {
		Repository repository = null;
		int id = (Integer) columnsValue.get("ID");
		repository = new Repository();
		// ID
		repository.setId(id);
		// 名字
		repository.setName(columnsValue.get("NAME") == null ? null : columnsValue.get("NAME").toString());
		// 描述
		repository.setDescription(columnsValue.get("DESCRIPTION") == null ? null : columnsValue.get("DESCRIPTION").toString());
		//RepositoryDBName
		repository.setRepository_db_name(columnsValue.get("REPOSITORY_DB_NAME") == null ? null : columnsValue.get("REPOSITORY_DB_NAME").toString());
		//RepositoryTYPE
		if(columnsValue.get("TYPE")!=null){
			int type = Integer.parseInt(columnsValue.get("TYPE").toString());
			repository.setRepository_type(type);
		}
		repository.setRepository_path(columnsValue.get("DB_PATH")== null?null:columnsValue.get("DB_PATH").toString());
		//TEAMID
		repository.setTeam_id(columnsValue.get("TEAM_ID") == null ? 0 : Integer.parseInt(columnsValue.get("TEAM_ID").toString()));
		// 唯一标识
		repository.setKey(columnsValue.get("KEY").toString());
		repository.setType(DSObjectType.REPOSITORY.value());
		return repository;
	}
	/**
	 * 根据ID获得RepostroyName
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param id repositoryID
	 *@param out 异常处理
	 *@return
	 */
	public String queryRepostroyName(int id, ReturnValue out){
		logger.debug(String.format("queryRepostroyName-id=%s", id));
		// 封装查询条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		WhereCondition condition = new WhereCondition();
		condition.setAttributeName("ID");
		condition.setDataType(DataStatusEnum.STRING);
		condition.setMatchType(MatchTypeEnum.EQ);
		condition.setValue(id);
		whereCondList.add(condition);
		// 定义集合接收
		Map<String, Object> columnsValue = adapter.queryRecord(null, SystemTableEnum.DS_SYS_REPOSITORY.toString(),
				whereCondList, out);
		if (columnsValue == null) {
			// 无数据返回
			out.setMessageCode(MessageCode.NODATA);
			return null;
		}
		// 将数据库查询结果转换为SquidFlow实体
		String name = columnsValue.get("NAME").toString();
		out.setMessageCode(MessageCode.SUCCESS);
		logger.debug(String.format("queryRepostroyName  by id(%s)...", name));
		return name;
	}
	
	/**
	 * 根据ID查询Repostory对象
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param id repositoryＩＤ
	 *@param out　异常处理
	 *@return
	 */
	public Repository queryRepository(int id, ReturnValue out){
		logger.debug(String.format("queryRepostroy-id=%s", id));
		// 封装查询条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		WhereCondition condition = new WhereCondition();
		condition.setAttributeName("ID");
		condition.setDataType(DataStatusEnum.STRING);
		condition.setMatchType(MatchTypeEnum.EQ);
		condition.setValue(id);
		whereCondList.add(condition);
		// 定义集合接收
		Map<String, Object> columnsValue = adapter.queryRecord(null, SystemTableEnum.DS_SYS_REPOSITORY.toString(),
				whereCondList, out);
		if (columnsValue == null) {
			// 无数据返回
			out.setMessageCode(MessageCode.NODATA);
			return null;
		}
		Repository repository = getRepostory(columnsValue);
		out.setMessageCode(MessageCode.SUCCESS);
		logger.debug(String.format("queryRepostroyName  by id(%s)...", repository));
		return repository;
	}
	
	/**
	 * 创建新的repository
	 * <p>
	 * 作用描述：
	 * 创建repository表结构
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@return
	 */
	public boolean createNewRepository(ReturnValue out){
		boolean create = false;
		create = adapter.createRepository(out);
		return create;
	}

	/**
	 * 根据ID删除Repository
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param id repositoryＩＤ
	 *@param out 异常处理
	 *@return
	 */
	public boolean deleteRepositoryById(int id, ReturnValue out) {
		logger.debug(String.format("deleteProject-id=%d", id));
		boolean delete = false;
		// 封装条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		WhereCondition condition = new WhereCondition();
		condition.setAttributeName("ID");
		condition.setDataType(DataStatusEnum.INT);
		condition.setMatchType(MatchTypeEnum.EQ);
		condition.setValue(id);
		whereCondList.add(condition);
		delete = adapter.delete(SystemTableEnum.DS_SYS_REPOSITORY.toString(), whereCondList, out);
		logger.debug(String.format("deleteRepository-id=%d,result=%s", id, delete));
		return delete;
	}
	
	/**
	 * 根据TeamId删除Repository
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param id ｔｅａｍＩＤ
	 *@param out　异常处理
	 *@return
	 */
	public boolean deleteRepositoryByTeamId(int id, ReturnValue out) {
		logger.debug(String.format("deleteProject-id=%d", id));
		boolean delete = false;
		// 封装条件
		List<WhereCondition> whereCondList = new ArrayList<WhereCondition>();
		WhereCondition condition = new WhereCondition();
		condition.setAttributeName("TEAM_ID");
		condition.setDataType(DataStatusEnum.INT);
		condition.setMatchType(MatchTypeEnum.EQ);
		condition.setValue(id);
		whereCondList.add(condition);
		delete = adapter.delete(SystemTableEnum.DS_SYS_REPOSITORY.toString(), whereCondList, out);
		logger.debug(String.format("deleteRepository-id=%d,result=%s", id, delete));
		return delete;
	}
	
	
}
