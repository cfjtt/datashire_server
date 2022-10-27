package com.eurlanda.datashire.sprint7.service.workspace;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IDBAdapter;
import com.eurlanda.datashire.entity.Repository;
import com.eurlanda.datashire.entity.operation.DataEntity;
import com.eurlanda.datashire.entity.operation.WhereCondition;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.enumeration.DataStatusEnum;
import com.eurlanda.datashire.enumeration.MatchTypeEnum;
import com.eurlanda.datashire.enumeration.SystemTableEnum;
import com.eurlanda.datashire.sprint7.packet.InfoPacket;
import com.eurlanda.datashire.sprint7.plug.GetParam;
import com.eurlanda.datashire.sprint7.plug.RepositoryPlug;
import com.eurlanda.datashire.sprint7.plug.SupportPlug;
import com.eurlanda.datashire.utility.CommonConsts;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class RepositoryService extends SupportPlug {
	static Logger logger = Logger.getLogger(RepositoryService.class);// 记录日志
	public RepositoryService(String token) {
		super(token);
	}
	/**
	 * 创建Repository对象集合
	 * 作用描述：
	 * 创建Repository集合对象业务处理
	 * 业务处理：
	 * 1、根据repositories集合批量创建repository对象
	 * 2、根据repositories集合里repository对象的key查询ID
	 * 修改说明：
	 *@param repositories Repository对象
	 *@param out 异常返回
	 *@return
	 */
	public List<InfoPacket> createRepository(List<Repository> repositories,ReturnValue out){
		logger.debug(String.format("createRepository-projectList.size()=%s", repositories.size()));
		boolean create = false;
		//切换到系统数据库
		IDBAdapter adapter = DataAdapterFactory.newInstance().getDefaultAdapter();
		//定义接收返回结果集
		List<InfoPacket> infoPackets = new ArrayList<InfoPacket>();
		try {
			RepositoryPlug plug = new RepositoryPlug(adapter);
			create = plug.createRepository(repositories, out);
			//新增成功
			if(create){
				//根据Key查询出新增的ID
				for (Repository repository : repositories) {
					//验证指定地址是否存在数据库
					if(new File(CommonConsts.HSQL_DB_PATH+repository.getRepository_db_name()).exists()){
						//物理数据库已存在
						out.setMessageCode(MessageCode.ERR_PDB_EXIST);
						return infoPackets;
					}
					//创建相应的物理数据库
					create = plug.createNewRepository(repository, out);
					if(create == false){
						//创建相应的物理数据失败
						out.setMessageCode(MessageCode.ERR_CREATE_PHYSICS_DATABASE);
						return null;
					}
					//获得Key
					String key =repository.getKey();
					int id = plug.getRepositoryId(key, out);
					InfoPacket infoPacket = new InfoPacket();
					infoPacket.setId(id);
					infoPacket.setType(DSObjectType.REPOSITORY);
					infoPacket.setKey(key);
					infoPackets.add(infoPacket);
				}
			}else{
				// rollback
				out.setMessageCode(MessageCode.ERR_CREATE_PHYSICS_DATABASE);
			}
		} catch (Exception e) {
			out.setMessageCode(MessageCode.DESERIALIZATION_FAILED);
		}finally{
			adapter.commitAdapter();
		}
		return infoPackets;
	}

	/**
	 * 注册Repository
	
	 * 作用描述：
	 * 根据前端传入的值查询是否有Repository的存在，
	 * 并验证该Repository的表结构是否完整，数据约束是否完整
	 
	
	 * 修改说明：
	 
	 *@param repository Repository对象
	 *@param out 异常处理
	 *@return
	 */
	public boolean createRegisterRepository(Repository repository,
			ReturnValue out) {
		logger.debug(String.format("createRegisterRepository-repository=%s",
				repository));
		boolean create = false;
		try {
			if (repository.isValidate()) {
				// 调用验证
				// 验证表结构是否完整，验证表约束是否存在
				RepositoryValidationProcess repositoryValidationProcess = new RepositoryValidationProcess(
						token);
				create = repositoryValidationProcess
						.checkAllTableStructure(repository,out);
			} else {
				create = true;
			}
		} catch (Exception e) {
			logger.error("createNewRepository-return=%s", e);
			out.setMessageCode(MessageCode.ERR_ARRAYS);
		} finally {
			
		}
		logger.debug(String.format("createNewRepository-return=%s", create));
		adapter.commitAdapter();
		return create;
	}
	/**
	 * 根据ID修改Repository对象集合
	 * 作用描述：
	 * 批量修改Repository信息
	 * 业务描述：
	 * 1、根据repositories集合的repository对象的ＩＤ批量修改repository对象
	 * 2、根据repositories集合里repository对象的key查询ID
	 * 修改说明：
	 *@param repositorys repository集合对象
	 *@param out 异常返回
	 *@return
	 */
	public List<InfoPacket> updateRepositorys(List<Repository> repositorys,ReturnValue out){
		logger.debug(String.format("updateRepositorys-RepositorysList.size()=%s", repositorys.size()));
		boolean create = false;
		//查询返回结果
		List<InfoPacket>  infoPackets = new ArrayList<InfoPacket>();
		try {
			//封装批量新增数据集
			List<List<DataEntity>>  paramList = new ArrayList<List<DataEntity>>();
			//封装条件集合
			List<List<WhereCondition>>  whereCondList = new ArrayList<List<WhereCondition>>();
			//调用转换类
			GetParam getParam = new GetParam();
			for (Repository repository: repositorys) {
				//参数
				List<DataEntity> dataEntitys =  new ArrayList<DataEntity>();
				getParam.getRepository(repository, dataEntitys);
				paramList.add(dataEntitys);
				//条件
				List<WhereCondition> whereConditions = new ArrayList<WhereCondition>();
				WhereCondition whereCondition = new WhereCondition();
				whereCondition.setAttributeName("ID");
				whereCondition.setDataType(DataStatusEnum.INT);
				whereCondition.setMatchType(MatchTypeEnum.EQ);
				whereCondition.setValue(repository.getId());
				whereConditions.add(whereCondition);
				whereCondList.add(whereConditions);
				
			}
			//调用批量修改
			create = adapter.updatBatch(SystemTableEnum.DS_SYS_REPOSITORY.toString(), paramList, whereCondList, out);
			logger.debug(String.format("updateRepositorys-return=%s",create ));
			
			if(create){
				RepositoryPlug repositoryPlug = new RepositoryPlug(adapter);
				
				//根据Key查询出新增的ID
				for (Repository repository : repositorys) {
					//获得Key
					String key = repository.getKey();
					InfoPacket infoPacket = new InfoPacket();
					infoPacket.setCode(1);
					infoPacket.setId(repository.getId());
					infoPacket.setType(DSObjectType.REPOSITORY);
					infoPacket.setKey(key);
					infoPackets.add(infoPacket);
				}
			}
		} catch (Exception e) {
			logger.error("updateRepositorys-return=%s",e);
			out.setMessageCode(MessageCode.ERR_ARRAYS);
		}finally{
			//释放
			adapter.commitAdapter();
		}
		
		return infoPackets;
	}

	/**
	 * 根据Team ID获得Repository对象集合信息
	 * 作用描述：
	 * 根据TeamId获得所有的Repository对象集合信息
	 * 验证：
	 * 调用方法前验证Team Id 
	 * 返回数据验证 Repository对象集合是否有数据
	 * 修改说明：
	 *@param id TeamID
	 *@param out 异常返回
	 *@return
	 */
	public Repository[] queryRepositorys(int id,ReturnValue out){
		logger.debug(String.format("queryRepositorys-id=%s", id));
		//根据Name更改路径获取数据
		//TODO 没有提供更改接口
		RepositoryPlug repositoryPlug  = new RepositoryPlug(adapter);
		Repository[] repositories = repositoryPlug.getRepository(id, out);
		if(repositories!=null){
			//查到数据返回正确Code
			out.setMessageCode(MessageCode.SUCCESS);
		}
		adapter.commitAdapter();
		return repositories;
	}
}
