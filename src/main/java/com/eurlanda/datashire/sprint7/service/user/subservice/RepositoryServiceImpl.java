package com.eurlanda.datashire.sprint7.service.user.subservice;

import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.entity.Repository;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.sprint7.packet.InfoPacket;
import com.eurlanda.datashire.utility.CommonConsts;
import com.eurlanda.datashire.utility.DatabaseException;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import com.eurlanda.datashire.utility.StringUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 仓库查询、创建处理类
 * 
 * @author dang.lu 2013.10.26
 *
 */
public class RepositoryServiceImpl extends AbstractService{
	
	/**
	 * 查询team下所有仓库列表
	 * @param team_id
	 * @return
	 */
	public List<Repository> getRepositoryByTeamId(int team_id) {
		Map<String, String> paramMap = new HashMap<String, String>();
		paramMap.put("TEAM_ID", Integer.toString(team_id, 10));
		return adapter.query2List(paramMap, Repository.class);
	}
	/**
	 * 查询所有仓库列表
	 * @param team_id
	 * @return
	 */
	public List<Repository> getAllRepository() {
		String sql="select * from DS_SYS_REPOSITORY";
		return adapter.query2List(sql, null, Repository.class);
	}
	/**
	 * 删除仓库
	 * @param id
	 * @return
	 */
	public boolean deleteRepositoryById(int id){
		Map<String, String> paramMap = new HashMap<String, String>();
		paramMap.put("ID", Integer.toString(id, 10));
		try {
			adapter.openSession();
			return adapter.delete(paramMap, Repository.class)>=1?true:false;
		} catch (SQLException e) {
			logger.error("删除仓库异常", e);
		}finally{
			adapter.closeSession();
		}
		return false;
	}
	
	/**
	 * 创建仓库
	 * @param repositories
	 * @param out
	 * @return
	 */
	public List<InfoPacket> add(List<Repository> repositories, ReturnValue out) {
		List<InfoPacket> infoPackets = new ArrayList<InfoPacket>();
		try {
			// 新增仓库-校验入参
			if(validate(repositories, out)){
				adapter.openSession();
				for (Repository repository : repositories) {
					// 创建物理数据库，执行SQL脚本
					createHsqlDB(repository.getRepository_db_name());
					// 系统-仓库表新增记录
					InfoPacket infoPacket = new InfoPacket();
					infoPacket.setId(adapter.insert2(repository));
					infoPacket.setType(DSObjectType.REPOSITORY);
					infoPacket.setKey(repository.getKey());
					infoPackets.add(infoPacket);
				}
			}else{
				logger.warn("校验入参失败"); // 具体前台发送数据已记录
			}
		} catch (Exception e) {
			out.setMessageCode(MessageCode.DESERIALIZATION_FAILED);
			logger.error("创建仓库异常", e);
		}finally{
			adapter.closeSession();
		}
		return infoPackets;
	}
	
	private boolean createHsqlDB(String dbName){
		String sql = StringUtils.toString(CommonConsts.Repository_SQL_PATH, "UTF-8");
		logger.debug("[创建数据库]\t"+sql);
		String[] ary = sql.split(";");
		IRelationalDataManager adapter = null; //TODO
		adapter.openSession();
		try {
			for(int i=0; i<ary.length; i++){
				if(StringUtils.isNotNull(ary[i]) 
						// 外键约束
						//&& !ary[i].trim().toLowerCase().startsWith("--alter ")
						){
					adapter.execute(ary[i].trim());
					//logger.debug("[创建数据库]\t"+adapter.execute(ary[i].trim())+"\t"+ary[i].trim());
				}
			}
			//adapter.execute(sql);
		} catch (DatabaseException e) {
			logger.error("[创建数据库异常]", e);
			return false;
		}finally{
			adapter.closeSession();
		}
		return true;
	}
	
	private boolean validate(List<Repository> repositories, ReturnValue out){
		if(repositories==null||repositories.isEmpty()){
			//repositories对象集合不能为空size不能等于0
			//异常返回
			out.setMessageCode(MessageCode.ERR_PROJECT_LIST_NULL);
			return false;
		}
		//验证数据是否完整
		for (Repository repository : repositories) {
			//验证Repository类型是否正确
			if(repository.getRepository_type()==0){
				out.setMessageCode(MessageCode.ERR_RESITORY_DBTYPE);
				return false;
			}
			//验证key是否有值
			if(StringUtils.isNull(repository.getKey())){
				out.setMessageCode(MessageCode.ERR_GUID_NULL);
				return false;
			}
			//数据库名是否为空
			if(StringUtils.isNull(repository.getRepository_db_name())){
				out.setMessageCode(MessageCode.ERR_RESITORY_DB);
				return false;
			}
			//验证Repository状态是否正确
			if(repository.getStatus() == 0){
				out.setMessageCode(MessageCode.ERR_REPOSITORY_STATUS);
				return false;
			}
			//验证Repository的TeamId是否正常、
			if(repository.getTeam_id() == 0){
				out.setMessageCode(MessageCode.ERR_REPOSITORY_TEAM_ID_NULL);
				return false;
			}
			// 物理数据库是否存在?
			
		}
		return true;
	}
	
	@Override
	public InfoPacket remove(int id) {
		return null;
	}

}