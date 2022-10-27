package com.eurlanda.datashire.sprint7.service.workspace;

import com.eurlanda.datashire.entity.RepoValidationResponse;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.server.utils.TokenUtil;
import com.eurlanda.datashire.sprint7.packet.ListMessagePacket;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.ReturnValue;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * 数据校验类，前端调用该类进行数据有效性校验，校验的具体内容为ds前缀的表(不包含ds_sys系统表),枚举类型的表，以及具有主外键约束的表
 * 
 * @version 1.0
 * @author lei.bin
 * @created 2013-09-17
 */
@Service
public class RepositoryValidationService implements IRepositoryValidationService {
	/**
	 * 对RepositoryValidationService的日志进行记录
	 */
	static Logger logger = Logger.getLogger(RepositoryValidationService.class);
	
	private String token;

	private String key;
	
	public RepositoryValidationService() {
	}
	
	public RepositoryValidationService(String token) {
		this.token = token;
	}

	/**
	 * 处理异常结果的对象
	 */
	ReturnValue out = new ReturnValue();

	/**
	 * 查询所有的元数据表信息,将校验结果返回给前端,具体操作由子类完成
	 * @param info 
	 * @return json对象
	 */
	public String queryAllTableName(String info) {
		logger.debug(String.format("queryAllTableName-strat====%s", info));
		RepositoryValidationProcess validationProcess = new RepositoryValidationProcess(
				TokenUtil.getToken());
		// 调用子类的具体操作业务
		List<RepoValidationResponse> repoValidationResponse = validationProcess
				.queryAllTableName();
		// 对返回值对象进行封装
		ListMessagePacket<RepoValidationResponse> listMessage = new ListMessagePacket<RepoValidationResponse>();
		listMessage.setCode(1);
		listMessage.setDataList(repoValidationResponse);
		listMessage.setType(DSObjectType.REPOSITORY);
		return JsonUtil.object2Json(listMessage);
	}

	/**
	 * 校验的具体内容为ds前缀的表(不包含ds_sys系统表),枚举类型的表，以及具有主外键约束的表
	 * @param info 
	 * @return json对象
	 */
	public String checkAllTable(String info) {
		logger.debug(String.format("checkAllTable-strat====%s", info));
		logger.debug("数据有效性校验进来了checkAllTable===");
		RepositoryValidationProcess validationProcess = new RepositoryValidationProcess(
				TokenUtil.getToken());
		//校验所有信息的操作类
		validationProcess.checkAllTable(info,out, key);
		//对返回值对象进行封装
		ListMessagePacket<RepoValidationResponse> listMessage = new ListMessagePacket<RepoValidationResponse>();
		listMessage.setCode(1);
		listMessage.setDataList(null);
		listMessage.setType(DSObjectType.REPOSITORY);
		return JsonUtil.object2Json(listMessage);
	}

}
