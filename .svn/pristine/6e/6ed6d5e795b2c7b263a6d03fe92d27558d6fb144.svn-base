
package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.dao.ISquidDao;
import com.eurlanda.datashire.dao.ISquidFlowDao;
import com.eurlanda.datashire.dao.ISquidFlowStatusDao;
import com.eurlanda.datashire.dao.ISquidLinkDao;
import com.eurlanda.datashire.dao.IVariableDao;
import com.eurlanda.datashire.dao.impl.SquidDaoImpl;
import com.eurlanda.datashire.dao.impl.SquidFlowDaoImpl;
import com.eurlanda.datashire.dao.impl.SquidFlowStatusDaoImpl;
import com.eurlanda.datashire.dao.impl.SquidLinkDaoImpl;
import com.eurlanda.datashire.dao.impl.VariableDaoImpl;
import com.eurlanda.datashire.entity.*;
import com.eurlanda.datashire.entity.operation.BeyondSquidException;
import com.eurlanda.datashire.entity.operation.SquidDataSet;
import com.eurlanda.datashire.entity.operation.StageSquidAndSquidLink;
import com.eurlanda.datashire.entity.operation.TransformationAndCloumn;
import com.eurlanda.datashire.enumeration.DMLType;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.enumeration.MessageBubbleCode;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.server.utils.TokenUtil;
import com.eurlanda.datashire.sprint7.packet.BasicMessagePacket;
import com.eurlanda.datashire.sprint7.packet.InfoMessagePacket;
import com.eurlanda.datashire.sprint7.packet.InfoNewMessagePacket;
import com.eurlanda.datashire.sprint7.packet.InfoNewPacket;
import com.eurlanda.datashire.sprint7.packet.InfoPacket;
import com.eurlanda.datashire.sprint7.packet.ListMessagePacket;
import com.eurlanda.datashire.sprint7.packet.PushMessagePacket;
import com.eurlanda.datashire.sprint7.service.squidflow.subservice.MessageBubbleService;
import com.eurlanda.datashire.sprint7.service.squidflow.subservice.SquidFlowServicesub;
import com.eurlanda.datashire.sprint7.service.squidflow.subservice.StageSquidServicesub;
import com.eurlanda.datashire.sprint7.service.workspace.RepositoryProcess;
import com.eurlanda.datashire.utility.*;
import com.eurlanda.datashire.validator.SquidValidationTask;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;
import weibo4j.Timeline;
import weibo4j.Users;
import weibo4j.model.StatusWapper;
import weibo4j.model.User;
import weibo4j.model.WeiboException;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

/**
 * DataShirService DataShir基础数据处理Server
 * 主要处理基础数据方法从socket层映射过来的数据进行相应业务处理调用相对应的process预处理类
 * Title : 
 * Description: 
 * Author :赵春花 2013-9-4
 * update :赵春花 2013-9-4
 * Department :  JAVA后端研发部
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司
 */
@Service
public class  DataShirService  implements  IDataShirService{
	private static Logger logger = Logger.getLogger(DataShirService.class);// 记录日志
	private String token;//令牌根据令牌得到相应的连接信息
	private String key;//key值

	public String getToken() {
		return token;
	}

	public void setToken(String token) {
		this.token = token;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}


	//异常处理机制
	ReturnValue out = null;

	/**
	 * 创建Project对象
	 * 从客户端接收到json数据包
	 * 传ProjectProcess处理类
	 * 对数据包序列化成Project对象集合调用业务处理方法
	 */
	public String createProjects(String info) {
		//返回List集合的形式
		out=new ReturnValue();
		ProjectProcess process = new ProjectProcess(TokenUtil.getToken());
		Map<String, Object> map = process.createProjects(info, out);
		// 封装返回结果
		return infoNewMessagePacket(map, DSObjectType.PROJECT, out);
	}

	/**
	 * 创建SquidFlow对象
	 * 作用描述：
	 * 从客户端接收到json数据包调用SquidFlowProcess序列成SquidFlow对象集合调用业务处理类
	 * 修改说明：
	 *@param info socket层映射过来的Json数据
	 *@return 返回InfoPacket集合类型的Json串
	 */
	public String createSquidFlows(String info) {
		//返回List集合的形式
		out=new ReturnValue();
		SquidFlowProcess process = new SquidFlowProcess(TokenUtil.getToken());

		Map<String, Object> map = process.createSquidFlows(info, out);
		// 封装返回结果
		return infoNewMessagePacket(map, DSObjectType.SQUID_FLOW, out);
	}
	
	/**
	 * 创建Transformation对象
	 * 作用描述：
	 * 从客户端接收到json数据包
	 * 对数据包序列化成Transformation对象集合调用TransformationProcess处理类
	 * 修改说明：
	 *@param info socket层映射过来的Json数据
	 *@return 返回InfoPacket集合类型的Json串
	 */
	public String createTransformations(String info) {
		//返回List集合的形式
		out=new ReturnValue();
		TransformationProcess process = new TransformationProcess(TokenUtil.getToken());
		List<InfoPacket> infoPacket  = process.createTransformations(info, out);
		// 封装返回结果
		return listMessagePacket(infoPacket,DSObjectType.TRANSFORMATION,out);
	}

	/**
	 * 创建TransformationLink对象
	 * 作用描述：
	 * 从客户端接收到json数据包调用TransformationLinkProcess处理类
	 * 修改说明：
	 *@param info socket层映射过来的Json数据
	 *@return 返回InfoPacket集合类型的Json串
	 */
	public String createTransformationLinks(String info) {
		out=new ReturnValue();
		@SuppressWarnings("unchecked")
		Map<String, Object> infoMap = JsonUtil.toHashMap(info);
		TransformationLink transLink = JsonUtil.toGsonBean(infoMap.get("TransLink")+"", TransformationLink.class);
		TransformationInputs currentInput = JsonUtil.toGsonBean(infoMap.get("Input")+"",TransformationInputs.class);
		Map<String, Object> map = new HashMap<String, Object>();
		if(transLink!=null){
			map = new RepositoryServiceHelper(TokenUtil.getToken()).addTransLink(currentInput,transLink, out);
		}else{
			out.setMessageCode(MessageCode.NODATA);
		}
		return infoNewMessagePacket(map,DSObjectType.TRANSFORMATIONLINK, out);
	}
	
	/**
	 * 创建column对象
	 * 作用描述：
	 * 从客户端接收到json数据包调用ColumnProcess处理类
	 * 修改说明：
	 *@param info socket层映射过来的Json数据
	 *@return 返回InfoPacket集合类型的Json串
	 */
	public String createColumns(String info) {
		out=new ReturnValue();
		List<Column> list = JsonUtil.toGsonList(info, Column.class);	
		ListMessagePacket<InfoPacket> packet = new ListMessagePacket<InfoPacket>();	
		if(list!=null && !list.isEmpty()){
			try {
				packet.setDataList(new RepositoryServiceHelper(TokenUtil.getToken()).createColumns(list));
				packet.setCode(MessageCode.SUCCESS.value());
			} catch (Exception e) {
				// TODO Auto-generated catch block
				packet.setCode(MessageCode.ERR.value());
				e.printStackTrace();
			}
		}else{ // 反序列化失败
			packet.setCode(MessageCode.DESERIALIZATION_FAILED.value());
		}
		packet.setType(DSObjectType.COLUMN);
		return JsonUtil.object2Json(packet);
	}
	
	/**
	 * 创建column对象
	 * 作用描述：
	 * 1、将Json对象传入SquidProcess处理类接收返回结果
	 * 修改说明：
	 *@param info socket层映射过来的Json数据
	 *@return 返回InfoPacket集合类型的Json串
	 */
	public String createDBsourceSquid(String info) {
		out = new ReturnValue();
		Map<String, Object> map = new HashMap<String, Object>();
		IRelationalDataManager adapter = DataAdapterFactory.getDefaultDataManager();
		try {
			HashMap<String, Object> data=JsonUtil.toHashMap(info);
			DBSourceSquid dbSourceSquid = JsonUtil.toGsonBean(data.get("DbSourceSquid").toString(),DBSourceSquid.class);
			if (null != dbSourceSquid) {
				adapter.openSession();
				ISquidDao squidDao = new SquidDaoImpl(adapter);
				int squidId = squidDao.insert2(dbSourceSquid);
                CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(), adapter, MessageBubbleService.setMessageBubble(squidId, squidId, dbSourceSquid.getName(), MessageBubbleCode.WARN_SQUID_NO_LINK.value())));
                CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(), adapter,  MessageBubbleService.setMessageBubble(squidId, squidId, dbSourceSquid.getName(), MessageBubbleCode.ERROR_SQL_SYNTAX_FILTER.value())));
				map.put("DbSourceSquidId", squidId);
			} else { // 反序列化失败
				out.setMessageCode(MessageCode.DESERIALIZATION_FAILED);
			}
		} catch (BeyondSquidException e){
			try {
				if (adapter!=null) adapter.rollback();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			out.setMessageCode(MessageCode.ERR_SQUID_COUNT_MAX);
			logger.error("创建DBsourcesquid异常",e );
		} catch (Exception e) {
			// TODO: handle exception
			try {
				adapter.rollback();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			out.setMessageCode(MessageCode.INSERT_ERROR);
			logger.error("创建DBsourcesquid出错", e);
		} finally {
			adapter.closeSession();
		}
		return infoNewMessagePacket(map, DSObjectType.DBSOURCE, out);
	}
	
	/**
	 * 将节点拖拽生成DBsourceSquid
	 * @param dbSourceSquids
	 * @return
	 */
	public List<InfoPacket> createDBsourcesquids(List<DBSourceSquid> dbSourceSquids,IRelationalDataManager adapter2) {
		out=new ReturnValue();
		List<InfoPacket> infoPacket = null;
		List<DbSquid> dbSquids = new ArrayList<DbSquid>();
		infoPacket=new RepositoryServiceHelper(TokenUtil.getToken()).save(dbSourceSquids, DMLType.INSERT.value());
		//添加连接信息 TODO
		DBSquidService dbSquidService = new DBSquidService(TokenUtil.getToken());
		int i = 0;
		for (DBSourceSquid dbSourceSquid : dbSourceSquids) {
			int id = infoPacket.get(i).getId();
			Dbconnection dbconnection = new Dbconnection();
			DbSquid dbSquid = new DbSquid();
			dbconnection.setConnectionString(dbSourceSquid.getConnectionString());
			dbconnection.setDb_name(dbSourceSquid.getDb_name());
			dbconnection.setDb_type(dbSourceSquid.getDb_type());
			dbconnection.setHost(dbSourceSquid.getHost());
			dbconnection.setPort(dbSourceSquid.getPort());
			dbconnection.setPassword(dbSourceSquid.getPassword());
			dbconnection.setUser_name(dbSourceSquid.getUser_name());
			dbconnection.setId(id);
			dbSquid.setConnectionString(dbSourceSquid.getConnectionString());
			dbSquid.setDb_name(dbSourceSquid.getDb_name());
			dbSquid.setDb_type(dbSourceSquid.getDb_type());
			dbSquid.setHost(dbSourceSquid.getHost());
			dbSquid.setPort(dbSourceSquid.getPort());
			dbSquid.setPassword(dbSourceSquid.getPassword());
			dbSquid.setUser_name(dbSourceSquid.getUser_name());
			dbSquid.setId(id);
			dbSquids.add(dbSquid);
			dbSquidService.add(dbconnection);
			//参数
			//SourcesTable
			ConnectProcess connectProcess = new ConnectProcess(TokenUtil.getToken());
			List<SourceTable> sourceTables = connectProcess.getConnect2(dbSquids,adapter2, out);
			dbSourceSquid.setSourceTableList(sourceTables);
			i++;
		
		}
		//新增连接信息
		out.setMessageCode(MessageCode.SUCCESS);
		return infoPacket;
	}
	
	/**
	 * 创建 stage squid (客户端从工具栏直接拖拽出来的)
	 * 		涉及两张表："DS_SQUID", "DS_DATA_SQUID"
	 * @param info
	 * @return
	 */
	public String createStageSquids(String info) {
		out=new ReturnValue();
		Map<String, Object> map=new HashMap<String, Object>();
		IRelationalDataManager adapter= null;
		try {
			HashMap<String, Object> data=JsonUtil.toHashMap(info);
			StageSquid stageSquid =  JsonUtil.toGsonBean(data.get("StageSquid").toString(), StageSquid.class);
			if(null!=stageSquid ){
				adapter = DataAdapterFactory.getDefaultDataManager();
				adapter.openSession();
				ISquidDao squidDao=new SquidDaoImpl(adapter);
				int squidId=squidDao.insert2(stageSquid);
                CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(), adapter, MessageBubbleService.setMessageBubble(squidId, squidId, stageSquid.getName(), MessageBubbleCode.WARN_SQUID_NO_LINK.value())));
                CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(), adapter,  MessageBubbleService.setMessageBubble(squidId, squidId, stageSquid.getName(), MessageBubbleCode.ERROR_SQL_SYNTAX_FILTER.value())));
				map.put("StageSquidId", squidId);
			} else {
				out.setMessageCode(MessageCode.DESERIALIZATION_FAILED);
			}
		} catch (BeyondSquidException e){
			try {
				if (adapter!=null) adapter.rollback();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			out.setMessageCode(MessageCode.ERR_SQUID_COUNT_MAX);
			logger.error("创建StageSquids异常",e );
		} catch (Exception e) {
			// TODO: handle exception
			try {
				if (adapter!=null) adapter.rollback();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			out.setMessageCode(MessageCode.INSERT_ERROR);
			logger.error("创建StageSquids异常",e );
		}finally {
			if (adapter!=null) adapter.closeSession();
		}
		return infoNewMessagePacket(map, DSObjectType.STAGE, out);
	}
	
	/**
	 * 创建ExtractSquid对象
	 * 作用描述：
	 * 从客户端接收到json数据包
	 * 对数据包序列化成ExtractSquid对象集合
	 * 修改说明：
	 *@param info socket层映射过来的Json数据
	 *@return 返回InfoPacket集合类型的Json串
	 */
	public String createExtractSquids(String info) {
		out=new ReturnValue();
		List<InfoPacket> infoPacket = null;
		List<TableExtractSquid> squidList =  JsonUtil.toGsonList(info, TableExtractSquid.class);
		if(squidList!=null && !squidList.isEmpty()){
			for(TableExtractSquid e : squidList){
				if(e!=null){ // 防止前台没有设置squid类型
					e.setSquid_type(SquidTypeEnum.EXTRACT.value());
				}
			}
			
			infoPacket = new com.eurlanda.datashire.sprint7.service.squidflow.subservice.
					SquidServicesub(TokenUtil.getToken()).save(squidList, DMLType.INSERT.value());
			out.setMessageCode(MessageCode.SUCCESS);
		}else{
			out.setMessageCode(MessageCode.DESERIALIZATION_FAILED);
		}
		return JsonUtil.toString(infoPacket, DSObjectType.EXTRACT, out);
	}
	/**
	 * 批量创建tableExtract
	 * 2014-12-29
	 * @author Akachi
	 * @E-Mail zsts@hotmail.com
	 * @param info
	 */
	public String createTableExtractSquids(String info) {
		out=new ReturnValue();
		SquidService server = new SquidService(TokenUtil.getToken());
		List<InfoPacket> infoPacket = null;
		Map<String, Object> map = JsonUtil.toHashMap(info);
		List<Integer> sourceTableIds=JsonUtil.toGsonList(map.get("SourceTableIds")+"", Integer.class);
		Integer squidFlowId = Integer.parseInt(map.get("SquidFlowId").toString());
		Integer connectionSquidId = Integer.parseInt(map.get("ConnectionSquidId").toString());
		Integer x = Integer.parseInt(map.get("X").toString());
		Integer y = Integer.parseInt(map.get("Y").toString());
		server.createTableExtractSquids(sourceTableIds,connectionSquidId, squidFlowId,out,"1001","0039",TokenUtil.getKey(),x,y);
		return null;
	}
	
	/**
	 * 创建DestinationSquid对象
	
	 * 作用描述：
	 * 从客户端接收到json数据包
	 * 对数据包序列化成DestinationSquid对象集合
	 * 修改说明：
	 *@param info socket层映射过来的Json数据
	 *@return 返回InfoPacket集合类型的Json串
	 */
	public String createDestinationSquids(String info) {
		out=new ReturnValue();
		// logger.debug(String.format("createDestinationSquids-info=%s", info));
		//返回List集合的形式
		List<InfoPacket> infoPacket = new ArrayList<InfoPacket>();
		SquidProcess process = new SquidProcess(TokenUtil.getToken());
		infoPacket = process.createDestinationSquids(info, out);
		// 封装返回结果
		return JsonUtil.toString(infoPacket, DSObjectType.DBSOURCE, out);
		// logger.debug(String.format("createDestinationSquids-return=%s", jsonObject.toString()));
		// return jsonObject.toString();
	}

	/**
	 * 创建Repository对象集合
	 * 作用描述：
	 * 接到Socket传入信息调用RepositoryProcess处理类序列化Json格式信息获得Repository对象信息
	 * RepositoryProcess调用下层业务处理类新增Repository信息并获得信息返回
	 * 修改说明：
	 *@param info socket层映射过来的Json数据
	 *@return 返回InfoPacket集合类型的Json串
	 */
	public String createRepositorys(String info) {
		out=new ReturnValue();
		return JsonUtil.toString(
				new RepositoryProcess(null).createRepositorys(info, out), 
				DSObjectType.REPOSITORY, out);
	}
	/** 
	 * 修改project对象集合
	 * 作用描述：
	 * 从客户端接收到json数据包
	 * 对数据包序列化成User对象集合
	 * 传入projectProcess调用Update批roject()方法
	 *	调用方法前确保Info对象不为空
	 * 修改说明：
	 *@param info socket层映射过来的Json数据
	 *@return 返回InfoPacket集合类型的Json串
	 */
	public String updateProjects(String info){
		out=new ReturnValue();
		// logger.debug(String.format("updateProjects-info=%s", info));
		ReturnValue out = new ReturnValue();
		ProjectProcess process = new ProjectProcess(TokenUtil.getToken());
		//返回List集合的形式
		List<InfoPacket> infoPacket = new ArrayList<InfoPacket>();
		//组装值返回
		infoPacket = process.updateProjects(info, out);
		// 封装返回结果
		return listMessagePacket(infoPacket, DSObjectType.PROJECT, out);
		// logger.debug(String.format("updateProjects-return=%s", jsonObject.toString()));
		// return jsonObject.toString();
	}
	/**
	 * 修改SquidLink对象集合
	 * 作用描述：
	 * 从客户端接收到json数据包
	 * 对数据包序列化成SquidLink对象集合
	 * 传入SquidLinkProcess调用UpdateSquidLink()方法
	 *	调用方法前确保Info对象不为空
	 * 修改说明：
	 *@param info socket层映射过来的Json数据
	 *@return 返回InfoPacket集合类型的Json串
	 */
	public String updateSquidLinks(String info){
		out=new ReturnValue();
		//new SquidLinkProcess(token).updateSquidLinks(info, out);
		List<SquidLink> list = JsonUtil.toGsonList(info, SquidLink.class);
		if(list!=null && !list.isEmpty()){
			new RepositoryServiceHelper(TokenUtil.getToken()).save(list, DMLType.UPDATE.value());
		}
		return null; // 这个前台关闭界面时调用，不用返回信息
	}
	
	/**
	 * 注册一个新的Repository
	
	 * 作用描述：
	 * 获得Json数据信息调用RepositoryProcess处理类
	 * 修改说明：
	 *@param info socket层映射过来的Json数据
	 *@return 返回InfoPacket集合类型的Json串
	 */
	public String createRegisterRepositorys(String info){
		out=new ReturnValue();
		RepositoryProcess process = new RepositoryProcess(TokenUtil.getToken());
		process.createRegisterRepositorys(info, out);
		return null;
	}

	/**
	 * 修改SquidFlow对象集合
	 * 作用描述：
	 * 从客户端接收到json数据包
	 * 对数据包序列化成SquidFlow对象集合
	 * 传入SquidFlowProcess调用UpdateSquidFlow()方法
	 *	调用方法前确保Info对象不为空
	 * 修改说明：
	 *@param info socket层映射过来的Json数据
	 *@return 返回InfoPacket集合类型的Json串
	 */
	public String updateSquidFlows(String info){
		out=new ReturnValue();
		// logger.debug(String.format("updateSquidFlows-info=%s", info));
		ReturnValue out = new ReturnValue();
		SquidFlowProcess process = new SquidFlowProcess(TokenUtil.getToken());
		//返回List集合的形式
		List<InfoPacket> infoPacket = new ArrayList<InfoPacket>();
		//组装值返回
		infoPacket = process.updateSquidFlows(info, out);
		// 封装返回结果
		return listMessagePacket(infoPacket, DSObjectType.SQUID_FLOW, out);
		// logger.debug(String.format("updateSquidFlows-return=%s", jsonObject.toString()));
		// return jsonObject.toString();
	}
	
	/**
	 * 修改Transformation的坐标
	 * @param info
	 * @return
	 */
	public String updateTransLocations(String info){
		out = new ReturnValue();
		TransformationService service = new TransformationService(TokenUtil.getToken());
		Map<String, Object> map = service.updateTransLocations(info, out);
		return JsonUtil.infoNewMessagePacket(map, DSObjectType.SQUID_FLOW, out);
	}
	
	/**
	 * 修改TransformationLink对象集合
	
	 * 作用描述：
	 * 从客户端接收到json数据包
	 * 对数据包序列化成TransformationLink对象集合
	 * 传入TransformationLinkProcess调用UpdateTransformationLink()方法
	 *	调用方法前确保Info对象不为空
	 * 修改说明：
	 *@param info socket层映射过来的Json数据
	 *@return 返回InfoPacket集合类型的Json串
	 */
	public String updateTransformationLinks(String info){
		out=new ReturnValue();
		// logger.debug(String.format("updateTransformationLinks-info=%s", info));
		TransformationLinkProcess process = new TransformationLinkProcess(TokenUtil.getToken());
		///返回List集合的形式
		List<InfoPacket> infoPacket = new ArrayList<InfoPacket>();
		//组装值返回
		infoPacket = process.updateTransformationLinks(info, out);
		// 封装返回结果
		return listMessagePacket(infoPacket, DSObjectType.TRANSFORMATIONLINK, out);
		// logger.debug(String.format("updateTransformationLinks-return=%s", jsonObject.toString()));
		// return jsonObject.toString();
	}
	
	/**
	 * 修改Column对象集合
	 * 作用描述：
	 * 从客户端接收到json数据包
	 * 对数据包序列化成Column对象集合
	 * 传入Column调用UpdateColumns()方法
	 *	调用方法前确保Info对象不为空
	 * 修改说明：
	 *@param info socket层映射过来的Json数据
	 *@return 返回InfoPacket集合类型的Json串
	 */
	public String updateColumns(String info){
		out=new ReturnValue();
		Map<String,Object> infoMap = JsonUtil.toHashMap(info);
		boolean IsNeedNameCheck = Boolean.parseBoolean(infoMap.get("IsNeedNameCheck")+"");
		List<Column> list = JsonUtil.toGsonList(infoMap.get("Columns")+"",Column.class);
		logger.debug(String.format("updateColumns============================", list));
		ListMessagePacket<InfoPacket> packet = new ListMessagePacket<InfoPacket>();	
		if(list!=null && !list.isEmpty()){
			//packet.setDataList(new RepositoryServiceHelper(token).save(list, DMLType.UPDATE.value()));
			List<InfoPacket> infos = new RepositoryServiceHelper(TokenUtil.getToken()).updateColumns(list, IsNeedNameCheck,out);
			if (infos!=null&&infos.size()>0){
				packet.setDataList(infos);
				packet.setCode(out.getMessageCode().value());
			}else{
				packet.setDataList(null);
				packet.setCode(MessageCode.ERR_COLUMN_DATA_INCOMPLETE.value());
			}
		}else{ // 反序列化失败
			packet.setCode(MessageCode.DESERIALIZATION_FAILED.value());
		}
		packet.setType(DSObjectType.COLUMN);
		return JsonUtil.object2Json(packet);
	}
	
	public String updateReferedColumn(String info){
		out=new ReturnValue();
		List<ReferenceColumn> list = JsonUtil.toGsonList(info, ReferenceColumn.class);	
		logger.debug(String.format("updateColumns============================", list));
		ListMessagePacket<InfoPacket> packet = new ListMessagePacket<InfoPacket>();	
		if(list!=null && !list.isEmpty()){
			packet.setDataList(new RepositoryServiceHelper(TokenUtil.getToken()).updateReferedColumn(list));
			packet.setCode(MessageCode.SUCCESS.value());
		}else{ // 反序列化失败
			packet.setCode(MessageCode.DESERIALIZATION_FAILED.value());
		}
		packet.setType(DSObjectType.COLUMN);
		return JsonUtil.object2Json(packet);
	}

	/**
	 * 修改Squid(只修改位置，前台界面退出时调用)
	 * 作用描述：
	 * 调用SquidProcess预处理类进行验证数据并得到数据返回
	 * 修改说明：
	 *@param info socket层映射过来的Json数据
	 *@return 返回InfoPacket集合类型的Json串
	 */
	public String updateSquids(String info){
		out=new ReturnValue();
		new SquidProcess(TokenUtil.getToken()).updateSquid(info, out);
		return null;
	}
	
	/**
	 * 修改dbsourcesquid对象集合
	 * 作用描述：
	 * 从客户端接收到json数据包
	 * 对数据包序列化成dbsourcesquid对象集合
	 * 传入dbsourcesquid调用Updatedbsourcesquids()方法
	 *	调用方法前确保Info对象不为空
	 * 修改说明：
	 *@param info
	 *@return
	 */
	public String updateDBsourcesquids(String info){
		out=new ReturnValue();
		SquidProcess process = new SquidProcess(TokenUtil.getToken());
		process.updateDbSourceSquid(info, out);
		return infoNewMessagePacket(null, DSObjectType.DBSOURCE, out);
	}
	
	/**
	 * 修改extractSquid对象集合
	 * 作用描述：
	 * 从客户端接收到json数据包
	 * 对数据包序列化成extractSquid对象集合
	 * 传入extractSquid调用UpdateExtractSquids()方法
	 *	调用方法前确保Info对象不为空
	 * 修改说明：
	 *@param info
	 *@return
	 */
	public String updateExtractSquids(String info){
		out=new ReturnValue();
		SquidProcess process = new SquidProcess();
		process.updateExtractSquids(info, out);
		return infoNewMessagePacket(null, DSObjectType.EXTRACT, out);
	}
	
	/**
	 * 修改stageSquid对象集合
	 * 作用描述：
	 * 从客户端接收到json数据包
	 * 对数据包序列化成stageSquid对象集合
	 * 传入stageSquid调用UpdatestageSquids()方法
	 *	调用方法前确保Info对象不为空
	 * 修改说明：
	 *@param info
	 *@return
	 */
	public String updateStageSquids(String info){
		out=new ReturnValue();
		SquidProcess process = new SquidProcess();
		process.updateStageSquids(info, out);
		return infoNewMessagePacket(null, DSObjectType.STAGE, out);
	}
	
	/**
	 * 修改DestinationSquid对象集合
	 * 作用描述：
	 * 从客户端接收到json数据包
	 * 对数据包序列化成DestinationSquid对象集合
	 * 传入DestinationSquid调用UpdateDestinationSquids()方法
	 *	调用方法前确保Info对象不为空
	 * 修改说明：
	 *@param info
	 *@return
	 */
	public String updateDestinationSquids(String info){
		out=new ReturnValue();
		// logger.debug(String.format("updateDestinationSquids-info=%s", info));
		SquidProcess process = new SquidProcess(TokenUtil.getToken());
		///返回List集合的形式
		List<InfoPacket> infoPacket = new ArrayList<InfoPacket>();
		//组装值返回
		infoPacket = process.updateDestinationSquids(info, out);
		// 封装返回结果
		return listMessagePacket(infoPacket, DSObjectType.DBSOURCE, out);
		// logger.debug(String.format("updateColumns-return=%s", jsonObject.toString()));
		// return jsonObject.toString();
	}
	
	/**
	 * 修改Repository
	
	 * 作用描述：
	 * 修改说明：
	 *@param info
	 *@return
	 */
	public String updateRepositorys(String info){
		out=new ReturnValue();
		// logger.debug(String.format("updateRepositorys-info=%s", info));
		RepositoryProcess process = new RepositoryProcess(TokenUtil.getToken());
		//返回List集合的形式
		List<InfoPacket> infoPacket = new ArrayList<InfoPacket>();
		//组装值返回
		infoPacket = process.updateRepositorys(info, out);
		// 封装返回结果
		return listMessagePacket(infoPacket, DSObjectType.REPOSITORY, out);
		// logger.debug(String.format("updateRepositorys-return=%s", jsonObject.toString()));
		// return jsonObject.toString();
	}
	
	/**
	 * 删除Repository信息统一方法
	 * 作用描述：
	 * 根据传值过来的类型删除相应的数据有关联关系的数据一并清除
	 * 修改说明：
	 *@param info
	 *@return
	 */
	public  synchronized  String deleteRepositoryObject(String info) {
		out=new ReturnValue();
		logger.debug("deleteRepositoryObject:=%s" + info);
		DataShirProcess repositoryProcess = new DataShirProcess(TokenUtil.getToken());
		repositoryProcess.deleteRepositoryObject(info, out);
		// 组装返回值
		BasicMessagePacket infoMessagePacket = new BasicMessagePacket();
		infoMessagePacket.setCode(out.getMessageCode().value());
		return JsonUtil.object2Json(infoMessagePacket);
	}

	/**
	 * 云端删除项目接口
	 * @param info
	 * @return
     */
	public synchronized String cloudDeleteRepositoryObject(String info){
		out = new ReturnValue();
		logger.debug("deleteRepositoryObject:=%s" + info);
		DataShirProcess repositoryProcess = new DataShirProcess(TokenUtil.getToken());
		List<InfoNewPacket> infoPackets = JsonUtil.toGsonList(info, InfoNewPacket.class);
		int projectId = infoPackets.get(0).getId();
		//查询该project下面的接口信息
		IRelationalDataManager adapter = DataAdapterFactory.getDefaultDataManager();
		try {
			adapter.openSession();
			String sql = "select repository_id from ds_project where id=" + projectId;
			Project project = adapter.query2Object(true, sql, null, Project.class);
			int repositoryId = project.getRepository_id();
			infoPackets.get(0).setRepositoryId(repositoryId);
			info = JsonUtil.toJSONString(infoPackets);
			//调用接口删除项目
			repositoryProcess.deleteRepositoryObject(info, out);
			// 组装返回值
			BasicMessagePacket infoMessagePacket = new BasicMessagePacket();
			infoMessagePacket.setCode(out.getMessageCode().value());
			return JsonUtil.object2Json(infoMessagePacket);
		} catch (Exception e){
			e.printStackTrace();
			try {
				adapter.rollback();
			} catch (Exception ex){
				ex.printStackTrace();
			}
		} finally {
			adapter.closeSession();
		}
		return null;
	}

	/**
	 * 根据ProjectID获得SquidFlow信息
	 * 作用描述：
	 * 根据客户端传入的ProjectID获得SquidFlow对象集合信息
	
	 * 修改说明：
	 *@param info
	 *@return
	 */
	public String queryAllSquidFlows(String info){
		out=new ReturnValue();
		SquidFlowProcess squidFlowProcess = new SquidFlowProcess(TokenUtil.getToken());
		Map<String, Object> map = squidFlowProcess.queryAllSquidFlows(info, out);
		return infoNewMessagePacket(map, DSObjectType.SQUID_FLOW, out);
	}
	
	/**
	 * 获得Project
	 * 作用描述：
	 * 根据客户端传入的Repository对象获得Name信息查询相对应路径下的物理数据查询所有Project对象集合返回
	 * 修改说明：
	 *@param info
	 *@return
	 */
	public String queryAllProjects(String info){
		out=new ReturnValue();
		ProjectProcess projectProcess = new ProjectProcess(TokenUtil.getToken());
		Map<String, Object> map = projectProcess.queryAllProjects(info, out);
		return infoNewMessagePacket(map, DSObjectType.PROJECT, out);
	}
	
	/**
	 * 获得所有的Repository对象集合
	 * 作用描述：
	 * 根据客户端传入的TEAMID信息查询相对应的Repostory对象集合信息返回
	 * 修改说明：
	 *@param info
	 *@return
	 */
	public String querylAllRepositorys(String info){
		out=new ReturnValue();
		RepositoryProcess repositoryProcess = new RepositoryProcess(TokenUtil.getToken());
		Repository[] repositories = repositoryProcess.querylAllRepositorys(info, out);
		return JsonUtil.toString(StringUtils.asList(repositories), DSObjectType.REPOSITORY, out);
	}
	
	/**
	 * 创建ExtractSquid
	 * 作用描述：
	 * 根据传入的ExtractSquid对象和SquidLink对象创建ExtractSquid信息并将相应的关联数据返回
	 * 修改说明：
	 *@param info
	 *@return
	 */
	public String createMoreExtractSquid(String info){
		out = new ReturnValue();
		return new ExtractDBSquidService(TokenUtil.getToken(), TokenUtil.getKey()).dragTableName2NewExtractSquid(info, out);
	}
	
	/** 创建目标列和对应的虚拟变换(从引用列拖拽导入) */
	public String createMoreStageColumn(String info){
		out=new ReturnValue();
		ListMessagePacket<InfoPacket> packet = new ListMessagePacket<InfoPacket>();	
		List<TransformationAndCloumn> list = JsonUtil.toGsonList(info, TransformationAndCloumn.class);	
		if(list!=null && !list.isEmpty()){
//			packet.setDataList(new RepositoryServiceHelper(token).importColumns(list));
			List<InfoPacket> infos = new RepositoryServiceHelper(TokenUtil.getToken()).dragColumns(list, out);
			if (infos!=null){
				packet.setDataList(infos);
				packet.setCode(MessageCode.SUCCESS.value()); // 仅表示程序执行正常，不代表数据添加/更新成功
			} else {
				packet.setCode(MessageCode.ERR_COLUMN_DATA_INCOMPLETE.value());
			}
		} else{
			// 反序列化失败
			packet.setCode(MessageCode.DESERIALIZATION_FAILED.value());
		}
		packet.setType(DSObjectType.TRANSFORMATION);
		return JsonUtil.object2Json(packet);
	}
	
	/**
	 * Reload SquidModelBase Column的信息
	 * @param info
	 * @return
	 * @author bo.dang
	 * @date 2014年5月27日
	 */
	public String reloadColumnsBySquidId(String info){
		out=new ReturnValue();
	    Map<String, Object> infoMap = JsonUtil.toHashMap(info);
	    // 得到SquidId
	    int squidId = Integer.parseInt(infoMap.get("SquidId").toString(), 10);
	    InfoNewMessagePacket<Map<String, Object>> infoMessage = new InfoNewMessagePacket<Map<String,Object>>();
	    Map<String, Object> resultMap = new RepositoryServiceHelper(TokenUtil.getToken()).reloadColumnsBySquidId(squidId, out);
	    if(out.getMessageCode()==MessageCode.SUCCESS){
	    	infoMessage.setInfo(resultMap);
	    }
	    infoMessage.setCode(out.getMessageCode().value());
        infoMessage.setDesc("Reload SquidModelBase Column的信息");
        return JsonUtil.object2Json(infoMessage);
	}
	
	/**
	 * 创建StageSquid信息
	 * 作用描述：
	 * 接收到Socket层传入的JSon信息传入DataShirProcess预处理类的createMoreStageSquidAndSquidLink()验证数据完整性
	 * 修改说明：
	 *@param info
	 *@return
	 */
	public String createMoreStageSquidLink(String info){
		out=new ReturnValue();
		StageSquidAndSquidLink squidAndSquidLink = JsonUtil.toGsonBean(info, StageSquidAndSquidLink.class);
		new StageSquidServicesub(TokenUtil.getToken()).link2StageSquid(squidAndSquidLink);
		return infoMessagePacket(squidAndSquidLink,DSObjectType.SQUIDLINK,out);
	}

	/**
	 * 从datasquid的目标列选择若干column放到stagesquid上 
	 * @param info
	 * @return
	 */
	public String drag2StageSquid(String info){
		out=new ReturnValue();
		StageSquidAndSquidLink vo = JsonUtil.toGsonBean(info, StageSquidAndSquidLink.class);
		InfoMessagePacket<StageSquidAndSquidLink> packet = new InfoMessagePacket<StageSquidAndSquidLink>();
		packet.setType(DSObjectType.STAGE);
		if(vo!=null){
			new StageSquidServicesub(TokenUtil.getToken()).drag2StageSquid(vo);
			packet.setInfo(vo);
		}else{
			packet.setCode(MessageCode.DESERIALIZATION_FAILED.value());
		}
		return JsonUtil.object2Json(packet);
	}
	
	/**
	 * 根据SquidFlowID查询Squid And SquidLink
	 * 作用描述：
	 * 接收到Socket层传入的JSon信息传入RepositoryProcess预处理类进行数据序列化以及数据验证
	 * 该类使用数据推送机制
	 * 修改说明：
	 *@param info
	 *@return
	 */
	public String querySquidAndSquidLink(String info){
		out=new ReturnValue();
		DataShirProcess repositoryProcess = new DataShirProcess(TokenUtil.getToken(), TokenUtil.getKey());
		int count = repositoryProcess.queryAllSquidAndSquidLink(info, out);
		return JsonUtil.toString(count, DSObjectType.SQUID, out);
	}
	
	/**
	 * 获得变幻序列中的数据
	 * 作用描述：
	 * 修改说明：
	 *@author
	 *@param info
	 *@return
	 */
	public String queryRuntimeData(String info){
		out=new ReturnValue();
		DataShirProcess repositoryProcess = new DataShirProcess(TokenUtil.getToken());
		SquidDataSet squidDataSet = repositoryProcess.queryRuntimeData(info, out);
		InfoMessagePacket<SquidDataSet> infoMessagePacket = new InfoMessagePacket<SquidDataSet>();
		infoMessagePacket.setCode(out.getMessageCode().value());
		infoMessagePacket.setInfo(squidDataSet);
		infoMessagePacket.setType(DSObjectType.UNKNOWN);
		return JsonUtil.object2Json(infoMessagePacket);
	}
	
	/**
	 * 得数据
	 * 作用描述：
	 * 修改说明：
	 *@param info
	 *@return
	 */
	public String queryData(String info){
		out=new ReturnValue();
		DataShirProcess repositoryProcess = new DataShirProcess(TokenUtil.getToken());
		Map<String, Object> map = repositoryProcess.queryDatas(info, out);
		return infoNewMessagePacket(map, DSObjectType.SQUID, out);
	}
	
	/**
	 * 根据SquidFlowId调用变换引擎接口，运行一个SquidFlow
	 * @param info
	 * @return
	 */
	public String runSquidFlow(String info) {
		out=new ReturnValue();
		PkgRunSquidFlow pkgRunSquidFlow = JsonUtil.toGsonBean(info, PkgRunSquidFlow.class);
		int squidFlowId=pkgRunSquidFlow.getSquidFlowId();
		int repositoryId=pkgRunSquidFlow.getRepositoryId();
		String taskId="";
		if(squidFlowId>0&&repositoryId>0){
			//taskId=new RPCUtils().postLaunchEngineJob(0, squidFlowId, repositoryId);
			//SquidFlow sf = new com.eurlanda.datashire.sprint7.service.squidflow.subservice.
					//SquidFlowServicesub(token).getSquidFlow(obj.getId());
			//logger.debug("\r\n"+JsonUtil.object2FmtJson(sf));
			//TSquidFlow tf = new SquidFlowConvertor(sf).getSquidFlow();
			//logger.debug("\r\n"+JsonUtil.object2FmtJson(tf));
			// TODO
			//com.eurlanda.datashire.engine.client.Client.getInstance().launch(sf);
		}else{
			logger.error("运行squid flow反序列化失败！"+info);
		}
		return taskId;
	}

	/**
	 * 根据前端传入的信息进行SquidJoin的创建
	 * @author lei.bin
	 * @return SpecialSquidJoin
	 */
	public String createSquidJoin(String info) {
		out=new ReturnValue();
		JoinProcess joinProcess = new JoinProcess(TokenUtil.getToken());
		//调用SquidJoin新增处理类
		List<SpecialSquidJoin> squidJoins = joinProcess.createSquidJoin(info,
				out);
		return listMessagePacket(squidJoins,
				DSObjectType.SQUIDJOIN, out);
		// return jsonObject.toString();
	}

	/**
	 * 根据前端传入的信息进行SquidJoin的更新
	 * 
	 * @author lei.bin
	 * @return SpecialSquidJoin
	 */
	public String updateSquidJoin(String info) {
		out=new ReturnValue();
		JoinProcess joinProcess = new JoinProcess(TokenUtil.getToken());
		//调用SquidJoin更新处理类
		List<SpecialSquidJoin> squidJoins = joinProcess.updateSquidJoin(info,
				out);
		return listMessagePacket(squidJoins,
				DSObjectType.SQUIDJOIN, out);
		// return jsonObject.toString();
	}
	
	/**
	 * 创建SquidLink同时添加Join信息
	
	 * 作用描述：
	 * 创建SquidLink，触发创建squidJoin
	
	 * 修改说明：
	 *@param info
	 *@return
	 */
	public String createSquidLinks(String info){
		out=new ReturnValue();
		SquidLinkProcess squidLinkProcess = new SquidLinkProcess(TokenUtil.getToken());
		List<InfoPacket> infoPackets = squidLinkProcess.createSquidLinksJoin(info, out);
		return listMessagePacket(infoPackets,
				DSObjectType.SQUIDLINK, out);
		// return jsonObject.toString();
	}

	/**
	 * 根据前端传送的dbsquid信息,查询数据源信息，存储DS_SOURCE_TABLE，DS_SOURCE_COLUMN,
	 * 并且将表和列的信息返回前端
	 * @param info
	 * @author binlei
	 * @return
	 * @throws Exception 
	 */
	public String createConnect(String info) throws Exception {
		Timer timer = new Timer();
		try {
			out = new ReturnValue();
			key = TokenUtil.getKey();
			token = TokenUtil.getToken();
			final Map<String,Object> returnMap = new HashMap<>();
			returnMap.put("1",1);
			timer.schedule(new TimerTask() {
				@Override
				public void run() {
					PushMessagePacket.pushMap(returnMap,DSObjectType.SQUID_FLOW,"0001","0102",key,token,MessageCode.BATCH_CODE.value());
				}
			},25*1000,25*1000);
			ConnectProcess connectProcess = new ConnectProcess(TokenUtil.getToken(), TokenUtil.getKey());
			List<SourceTable> sourceTables = connectProcess.getConnect(info, 1, out);
			// 对返回值对象进行封装
			ListMessagePacket<SourceTable> listMessage = new ListMessagePacket<SourceTable>();
			listMessage.setCode(out.getMessageCode().value());
			if (sourceTables == null) {
				sourceTables = new ArrayList<>();
			}
			listMessage.setDataList(sourceTables);
			return JsonUtil.object2Json(listMessage);
		} catch (Exception e){
			e.printStackTrace();
		} finally {
			timer.purge();
			timer.cancel();
		}
		return null;
	}
	
	/**
	 * 拖拽source column集合到空的extract，并生成transformation (或link)
	 * @param info
	 * @return
	 */
	public String drag2EmptyExtractSquid(String info){
		out=new ReturnValue();
		ExtractDBSquidService service = new ExtractDBSquidService(TokenUtil.getToken(), TokenUtil.getKey());
		String ret = service.drag2EmptyExtractSquid(info, out);
		this.setKey(service.getKey());
		return ret;
	}
	
	/**
	 * 拖拽source column集合到空白区，并生成extract、link和transformation
	 * @param info
	 * @return
	 */
	public String drag2NewExtractSquid(String info){
		out=new ReturnValue();
		return new ExtractDBSquidService(TokenUtil.getToken(), TokenUtil.getKey()).drag2NewExtractSquid(info, out);
	}
	
	
	/**
	 * 单对象转换成Json格式
	 * 作用描述：
	 * 修改说明：
	 * @param <T>
	 *@param object
	 *@return
	 *	 *@deprecated 请参考 JsonUtil.toString(...)
	 */
	private <T> String infoMessagePacket(T object,DSObjectType type,ReturnValue out) {
		return JsonUtil.toString(object, type, out);
	}
	
	/**
	 * List类型数据封装
	 * 作用描述：
	 * 将返回List类型的业务处理后数据封装成Json格式封装返回给Socket层
	 * 修改说明：
	 *@param dataList 数据集合
	 *@param type 返回数据的相应的数据类型
	 *@param out 异常处理
	 *@return 返回相应的JSon格式
	 *@deprecated 请参考 JsonUtil.toString(...)
	 */
	private <T> String listMessagePacket(List<T> dataList,DSObjectType type,ReturnValue out) {
		return JsonUtil.toString(dataList, type, out);
	}
	
	/**
	 * 单对象转换成Json格式
	 * 作用描述：
	 * 修改说明：
	 * @param <T>
	 * @return
	 */
	private <T> String infoNewMessagePacket(T object,DSObjectType type,ReturnValue out) {
		return JsonUtil.toJsonString(object, type, out.getMessageCode());
	}
	
	/**
	 * 拖动transformation打断transformationlink
	 * 先删除原先的link,然后创建2个新的link
	 * @return
	 */
	public String drapTransformationAndLink(String info)
	{
		out=new ReturnValue();
		TransAndLinkspProcess transAndLinkspProcess=new TransAndLinkspProcess(TokenUtil.getToken());
		List<SpecialTransformationAndLinks> specialTransformationAndLinks=transAndLinkspProcess.drapTransformationAndLink(info, out);
		return listMessagePacket(specialTransformationAndLinks,
				DSObjectType.TRANSFORMATION, out);
		// return jsonObject.toString();
	}

	/**
	 * 根据前端传送过来的ReferenceColumnGroup进行批量更新
	 * @param info
	 * @return
	 */
	public String updateReferenceColumnGroup(String info) {
		out=new ReturnValue();
		ReferenceColumnGroupProcess columnGroupProcess = new ReferenceColumnGroupProcess(
				TokenUtil.getToken());
		List<InfoPacket> infoPackets = columnGroupProcess
				.updateReferenceColumnGroup(info, out);
		return listMessagePacket(infoPackets, DSObjectType.COLUMNGROUP, out);
	}
	
	/**
	 * 查询所有squid key
	 * @param info
	 * @return
	 * @throws Exception 
	 */
	public String getAllSquidKey(String info) throws Exception {
		out=new ReturnValue();
		IRelationalDataManager adapter = DataAdapterFactory.getDefaultDataManager();
		//InfoPacket obj = JsonUtil.json2Object(info, InfoPacket.class, FieldNamingPolicy.UPPER_CAMEL_CASE);
		Map<String, Object> map = JsonUtil.toHashMap(info);
		int squidFlowId = Integer.parseInt(map.get("SquidFlowId").toString());
		int project_id=Integer.parseInt(map.get("ProjectId").toString());
		int repository_id=Integer.parseInt(map.get("RepositoryId").toString());
		Map<String, Object> countMap = new HashMap<String, Object>();
		try {
			if(squidFlowId>0){
				adapter.openSession();
				ISquidFlowDao squidFlowDao = new SquidFlowDaoImpl(adapter);
				ISquidLinkDao squidLinkDao = new SquidLinkDaoImpl(adapter);
				ISquidFlowStatusDao squidFlowStatusDao = new SquidFlowStatusDaoImpl(adapter);
				IVariableDao variableDao = new VariableDaoImpl(adapter);
				int count = squidFlowDao.getSquidCountBySquidFlow(squidFlowId);
				logger.info("SquidFlow:"+squidFlowId+", squidCount:"+count);
				countMap.put("Count", count);
				//添加squidLink集合
				List<SquidLink> lists = squidLinkDao.getSquidLinkListBySquidFlow(squidFlowId);
				if (lists==null){
					lists = new ArrayList<SquidLink>();
				}
				countMap.put("SquidLinks", lists);
				//查询变量集合
				List<DSVariable> variables = variableDao.getDSVariableByScope(1, squidFlowId);
				if (variables==null){
					variables = new ArrayList<DSVariable>();
				}
				countMap.put("Variables", variables);
				//查询squidFlow的状态
				List<SquidFlowStatus> list= squidFlowStatusDao.getSquidFlowStatusBySquidFlow(repository_id, squidFlowId);
				if(StringUtils.isNotNull(list) && !list.isEmpty()){
					Map<String, Object> calcMap=CalcSquidFlowStatus.calcStatus(list);
					if(Integer.parseInt(calcMap.get("checkout").toString())>0
							||Integer.parseInt(calcMap.get("schedule").toString())>0){
						//表示该squidflow已经被加锁
						out.setMessageCode(MessageCode.WARN_GETSQUIDFLOW);
						logger.info("getAllSquidKey: "+JsonUtil.infoNewMessagePacket(countMap, DSObjectType.SQUID_FLOW, out));
						return JsonUtil.infoNewMessagePacket(countMap, DSObjectType.SQUID_FLOW, out);
					}
				}
				//调用squidFlow加锁
				boolean flag = squidFlowDao.getLockOnSquidFlow(squidFlowId, project_id, repository_id, 1, TokenUtil.getToken());
				if (flag){
					LockSquidFlowProcess lockSquidFlowProcess = new LockSquidFlowProcess(TokenUtil.getToken());
					lockSquidFlowProcess.sendAllClient(repository_id, squidFlowId, 1);
				}
				return JsonUtil.infoNewMessagePacket(countMap, DSObjectType.SQUID_FLOW, out);
			}else{
				out.setMessageCode(MessageCode.DESERIALIZATION_FAILED);
				logger.error("getAllSquidKey反序列化失败！"+info);
			}
		} catch (Exception e) {
			adapter.rollback();
			e.printStackTrace();
			out.setMessageCode(MessageCode.SQL_ERROR);
			logger.error("getAllSquidKey异常！", e);
		}finally{
			adapter.closeSession();
		}
		logger.info("getAllSquidKey： "+JsonUtil.infoNewMessagePacket(countMap, DSObjectType.SQUID_FLOW, out));
		return JsonUtil.infoNewMessagePacket(countMap, DSObjectType.SQUID_FLOW, out);
	}
	
	/**
	 * 查询所有squid
	 * @param info
	 * @return
	 */
	public String pushAllSquid(String info)throws Exception {
		out = new ReturnValue();
		//InfoPacket obj = JsonUtil.json2Object(info, InfoPacket.class, FieldNamingPolicy.UPPER_CAMEL_CASE);
		Map<String, Object> map = JsonUtil.toHashMap(info);
		int squidFlowId = Integer.parseInt(map.get("SquidFlowId")
				.toString());
		int repository_id=Integer.parseInt(map.get("RepositoryId")
				.toString());

		if(squidFlowId>0){
			/*new com.eurlanda.datashire.sprint7.service.squidflow.subservice.
					SquidFlowServicesub(token).pushAllSquid(key, squidFlowId,repository_id,map);*/
			new com.eurlanda.datashire.sprint7.service.squidflow.subservice.
					SquidFlowServicesub(TokenUtil.getToken()).pushAllSquid(TokenUtil.getKey(), squidFlowId,repository_id);
			// 推送所有的消息泡
//			boolean isPush = Boolean.parseBoolean(SysConf.getValue("executeValidationTaskFlag"));
//			if (isPush){
//				new com.eurlanda.datashire.sprint7.service.squidflow.subservice.
//	                    SquidFlowServicesub(token).pushAllMessageBubble(key, squidFlowId);
//			}
		}else{
			logger.error("加载squid flow反序列化失败！"+info);
		}
		return null;
	}
	
	/**
	 * 查询所有squid link
	 * @param info
	 * @return
	 * @throws Exception 
	 */
	/*public String getAllSquidLink(String info) throws Exception {
		out=new ReturnValue();
		//InfoPacket obj = JsonUtil.json2Object(info, InfoPacket.class, FieldNamingPolicy.UPPER_CAMEL_CASE);
		Map<String, Object> map = JsonUtil.toHashMap(info);
		int squidFlowId = Integer.parseInt(map.get("SquidFlowId").toString());
		Map<String, Object> linksMap = new HashMap<String, Object>();
		if(squidFlowId>0){
			IRelationalDataManager adapter = null;
			try {
				adapter = DataAdapterFactory.getDefaultDataManager();
				adapter.openSession();
				ISquidLinkDao squidLinkDao = new SquidLinkDaoImpl(adapter);
				List<SquidLink> lists = squidLinkDao.getSquidLinkListBySquidFlow(squidFlowId);
				linksMap.put("SquidLinks", lists);
				return JsonUtil.infoNewMessagePacket(linksMap, DSObjectType.SQUID_FLOW, out);
			} catch (Exception e) {
				out.setMessageCode(MessageCode.SQL_ERROR);
				e.printStackTrace();
			} finally{
				adapter.closeSession();
			}
		}else{
			out.setMessageCode(MessageCode.DESERIALIZATION_FAILED);
			logger.error("getAllSquidLink反序列化失败！"+info);
		}
		return JsonUtil.infoNewMessagePacket(linksMap, DSObjectType.SQUID_FLOW, out);
	}*/
	
	/**
	 * 新浪微博相关
			weibo.put("QUERYUSER_BY_UID", "1170673445"); // 根据UID查询用户信息
			weibo.put("QUERYUSER_BY_DOMAIN", "billhe"); // 根据个性化域名查询用户信息
			weibo.put("SEARCHWEIBO_BY_KEYWORD", "大数据"); // 根据(#微博话题#)关键字搜索微博
			weibo.put("GET_HOMEWEIBO", null);   // 获取当前登录用户及其所关注用户的最新微博消息
	 * @param info
	 * @return
	 */
	public String handleSinaWeibo(String info) {
		out=new ReturnValue();
		CommonSocketPacket p = FastJson.json2Object(info, CommonSocketPacket.class);
				//JsonUtil.json2Object(info, CommonSocketPacket.class, FieldNamingPolicy.UPPER_CAMEL_CASE);
		Map<String, String> map = p==null?null:p.getMap();
		out = new ReturnValue();
		if(map!=null && !map.isEmpty()){
			String access_token = "2.00ldBORBqht9DE4df836d00fplgmfC";
			if(map.containsKey("QUERYUSER_BY_UID")){
				Users um = new Users();
				um.client.setToken(access_token);
				try {
					User user = um.showUserById(map.get("QUERYUSER_BY_UID"));
					user.setKey(p.getKey());
					logger.debug("QUERYUSER_BY_UID = "+map.get("QUERYUSER_BY_UID")+"\r\n"+user);
					return FastJson.toString(user, DSObjectType.WEIBO_USER, out);
				} catch (WeiboException e) {
					logger.error("handleSinaWeibo"+info,e);
				}
			}
			else if(map.containsKey("QUERYUSER_BY_DOMAIN")){
				Users um = new Users();
				um.client.setToken(access_token);
				try {
					User user = um.showUserByDomain(map.get("QUERYUSER_BY_DOMAIN"));
					user.setKey(p.getKey());
					return FastJson.toString(user, DSObjectType.WEIBO_USER, out);
				} catch (WeiboException e) {
					logger.error("handleSinaWeibo"+info,e);
				}
			}
			else if(map.containsKey("GET_HOMEWEIBO")){
				Timeline tm = new Timeline();
				tm.client.setToken(access_token);
				StatusWapper status = null;
				try {
					status = tm.getHomeTimeline();
					if(status!=null&&status.getStatuses()!=null&&!status.getStatuses().isEmpty()){
						for(int i=0; i<status.getStatuses().size(); i++){
							status.getStatuses().get(i).setKey(p.getKey());
							//status.getStatuses().get(i).setId("0"); // 前台ID都是统一的int类型？（而且子类还不能覆盖父类？）
						}
					}
					return FastJson.toString(status.getStatuses(), DSObjectType.WEIBO_LIST, MessageCode.SUCCESS);
				} catch (WeiboException e) {
					logger.error("handleSinaWeibo"+info,e);
				}
			}
		}else{
			logger.error("handleSinaWeibo反序列化失败！"+info);
		}
		out.setMessageCode(MessageCode.NODATA);
		return FastJson.toString(null, DSObjectType.WEIBO_ERROR, out);
	}
	
	/**
	 * 位置信息更新(前台定期延时发送)
	 * @param info
	 * @return
	 */
	public String updateLocation(String info) {
		out=new ReturnValue();
		SquidFlowServicesub servicesub=new SquidFlowServicesub(TokenUtil.getToken());
		servicesub.updateLocation(info, out);
		return infoNewMessagePacket(null, DSObjectType.SQUID_FLOW, out);
	}

	/**
	 * 更新编译状态
	 * @param info
	 * @return
	 * @author bo.dang
	 * @date 2014年6月20日
	 */
	public String updateSquidFlowCompilationStatus(String info){
	    Map<String, Object> infoMap = JsonUtil.toHashMap(info);
	    int squidFlowId = Integer.parseInt(infoMap.get("SquidFlowId").toString(), 10);
	    SquidFlowService squidFlowService = new SquidFlowService(TokenUtil.getToken());
	    Boolean updateFlag = squidFlowService.updateSquidFlowCompilationStatus(squidFlowId, out);
        // 更新XmlExtractSquid
        InfoNewMessagePacket<Map<String, Object>> infoMessage = new InfoNewMessagePacket<Map<String, Object>>();
        Map<String, Object> resultMap = new HashMap<String, Object>();
        resultMap.put("UpdateFlag", updateFlag);
        infoMessage.setInfo(resultMap);
        infoMessage.setDesc("更新编译状态");
        return JsonUtil.object2Json(infoMessage);
	}

	/**
	 * 通过transformation创建column和link
	 * @param info
	 * @return
	 */
	public String createColumnAndLinkByTrans(String info) {
		TransformationService transformationService = new TransformationService(TokenUtil.getToken());
		ReturnValue out = new ReturnValue();
		Map<String, Object> map = transformationService.createColumnAndLinkByTrans(info, out);

		return JsonUtil.infoNewMessagePacket(map,DSObjectType.COLUMN,out);
	}

	/**
	 * 创建ID Column
	 * @param info
	 * @return
     */
	public String createIDColumn(String info){
		TransformationService transformationService = new TransformationService(TokenUtil.getToken());
		ReturnValue out = new ReturnValue();
		Map<String, Object> map = null;
		try {
			map = transformationService.createIDColumn(info, out);
		} catch (Exception e) {
			e.printStackTrace();
		}

		InfoNewMessagePacket infoPacket = new InfoNewMessagePacket(map, out.getMessageCode().value());
		return infoPacket.toJsonString();
	}

	public String createExtractionDateColumn(String info){
		TransformationService transformationService = new TransformationService(TokenUtil.getToken());
		ReturnValue out = new ReturnValue();

		Map<String, Object> map = transformationService.createExtractionDateColumn(info, out);
		InfoNewMessagePacket infoPacket = new InfoNewMessagePacket(map, out.getMessageCode().value());
		return infoPacket.toJsonString();
	}

	/**
	 * 自动匹配referenceColumn与column，名字，类型，都一直，且column
	 * @param info
	 * @return
	 */
	public String autoMatchColumnByNameAndType(String info) {
		ColumnService columnService = new ColumnService(TokenUtil.getToken());
		ReturnValue out = new ReturnValue();
		List<TransformationLink> links = columnService.autoMatchColumnByNameAndType(info, out);
		Map<String, Object> map = new HashMap<>();
		map.put("NewLinkList", links);
		return JsonUtil.infoNewMessagePacket(map, DSObjectType.TRANSFORMATIONLINK,out);
	}

	/**
	 * 复制粘贴column 
	 * @param info
	 * @return
	 */
	public String copySquidColumns(String info) {
		ColumnService columnService = new ColumnService(TokenUtil.getToken());
		ReturnValue out = new ReturnValue();
		Map<String, Object> map = columnService.copySquidColumns(info, out);
		return JsonUtil.infoNewMessagePacket(map, DSObjectType.COLUMN,out);
	}

	public static void main(String[] args) {
		String ss = "a";
		try {
			ss(ss);
		} catch (UnsupportedOperationException e){
			logger.error("", e);
			System.out.println("a");
		} catch (Exception e) {
			// TODO: handle exception
		}
	}
	
	private static void ss(String ss) throws SQLException{
		if (ss==""){
		}else if (ss=="a"){
			throw new UnsupportedOperationException("");
		}else{
		}
	}

	/**
	 * 删除Squidflow内所有的Squid
	 * @param info
	 * @return
     */
	public String deleteAllSquidsInSquidflow(String info){
		DataShirProcess process = new DataShirProcess(TokenUtil.getToken());
		return process.deleteAllSquidsInSquidflow(info);
	}


}
