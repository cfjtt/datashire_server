package com.eurlanda.datashire.sprint7.service.metadata;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.entity.*;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.enumeration.DataBaseType;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.enumeration.TransformationTypeEnum;
import com.eurlanda.datashire.exception.SystemErrorException;
import com.eurlanda.datashire.sprint7.packet.InfoPacket;
import com.eurlanda.datashire.sprint7.service.squidflow.ConnectProcess;
import com.eurlanda.datashire.sprint7.service.squidflow.DataShirService;
import com.eurlanda.datashire.sprint7.service.squidflow.SquidService;
import com.eurlanda.datashire.sprint7.service.squidflow.subservice.ColumnServicesub;
import com.eurlanda.datashire.sprint7.service.squidflow.subservice.TransformationServicesub;
import com.eurlanda.datashire.utility.DatabaseException;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import com.eurlanda.datashire.utility.StringUtils;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 树实现
 * @author chunhua.zhao
 * @version 1.0
 * @created 09-一月-2014 14:39:03
 */

public class MetadataTreeServiceImpl  implements IMetadataTreeService{
	protected static IRelationalDataManager adapter =  DataAdapterFactory.getDefaultDataManager();
	protected static String token;
	public MetadataTreeServiceImpl(){
		
		
	}
	/*public MetadataTreeServiceImpl(String token){
		super(token);
		this.token = token;
	}*/
	
	public void finalize() throws Throwable {

	}
	
	public String getToken() {
		return token;
	}

	public void setToken(String token) {
		
		this.token = token;
	}

	@Override
	public List<MetadataNode> addNode(List<MetadataNode> nodes){
		try {
			adapter.openSession();
			//新增MetadatNode表信息
			for (MetadataNode metadataNode : nodes) {
				int nodId = inserNode(metadataNode);
				List<MetadataNode> metadataNodeList = metadataNode.getMetadataNodeList();
				if(metadataNodeList != null){
					//子节点
					for (MetadataNode node : metadataNodeList) {
						node.setParentNodeId(nodId);
						 inserNode(node);
					}
				}
			}
			
		}catch (EnumConstantNotPresentException e) {
			throw new SystemErrorException(MessageCode.ERR_ENUM,"枚举错误");
		} catch (Exception e) {
			throw new SystemErrorException(MessageCode.ERR_EXECUTE,"异常错误");
		}finally{
			adapter.closeSession();
		}
		return nodes;
	}

	/**
	 * 新增节点
	 * @param metadataNode
	 * @return
	 * @throws SQLException
	 */
	private int inserNode(MetadataNode metadataNode) throws SQLException {
		DsSysMetadataNode dsSysMetadataNode = new DsSysMetadataNode();
		Timestamp date = new Timestamp(System.currentTimeMillis());
		metadataNode.setAddDate(date.toString());
		dsSysMetadataNode.setCreationDate(date);
		dsSysMetadataNode.setNodeDesc(metadataNode.getDesc());
		dsSysMetadataNode.setNodeName(metadataNode.getNodeName());
		dsSysMetadataNode.setNodeType(metadataNode.getNodeType());
		dsSysMetadataNode.setOrderNumber(metadataNode.getOrderNumber());
		dsSysMetadataNode.setParentId(metadataNode.getParentNodeId());
		dsSysMetadataNode.setKey(metadataNode.getKey());
		dsSysMetadataNode.setColumnAttribute(metadataNode.getColumnAttribute());
		int result = adapter.insert2(dsSysMetadataNode);
		metadataNode.setId(result);
		//新增
		addDatabaseAttr(metadataNode, result);
		return result;
	}

	/**
	 * 新增连接信息
	 * @param node
	 * @param dsSysMetadataNode
	 * @throws SQLException
	 */
	private void addDatabaseAttr(MetadataNode node,
			int nodeId) throws SQLException {
		DsSysMetadataNodeAttr dsSysMetadataNodeAttr = new DsSysMetadataNodeAttr();
		Map<String, String> map = node.getAttributeMap();
		if(map != null && map.size()>0){
		for (String key : map.keySet()) {
		    String value = map.get(key);
		    dsSysMetadataNodeAttr.setAttrName(key);
			dsSysMetadataNodeAttr.setAttrValue(value);
			Timestamp date = new Timestamp(System.currentTimeMillis());
			dsSysMetadataNodeAttr.setCreationDate(date);
			dsSysMetadataNodeAttr.setMetadataNodeId(nodeId);
			int result = adapter.insert2(dsSysMetadataNodeAttr);
		}
		}
	}

	private void renameDatabaseAttr(MetadataNode node,
			int nodeId) throws SQLException, DatabaseException {
		DsSysMetadataNodeAttr dsSysMetadataNodeAttr = new DsSysMetadataNodeAttr();
		Map<String, String> map = node.getAttributeMap();
		if(map.size()>0){
			for (String key : map.keySet()) {
			    String value = map.get(key);
			    dsSysMetadataNodeAttr.setAttrName(key);
				dsSysMetadataNodeAttr.setAttrValue(value);
				Timestamp date = new Timestamp(System.currentTimeMillis());
				dsSysMetadataNodeAttr.setMetadataNodeId(nodeId);
				Map<String,String> params = new HashMap<String, String>();
				params.clear();
				adapter.execute("UPDATE DS_SYS_METADATA_NODE_ATTR SET ATTR_VALUE= '"
						+dsSysMetadataNodeAttr.getAttrValue()+"',ATTR_NAME ='"+dsSysMetadataNodeAttr.getAttrName()+"'where NODE_ID ="+dsSysMetadataNodeAttr.getMetadataNodeId()+"and ATTR_NAME = '"+dsSysMetadataNodeAttr.getAttrName()+"'");
			}
		}
		
	}
	@Override
	public boolean deleteNode(Integer nodeId){
		boolean delete = true;
		try {
			//TODO 可以做级联删除不？
			adapter.openSession();
			Map<String, String> params = new HashMap<String, String>();
			//遍历子类是否有连接信息
			List<MetadataNode> metadataNode  = new ArrayList<MetadataNode>();
			metadataNode = adapter.query2List("select ID from DS_SYS_METADATA_NODE where PARENT_ID = "+nodeId, null, MetadataNode.class);
			for (MetadataNode node : metadataNode) {
				//删除子类连接信息
				params.put("NODE_ID", ""+node.getId()+"");
				adapter.delete(params, "DS_SYS_METADATA_NODE_ATTR");
				
			}
			//删除子类资料
			params = new HashMap<String, String>();
			params.put("PARENT_ID", nodeId.toString());
			
			adapter.delete(params, "DS_SYS_METADATA_NODE");
			
			//删除本身的连接信息
			params = new HashMap<String, String>();
			params.put("NODE_ID", nodeId.toString());
			adapter.delete(params, "DS_SYS_METADATA_NODE_ATTR");
			//删除本身信息
			params = new HashMap<String, String>();
			params.put("ID", nodeId.toString());
			adapter.deleteByTableName(params, "DS_SYS_METADATA_NODE");
		} catch (Exception e) {
			delete = false;
			try {
				adapter.rollback();
			} catch (SQLException e1) {
			}
		}finally{
			adapter.closeSession();
		}
		return delete;
	}


	@Override
	public List<MetadataNode> renameNode(List<MetadataNode> nodes){
		try {
			adapter.openSession();
			//新增MetadatNode表信息
			for (MetadataNode metadataNode : nodes) {
				DsSysMetadataNode dsSysMetadataNode = new DsSysMetadataNode();
				dsSysMetadataNode.setNodeDesc(metadataNode.getDesc());
				dsSysMetadataNode.setNodeName(metadataNode.getNodeName());
				dsSysMetadataNode.setNodeType(metadataNode.getNodeType());
				dsSysMetadataNode.setOrderNumber(metadataNode.getOrderNumber());
				dsSysMetadataNode.setParentId(metadataNode.getParentNodeId());
				dsSysMetadataNode.setKey(metadataNode.getKey());
				dsSysMetadataNode.setId(metadataNode.getId());
				boolean result = adapter.update2(dsSysMetadataNode);
				renameDatabaseAttr(metadataNode, metadataNode.getId());
			}
			
		}catch (EnumConstantNotPresentException e) {
			throw new SystemErrorException(MessageCode.ERR_ENUM,"枚举错误");
		} catch (Exception e) {
			throw new SystemErrorException(MessageCode.ERR_EXECUTE,"异常错误");
		}finally{
			adapter.closeSession();
		}
		return nodes;
	}

	@Override
	public List<DBSourceSquid> dropToSquidFlow(List<MetadataNode> nodes){
 		List<DBSourceSquid>  dbSourceSquids = new ArrayList<DBSourceSquid>();
		List<InfoPacket> infoPackets = null;
		IRelationalDataManager adapter2 = DataAdapterFactory.newInstance().getDataManagerByToken(token);
		try {
			adapter2.openSession();
			//获得数据库连接信息TODO Adapter有改变
			DataShirService dataShirService = new DataShirService();
			dataShirService.setToken(token);
			Timestamp date = new Timestamp(System.currentTimeMillis());
			for (MetadataNode metadataNode : nodes) {
				//key
				DBSourceSquid dbSourceSquid = new DBSourceSquid();
				int squidflowId = metadataNode.getSquidFLowId();
				Map<String,String>  map = metadataNode.getAttributeMap();
				//TODO 何晶说ip 地址 前台要显示 得添加信息
				String connectionString = map.get("ConnetionString");
				String userName = map.get("UserName");
				String password = map.get("Password");
				String name = map.get("Name");
				String key = map.get("SquidKey");
				String nodeType = map.get("Type");
				String sourceLocationX = map.get("SourceLocationX");
				String sourceLocationY = map.get("SourceLocationY");
				dbSourceSquid.setConnectionString(connectionString);
				dbSourceSquid.setSquidflow_id(squidflowId);
				dbSourceSquid.setKey(key);
				dbSourceSquid.setLocation_x(Integer.parseInt(sourceLocationX));
				dbSourceSquid.setLocation_y(Integer.parseInt(sourceLocationY));
				dbSourceSquid.setName(name);
				dbSourceSquid.setState(1);
				dbSourceSquid.setCreation_date(date.toString());
				dbSourceSquid.setUser_name(userName);
				dbSourceSquid.setPassword(password);
				dbSourceSquid.setDb_name(name);
				//数据库类型
				if(nodeType.equals("1")){
					String host = map.get("Host");
					int port = Integer.parseInt(map.get("Port"));
					String database = map.get("Database");
					dbSourceSquid.setHost(host);
					dbSourceSquid.setPassword(password);
					dbSourceSquid.setUser_name(userName);
					dbSourceSquid.setPort(port);
					dbSourceSquid.setDb_name(database);
					dbSourceSquid.setDb_type(DataBaseType.SQLSERVER.value());
				}
				dbSourceSquids.add(dbSourceSquid);
				
				
			}
			
			infoPackets = dataShirService.createDBsourcesquids(dbSourceSquids,adapter2);
			//是否返回squid
		} catch (Exception e) {
			throw new SystemErrorException(MessageCode.ERR_EXECUTE,"异常错误");
		}finally{
			adapter2.closeSession();
		}
		return dbSourceSquids;
	}
	
	/**
	 * 从数据源浏览器中拖动数据源覆盖SourceSquid
	 * @param nodes
	 * @return
	 */
	public List<DbSquid>  dropToSquidFlowCoverSourceSquid(List<MetadataNode> nodes){
		List<DbSquid>  dbSquids = new ArrayList<DbSquid>();
		SquidService squidService = new SquidService(token);
		IRelationalDataManager adapter2 = DataAdapterFactory.newInstance().getDataManagerByToken(token);
		try {
			adapter2.openSession();
			DataShirService dataShirService = new DataShirService();
			dataShirService.setToken(token);
			for (MetadataNode metadataNode : nodes) {
				DBSourceSquid dbSourceSquid = new DBSourceSquid();
				int squidflowId = metadataNode.getSquidFLowId();
				int squidId = metadataNode.getSquidId();
				Map<String,String>  map = metadataNode.getAttributeMap();
				String connectionString = map.get("ConnectionString");
				String userName = map.get("UserName");
				String password = map.get("Password");
				String name = map.get("Name");
				String nodeType = map.get("Type");
				dbSourceSquid.setConnectionString(connectionString);
				dbSourceSquid.setSquidflow_id(squidflowId);
				dbSourceSquid.setKey(StringUtils.generateGUID());
				dbSourceSquid.setName(name);
				dbSourceSquid.setId(squidId);
				dbSourceSquid.setState(1);
				dbSourceSquid.setUser_name(userName);
				dbSourceSquid.setPassword(password);
				//数据库类型
				if(nodeType.equals("1")){
					String host = map.get("Host");
					int port = Integer.parseInt(map.get("Port"));
					String database = map.get("Database");
					dbSourceSquid.setHost(host);
					dbSourceSquid.setPassword(password);
					dbSourceSquid.setUser_name(userName);
					dbSourceSquid.setPort(port);
					dbSourceSquid.setDb_name(database);
					dbSourceSquid.setDb_type(DataBaseType.SQLSERVER.value());
				}
				dbSquids.add(dbSourceSquid);
			}
			
			//修改
			ReturnValue out = new ReturnValue();
			dbSquids = squidService.updatedbSourceSquids2(dbSquids,adapter2,out);
		} catch (Exception e) {
			throw new SystemErrorException(MessageCode.ERR_EXECUTE,"异常错误");
		}finally{
			adapter2.closeSession();
		}
		return dbSquids;
	}
	/**
	 * 从数据源浏览器中拖动数据表或文件信息覆盖ExtractSquid
	 * @param nodes
	 * @return
	 */
	public List<TableExtractSquid> dropToSquidFlowCoverExtractSquid(List<MetadataNode> nodes){
		List<TableExtractSquid>  extractSquids = new ArrayList<TableExtractSquid>();
		IRelationalDataManager adapter2 = DataAdapterFactory.newInstance().getDataManagerByToken(token);
		try {
			adapter2.openSession();
			adapter.openSession();
			DataShirService dataShirService = new DataShirService();
			dataShirService.setToken(token);
			for (MetadataNode metadataNode : nodes) {
				TableExtractSquid extractSquid = new TableExtractSquid();
				int squidId = metadataNode.getSquidId();
				Map<String,String>  map = metadataNode.getAttributeMap();
				String tableName = map.get("TableName");
				String columnName = map.get("ColumnName");
				String nodeName = map.get("DataBaseName");
				//是否覆盖
				String isOerride = map.get("IsOverride");
				extractSquid.setId(squidId);
				extractSquid.setName(metadataNode.getName());
				extractSquid.setKey(metadataNode.getKey());
				extractSquid.setTable_name(nodeName);
				extractSquid.setName(nodeName);
				adapter2.update2(extractSquid);
				adapter2.execute("UPDATE DS_SOURCE_TABLE SET  name ="+nodeName+" TABLE_NAME = "+nodeName+"WHERE ID="+squidId);
				//查询来源ＩＤ
				Map<String, Object> map2 = adapter2.query2Object("SELECT from_squid_id FROM ds_squid_link where to_squid_id = "+squidId, null);
				int sourceSquidId = Integer.parseInt(map2.get("FROM_SQUID_ID").toString());
				//清空 信息覆盖
				if(isOerride == "1"){
					clearMessage(adapter2, squidId);
					//调用新增信息覆盖原本的信息
					createExtractSquidData(tableName, columnName, sourceSquidId,
							extractSquid, squidId,adapter2,null);
				}else{
					//不覆盖
					createExtractSquidData(tableName, columnName, sourceSquidId,
							extractSquid, squidId,adapter2,metadataNode.getColumnList());
				}
				
				
				extractSquids.add(extractSquid);
			}
		} catch (Exception e) {
			throw new SystemErrorException(MessageCode.ERR_EXECUTE,"异常错误");
		}finally{
			adapter2.closeSession();
			adapter.closeSession();
		}
		return extractSquids;
	}
	
	/**
	 * 情况原本信息
	 * @param adapter2
	 * @param squidId
	 */
	private void clearMessage(IRelationalDataManager adapter2, int squidId) {
		//清空原本ExtractSquid的column信息，Transformation，TransformationLink信息
		boolean delete = true;
		Map<String, String> paramMap = new HashMap<String, String>();
		ReturnValue out = new ReturnValue();
		if (delete) {
			// 根据squid_id去查询transformation的id
			paramMap.clear();
			paramMap.put("squid_id", String.valueOf(squidId));
			List<Transformation> transformations = adapter2.query2List(true, paramMap, Transformation.class);
			if(null!=transformations&&transformations.size()>0)
			{
				TransformationServicesub transformationService=new TransformationServicesub(token, adapter2);
				for(Transformation transformation:transformations)
				{
					delete=	transformationService.deleteTransformation(transformation.getId(), out);
				}
			}
			if (delete) {
				// 删除column
				paramMap.clear();
				paramMap.put("squid_id", String.valueOf(squidId));
				List<Column> columns=adapter2.query2List(true, paramMap, Column.class);
				if(null!=columns&&columns.size()>0)
				{
					ColumnServicesub columnService=new ColumnServicesub(token, adapter2);
					for(Column column:columns)
					{
						delete=	columnService.deleteColumn(column.getId(), out);
					}
				}
			}
		}
	}

	
	@Override
	public List<Squid> dropToSquid(List<MetadataNode> nodes){
		return null;
		
	}
	
	/**
	 * 从数据源浏览器中拖动列创建SourceSquid和ExtractSquid
	 * @param nodes
	 * @return
	 */
	@Override
	public List<SpecialSourceSquidAndSquidLinkAndExtranctSquid> dropToSpecialSquid(List<MetadataNode> nodes){
		DataShirService dataShirService = new DataShirService();
		dataShirService.setToken(token);
		IRelationalDataManager adapter2 = DataAdapterFactory.newInstance().getDataManagerByToken(token);
		List<SpecialSourceSquidAndSquidLinkAndExtranctSquid>  specialList = new ArrayList<SpecialSourceSquidAndSquidLinkAndExtranctSquid>();
		try {
			adapter.openSession();
			adapter2.openSession();
			for (MetadataNode metadataNode : nodes) {
				SpecialSourceSquidAndSquidLinkAndExtranctSquid special 
				= new SpecialSourceSquidAndSquidLinkAndExtranctSquid();
				 createSquid(metadataNode, special,adapter2);
				 specialList.add(special);
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new SystemErrorException(MessageCode.ERR_EXECUTE,e);
		}finally{
			adapter.closeSession();
			adapter2.closeSession();
		}
		return specialList;
	}

	/**
	 * 新增SourceSquid，ExtractSquid 和SquidLink信息
	 * @param metadataNode
	 * @param special
	 * @throws SQLException
	 */
	private void createSquid(MetadataNode metadataNode,
			SpecialSourceSquidAndSquidLinkAndExtranctSquid special,IRelationalDataManager adapter2)
			throws SQLException {
		// 临时变量
		//列信息
		DBSourceSquid dbSourceSquid = new DBSourceSquid();
		int squidflowId = metadataNode.getSquidFLowId();
		Map<String,String>  map = metadataNode.getAttributeMap();
		String connectionString = map.get("ConnectionString");
		String userName = map.get("UserName");
		String password = map.get("Password");
		String name = map.get("DBSourceName");
		String extracSquidName = map.get("ExtractName");
		String tableName = map.get("TableName");
		String columnName = map.get("ColumnName");
		String nodeType = map.get("Type");
		String sourceLocationX = map.get("SourceLocationX");
		String sourceLocationY = map.get("SourceLocationY");
		String targerLocationX = map.get("TargerLocationX");
		String targerLocationY = map.get("TargerLocationY");
		dbSourceSquid.setConnectionString(connectionString);
		dbSourceSquid.setSquidflow_id(squidflowId);
		dbSourceSquid.setKey(StringUtils.generateGUID());
		dbSourceSquid.setName(name);
		dbSourceSquid.setState(1);
		dbSourceSquid.setUser_name(userName);
		dbSourceSquid.setPassword(password);
		dbSourceSquid.setSquid_type(SquidTypeEnum.DBSOURCE.value());
		dbSourceSquid.setType(SquidTypeEnum.DBSOURCE.value());

		dbSourceSquid.setLocation_x(Integer.parseInt(sourceLocationX));
		dbSourceSquid.setLocation_y(Integer.parseInt(sourceLocationY));
		//新增DBSourceSquid
		int sourceSquidId = adapter2.insert2(dbSourceSquid);
		adapter2.commit();
		dbSourceSquid.setId(sourceSquidId);
		special.setDbSourceSquid(dbSourceSquid);
		//新增连接信息
		List<DbSquid> dbSquids = new ArrayList<DbSquid>();
		DbSquid dbSquid = new DbSquid();
		
		if(nodeType.equals("1")){
			String host = map.get("Host");
			String dbsourceName = map.get("DBSourceName");
			int port = Integer.parseInt(map.get("Port"));
			String database = map.get("Database");
			dbSquid.setHost(host);
			dbSquid.setPassword(password);
			dbSquid.setUser_name(userName);
			dbSquid.setPort(port);
			dbSquid.setDb_name(database);
			dbSquid.setDb_type(DataBaseType.SQLSERVER.value());
			dbSquid.setConnectionString(connectionString);
			dbSquid.setId(sourceSquidId);
			//int dbsquidId = adapter2.insert2(dbSquid);
			dbSquids.add(dbSquid);
		}
		//调用新增缓存表信息
		//SourcesTable
		String info = JsonUtil.toGsonString(dbSquids);
		ConnectProcess connectProcess = new ConnectProcess(token);
		ReturnValue out = new ReturnValue();
		List<SourceTable> sourceTables = connectProcess.getConnect2(dbSquids,adapter2, out);
		dbSourceSquid.setSourceTableList(sourceTables);
		//新增ExtractSquid
		//adapter2.openSession();
		TableExtractSquid extractSquid = new TableExtractSquid();
		extractSquid.setName(extracSquidName);
		extractSquid.setSquid_type(SquidTypeEnum.EXTRACT.value());
		extractSquid.setSquidflow_id(squidflowId);
		extractSquid.setLocation_x(Integer.parseInt(targerLocationX));
		extractSquid.setLocation_y(Integer.parseInt(targerLocationY));
		extractSquid.setKey(StringUtils.generateGUID());
		int extractSquidId = adapter2.insert2(extractSquid);
		//创建ExtractSquid相关信息
		createExtractSquidData(tableName, columnName, sourceSquidId,
				extractSquid, extractSquidId,adapter2,null);

		special.setExtractSquid(extractSquid);
		//新增SquidLink
		SquidLink squidLink = new SquidLink();
		squidLink.setSquid_flow_id(squidflowId);
		squidLink.setType(DSObjectType.SQUIDLINK.value());
		squidLink.setFrom_squid_id(sourceSquidId);
		squidLink.setTo_squid_id(extractSquidId);
		squidLink.setKey(StringUtils.generateGUID());
		squidLink.setLine_type(1);
		int squidLinkId = adapter2.insert2(squidLink);
		squidLink.setId(squidLinkId);
		special.setSquidLink(squidLink);
	}

	
	/**
	 * 新增ExtractSquid的关联信息
	 * @param tableID
	 * @param columnID
	 * @param sourceSquidId
	 * @param extractSquid
	 * @param extractSquidId
	 * @throws SQLException
	 */
	private void createExtractSquidData(String tableID, String columnID, int sourceSquidId,
			TableExtractSquid extractSquid, int extractSquidId,IRelationalDataManager adapter2,List<String> columnNameList) throws SQLException {
		Map<String, Object> params = new HashMap<String, Object>();
		List<ReferenceColumn> referenceColumns;
		List<Column> columns;
		List<Transformation> transformations;
		List<TransformationLink> transformationLinks;
		int tmpIndex;
		int sourceColumnSize;
		int tmpFromTransId;
		int tmpToTransId;
		Transformation tmpTrans;
		String[] columnIdArry;
		List<MetadataNode> columnNodes;
		if(tableID!= null){
			//查询表下面的列信息
			int id = Integer.parseInt(tableID);
			columnNodes = adapter.query2List("select ID,PARENT_ID,NODE_NAME from DS_SYS_METADATA_NODE where PARENT_ID = "+id, null, MetadataNode.class);
			sourceColumnSize = columnNodes.size();
			
		}else{
			columnNodes = new ArrayList<MetadataNode>();
			columnIdArry = columnID.split("[,]");
			for (String str : columnIdArry) {
				MetadataNode node = new MetadataNode();
				int id = Integer.parseInt(str);
				node.setId(id);
				columnNodes.add(node);
			}
			sourceColumnSize = columnIdArry.length;
		}
		// 创建引用列组
		ReferenceColumnGroup columnGroup = new ReferenceColumnGroup();
		columnGroup.setKey(StringUtils.generateGUID());
		columnGroup.setReference_squid_id(extractSquidId);
		columnGroup.setRelative_order(1);
		columnGroup.setId(adapter2.insert2(columnGroup));
		referenceColumns = new ArrayList<ReferenceColumn>();
		columns = new ArrayList<Column>();
		transformations = new ArrayList<Transformation>();
		transformationLinks = new ArrayList<TransformationLink>();
		
		
		int i = 0;
		for (MetadataNode columnNode : columnNodes) {
			List<DsSysMetadataNodeAttr> dsSysMetadataNodeAttrs = adapter.query2List("select ID,ATTR_NAME,ATTR_VALUE,NODE_ID,CREATION_DATE from DS_SYS_METADATA_NODE_ATTR where NODE_ID = "+columnNode.getId(), null, DsSysMetadataNodeAttr.class);
			// 目标ExtractSquid真实列（变换面板左边）
			Column column = new Column();
			// 目标ExtractSquid引用列（变换面板右边，引用db_source_squid）
			ReferenceColumn ref = new ReferenceColumn();
			for (DsSysMetadataNodeAttr dsSysMetadataNodeAttr : dsSysMetadataNodeAttrs) {
				if(dsSysMetadataNodeAttr.getAttrName().equals("precision")){
					column.setPrecision(Integer.parseInt(dsSysMetadataNodeAttr.getAttrValue()));
				}
				if(dsSysMetadataNodeAttr.getAttrName().equals("data_type")){
					column.setData_type(Integer.parseInt(dsSysMetadataNodeAttr.getAttrValue()));
				}
				if(dsSysMetadataNodeAttr.getAttrName().equals("length")){
					column.setLength(Integer.parseInt(dsSysMetadataNodeAttr.getAttrValue()));
				}
				if(dsSysMetadataNodeAttr.getAttrName().equals("nullable")){
					column.setNullable(Boolean.parseBoolean(dsSysMetadataNodeAttr.getAttrValue()));
				}
				if(dsSysMetadataNodeAttr.getAttrName().equals("colmunname")){
					column.setName(dsSysMetadataNodeAttr.getAttrValue());
				}
				/*if(dsSysMetadataNodeAttr.getAttrName().equals("key")){
					column.setKey(dsSysMetadataNodeAttr.getAttrValue());
				}*/
				
				column.setKey(StringUtils.generateGUID());
				column.setRelative_order(i+1);
				column.setSquid_id(extractSquidId);
				
			}
			//Column名称改变
			if(columnNameList!= null && columnNameList.size()>0){
				column.setName(columnNameList.get(i));
			}
			int columnId = adapter2.insert2(column);
			column.setId(columnId);
			columns.add(column);
			
			ref.setColumn_id(column.getId());
			ref.setId(ref.getColumn_id());
			ref.setData_type(column.getData_type());
			ref.setKey(StringUtils.generateGUID());
			ref.setName(column.getName());
			ref.setNullable(column.isNullable());
			ref.setRelative_order(i+1);
			ref.setSquid_id(extractSquidId);
			ref.setReference_squid_id(extractSquid.getId());
			ref.setHost_squid_id(extractSquidId);
			ref.setIs_referenced(true);
			ref.setGroup_id(columnGroup.getId());
			adapter2.insert2(ref);
			referenceColumns.add(ref);
			i++;
		}
		extractSquid.setColumns(columns);
		extractSquid.setSourceColumns(referenceColumns);
		//新增Transformation
		for (tmpIndex=0; tmpIndex<sourceColumnSize; tmpIndex++) {
			// 创建目标 transformation
			Transformation newTrans = new Transformation();
			newTrans.setColumn_id(columns.get(tmpIndex).getId());
			newTrans.setKey(StringUtils.generateGUID());
			newTrans.setLocation_x(0);
			newTrans.setLocation_y((tmpIndex+1) * 25 + 25 / 2);
			newTrans.setSquid_id(extractSquidId);
			newTrans.setTranstype(TransformationTypeEnum.VIRTUAL.value());
			newTrans.setId(adapter2.insert2(newTrans));
			transformations.add(newTrans);
			tmpToTransId = newTrans.getId();
			params.clear();
			params.put("SQUID_ID", sourceSquidId);
			params.put("column_id", referenceColumns.get(tmpIndex).getColumn_id());
			params.put("transformation_type_id", TransformationTypeEnum.VIRTUAL.value());
			tmpTrans = adapter2.query2Object(true, params, Transformation.class);
			if(tmpTrans == null){
				// 创建来源 transformation
				newTrans = new Transformation();
				newTrans.setColumn_id(referenceColumns.get(tmpIndex).getColumn_id());
				newTrans.setKey(StringUtils.generateGUID());
				newTrans.setLocation_x(199);
				newTrans.setLocation_y((tmpIndex+1) * 25 + 25 / 2);
				newTrans.setSquid_id(sourceSquidId);
				newTrans.setTranstype(TransformationTypeEnum.VIRTUAL.value());
				newTrans.setId(adapter2.insert2(newTrans));
				transformations.add(newTrans);
				tmpFromTransId = newTrans.getId();
			}else{
				tmpFromTransId = tmpTrans.getId();
				transformations.add(tmpTrans);
			}
			// 创建 transformation link
			TransformationLink transLink = new TransformationLink();
			transLink.setIn_order(tmpIndex+1);
			transLink.setFrom_transformation_id(tmpFromTransId);
			transLink.setTo_transformation_id(tmpToTransId);
			transLink.setKey(StringUtils.generateGUID());
			transLink.setId(adapter2.insert2(transLink));
			transformationLinks.add(transLink);
		}
		extractSquid.setTransformations(transformations);
		extractSquid.setTransformationLinks(transformationLinks);
	}




	@Override
	public List<MetadataNode> getRootNodes() {
		//TODO没有用
		adapter.openSession();
		List<MetadataNode> metadataNode  = new ArrayList<MetadataNode>();
		try {
			metadataNode = adapter.query2List("select ID,PARENT_ID,NODE_TYPE,NODE_NAME,ORDER_NUMBER,NODE_DESC,CREATION_DATE from DS_SYS_METADATA_NODE where PARENT_ID = 0", null, MetadataNode.class);
		} catch (Exception e) {
			throw new SystemErrorException(MessageCode.ERR_EXECUTE,"异常错误");
		}finally{
			adapter.closeSession();
		}
		return metadataNode;
	}

	@Override
	public List<MetadataNode> getSubNodes(Integer parentNodeId) {
		adapter.openSession();
		List<MetadataNode> metadataNodes  = new ArrayList<MetadataNode>();
		try {
			metadataNodes = adapter.query2List("select ID,PARENT_ID,NODE_TYPE,NODE_NAME,ORDER_NUMBER,NODE_DESC,COLUMNATTRIBUTE,CREATION_DATE,KEY from DS_SYS_METADATA_NODE where PARENT_ID = "+parentNodeId, null, MetadataNode.class);
			if(metadataNodes == null || metadataNodes.size()==0){
				metadataNodes = null;
				//没有数据
				throw new SystemErrorException(MessageCode.NODATA,"没有子节点信息！");
			}
		
			for (MetadataNode metadataNode : metadataNodes) {
				
				List<DsSysMetadataNodeAttr> dsSysMetadataNodeAttrs = adapter.query2List("select ID,ATTR_NAME,ATTR_VALUE,NODE_ID,CREATION_DATE from DS_SYS_METADATA_NODE_ATTR where NODE_ID = "+metadataNode.getId(), null, DsSysMetadataNodeAttr.class);
				if(dsSysMetadataNodeAttrs != null){
					Map<String,String> map = new HashMap<String, String>();
					for (DsSysMetadataNodeAttr dsSysMetadataNodeAttr : dsSysMetadataNodeAttrs) {
						//赋值
						map.put(dsSysMetadataNodeAttr.getAttrName(), dsSysMetadataNodeAttr.getAttrValue());
					}
					metadataNode.setAttributeMap(map);
				}
			}
		} catch (SystemErrorException e){
			throw new SystemErrorException(e.getCode(),e.getMessage());
		}catch (Exception e) {
			throw new SystemErrorException(MessageCode.ERR_EXECUTE,"异常错误");
		}finally{
			adapter.closeSession();
		}
		return metadataNodes;
	}

	@Override
	public String getMetadataTree() {
		// TODO Auto-generated method stub
		return null;
	}

}