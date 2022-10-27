package com.eurlanda.datashire.sprint7.service.squidflow.subservice;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IDBAdapter;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.entity.*;
import com.eurlanda.datashire.enumeration.DMLType;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.enumeration.MessageBubbleCode;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.enumeration.TransformationTypeEnum;
import com.eurlanda.datashire.sprint7.packet.InfoNewPacket;
import com.eurlanda.datashire.sprint7.packet.InfoPacket;
import com.eurlanda.datashire.sprint7.packet.PushMessagePacket;
import com.eurlanda.datashire.sprint7.service.squidflow.AbstractRepositoryService;
import com.eurlanda.datashire.sprint7.service.squidflow.ConnectProcess;
import com.eurlanda.datashire.utility.DatabaseException;
import com.eurlanda.datashire.utility.StringUtils;
import com.eurlanda.datashire.validator.SQLValidator;
import org.apache.oro.text.regex.MalformedPatternException;
import org.apache.oro.text.regex.PatternCompiler;
import org.apache.oro.text.regex.Perl5Compiler;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
/**
 * 消息泡业务处理类
 * @author lei.bin
 *
 */
public class MessageBubbleService extends AbstractRepositoryService implements IMessageBubbleService {

    private static String token;
    
    private IRelationalDataManager adapter;
    
	public MessageBubbleService(String token) {
		super(token);
		setToken(token);
	}
	public MessageBubbleService(IRelationalDataManager newAdapter){
		super(newAdapter);
	}
	public MessageBubbleService(String token, IRelationalDataManager newAdapter){
		super(token, newAdapter);
		setToken(token);
		openSessionForMessageBubble(newAdapter);
	}
	
	public static List<MessageBubble> messageBubbleList = new ArrayList<MessageBubble>();
	
	public static CallableStatement callStatement = null;
	
	public static boolean loopFlag = false; 
	
	public static Queue<List<MessageBubble>> queue = new LinkedList<List<MessageBubble>>();
	
	/**
	 * 推送消息泡
	 * @param mess
	 * @author bo.dang
	 * @date 2014年6月4日
	 */
	public static void pushMessageBubble(MessageBubble mess){
	    messageBubbleList.clear();
	    messageBubbleList.add(mess);
	    PushMessagePacket.push(messageBubbleList, getToken());
	}

/*	*//**
	 * 校验squid是否孤立 新增squid,删除squid,删除link,面板加载时触发
	 * 
	 * @param operateType 操作类型
	 * @param type 枚举类型
	 * @param squidLinks集合
	 * @param infoPackets
	 * @return
	 *//*
	public void isolateSquid(String operateType, DSObjectType type,
			List<SquidLink> squidLinks, List<InfoNewPacket> infoNewPackets,
			List<InfoPacket> infoPackets) {
		try {

			logger.info("数据校验 - 消息气泡 (squid是否孤立)");
			List<MessageBubble> messageBubbles = new ArrayList<MessageBubble>();
			// 单独创建squid
			if ("create".equals(operateType)
					&& (type == DSObjectType.DBSOURCE
							|| type == DSObjectType.EXTRACT
							|| type == DSObjectType.STAGE || type == DSObjectType.DBSOURCE
							|| type == DSObjectType.REPORT)) {
				logger.info("数据校验 - 消息气泡 (squid是否孤立---单独创建squid)");
				MessageBubble messageBubble = new MessageBubble();
				this.setMessageBubble(messageBubble, infoPackets.get(0).getKey(), infoPackets.get(0).getKey(),MessageBubbleCode.WARN_SQUID_NO_LINK.value(), false, 0);
				messageBubbles.add(messageBubble);
				PushMessagePacket.push(messageBubbles, token);
			} else if ("create".equals(operateType)
					&& type == DSObjectType.SQUIDLINK && null != squidLinks) {// 创建squidlink时，消息泡消失
				
			} else if ("delete".equals(operateType)
					&& type == DSObjectType.SQUIDLINK && null != squidLinks) {// 删除squidlink时，如果
			}
		} catch (Exception e) {
			logger.error("数据校验 - 消息气泡 (squid是否孤立)exception",e);
		}
	}*/

	/**
	 * 校验transformation是否孤立
	 * 
	 * @return
	 */
	public List<MessageBubble> isolateTransformation(List<Transformation> transformations,List<InfoPacket> infoPackets,List<InfoNewPacket> infoNewPackets) {
		logger.info("数据校验 - 消息气泡 (transformation是否孤立)");
		List<MessageBubble> messageBubbles = new ArrayList<MessageBubble>();
		//新增Transformation調用
		if(null!=transformations &&transformations.size()>0)
		{
			for(int i=0;i<transformations.size();i++)
			{
				MessageBubble messageBubble = new MessageBubble();
				this.setMessageBubble(messageBubble, transformations.get(i).getKey(), transformations.get(i).getKey(), MessageBubbleCode.WARN_TRANSFORMATION_NO_LINK.value(), false, 1);
			}
		}
		/*else if(null !=infoPackets&&infoPackets.size()>0)
		{//新增transformationLink调用
			for(int j=0;j<infoPackets.size();j++)
			{
				int linkId=infoPackets.get(j).getId();
				//如果新增后transformation的两边都有连线,发送消息泡给前台
			}
		}else if(null !=infoNewPackets && infoNewPackets.size()>0){
			//删除transformationLink调用
			for(InfoNewPacket infoNewPacket:infoNewPackets)
			{
				int linkId=infoNewPacket.getId();
				//如果只删除一边的线,则发送消息泡，如果2根都删除，则不发送
			}
		}*/
		
		return messageBubbles;
	}

	/**
     * 4.校验transformation是否孤立
     * 
     * @return
	 * @throws SQLException 
     */
    public void validateIsolateTransformation(int squidId, int childId, String name, int code, String call, int level, String text) {
        logger.info("数据校验 - 消息气泡 (transformation是否孤立)");
/*        String sql = "select * from ds_transformation_link where from_transformation_id =" + childId + " or to_transformation_id = " + childId;
        try {
        TransformationLink transformationLink;
            transformationLink = adapter.query2Object(false, sql, null, TransformationLink.class);
        if(StringUtils.isNull(transformationLink)){
            // 推送消息泡
            messageBubbleList.add(setMessageBubble(false, squidId, childId, name, MessageBubbleCode.WARN_TRANSFORMATION_NO_LINK.value(), MessageBubbleCode.WARN, name + "已定义，但未参与有效的数据变换"));
        }
        else {
            // 消泡
            messageBubbleList.add(setMessageBubble(true, squidId, childId, name, MessageBubbleCode.WARN_TRANSFORMATION_NO_LINK.value(), MessageBubbleCode.WARN, ""));
        }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }*/
        Boolean callFlag = true;
        if(squidId == -1){
            Map<String, String> paramsMap = new HashMap<String, String>();
            paramsMap.put("id", Integer.toString(childId, 10));
            Transformation transformation = adapter.query2Object(paramsMap, Transformation.class);
            if(StringUtils.isNotNull(transformation)){
            	if(TransformationTypeEnum.VIRTUAL.value() == transformation.getTranstype()){
            		callFlag = false;
            	}
            	squidId = transformation.getSquid_id();
            	name = transformation.getName();
            }
            else {
            	callFlag = false;
            }
        }
        if(callFlag){
            //推送消息泡
            prepareCall(squidId, childId, name, code, call, level, text);
        }
        
    }
	
	/**
	 * 校验Connection连接信息是否完整、正确
	 * 
	 * @return
	 */
	public void connectionValidation(List<DbSquid> dbSquids) {
		logger.info("数据校验 - 消息气泡 (Connection连接信息是否完整、正确)");
		List<MessageBubble> messageBubbles = new ArrayList<MessageBubble>();
		for (int i = 0; i < dbSquids.size(); i++) {
			MessageBubble messageBubble = new MessageBubble();
			try {
				// 校验传输信息是否完整
				if (StringUtils.isBlank(dbSquids.get(i).getHost())
						|| 0 == dbSquids.get(i).getPort()
						|| StringUtils.isBlank(dbSquids.get(i).getDb_name())
						|| StringUtils.isBlank(dbSquids.get(i).getUser_name())
						|| StringUtils.isBlank(dbSquids.get(i).getPassword())
						|| 0 == dbSquids.get(i).getDb_type()) {
					this.setMessageBubble(messageBubble, dbSquids.get(i).getKey(),
							dbSquids.get(i).getKey(),
							MessageBubbleCode.ERROR_DB_CONNECTION.value(), false, 1);
					logger.info("数据校验 - 消息气泡 (连接信息不完整)");
				} else {// 校验数据库连接是否正常
					ConnectProcess connectProcess = new ConnectProcess(adapter);
					IDBAdapter dataAdapterBase=  connectProcess.getAdapter(dbSquids.get(i));
					if (dataAdapterBase.isValid(5000)) {
						this.setMessageBubble(messageBubble, dbSquids.get(i)
								.getKey(), dbSquids.get(i).getKey(),
								MessageBubbleCode.ERROR_DB_CONNECTION.value(),
								true, 0);
						logger.info("数据校验 - 消息气泡 (连接信息正确)");
					} else {
						this.setMessageBubble(messageBubble, dbSquids.get(i)
								.getKey(), dbSquids.get(i).getKey(),
								MessageBubbleCode.ERROR_DB_CONNECTION.value(),
								false, 1);
						logger.info("数据校验 - 消息气泡 (连接信息不正确)");
					}
				}
				messageBubbles.add(messageBubble);
				PushMessagePacket.push(messageBubbles, getToken());
			} catch (Exception e) {
				this.setMessageBubble(messageBubble, dbSquids.get(i)
						.getKey(), dbSquids.get(i).getKey(),
						MessageBubbleCode.ERROR_DB_CONNECTION.value(),
						false, 1);
				messageBubbles.add(messageBubble);
				PushMessagePacket.push(messageBubbles, getToken());
				logger.info("数据校验 - 消息气泡 (连接信息不正确,Exception)");
			}
		}
	}

	/**
	 * 有destination squid link,表名不符合一般关系型数据库表名规范
	 * 
	 * @return
	 */
	public void destinationValidation(
			List<StageSquid> stageSquids) {
		logger.info("数据校验 - 消息气泡 (数据库表名规范)");
		List<MessageBubble> messageBubbles = new ArrayList<MessageBubble>();
		String tableName = null;
		String filter = null;
		for (int i = 0; i < stageSquids.size(); i++) {
			MessageBubble messageBubble = new MessageBubble();
			tableName = stageSquids.get(i).getTable_name();
			filter = stageSquids.get(i).getFilter();
			SQLValidator.validateSquidFilter(filter, stageSquids.get(i).getId(), stageSquids.get(i).getId(), stageSquids.get(i).getName());
			if (StringUtils.isNotBlank(tableName)) {
				// 如果表名不为空,校验表名的正确性
				if (this.checkTableName(tableName)) {// 表名符合规范
					logger.info("数据校验 - 消息气泡 (数据库表名规范---表名正确)");
					this.setMessageBubble(messageBubble, stageSquids.get(i)
							.getKey(), stageSquids.get(i).getKey(),
							MessageBubbleCode.ERROR_DB_TABLE_NAME.value(),
							true, 0);
				} else {// 不符合规范
					logger.info("数据校验 - 消息气泡 (数据库表名规范---表名不正确)");
					this.setMessageBubble(messageBubble, stageSquids.get(i)
							.getKey(), stageSquids.get(i).getKey(),
							MessageBubbleCode.ERROR_DB_TABLE_NAME.value(),
							false, 1);
				}
			} else {
				logger.info("数据校验 - 消息气泡 (数据库表名规范---表名不正确)");
				this.setMessageBubble(messageBubble, stageSquids.get(i)
						.getKey(), stageSquids.get(i).getKey(),
						MessageBubbleCode.ERROR_DB_TABLE_NAME.value(), false, 1);
			}
			messageBubbles.add(messageBubble);
			PushMessagePacket.push(messageBubbles, getToken());
		}
	}

	/**
	 * 校验表名的正则表达式
	 * 
	 * @param tableName
	 * @return
	 */
	public boolean checkTableName(String tableName) {
		String patternname = "[a-zA-Z][a-zA-Z0-9#$_]{0,29}"; // 正则表达式
		Pattern pattern = Pattern.compile(patternname);
		Matcher matcher = pattern.matcher(tableName);
		return matcher.matches();
	}
	
	   /**
     * 校验表名的正则表达式
     * 
     * @param tableName
     * @return
     */
    public boolean validateTableName(String tableName) {
        String patternname = "[a-zA-Z][a-zA-Z0-9#$_]{0,29}"; // 正则表达式
        Pattern pattern = Pattern.compile(patternname);
        Matcher matcher = pattern.matcher(tableName);
        return matcher.matches();
    }

	/**
	 * 封装消息泡返回值
	 */
	public void setMessageBubble(MessageBubble messageBubble, String squidKey,
			String key, int bubble_code, boolean status, int level) {
		messageBubble.setSquidKey(squidKey);
		messageBubble.setKey(key);
		messageBubble.setBubble_code(bubble_code);
		messageBubble.setStatus(status);
		messageBubble.setLevel(level);
	}
	
	/**
     * 封装消息泡返回值
     */
    public void setMessageBubble(MessageBubble messageBubble, int squidId,
            int childId, int bubble_code, boolean status, int level) {
        //messageBubble.setSquid_id(squidId);
        //messageBubble.setChild_id(childId);
        messageBubble.setBubble_code(bubble_code);
        messageBubble.setStatus(status);
        messageBubble.setLevel(level);
    }
	
    /**
     * 封装消息泡返回值
     */
    public MessageBubble setMessageBubble(boolean status, int squidId,
            int childId, String name, int code, int level, String text) {
        //messageBubble.setSquid_id(squidId);
        //messageBubble.setChild_id(childId);
//        messageBubble.setCode(code);
//        messageBubble.setStatus(status);
//        messageBubble.setLevel(level);
          //this.messageBubbleList.clear();
        return new MessageBubble(status, squidId, childId, name, code, level, text);
    }
    
    /**
     * 封装消息泡返回值
     */
    public static MessageBubble setMessageBubble(int squidId,
            int childId, String name, int bubble_code) {
        //messageBubble.setSquid_id(squidId);
        //messageBubble.setChild_id(childId);
//        messageBubble.setCode(code);
//        messageBubble.setStatus(status);
//        messageBubble.setLevel(level);
        return new MessageBubble(squidId, childId, name, bubble_code);
    }
    
	/**
	 * 1.Name是否为空 || 9.1 Name：string，可改，不能为空，否则出错误消息泡。在一个squidFlow内，不可重复。
	 * 1/9/26校验SquidName
	 * @param squidName
	 * @return
	 * @author bo.dang
	 * @date 2014年6月5日
	 */
	public void validateSquidName(int squidId, int childId, String squidName, int code){
/*	    Map<String, String> paramsMap = new HashMap<String, String>();
	    paramsMap.put("id", Integer.toString(squidId, 10));
	    SquidModelBase squid = adapter.query2Object(paramsMap, SquidModelBase.class);
	    
	    if(StringUtils.isNotNull(squid)){
	        
	    }*/
	    
	    messageBubbleList.clear();
	    logger.info("数据校验 - 校验SquidName是否为空");
	    if(StringUtils.isNull(squidName)){
	        messageBubbleList.add(this.setMessageBubble(false, squidId, childId, squidName, MessageBubbleCode.ERROR_SQUID_NO_NAME.value(), MessageBubbleCode.ERROR, "Name不能为空"));
	    }
	    else {
	        
	        messageBubbleList.add(this.setMessageBubble(true, squidId, childId, squidName, MessageBubbleCode.ERROR_SQUID_NO_NAME.value(), MessageBubbleCode.ERROR, "Name已填"));
	        // 校验在同一个squidFlow内，name不能重复
	        Map<String, String> paramsMap = new HashMap<String, String>();
	        paramsMap.put("id", Integer.toString(squidId, 10));
	        paramsMap.put("name", squidName);
	        Squid squid = adapter.query2Object(paramsMap, Squid.class);
	        // 如果Squid不为null，那么说明在这个squidFlow中，已经有存在这个Name的Squid
	        if(StringUtils.isNotNull(squid)){
	            messageBubbleList.add(this.setMessageBubble(false, squidId, childId, squidName, MessageBubbleCode.ERROR_SQUID_FLOW_DUPLICATE_NAME.value(), MessageBubbleCode.ERROR, "在一个SquidFlow内，Squid的Name不可重复"));
	        }
	        else {
	            messageBubbleList.add(this.setMessageBubble(true, squidId, childId, squidName, MessageBubbleCode.ERROR_SQUID_FLOW_DUPLICATE_NAME.value(), MessageBubbleCode.ERROR, "在一个SquidFlow内，Squid的Name没有重复，可以使用"));
	        }
	    }
	    PushMessagePacket.push(messageBubbleList, getToken());
	}
	
    /**
     * 2.的 filter 过滤条件
     * @param filter
     * @param key
     */
    public void validateSquidFilter(int squidId, int childId, String name, int code){
        //messageBubbleList = new ArrayList<MessageBubble>();
        String error = null;
        String filter = null;
        if(squidId !=0){
            Map<String,String> paramsMap = new HashMap<String, String>();
            paramsMap.put("id", Integer.toString(squidId, 10));
            Squid squid = adapter.query2Object(paramsMap, Squid.class);
            if(StringUtils.isNotNull(squid)){
            filter = squid.getFilter();
            name = squid.getName();
            if(StringUtils.isNotNull(filter)){
                if(SquidTypeEnum.DOC_EXTRACT.value() == squid.getSquid_type()
                        || SquidTypeEnum.EXTRACT.value() == squid.getSquid_type()
                        || SquidTypeEnum.XML_EXTRACT.value() == squid.getSquid_type()
                        || SquidTypeEnum.WEIBOEXTRACT.value() == squid.getSquid_type()
                        || SquidTypeEnum.WEBEXTRACT.value() == squid.getSquid_type()
                        || SquidTypeEnum.WEBLOGEXTRACT.value() == squid.getSquid_type()
                        || SquidTypeEnum.STAGE.value() == squid.getSquid_type()){
                error = SQLValidator.parse(SQLValidator.SQL_SELECT+filter);
                // 如果校验信息为空,那么验证通过
                if(StringUtils.isNull(error)){
                    messageBubbleList.clear();
                    messageBubbleList.add(this.setMessageBubble(true, squidId, 0, name, MessageBubbleCode.ERROR_SQL_SYNTAX_FILTER.value(), MessageBubbleCode.ERROR, "表达式校验正确"));
                    logger.debug("Queue消息泡校验Middle：validateSquidFilter" + ":" + messageBubbleList.size());
                    PushMessagePacket.push(messageBubbleList, getToken());
                    // 消除消息泡
                }
                // 校验信息
                else {
                    // 推送消息泡
                    messageBubbleList.clear();
                    messageBubbleList.add(this.setMessageBubble(false, squidId, 0, name, MessageBubbleCode.ERROR_SQL_SYNTAX_FILTER.value(), MessageBubbleCode.ERROR, error));
                    logger.debug("Queue消息泡校验Middle：validateSquidFilter" + ":" + messageBubbleList.size());
                    PushMessagePacket.push(messageBubbleList, getToken());
                }
                }
                
            }
        }
    }
        else {
            
            //String sql = "select id, squid_type_id, name, filter from ds_squid where squid_flow_id = " + childId + " and filter is not null and trim(filter) != ''";
            String sql = "select id, squid_type_id, name, filter from ds_squid where squid_flow_id = " + childId + " and squid_type_id in (" 
/*                    + SquidTypeEnum.DBSOURCE.value() + ", "
                    + SquidTypeEnum.FILEFOLDER.value() + ", "
                    + SquidTypeEnum.FTP.value() + ", "
                    + SquidTypeEnum.HDFS.value() + ", "
                    + SquidTypeEnum.WEB.value() + ", "
                    + SquidTypeEnum.WEIBO.value() + ", "*/
                    + SquidTypeEnum.EXTRACT.value() + ", "
                    + SquidTypeEnum.DOC_EXTRACT.value() + ", "
                    + SquidTypeEnum.XML_EXTRACT.value() + ", "
                    + SquidTypeEnum.WEBLOGEXTRACT.value() + ", "
                    + SquidTypeEnum.WEBEXTRACT.value() + ", "
                    + SquidTypeEnum.WEIBOEXTRACT.value() + ", "
                    + SquidTypeEnum.STAGE.value() + ")";
            List<Squid> squidList = adapter.query2List(sql, null, Squid.class);
            Squid squid = null;
            int squidTypeId = 0;
            int count = 1;
            if(StringUtils.isNotNull(squidList)){
                for (int i = 0; i < squidList.size(); i++) {
                      squid = squidList.get(i);
                      squidId = squid.getId();
                      name = squid.getName();
                      squidTypeId = squid.getSquid_type();
                      filter = squid.getFilter();
                         if(StringUtils.isNotNull(filter)){
                          error = SQLValidator.parse(SQLValidator.SQL_SELECT + filter);
                          // 如果校验信息为空,那么验证通过
                          if(StringUtils.isNull(error)){
                              logger.debug("消息泡校验：validateSquidFilter : " + count++ + "," + true + "," + squidId + "," + 0 + "," + name + "," + code + "," + 1 + ",表达式校验正确");
                              // 消除消息泡
                              messageBubbleList.clear();
                              messageBubbleList.add(this.setMessageBubble(true, squidId, 0, name, MessageBubbleCode.ERROR_SQL_SYNTAX_FILTER.value(), MessageBubbleCode.ERROR, "表达式校验正确"));
                              logger.debug("Queue消息泡校验Middle：validateSquidFilter" + ":" + messageBubbleList.size());
                              PushMessagePacket.push(messageBubbleList, getToken());
                          }
                          else {
                              logger.debug("消息泡校验：validateSquidFilter : " + count++ + "," + false + "," + squidId + "," + 0 + "," + name + "," + code + "," + 1 + "," + error);
                              // 推送消息泡
                              messageBubbleList.clear();
                              messageBubbleList.add(this.setMessageBubble(false, squidId, 0, name, MessageBubbleCode.ERROR_SQL_SYNTAX_FILTER.value(), MessageBubbleCode.ERROR, error));
                              logger.debug("Queue消息泡校验Middle：validateSquidFilter" + ":" + messageBubbleList.size());
                              PushMessagePacket.push(messageBubbleList, getToken());
                          }
                          
                         }
                }
            }
            
        }
            //PushMessagePacket.push(messageBubbleList, token);
    }
	
    /**
     * 2.校验Join
     * @param filter
     * @param key
     */
    public void validateJoin(int squidId, int childId, String joinCondition, int code){
        //messageBubbleList = new ArrayList<MessageBubble>();
        String error = null;
        try {
        	String squidName = null;
        logger.debug("消息泡校验： validateJoin : (" + squidId + "," + childId + "," + joinCondition + "," + code + ") strat!");
/*        if(squidId !=0 && StringUtils.isNull(joinCondition)){
        	String sql = "select dss.id squidId, dsj.id childId, dss.name name, dsj.join_condition join_condition from ds_squid dss inner join ds_join dsj on dss.id = dsj.joined_squid_id where dsj.id = " + childId + " and dsj.joined_squid_id =" + squidId;
        	Map<String, Object> joinMap = adapter.query2Object(false, sql, null);
            squidId = Integer.parseInt(joinMap.get("SQUIDID").toString(), 10);
            childId = Integer.parseInt(joinMap.get("CHILDID").toString(), 10);
            squidName = joinMap.get("NAME").toString();
            joinCondition = joinMap.get("JOIN_CONDITION")==null?null:joinMap.get("JOIN_CONDITION").toString();
        	// 消除消息泡
            messageBubbleList.clear();
            messageBubbleList.add(this.setMessageBubble(true, squidId, childId, squidName, MessageBubbleCode.ERROR_SQL_SYNTAX_JOIN.value(), MessageBubbleCode.ERROR, "表达式校验正确"));
            logger.debug("Queue消息泡校验Middle：validateJoin" + ":" + messageBubbleList.size());
            PushMessagePacket.push(messageBubbleList, token);
            //String err = parse(SQL_SELECT+filter);
            PushMessagePacket.push(new MessageBubble(key, key, 
                    MessageBubbleCode.ERROR_SQL_SYNTAX_FILTER.value(), StringUtils.isNull(err), err), token);
            messageBubbleList.clear();
            messageBubbleList.add(this.setMessageBubble(false, squidId, childId, squidName, MessageBubbleCode.WARN_NO_JOIN_CONDITION.value(), MessageBubbleCode.WARN, "JOINS Condition为空"));
            logger.debug("Queue消息泡校验Middle：validateJoin" + ":" + messageBubbleList.size());
            PushMessagePacket.push(messageBubbleList, token);
        }
        else if (squidId !=0 && StringUtils.isNotNull(joinCondition)){
            // 消除表达式为空的消息泡
            messageBubbleList.clear();
            messageBubbleList.add(this.setMessageBubble(true, squidId, childId, joinCondition, MessageBubbleCode.WARN_NO_JOIN_CONDITION.value(), MessageBubbleCode.WARN, "JOINS Condition为空"));
            logger.debug("Queue消息泡校验Middle：validateJoin" + ":" + messageBubbleList.size());
            PushMessagePacket.push(messageBubbleList, token);
            //adapter.execute(sql);
            // 校验信息
            error = SQLValidator.parse(SQLValidator.SQL_SELECT+joinCondition);
            // 如果校验信息为空,那么验证通过
            if(StringUtils.isNull(error)){
                // 消除消息泡
                messageBubbleList.clear();
                messageBubbleList.add(this.setMessageBubble(true, squidId, childId, joinCondition, MessageBubbleCode.ERROR_SQL_SYNTAX_JOIN.value(), MessageBubbleCode.ERROR, "表达式校验正确"));
                logger.debug("Queue消息泡校验Middle：validateJoin" + ":" + messageBubbleList.size());
                PushMessagePacket.push(messageBubbleList, token);
            }
            else {
                // 推送消息泡
                messageBubbleList.clear();
                messageBubbleList.add(this.setMessageBubble(false, squidId, childId, joinCondition, MessageBubbleCode.ERROR_SQL_SYNTAX_JOIN.value(), MessageBubbleCode.ERROR, error));
                logger.debug("Queue消息泡校验Middle：validateJoin" + ":" + messageBubbleList.size());
                PushMessagePacket.push(messageBubbleList, token);
            }
        
       }*/
        if(squidId !=0){
        	String sql = "select dsj.joined_squid_id as squidId, dsj.id as childId, dss.name as name, dsj.join_condition as join_condition from ds_squid dss inner join ds_join dsj on dss.id = dsj.target_squid_id where dsj.join_type_id != 0 and dsj.id = " + childId + " and dsj.joined_squid_id =" + squidId;
        	Map<String, Object> joinMap = adapter.query2Object4(true, sql, null);
        	
        	if(StringUtils.isNull(joinMap)){
        		// 消除消息泡
        		messageBubbleList.clear();
        		messageBubbleList.add(this.setMessageBubble(true, squidId, childId, squidName, MessageBubbleCode.ERROR_SQL_SYNTAX_JOIN.value(), MessageBubbleCode.ERROR, "表达式校验正确"));
        		logger.debug("Queue消息泡校验Middle：validateJoin" + ":" + messageBubbleList.size());
        		PushMessagePacket.push(messageBubbleList, getToken());
                // 消除表达式为空的消息泡
                messageBubbleList.clear();
                messageBubbleList.add(this.setMessageBubble(true, squidId, childId, squidName, MessageBubbleCode.WARN_NO_JOIN_CONDITION.value(), MessageBubbleCode.WARN, "JOINS Condition为空"));
                logger.debug("Queue消息泡校验Middle：validateJoin" + ":" + messageBubbleList.size());
                PushMessagePacket.push(messageBubbleList, getToken());
        	}
        	else {        	
            squidId = Integer.parseInt(joinMap.get("SQUIDID").toString(), 10);
            childId = Integer.parseInt(joinMap.get("CHILDID").toString(), 10);
            squidName = joinMap.get("NAME").toString();
            joinCondition = joinMap.get("JOIN_CONDITION")==null?null:joinMap.get("JOIN_CONDITION").toString();
            
            if(StringUtils.isNull(joinCondition)){
            //String err = parse(SQL_SELECT+filter);
/*            PushMessagePacket.push(new MessageBubble(key, key, 
                    MessageBubbleCode.ERROR_SQL_SYNTAX_FILTER.value(), StringUtils.isNull(err), err), token);*/
            messageBubbleList.clear();
            messageBubbleList.add(this.setMessageBubble(false, squidId, childId, squidName, MessageBubbleCode.WARN_NO_JOIN_CONDITION.value(), MessageBubbleCode.WARN, "JOINS Condition为空"));
            logger.debug("Queue消息泡校验Middle：validateJoin" + ":" + messageBubbleList.size());
            PushMessagePacket.push(messageBubbleList, getToken());
        }
        else {
            // 消除表达式为空的消息泡
            messageBubbleList.clear();
            messageBubbleList.add(this.setMessageBubble(true, squidId, childId, squidName, MessageBubbleCode.WARN_NO_JOIN_CONDITION.value(), MessageBubbleCode.WARN, "JOINS Condition为空"));
            logger.debug("Queue消息泡校验Middle：validateJoin" + ":" + messageBubbleList.size());
            PushMessagePacket.push(messageBubbleList, getToken());
            //adapter.execute(sql);
            // 校验信息
            error = SQLValidator.parse(SQLValidator.SQL_SELECT+joinCondition);
            // 如果校验信息为空,那么验证通过
            if(StringUtils.isNull(error)){
                // 消除消息泡
                messageBubbleList.clear();
                messageBubbleList.add(this.setMessageBubble(true, squidId, childId, squidName, MessageBubbleCode.ERROR_SQL_SYNTAX_JOIN.value(), MessageBubbleCode.ERROR, "表达式校验正确"));
                logger.debug("Queue消息泡校验Middle：validateJoin" + ":" + messageBubbleList.size());
                PushMessagePacket.push(messageBubbleList, getToken());
            }
            else {
                // 推送消息泡
                messageBubbleList.clear();
                messageBubbleList.add(this.setMessageBubble(false, squidId, childId, squidName, MessageBubbleCode.ERROR_SQL_SYNTAX_JOIN.value(), MessageBubbleCode.ERROR, error));
                logger.debug("Queue消息泡校验Middle：validateJoin" + ":" + messageBubbleList.size());
                PushMessagePacket.push(messageBubbleList, getToken());
            }
        
         } 
        }
        } 
        
        else {
//        	String sql = "select dss.id as id, dss.name as name, dsj.join_condition as join_condition from ds_squid dss, ds_join dsj where dss.squid_flow_id = " + childId + " and dss.id = dsj.joined_squid_id and dsj.target_squid_id is not null and dsj.target_squid_id != 0 and dsj.join_type_id != 0 and dss.squid_type_id = " + SquidTypeEnum.STAGE.value();
        	String sql = "select pre.id, pre.childId, nex.name, pre.join_condition from (select dss.id as id,dsj.id as childId, dsj.target_squid_id, dsj.join_condition as join_condition from ds_squid dss, ds_join dsj where dss.squid_flow_id = " + childId + " and dss.id = dsj.joined_squid_id and dsj.target_squid_id is not null and dsj.target_squid_id != 0 and dsj.join_type_id != 0 and dss.squid_type_id = " + SquidTypeEnum.STAGE.value() + ") as pre inner join (select dsj.target_squid_id, dss.name from ds_squid dss inner join ds_join dsj on dss.id = dsj.target_squid_id where dsj.target_squid_id in (select dsj.target_squid_id from ds_squid dss, ds_join dsj where dss.squid_flow_id = " + childId + " and dss.id = dsj.joined_squid_id and dsj.target_squid_id is not null and dsj.target_squid_id != 0 and dsj.join_type_id != 0 and dss.squid_type_id = " + SquidTypeEnum.STAGE.value() + ")) as nex on pre.target_squid_id = nex.target_squid_id";
                List<Map<String, Object>> joinMapList = adapter.query2List(false, sql, null);
                if(StringUtils.isNotNull(joinMapList)){
                for(Map<String, Object> joinMap : joinMapList){
                    squidId = Integer.parseInt(joinMap.get("ID").toString(), 10);
                    childId = Integer.parseInt(joinMap.get("CHILDID").toString(), 10);
                    squidName = joinMap.get("NAME").toString();
                    joinCondition = joinMap.get("JOIN_CONDITION")==null?null:joinMap.get("JOIN_CONDITION").toString();
                    if(StringUtils.isNull(joinCondition)){
                    //String err = parse(SQL_SELECT+filter);
                    /*            PushMessagePacket.push(new MessageBubble(key, key, 
                                        MessageBubbleCode.ERROR_SQL_SYNTAX_FILTER.value(), StringUtils.isNull(err), err), token);*/
                                messageBubbleList.clear();
                                messageBubbleList.add(this.setMessageBubble(false, squidId, childId, squidName, MessageBubbleCode.WARN_NO_JOIN_CONDITION.value(), MessageBubbleCode.WARN, "JOINS Condition为空"));
                                logger.debug("Queue消息泡校验Middle：validateJoin" + ":" + messageBubbleList.size());
                                PushMessagePacket.push(messageBubbleList, getToken());
                    }
                    else if (StringUtils.isNotNull(joinCondition)){
                                // 消除表达式为空的消息泡
                                messageBubbleList.clear();
                                messageBubbleList.add(this.setMessageBubble(true, squidId, childId, squidName, MessageBubbleCode.WARN_NO_JOIN_CONDITION.value(), MessageBubbleCode.WARN, "JOINS Condition为空"));
                                logger.debug("Queue消息泡校验Middle：validateJoin" + ":" + messageBubbleList.size());
                                PushMessagePacket.push(messageBubbleList, getToken());
                                //adapter.execute(sql);
                                // 校验信息
                                error = SQLValidator.parse(SQLValidator.SQL_SELECT+joinCondition);
                                // 如果校验信息为空,那么验证通过
                                if(StringUtils.isNull(error)){
                                    // 消除消息泡
                                    messageBubbleList.clear();
                                    messageBubbleList.add(this.setMessageBubble(true, squidId, childId, joinCondition, MessageBubbleCode.ERROR_SQL_SYNTAX_JOIN.value(), MessageBubbleCode.ERROR, "表达式校验正确"));
                                    logger.debug("Queue消息泡校验Middle：validateJoin" + ":" + messageBubbleList.size());
                                    PushMessagePacket.push(messageBubbleList, getToken());
                                }
                                else {
                                    // 推送消息泡
                                    messageBubbleList.clear();
                                    messageBubbleList.add(this.setMessageBubble(false, squidId, childId, joinCondition, MessageBubbleCode.ERROR_SQL_SYNTAX_JOIN.value(), MessageBubbleCode.ERROR, error));
                                    logger.debug("Queue消息泡校验Middle：validateJoin" + ":" + messageBubbleList.size());
                                    PushMessagePacket.push(messageBubbleList, getToken());
                                }
                    
                }
                
                }
                }
            
        
        }
        }catch (SQLException e) {
        	// TODO Auto-generated catch block
        	e.printStackTrace();
        }
            //logger.debug("消息泡校验： validateJoin : (" + squidId + "," + childId + "," + joinCondition + "," + code + ") end!");
            //PushMessagePacket.push(messageBubbleList, token);
    }
    
	/**
     * 10.14.17.20.校验HostAddress
     * @param host
     * @return
     * @author bo.dang
	 * @throws SQLException 
     * @date 2014年6月5日
     */
    public void validateHostAddress(int squidId, int childId, String host, int code){
        try {
        CallableStatement call = adapter.prepareCall(false, "call validateHostAddress(?, ?, ?)");
            call.setInt(1, squidId);
        call.execute();
        Boolean flag = call.getBoolean(3);
        messageBubbleList.clear();
        logger.info("数据校验 - 校验HostAddress是否为空");
        if(!flag){
            messageBubbleList.add(this.setMessageBubble(false, squidId, childId, host, MessageBubbleCode.ERROR_SQUID_CONNECTION_NO_HOST.value(), MessageBubbleCode.ERROR, "连接主机未设置"));
        }
        else {
            messageBubbleList.add(this.setMessageBubble(true, squidId, childId, host, MessageBubbleCode.ERROR_SQUID_CONNECTION_NO_HOST.value(), MessageBubbleCode.ERROR, "连接主机已设置"));
        }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        PushMessagePacket.push(messageBubbleList, getToken());
    }
    
    /**
     * 11.校验RDBMSType
     * @param type
     * @return
     * @author bo.dang
     * @date 2014年6月5日
     */
    public void validateRDBMSType(int squidId, int childId, String type, int code){
        messageBubbleList.clear();
        logger.info("数据校验 - 校验RDBMSType是否为空");
        if(StringUtils.isNull(type)){
            messageBubbleList.add(this.setMessageBubble(false, squidId, childId, type, MessageBubbleCode.ERROR_SQUID_CONNECTION_NO_RDBMSTYPE.value(), MessageBubbleCode.ERROR, "RDBMSType未设置"));
        }
        else {
            messageBubbleList.add(this.setMessageBubble(true, squidId, childId, type, MessageBubbleCode.ERROR_SQUID_CONNECTION_NO_RDBMSTYPE.value(), MessageBubbleCode.ERROR, "RDBMSType已设置"));
        }
        PushMessagePacket.push(messageBubbleList, getToken());
    }
	
    /**
     * 12.校验DataBaseName
     * @param dataBaseName
     * @return
     * @author bo.dang
     * @date 2014年6月5日
     */
    public void validateDataBaseName(int squidId, int childId, String dataBaseName, int code){
        messageBubbleList.clear();
        logger.info("数据校验 - 校验DataBaseName是否为空");
        if(StringUtils.isNull(dataBaseName)){
            messageBubbleList.add(this.setMessageBubble(false, squidId, childId, dataBaseName, MessageBubbleCode.ERROR_SQUID_CONNECTION_NO_DATABASE_NAME.value(), MessageBubbleCode.ERROR, "DataBaseName未设置"));
        }
        else {
            messageBubbleList.add(this.setMessageBubble(true, squidId, childId, dataBaseName, MessageBubbleCode.ERROR_SQUID_CONNECTION_NO_DATABASE_NAME.value(), MessageBubbleCode.ERROR, "DataBaseName已设置"));
        }
        PushMessagePacket.push(messageBubbleList, getToken());
    }
    
    /**
     * 15.18.21.校验FilePath
     * @param filePath
     * @return
     * @author bo.dang
     * @date 2014年6月5日
     */
    public void validateFilePath(int squidId, int childId, String filePath, int code){
        messageBubbleList.clear();
        logger.info("数据校验 - 校验FilePath是否为空");
        if(StringUtils.isNull(filePath)){
            messageBubbleList.add(this.setMessageBubble(false, squidId, childId, filePath, MessageBubbleCode.ERROR_SQUID_CONNECTION_NO_FILE_PATH.value(), MessageBubbleCode.ERROR, "FilePath文件路径未设置"));
        }
        else {
            messageBubbleList.add(this.setMessageBubble(true, squidId, childId, filePath, MessageBubbleCode.ERROR_SQUID_CONNECTION_NO_FILE_PATH.value(), MessageBubbleCode.ERROR, "FilePath文件路径已设置"));
        }
        PushMessagePacket.push(messageBubbleList, getToken());
    }
    
    
    /**
     * 23.校验ServiceProvider微博提供商设置
     * @param filePath
     * @return
     * @author bo.dang
     */
    public void validateServiceProvider(int squidId, int childId, String ServiceProvider, int code){
        messageBubbleList.clear();
        logger.info("数据校验 - 校验微博提供商设置是否为空");
        if(StringUtils.isNull(ServiceProvider)){
            messageBubbleList.add(this.setMessageBubble(false, squidId, childId, ServiceProvider, MessageBubbleCode.ERROR_WEIBO_CONNECTION_NO_SERVICE_PROVIDER.value(), MessageBubbleCode.ERROR, "ServiceProvider微博提供商未设置"));
        }
        else {
            messageBubbleList.add(this.setMessageBubble(true, squidId, childId, ServiceProvider, MessageBubbleCode.ERROR_WEIBO_CONNECTION_NO_SERVICE_PROVIDER.value(), MessageBubbleCode.ERROR, "ServiceProvider微博提供商已设置"));
        }
        PushMessagePacket.push(messageBubbleList, getToken());
    }
    
    /**
     * 25.校验WebConnection的URL未设置
     * @param filePath
     * @return
     * @author bo.dang
     * @date 2014年6月5日
     */
    public void validateURLList(int squidId, int childId, String name, int code){
        String sql = "select dsu.url from ds_web_connection dwc, ds_url dsu where dwc.id = dsu.squid_id";
        List<Map<String, Object>> urlMapList;
        try {
            urlMapList = adapter.query2List(false, sql, null);
        
        messageBubbleList.clear();
        logger.info("数据校验 - 校验URL设置是否为空");
        if(StringUtils.isNull(urlMapList)){
            messageBubbleList.add(this.setMessageBubble(false, squidId, childId, "URL", MessageBubbleCode.WARN_WEB_CONNECTION_NO_URL_LIST.value(), MessageBubbleCode.WARN, "URL未设置"));
        }
        else {
            messageBubbleList.add(this.setMessageBubble(true, squidId, childId, "URL", MessageBubbleCode.WARN_WEB_CONNECTION_NO_URL_LIST.value(), MessageBubbleCode.WARN, "URL已设置"));
        }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        PushMessagePacket.push(messageBubbleList, getToken());
    }
    
    /**
     * 追踪历史开关已打开，但没设置Business Key
     * @param squidId
     * @param childId
     * @param code
     * @author bo.dang
     * @date 2014年6月5日
     */
    public void validateCDC(int squidId, int childId, int code){
        
    }
    
    /**
     * 30.追踪历史开关已打开
     * @param squidId
     * @param childId
     * @param code
     * @author bo.dang
     * @date 2014年6月6日
     */
    public void validateBusinessKey(int squidId, int childId, String name, int code){
        messageBubbleList.clear();
        callStatement = adapter.prepareCall(false, "call validateBusinessKey(?, ?, ?, ?)");
        try {
        callStatement.setInt(1, squidId);
        callStatement.setInt(2, 0);
        callStatement.setInt(3, 0);
        callStatement.execute();
        Integer bk = callStatement.getInt(4);
        callStatement.close();
        // bk=1 : 追踪历史开关已打开，已设置Business Key; bk=2 : 追踪历史开关已打开，但没设置Business Key; bk=0: 不需要推送消息泡
        if(bk == 1){
            messageBubbleList.add(this.setMessageBubble(true, squidId, childId, name, MessageBubbleCode.WARN_DATA_SQUID_NO_BUSINESS_KEY.value(), MessageBubbleCode.WARN, "追踪历史开关已打开，已设置Business Key"));
        }
        else if(bk == 2){
            messageBubbleList.add(this.setMessageBubble(false, squidId, childId, name, MessageBubbleCode.WARN_DATA_SQUID_NO_BUSINESS_KEY.value(), MessageBubbleCode.WARN, "追踪历史开关已打开，但没设置Business Key"));
        }
        
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        PushMessagePacket.push(messageBubbleList, getToken());
    }
    
    /**
     * 31.异常squid已定义
     * @param squidId
     * @param childId
     * @param name
     * @param code
     * @author bo.dang
     * @date 2014年6月6日
     */
    public void validateExceptionSquid(int squidId, int childId, String name, int code){
        //messageBubbleList = new ArrayList<MessageBubble>();
        try {
            Map<String, String> paramsMap = new HashMap<String, String>();
            List<Map<String, Object>> toSquidIdMapList = null;
            Squid squid = null;
        if(squidId != 0){
        paramsMap.put("from_squid_id", Integer.toString(squidId, 10));
//        String sql2 = "select dsl.to_squid_id as to_squid_id from ds_data_squid dds, ds_squid_link dsl where dds.id = " + squidId + " and dds.exception_handling_flag = 0 and dds.id = dsl.from_squid_id";
        String sql2 = "select dds.id as id, dsl.to_squid_id as to_squid_id from ds_data_squid dds left join ds_squid_link dsl on dds.id = dsl.from_squid_id where dds.id = " + squidId + " and dds.exception_handling_flag = 0 union select dde.id as id, dsl.to_squid_id as to_squid_id from ds_doc_extract dde left join ds_squid_link dsl on dde.id = dsl.from_squid_id where dde.id = " + squidId + " and dde.exception_handling_flag = 0";
        
        toSquidIdMapList = adapter.query2List(sql2, null);
        
        if(StringUtils.isNotNull(toSquidIdMapList)){
            SquidLink squidLink = null;
            int toSquidId = 0;
            int count = 0;
            for (Map<String, Object> toSquidIdMap : toSquidIdMapList) {
                if(null == toSquidIdMap.get("TO_SQUID_ID")){
                    messageBubbleList.clear();
                    logger.debug("消息泡校验：validateExceptionSquid : " + count++ + "," + false + "," + squidId + "," + 0 + "," + name + "," + code + "," + MessageBubbleCode.WARN_DATA_SQUID_NO_EXCEPTION_SQUID.value() + "," + "异常squid未定义");
                    messageBubbleList.add(this.setMessageBubble(false, squidId, 0, name, MessageBubbleCode.WARN_DATA_SQUID_NO_EXCEPTION_SQUID.value(), MessageBubbleCode.WARN, "异常squid未定义"));
                    logger.debug("Queue消息泡校验Middle：validateExceptionSquid" + ":" + messageBubbleList.size());
                    PushMessagePacket.push(messageBubbleList, getToken());
                    continue;
                }
                else {
                    
                    toSquidId = Integer.parseInt(toSquidIdMap.get("TO_SQUID_ID").toString());
                }
                paramsMap.clear();
                paramsMap.put("id", Integer.toString(toSquidId, 10));
                paramsMap.put("squid_type_id", Integer.toString(SquidTypeEnum.EXCEPTION.value(), 10));
                squid = adapter.query2Object(paramsMap, Squid.class);
                if(StringUtils.isNull(squid)){
                    messageBubbleList.clear();
                    logger.debug("消息泡校验：validateExceptionSquid : " + count++ + "," + false + "," + squidId + "," + 0 + "," + name + "," + code + "," + MessageBubbleCode.WARN_DATA_SQUID_NO_EXCEPTION_SQUID.value() + "," + "异常squid未定义");
                    messageBubbleList.add(this.setMessageBubble(false, squidId, 0, name, MessageBubbleCode.WARN_DATA_SQUID_NO_EXCEPTION_SQUID.value(), MessageBubbleCode.WARN, "异常squid未定义"));
                    logger.debug("Queue消息泡校验Middle：validateExceptionSquid" + ":" + messageBubbleList.size());
                    PushMessagePacket.push(messageBubbleList, getToken());
                }
                else {
                    messageBubbleList.clear();
                    logger.debug("消息泡校验：validateExceptionSquid : " + count++ + "," + true + "," + squidId + "," + 0 + "," + name + "," + code + "," + MessageBubbleCode.WARN_DATA_SQUID_NO_EXCEPTION_SQUID.value() + "," + "异常squid未定义");
                    messageBubbleList.add(this.setMessageBubble(true, squidId, 0, name, MessageBubbleCode.WARN_DATA_SQUID_NO_EXCEPTION_SQUID.value(), MessageBubbleCode.WARN, "异常squid已定义"));
                    logger.debug("Queue消息泡校验Middle：validateExceptionSquid" + ":" + messageBubbleList.size());
                    PushMessagePacket.push(messageBubbleList, getToken());
                }
            }
            
        }
        }
        else {
            paramsMap.put("from_squid_id", Integer.toString(squidId, 10));
//            String sql3 = "select dds.id as id, dsl.to_squid_id as to_squid_id from ds_squid dss, ds_data_squid dds, ds_squid_link dsl where dss.squid_flow_id = " + childId + " and dss.id = dds.id and dds.exception_handling_flag = 0 and dds.id = dsl.from_squid_id";
            String sql3 = "select dds.id as id, dsl.to_squid_id as to_squid_id from ds_squid dss inner join ds_data_squid dds on dss.id = dds.id left join ds_squid_link dsl on dds.id = dsl.from_squid_id where dss.squid_flow_id = " + childId + " and dss.squid_type_id != 20 and dds.exception_handling_flag = 0 union select dde.id as id, dsl.to_squid_id as to_squid_id from ds_squid dss inner join ds_doc_extract dde on dss.id = dde.id left join ds_squid_link dsl on dde.id = dsl.from_squid_id where dss.squid_flow_id = " + childId + " and dss.squid_type_id in and dde.exception_handling_flag = 0";
            
            toSquidIdMapList = adapter.query2List(sql3, null);
            
            if(StringUtils.isNotNull(toSquidIdMapList)){
                SquidLink squidLink = null;
                int toSquidId = 0;
                int count = 0;
                for (Map<String, Object> toSquidIdMap : toSquidIdMapList) {
                    squidId = Integer.parseInt(toSquidIdMap.get("ID").toString());
                    if(null == toSquidIdMap.get("TO_SQUID_ID")){
                        messageBubbleList.clear();
                        logger.debug("消息泡校验：validateExceptionSquid : " + count++ + "," + false + "," + squidId + "," + 0 + "," + name + "," + code + "," + MessageBubbleCode.WARN_DATA_SQUID_NO_EXCEPTION_SQUID.value() + "," + "异常squid未定义");
                        messageBubbleList.add(this.setMessageBubble(false, squidId, 0, name, MessageBubbleCode.WARN_DATA_SQUID_NO_EXCEPTION_SQUID.value(), MessageBubbleCode.WARN, "异常squid未定义"));
                        logger.debug("Queue消息泡校验Middle：validateExceptionSquid" + ":" + messageBubbleList.size());
                        PushMessagePacket.push(messageBubbleList, getToken());
                        continue;
                    }
                    else {
                        
                        toSquidId = Integer.parseInt(toSquidIdMap.get("TO_SQUID_ID").toString());
                    }
                    paramsMap.clear();
                    paramsMap.put("id", Integer.toString(toSquidId, 10));
                    paramsMap.put("squid_type_id", Integer.toString(SquidTypeEnum.EXCEPTION.value(), 10));
                    squid = adapter.query2Object(paramsMap, Squid.class);
                    if(StringUtils.isNull(squid)){
                        messageBubbleList.clear();
                        logger.debug("消息泡校验：validateExceptionSquid : " + count++ + "," + false + "," + squidId + "," + 0 + "," + name + "," + code + "," + MessageBubbleCode.WARN_DATA_SQUID_NO_EXCEPTION_SQUID.value() + "," + "异常squid未定义");
                        messageBubbleList.add(this.setMessageBubble(false, squidId, 0, name, MessageBubbleCode.WARN_DATA_SQUID_NO_EXCEPTION_SQUID.value(), MessageBubbleCode.WARN, "异常squid未定义"));
                        logger.debug("Queue消息泡校验Middle：validateExceptionSquid" + ":" + messageBubbleList.size());
                        PushMessagePacket.push(messageBubbleList, getToken());
                    }
                    else {
                        messageBubbleList.clear();
                        logger.debug("消息泡校验：validateExceptionSquid : " + count++ + "," + true + "," + squidId + "," + 0 + "," + name + "," + code + "," + MessageBubbleCode.WARN_DATA_SQUID_NO_EXCEPTION_SQUID.value() + "," + "异常squid未定义");
                        messageBubbleList.add(this.setMessageBubble(true, squidId, 0, name, MessageBubbleCode.WARN_DATA_SQUID_NO_EXCEPTION_SQUID.value(), MessageBubbleCode.WARN, "异常squid已定义"));
                        logger.debug("Queue消息泡校验Middle：validateExceptionSquid" + ":" + messageBubbleList.size());
                        PushMessagePacket.push(messageBubbleList, getToken());
                    }
                }
                
            }
        }
        
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        //PushMessagePacket.push(messageBubbleList, token);
    }
    
    /**
     * 2.32.33incremental expression 校验增量抽取表达式
     * @param squidId
     * @param childId
     * @param name
     * @param code
     * @author bo.dang
     * @date 2014年6月6日
     */
    public void validateIncrement(int squidId, int childId, String name, int code){
        String error = null;
        List<Map<String, Object>> incrementMapList = null;
        //messageBubbleList = new ArrayList<MessageBubble>();
        try {
        if(squidId != 0) {
            String sql2 = "select dss.id as id, dss.name as name, dds.incremental_expression as incremental_expression from ds_squid dss, ds_data_squid dds where dss.id = dds.id and dss.id = " + squidId + " and dss.squid_type_id = 2 and dds.is_incremental = 'Y'";
        Map<String, String> paramsMap = new HashMap<String, String>();
        paramsMap.put("id", Integer.toString(squidId, 10));
        paramsMap.put("is_incremental", "Y");
        DataSquid dataSquid = adapter.query2Object(paramsMap, DataSquid.class);
            incrementMapList = adapter.query2List(sql2, null);
        
        if(StringUtils.isNull(incrementMapList) || incrementMapList.isEmpty()){
            messageBubbleList.clear();
            messageBubbleList.add(this.setMessageBubble(true, squidId, childId, name, MessageBubbleCode.ERROR_DATA_SQUID_NO_INCREMENTAL_EXPRESSION.value(), MessageBubbleCode.ERROR, "增量抽取表达式未设置"));
            logger.debug("Queue消息泡校验Middle：validateIncrement" + ":" + messageBubbleList.size());
            PushMessagePacket.push(messageBubbleList, getToken());
        }
        else {
            String squidName = null;
            String incrementalExpression = null;
            for(Map<String, Object> incrementMap : incrementMapList){
                squidId = Integer.parseInt(incrementMap.get("ID").toString(), 10);
                squidName = incrementMap.get("NAME").toString();
                incrementalExpression = incrementMap.get("INCREMENTAL_EXPRESSION") == null ? null:incrementMap.get("INCREMENTAL_EXPRESSION").toString();
        
            incrementalExpression = dataSquid.getIncremental_expression();
        
        if(StringUtils.isNotNull(incrementalExpression)){
            messageBubbleList.clear();
            messageBubbleList.add(this.setMessageBubble(true, squidId, childId, squidName, MessageBubbleCode.ERROR_DATA_SQUID_NO_INCREMENTAL_EXPRESSION.value(), MessageBubbleCode.ERROR, "增量抽取表达式未设置"));
            logger.debug("Queue消息泡校验Middle：validateIncrement" + ":" + messageBubbleList.size());
            PushMessagePacket.push(messageBubbleList, getToken());
            error = SQLValidator.parse(SQLValidator.SQL_SELECT + incrementalExpression);
            if(StringUtils.isNotNull(error)){
                messageBubbleList.clear();
                messageBubbleList.add(this.setMessageBubble(false, squidId, childId, squidName, MessageBubbleCode.ERROR_SQL_SYNTAX_INCREMENT.value(), MessageBubbleCode.ERROR, "增量抽取表达式设置非法："  + error));
                logger.debug("Queue消息泡校验Middle：validateIncrement" + ":" + messageBubbleList.size());
                PushMessagePacket.push(messageBubbleList, getToken());
            }
            else {
                messageBubbleList.clear();
                messageBubbleList.add(this.setMessageBubble(true, squidId, childId, squidName, MessageBubbleCode.ERROR_SQL_SYNTAX_INCREMENT.value(), MessageBubbleCode.ERROR, "增量抽取表达式设置非法"));
                logger.debug("Queue消息泡校验Middle：validateIncrement" + ":" + messageBubbleList.size());
                PushMessagePacket.push(messageBubbleList, getToken());
            }
        }
        else {
            messageBubbleList.clear();
            messageBubbleList.add(this.setMessageBubble(false, squidId, childId, squidName, MessageBubbleCode.ERROR_DATA_SQUID_NO_INCREMENTAL_EXPRESSION.value(), MessageBubbleCode.ERROR, "增量抽取表达式未设置"));
            logger.debug("Queue消息泡校验Middle：validateIncrement" + ":" + messageBubbleList.size());
            PushMessagePacket.push(messageBubbleList, getToken());
        }
        }
        }
        } else {
            String sql3 = "select dss.id as id, dss.name as name, dds.incremental_expression as incremental_expression from ds_data_squid dds, ds_squid dss where dss.squid_flow_id = " + childId + " and dss.id = dds.id and dss.squid_type_id = 2 and dds.is_incremental = 'Y'";
                incrementMapList = adapter.query2List(sql3, null);
                String squidName = null;
                String incrementalExpression = null;
                for(Map<String, Object> incrementMap : incrementMapList){
                    squidId = Integer.parseInt(incrementMap.get("ID").toString(), 10);
                    squidName = incrementMap.get("NAME").toString();
                    incrementalExpression = incrementMap.get("INCREMENTAL_EXPRESSION") == null ? null:incrementMap.get("INCREMENTAL_EXPRESSION").toString();
                    if(StringUtils.isNotNull(incrementalExpression)){
                        messageBubbleList.clear();
                        messageBubbleList.add(this.setMessageBubble(true, squidId, childId, squidName, MessageBubbleCode.ERROR_DATA_SQUID_NO_INCREMENTAL_EXPRESSION.value(), MessageBubbleCode.ERROR, "增量抽取表达式未设置"));
                        logger.debug("Queue消息泡校验Middle：validateIncrement" + ":" + messageBubbleList.size());
                        PushMessagePacket.push(messageBubbleList, getToken());
                        error = SQLValidator.parse(SQLValidator.SQL_SELECT + incrementalExpression);
                        if(StringUtils.isNotNull(error)){
                            messageBubbleList.clear();
                            messageBubbleList.add(this.setMessageBubble(false, squidId, childId, squidName, MessageBubbleCode.ERROR_SQL_SYNTAX_INCREMENT.value(), MessageBubbleCode.ERROR, "增量抽取表达式设置非法：" + error));
                            logger.debug("Queue消息泡校验Middle：validateIncrement" + ":" + messageBubbleList.size());
                            PushMessagePacket.push(messageBubbleList, getToken());
                        }
                        else {
                            messageBubbleList.clear();
                            messageBubbleList.add(this.setMessageBubble(true, squidId, childId, squidName, MessageBubbleCode.ERROR_SQL_SYNTAX_INCREMENT.value(), MessageBubbleCode.ERROR, "增量抽取表达式设置非法"));
                            logger.debug("Queue消息泡校验Middle：validateIncrement" + ":" + messageBubbleList.size());
                            PushMessagePacket.push(messageBubbleList, getToken());
                        }
                    }
                    else {
                        messageBubbleList.clear();
                        messageBubbleList.add(this.setMessageBubble(false, squidId, childId, squidName, MessageBubbleCode.ERROR_DATA_SQUID_NO_INCREMENTAL_EXPRESSION.value(), MessageBubbleCode.ERROR, "增量抽取表达式未设置"));
                        logger.debug("Queue消息泡校验Middle：validateIncrement" + ":" + messageBubbleList.size());
                        PushMessagePacket.push(messageBubbleList, getToken());
                    }
                }
            
        }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        //PushMessagePacket.push(messageBubbleList, token);
    } 
    
    /**
     * 34.文档抽取Squid(DocExtractSquid)验证Delimiter
     * @param squidId
     * @param childId
     * @param name
     * @param code
     * @author bo.dang
     * @date 2014年6月6日
     */
    public void validateDelimiter(int squidId, int childId, String name, int code){
        messageBubbleList.clear();
        callStatement = adapter.prepareCall(false, "call validateDelimiter(?, ?, ?, ?)");
        try {
        callStatement.setInt(1, squidId);
        callStatement.setInt(2, 0);
        callStatement.setInt(3, 0);
        callStatement.execute();
        //callStatement.getArray(parameterName);
        String delimiter = callStatement.getString(4);
        callStatement.close();
        if(StringUtils.isNull(delimiter)){
            messageBubbleList.add(this.setMessageBubble(false, squidId, childId, name, MessageBubbleCode.ERROR_DOC_EXTRACT_SQUID_NO_DELIMITER.value(), MessageBubbleCode.ERROR, "分隔符未设置"));
        }
        else {
            messageBubbleList.add(this.setMessageBubble(true, squidId, childId, name, MessageBubbleCode.ERROR_DOC_EXTRACT_SQUID_NO_DELIMITER.value(), MessageBubbleCode.ERROR, "分隔符已设置"));
        }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    /**
     * 35.FieldLength字段长度设置非法
     * @param squidId
     * @param childId
     * @param name
     * @param code
     * @author bo.dang
     * @date 2014年6月6日
     */
    public void validateFieldLength(int squidId, int childId, String name, int code){
        messageBubbleList.clear();
        callStatement = adapter.prepareCall(false, "call validateFieldLength(?, ?, ?, ?)");
        try {
        callStatement.setInt(1, squidId);
        callStatement.setInt(2, 0);
        callStatement.setInt(3, 0);
        callStatement.execute();
        int fieldLength = callStatement.getInt(4);
        callStatement.close();
        if(StringUtils.isNull(fieldLength) || fieldLength == 0){
            messageBubbleList.add(this.setMessageBubble(false, squidId, childId, name, MessageBubbleCode.ERROR_DOC_EXTRACT_SQUID_NO_FIELD_LENGTH.value(), MessageBubbleCode.ERROR, "字段长度设置非法"));
        }
        else {
            messageBubbleList.add(this.setMessageBubble(true, squidId, childId, name, MessageBubbleCode.ERROR_DOC_EXTRACT_SQUID_NO_FIELD_LENGTH.value(), MessageBubbleCode.ERROR, "字段长度设置正确"));
        }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    /**
     * 36.校验WebLogExtractSquid是否完成获取元数据
     * @param squidId
     * @param childId
     * @param name
     * @param code
     * @author bo.dang
     * @date 2014年6月6日 
     */
    public void validateWebLogExtractSquidSourceData(int squidId, int childId, String name, int code){
        messageBubbleList.clear();
        callStatement = adapter.prepareCall(false, "call validateWebLogExtractSquidSourceData(?, ?, ?, ?)");
        try {
        callStatement.setInt(1, squidId);
        callStatement.setInt(2, 0);
        callStatement.setInt(3, 0);
        callStatement.execute();
        int fieldLength = callStatement.getInt(4);
        callStatement.close();
        if(fieldLength == 0){
            messageBubbleList.add(this.setMessageBubble(false, squidId, childId, name, MessageBubbleCode.ERROR_WEBLOG_EXTRACT_SQUID_NO_SOURCE_DATA.value(), MessageBubbleCode.ERROR, "尚未完成元数据设置"));
        }
        else {
            messageBubbleList.add(this.setMessageBubble(true, squidId, childId, name, MessageBubbleCode.ERROR_WEBLOG_EXTRACT_SQUID_NO_SOURCE_DATA.value(), MessageBubbleCode.ERROR, "已完成元数据设置"));
        }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        //推送消息泡
        PushMessagePacket.push(messageBubbleList, getToken());
    }
    
    /**
     * 41.Transform的实参个数和其定义的形参个数
     * @param squidId
     * @param childId
     * @param name
     * @param code
     * @author bo.dang
     * @date 2014年6月10日
     */
    public void validateTranParameters(int squidId, int childId, String name, int code){
        // fixed bug 962 start by bo.dang
        //String sql2 = "select * from ds_transformation dst, ds_tran_inputs dti where dst.squid_id = " + squidId + "dst.id = dti.transformation_id and dti.source_transform_id is not null and dti.source_transform_id ！= 0 ";
        //messageBubbleList = new ArrayList<MessageBubble>();
        try {
            String sql2 = null;
        List<Map<String, Object>> squidMapList = null;
        List<Map<String, Object>> tranMapList = null;
        Map<String, Object> tranDefinitionMap = null;
        int tranTypeId = 0;
        int max_input_order = 0;
        String tranName = null;
        if(squidId != -1){
        if(squidId != 0){
            sql2 = "select dst.squid_id as squidId, dst.id as childId, dst.name as name, dst.transformation_type_id as transformation_type_id from ds_transformation dst, ds_tran_inputs dti where dst.squid_id = " + squidId + " and dst.transformation_type_id != 0 and dst.id = dti.transformation_id and dti.source_transform_id is not null and dti.source_transform_id != 0";
        }
        else {
            sql2 = "select dss.id as squidId, dst.id as childId, dst.name as name, dst.transformation_type_id as transformation_type_id from ds_squid dss inner join ds_transformation dst on dss.id = dst.squid_id inner join ds_tran_inputs dti on dst.id = dti.transformation_id where dss.squid_flow_id = " + childId + " and dst.transformation_type_id != 0 and dti.source_transform_id is not null and dti.source_transform_id != 0";
        }
//                String sql2 = "select dst.name as name, dst.transformation_type_id as transformation_type_id from ds_transformation dst, ds_tran_inputs dti where dst.squid_id = " + squidId + " and dst.transformation_type_id != 0 and dst.id = dti.transformation_id and dti.source_transform_id is not null and dti.source_transform_id ！= 0 ";
                //String sql2 = "select * from ds_transformation dst, ds_tran_inputs dti where dst.id = " + tranId + " dst.id = dti.transformation_id and dti.source_transform_id is not null and dti.source_transform_id ！= 0 ";
//                    List<Map<String, Object>> tranMapList = adapter.query2List(false, sql2, null);
                    squidMapList = adapter.query2List4(true, sql2, null);
                    if(StringUtils.isNotNull(squidMapList) && !squidMapList.isEmpty()){
                        for(Map<String, Object> squidMap : squidMapList){
                            squidId = Integer.parseInt(squidMap.get("SQUIDID").toString());
                            childId = Integer.parseInt(squidMap.get("CHILDID").toString());
                            tranName = squidMap.get("NAME") == null ? "":squidMap.get("NAME").toString();
                            tranTypeId = Integer.parseInt(squidMap.get("TRANSFORMATION_TYPE_ID").toString(), 10);
                            String sql4 = "select dst.name as name, dst.transformation_type_id as transformation_type_id from ds_transformation dst, ds_tran_inputs dti where dst.id = " + childId + " and dst.transformation_type_id != 0 and dst.id = dti.transformation_id and dti.source_transform_id is not null and dti.source_transform_id != 0";
                            tranMapList = adapter.query2List4(true, sql4, null);
                            if(StringUtils.isNull(tranMapList)){
                                messageBubbleList.clear();
                                messageBubbleList.add(this.setMessageBubble(false, squidId, childId, tranName, MessageBubbleCode.ERROR_TRAN_PARAMETERS.value(), MessageBubbleCode.ERROR, "输入设置非法"));
                                logger.debug("Queue消息泡校验Middle：validateTranParameters" + ":" + messageBubbleList.size());
                                PushMessagePacket.push(messageBubbleList, getToken());
                            }
                            else {
                            String sql3 = "select max(dtid.input_order) as max_input_order from ds_transformation_type dtt, ds_tran_input_definition dtid where dtt.id = " + tranTypeId + " and dtt.code = dtid.code ";
                            tranDefinitionMap = adapter.query2Object(true, sql3, null);
                            if(StringUtils.isNotNull(tranDefinitionMap) && !tranDefinitionMap.isEmpty()) {
                                max_input_order = Integer.parseInt(tranDefinitionMap.get("MAX_INPUT_ORDER").toString());
/*                                if(-1 == max_input_order || 0 == max_input_order) {
                                    if(1 == tranMapList.size()){
                                        messageBubbleList.clear();
                                        messageBubbleList.add(this.setMessageBubble(true, squidId, childId, tranName, MessageBubbleCode.ERROR_TRAN_PARAMETERS.value(), MessageBubbleCode.ERROR, "输入设置正确"));
                                        logger.debug("Queue消息泡校验Middle：validateTranParameters" + ":" + messageBubbleList.size());
                                        PushMessagePacket.push(messageBubbleList, token);
                                    }
                                    else {
                                        messageBubbleList.clear();
                                        messageBubbleList.add(this.setMessageBubble(false, squidId, childId, tranName, MessageBubbleCode.ERROR_TRAN_PARAMETERS.value(), MessageBubbleCode.ERROR, "输入设置非法"));
                                        logger.debug("Queue消息泡校验Middle：validateTranParameters" + ":" + messageBubbleList.size());
                                        PushMessagePacket.push(messageBubbleList, token);
                                    }
                                }*/
                                if(1 == max_input_order){
                                    if(2 == tranMapList.size()){
                                        messageBubbleList.clear();
                                        messageBubbleList.add(this.setMessageBubble(true, squidId, childId, tranName, MessageBubbleCode.ERROR_TRAN_PARAMETERS.value(), MessageBubbleCode.ERROR, "输入设置正确"));
                                        logger.debug("Queue消息泡校验Middle：validateTranParameters" + ":" + messageBubbleList.size());
                                        PushMessagePacket.push(messageBubbleList, getToken());
                                    }
                                    else {
                                        messageBubbleList.clear();
                                        messageBubbleList.add(this.setMessageBubble(false, squidId, childId, tranName, MessageBubbleCode.ERROR_TRAN_PARAMETERS.value(), MessageBubbleCode.ERROR, "输入设置非法"));
                                        logger.debug("Queue消息泡校验Middle：validateTranParameters" + ":" + messageBubbleList.size());
                                        PushMessagePacket.push(messageBubbleList, getToken());
                                    }
                                }
/*                                else if(9999 == max_input_order) {
                                    if(tranMapList.size() <= max_input_order) {
                                        messageBubbleList.clear();
                                        messageBubbleList.add(this.setMessageBubble(true, squidId, childId, tranName, MessageBubbleCode.ERROR_TRAN_PARAMETERS.value(), MessageBubbleCode.ERROR, "输入设置正确"));
                                        logger.debug("Queue消息泡校验Middle：validateTranParameters" + ":" + messageBubbleList.size());
                                        PushMessagePacket.push(messageBubbleList, token);
                                    }
                                }*/
                                
                            }
                            else {
                                messageBubbleList.clear();
                                messageBubbleList.add(this.setMessageBubble(false, squidId, childId, tranName, MessageBubbleCode.ERROR_TRAN_PARAMETERS.value(), MessageBubbleCode.ERROR, "输入设置非法"));
                                logger.debug("Queue消息泡校验Middle：validateTranParameters" + ":" + messageBubbleList.size());
                                PushMessagePacket.push(messageBubbleList, getToken());
                                
                            }
                        }
                        }
                    }
        }
        else if(squidId == -1){
            String sql4 = "select dst.squid_id as squidId, dst.id as childId, dst.name as name, dst.transformation_type_id as transformation_type_id, dti.source_transform_id as source_transform_id from ds_transformation dst, ds_tran_inputs dti where dst.id = " + childId + " and dst.transformation_type_id != 0 and dst.id = dti.transformation_id";
            tranMapList = adapter.query2List4(true, sql4, null);
            if(StringUtils.isNotNull(tranMapList) && !tranMapList.isEmpty()){
                squidId = Integer.parseInt(tranMapList.get(0).get("SQUIDID").toString());
                childId = Integer.parseInt(tranMapList.get(0).get("CHILDID").toString());
                tranName = tranMapList.get(0).get("NAME") == null ? "":tranMapList.get(0).get("NAME").toString();
                tranTypeId = Integer.parseInt(tranMapList.get(0).get("TRANSFORMATION_TYPE_ID").toString(), 10);
                int count = tranMapList.size();
                Integer source_transform_id = null;
                for (int i = 0; i < tranMapList.size(); i++) {
                    source_transform_id = tranMapList.get(i).get("SOURCE_TRANSFORM_ID") == null ? null: Integer.parseInt(tranMapList.get(i).get("SOURCE_TRANSFORM_ID").toString());
                    if(null == source_transform_id || 0 == source_transform_id){
                        count--;
                    }
                }
                
            String sql3 = "select max(dtid.input_order) as max_input_order from ds_transformation_type dtt, ds_tran_input_definition dtid where dtt.id = " + tranTypeId + " and dtt.code = dtid.code ";
            tranDefinitionMap = adapter.query2Object(true, sql3, null);
            if(StringUtils.isNotNull(tranDefinitionMap) && !tranDefinitionMap.isEmpty()){
/*                if(count == tranDefinitionMap.size()){
                    messageBubbleList.clear();
                    messageBubbleList.add(this.setMessageBubble(true, squidId, childId, tranName, MessageBubbleCode.ERROR_TRAN_PARAMETERS.value(), MessageBubbleCode.ERROR, "输入设置正确"));
                    logger.debug("Queue消息泡校验Middle：validateTranParameters" + ":" + messageBubbleList.size());
                    PushMessagePacket.push(messageBubbleList, token);
                }
                else if(count >0 && count <tranDefinitionMapList.size()){
                    messageBubbleList.clear();
                    messageBubbleList.add(this.setMessageBubble(false, squidId, childId, tranName, MessageBubbleCode.ERROR_TRAN_PARAMETERS.value(), MessageBubbleCode.ERROR, "输入设置非法"));
                    logger.debug("Queue消息泡校验Middle：validateTranParameters" + ":" + messageBubbleList.size());
                    PushMessagePacket.push(messageBubbleList, token);
                }
                else if(count == 0){
                    messageBubbleList.clear();
                    messageBubbleList.add(this.setMessageBubble(true, squidId, childId, tranName, MessageBubbleCode.ERROR_TRAN_PARAMETERS.value(), MessageBubbleCode.ERROR, "输入设置非法"));
                    logger.debug("Queue消息泡校验Middle：validateTranParameters" + ":" + messageBubbleList.size());
                    PushMessagePacket.push(messageBubbleList, token);
                }*/
                
                max_input_order = Integer.parseInt(tranDefinitionMap.get("MAX_INPUT_ORDER").toString());
/*                if(-1 == max_input_order || 0 == max_input_order) {
                    if(1 == tranMapList.size()){
                        messageBubbleList.clear();
                        messageBubbleList.add(this.setMessageBubble(true, squidId, childId, tranName, MessageBubbleCode.ERROR_TRAN_PARAMETERS.value(), MessageBubbleCode.ERROR, "输入设置正确"));
                        logger.debug("Queue消息泡校验Middle：validateTranParameters" + ":" + messageBubbleList.size());
                        PushMessagePacket.push(messageBubbleList, token);
                    }
                    else {
                        messageBubbleList.clear();
                        messageBubbleList.add(this.setMessageBubble(false, squidId, childId, tranName, MessageBubbleCode.ERROR_TRAN_PARAMETERS.value(), MessageBubbleCode.ERROR, "输入设置非法"));
                        logger.debug("Queue消息泡校验Middle：validateTranParameters" + ":" + messageBubbleList.size());
                        PushMessagePacket.push(messageBubbleList, token);
                    }
                }*/
                if(1 == max_input_order){
                    if(count == tranMapList.size()){
                        messageBubbleList.clear();
                        messageBubbleList.add(this.setMessageBubble(true, squidId, childId, tranName, MessageBubbleCode.ERROR_TRAN_PARAMETERS.value(), MessageBubbleCode.ERROR, "输入设置正确"));
                        logger.debug("Queue消息泡校验Middle：validateTranParameters" + ":" + messageBubbleList.size());
                        PushMessagePacket.push(messageBubbleList, getToken());
                    }
                    else if(count >0 && count <tranMapList.size()){
                        messageBubbleList.clear();
                        messageBubbleList.add(this.setMessageBubble(false, squidId, childId, tranName, MessageBubbleCode.ERROR_TRAN_PARAMETERS.value(), MessageBubbleCode.ERROR, "输入设置非法"));
                        logger.debug("Queue消息泡校验Middle：validateTranParameters" + ":" + messageBubbleList.size());
                        PushMessagePacket.push(messageBubbleList, getToken());
                    }
                    else if(count == 0){
                        messageBubbleList.clear();
                        messageBubbleList.add(this.setMessageBubble(true, squidId, childId, tranName, MessageBubbleCode.ERROR_TRAN_PARAMETERS.value(), MessageBubbleCode.ERROR, "输入设置非法"));
                        logger.debug("Queue消息泡校验Middle：validateTranParameters" + ":" + messageBubbleList.size());
                        PushMessagePacket.push(messageBubbleList, getToken());
                    }
                }
            }
            else {
                messageBubbleList.clear();
                messageBubbleList.add(this.setMessageBubble(false, squidId, childId, tranName, MessageBubbleCode.ERROR_TRAN_PARAMETERS.value(), MessageBubbleCode.ERROR, "输入设置非法"));
                logger.debug("Queue消息泡校验Middle：validateTranParameters" + ":" + messageBubbleList.size());
                PushMessagePacket.push(messageBubbleList, getToken());
                
            }
        }
            else {
                messageBubbleList.clear();
                messageBubbleList.add(this.setMessageBubble(true, squidId, childId, tranName, MessageBubbleCode.ERROR_TRAN_PARAMETERS.value(), MessageBubbleCode.ERROR, "输入设置非法"));
                logger.debug("Queue消息泡校验Middle：validateTranParameters" + ":" + messageBubbleList.size());
                PushMessagePacket.push(messageBubbleList, getToken());
            }
        }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        // fixed bug 962 end by bo.dang
        //推送消息泡
        //PushMessagePacket.push(messageBubbleList, token);
    }
    
    /**
     * 42.校验TermExtract表达式设置语法错误
     * @param squidId
     * @param childId
     * @param name
     * @param code
     * @author bo.dang
     * @date 2014年6月10日
     */
    public void validateTermExtract(int squidId, int childId, String name, int code){
        //messageBubbleList = new ArrayList<MessageBubble>();
        Map<String, String> paramsMap = new HashMap<String, String>();
        paramsMap.put("squid_id", Integer.toString(squidId, 10));
        
        List<Transformation> transformationList = adapter.query2List(paramsMap, Transformation.class);
        
        Transformation transformation = null;
        int tranId = 0;
        int tranTypeId = 0;
        String tranName = null;
        String regExpression = null;
        for (int i = 0; i < transformationList.size(); i++) {
            transformation = transformationList.get(i);
            //regExpression = transformation.getReg_expression();
            Pattern pattern = Pattern.compile(regExpression);
            Matcher matcher = pattern.matcher(null);
            
            //pattern.
            if(validateTableName(regExpression)){
                messageBubbleList.clear();
                messageBubbleList.add(this.setMessageBubble(true, squidId, childId, name, MessageBubbleCode.ERROR_TERMEXTRACT_REG_EXPRESSION_SYNTAX.value(), MessageBubbleCode.ERROR, "TermExtract表达式设置正确"));
                logger.debug("Queue消息泡校验Middle：validateTermExtract" + ":" + messageBubbleList.size());
                PushMessagePacket.push(messageBubbleList, getToken());
            }
            else {
                messageBubbleList.clear();
                messageBubbleList.add(this.setMessageBubble(false, squidId, childId, name, MessageBubbleCode.ERROR_TERMEXTRACT_REG_EXPRESSION_SYNTAX.value(), MessageBubbleCode.ERROR, "TermExtract表达式设置语法错误"));
                logger.debug("Queue消息泡校验Middle：validateTermExtract" + ":" + messageBubbleList.size());
                PushMessagePacket.push(messageBubbleList, getToken());
            }
        }
        //推送消息泡
        //PushMessagePacket.push(messageBubbleList, token);
    }
    
    /**
     * 39.40.TranLink显式和阴式闭环校验
     * @param squidId
     * @param childId
     * @param name
     * @param code
     * @author bo.dang
     * @date 2014年6月9日
     */
    public void validateTranLinkLoop(int squidId, int childId, String name, int code){
        try {
            String sql2 = null;
            List<Integer> squidIdList = new ArrayList<Integer>();
        if(squidId > 0){
            sql2 = "select dss.id as id, dtl.from_transformation_id as from_transformation_id, dtl.to_transformation_id as to_transformation_id from ds_transformation_link dtl, ds_transformation dst, ds_squid dss where dst.squid_id = dss.id and dss.id = " + squidId + " and (dtl.from_transformation_id = dst.id or dtl.to_transformation_id = dst.id)";
            squidIdList.add(squidId);
        }
        else {
            // 显式闭环   
            sql2 = "select dss.id as id, dtl.from_transformation_id as from_transformation_id, dtl.to_transformation_id as to_transformation_id, dst.id as tran_id, dst.tran_condition as tran_condition from ds_transformation_link dtl, ds_transformation dst, ds_squid dss where dst.squid_id = dss.id and dss.squid_flow_id = " + childId + " and (dtl.from_transformation_id = dst.id or dtl.to_transformation_id = dst.id)";
            String sql3 = "select distinct dss.id from ds_transformation_link dtl, ds_transformation dst, ds_squid dss where dst.squid_id = dss.id and dss.squid_flow_id = " + childId + " and (dtl.from_transformation_id = dst.id or dtl.to_transformation_id = dst.id)";
            List<Map<String, Object>> squidIdMapList = adapter.query2List(sql3, null);
            if(StringUtils.isNotNull(squidIdMapList)){
                squidIdList = new ArrayList<Integer>();
                for(Map<String, Object> squidIdMap : squidIdMapList){
                    squidIdList.add(Integer.valueOf(squidIdMap.get("ID").toString()));
                }
            }
        }
            List<Map<String, Object>> tranLinkMapList = adapter.query2List(sql2, null);
            List<LoopNode> loopNodeList = new ArrayList<LoopNode>();
            LoopNode loopNode = null;
            int tran_id = 0;
            String tran_condition = null;
            if(StringUtils.isNotNull(tranLinkMapList)){
                for(Map<String, Object> tranLinkMap : tranLinkMapList){
                    loopNode = new LoopNode();
                    loopNode.setSquid_id(Integer.valueOf(tranLinkMap.get("ID").toString()));
                    loopNode.setFrom_id(Integer.valueOf(tranLinkMap.get("FROM_TRANSFORMATION_ID").toString()));
                    loopNode.setTo_id(Integer.valueOf(tranLinkMap.get("TO_TRANSFORMATION_ID").toString()));
                    tran_id = Integer.parseInt(tranLinkMap.get("TRAN_ID").toString());
                    tran_condition = tranLinkMap.get("tran_condition") == null ? null : tranLinkMap.get("tran_condition").toString();
                    if(StringUtils.isNotNull(tran_condition)){
                        loopNode.setFrom_id(tran_id);
                        //loopNode.setTo_id(tran_condition);
                    }
                    loopNodeList.add(loopNode);
                }
            }
            // 隐式闭环 TODO
            
           
            //messageBubbleList = new ArrayList<MessageBubble>();
            if(StringUtils.isNull(loopNodeList)){
                messageBubbleList.clear();
                messageBubbleList.add(this.setMessageBubble(true, squidId, childId, name, MessageBubbleCode.ERROR_TRAN_LINK_LOOP.value(), MessageBubbleCode.ERROR, "Transform之间不存在交叉引用"));
                logger.debug("Queue消息泡校验Middle：validateTranLinkLoop" + ":" + messageBubbleList.size());
                PushMessagePacket.push(messageBubbleList, getToken());
            }
            else {
                if(loopNodeList.size() > 2){
                    // 调用检查是否闭环的方法
                    validateLoop(loopNodeList);
                    // 闭环
                    if(loopFlag){
                        for (int i = 0; i < squidIdList.size(); i++) {
                        messageBubbleList.clear();
                        messageBubbleList.add(this.setMessageBubble(false, squidIdList.get(i), childId, name, MessageBubbleCode.ERROR_TRAN_LINK_LOOP.value(), MessageBubbleCode.ERROR, "Transform之间存在交叉引用"));
                        logger.debug("Queue消息泡校验Middle：validateTranLinkLoop" + ":" + messageBubbleList.size());
                        PushMessagePacket.push(messageBubbleList, getToken());
                        }
                    }
                    else {
                        for (int i = 0; i < squidIdList.size(); i++) {
                        messageBubbleList.clear();
                        messageBubbleList.add(this.setMessageBubble(true, squidIdList.get(i), childId, name, MessageBubbleCode.ERROR_TRAN_LINK_LOOP.value(), MessageBubbleCode.ERROR, "Transform之间不存在交叉引用"));
                        logger.debug("Queue消息泡校验Middle：validateTranLinkLoop" + ":" + messageBubbleList.size());
                        PushMessagePacket.push(messageBubbleList, getToken());
                        }
                    }
                }
                else {
                    for (int i = 0; i < squidIdList.size(); i++) {
                    messageBubbleList.clear();
                    messageBubbleList.add(this.setMessageBubble(true, squidIdList.get(i), childId, name, MessageBubbleCode.ERROR_TRAN_LINK_LOOP.value(), MessageBubbleCode.ERROR, "Transform之间不存在交叉引用"));
                    logger.debug("Queue消息泡校验Middle：validateTranLinkLoop" + ":" + messageBubbleList.size());
                    PushMessagePacket.push(messageBubbleList, getToken());
                    }
                    
                }
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
           //推送消息泡
           //PushMessagePacket.push(messageBubbleList, token);
    }
    
    
    /**
     * 37.校验显式SquidLink直接构成闭环和复杂闭环
     * @param squidId
     * @param childId
     * @param name
     * @param code
     * @author bo.dang
     * @date 2014年6月9日
     */
    public void validateSquidLinkLoop(int squidId, int childId, String name, int code){
        //messageBubbleList = new ArrayList<MessageBubble>();
        try {
            List<LoopNode> loopNodeList = new ArrayList<LoopNode>();
            List<LoopNode> tempLoopNodeList = new ArrayList<LoopNode>();
            LoopNode loopNode = null;
         if(squidId != 0){
             // PREDICT(65)、TOKENIZATION(64)
             String sql2 = "select squid_id, id, name, dictionary_squid_id, model_squid_id from ds_transformation where id = " + childId + " and (transformation_type_id = 65 or transformation_type_id = 64)";
             List<Map<String, Object>> transMapList = adapter.query2List4(false, sql2, null);
             Integer dictionary_squid_id = 0;
             Integer model_squid_id = 0;
             
             if(StringUtils.isNotNull(transMapList) && transMapList.size() != 0){
                 for(Map<String, Object> transMap : transMapList){
                     squidId = Integer.parseInt(transMap.get("SQUID_ID").toString(), 10);
                     childId = Integer.parseInt(transMap.get("ID").toString(), 10);
                     name = transMap.get("NAME").toString();
                     dictionary_squid_id = transMap.get("DICTIONARY_SQUID_ID") == null ? null:Integer.parseInt(transMap.get("DICTIONARY_SQUID_ID").toString(), 10);
                     model_squid_id = transMap.get("MODEL_SQUID_ID") == null ? null:Integer.parseInt(transMap.get("MODEL_SQUID_ID").toString(), 10);
                     loopNode = new LoopNode();
                     if(StringUtils.isNotNull(dictionary_squid_id) && dictionary_squid_id != 0){
                         loopNode.setFrom_id(dictionary_squid_id);
                         loopNode.setTo_id(squidId);
                     }
                     else if(StringUtils.isNotNull(model_squid_id) && model_squid_id != 0){
                         loopNode.setFrom_id(model_squid_id);
                         loopNode.setTo_id(squidId);
                     }
                     tempLoopNodeList.add(loopNode);
                 }
                String sql3 = "select dsl.from_squid_id from_squid_id, dsl.to_squid_id to_squid_id from ds_squid_link dsl inner join ds_squid dss on dsl.squid_flow_id = dss.squid_flow_id where dss.id = " + squidId;
                List<Map<String, Object>> squidMapList = adapter.query2List(sql3, null);
                if(StringUtils.isNotNull(squidMapList) && squidMapList.size() != 0){
                for(Map<String, Object> squidMap : squidMapList){
                    loopNode = new LoopNode();
                    loopNode.setFrom_id(Integer.parseInt(squidMap.get("FROM_SQUID_ID").toString(), 10));
                    loopNode.setTo_id(Integer.parseInt(squidMap.get("TO_SQUID_ID").toString(), 10));
                    loopNodeList.add(loopNode);
                }
                }
                loopNodeList.addAll(tempLoopNodeList); 
             }
             
         }
         else {
        // 1.显式闭环：显式的squidLink直接构成闭环
        Map<String, String> paramsMap = new HashMap<String, String>();
        paramsMap.put("squid_flow_id", Integer.toString(childId, 10));
        
        List<SquidLink> squidLinkList = adapter.query2List(paramsMap, SquidLink.class);
        
        if(StringUtils.isNull(squidLinkList) || squidLinkList.isEmpty()){
            messageBubbleList.clear();
            messageBubbleList.add(this.setMessageBubble(true, squidId, childId, name, MessageBubbleCode.ERROR_SQUID_LINK_LOOP.value(), MessageBubbleCode.ERROR, "SquidModelBase Link交叉引用"));
            logger.debug("Queue消息泡校验Middle：validateSquidLinkLoop" + ":" + messageBubbleList.size());
            PushMessagePacket.push(messageBubbleList, getToken());
        }
        else{
            
            for(int i=0; i<squidLinkList.size(); i++){
                loopNode = new LoopNode();
                loopNode.setFrom_id(squidLinkList.get(i).getFrom_squid_id());
                loopNode.setTo_id(squidLinkList.get(i).getTo_squid_id());
                loopNodeList.add(loopNode);
            }
        }
        // 2.隐式闭环:datasquid落实时所指定的destination squid
        // 取得DataSquid的DestinationSquid Id
/*        paramsMap.clear();
        String sql2 = "select dss.id as id, dds.destination_squid_id as destination_squid_id from ds_squid dss, ds_data_squid dds where dss.squid_flow_id = " + childId + "and dds.destination_squid_id is not null and dds.destination_squid_id !=0";
        
        List<Map<String, Object>> dataSquidMapList = adapter.query2List(sql2, null);
        
        if(StringUtils.isNotNull(dataSquidMapList) && !dataSquidMapList.isEmpty()){
            
            for(Map<String, Object> dataSquidMap : dataSquidMapList){
                loopNode = new LoopNode();
                loopNode.setFrom_id(Integer.valueOf(dataSquidMap.get("ID").toString()));
                loopNode.setTo_id(Integer.valueOf(dataSquidMap.get("DESTINATION_SQUID_ID").toString()));
                loopNodeList.add(loopNode);
            }
            
        }*/
        int a = 0;
        // 2.隐式闭环:squid包含predict transform，而它引用了另一个data mining squid
        // 取得Predict Transformation的model_squid_id
        String sql3 = "select dss.id as id, dst.model_squid_id as model_squid_id from ds_transformation dst, ds_squid dss where dst.squid_id = dss.id and dss.squid_flow_id = " + childId + " and dst.model_squid_id != 0 and transformation_type_id = " + TransformationTypeEnum.PREDICT.value();
        List<Map<String, Object>> predictMapList = adapter.query2List(sql3, null);
        
        if(StringUtils.isNotNull(predictMapList) && !predictMapList.isEmpty()){
            
            for(Map<String, Object> predictMap : predictMapList){
                loopNode = new LoopNode();
                loopNode.setFrom_id(Integer.valueOf(predictMap.get("MODEL_SQUID_ID").toString()));
                loopNode.setTo_id(Integer.valueOf(predictMap.get("ID").toString()));
                loopNodeList.add(loopNode);
            }
        }
        // 2.隐式闭环:分词tran引起其他data squid
        String sql4 = "select dss.id as id, dst.dictionary_squid_id as dictionary_squid_id from ds_transformation dst, ds_squid dss where dst.squid_id = dss.id and dss.squid_flow_id = " + childId + " and dst.dictionary_squid_id != 0 and transformation_type_id = " + TransformationTypeEnum.TOKENIZATION.value();
        List<Map<String, Object>> tokenizationMapList = adapter.query2List(sql4, null);
        
        if(StringUtils.isNotNull(tokenizationMapList) && !tokenizationMapList.isEmpty()){
            
            for(Map<String, Object> tokenizationMap : tokenizationMapList){
                loopNode = new LoopNode();
                loopNode.setFrom_id(Integer.valueOf(tokenizationMap.get("DICTIONARY_SQUID_ID").toString()));
                loopNode.setTo_id(Integer.valueOf(tokenizationMap.get("ID").toString()));
                loopNodeList.add(loopNode);
            }
            
        }
         }
        if(StringUtils.isNull(loopNodeList)){
            messageBubbleList.clear();
            messageBubbleList.add(this.setMessageBubble(true, squidId, childId, name, MessageBubbleCode.ERROR_SQUID_LINK_LOOP.value(), MessageBubbleCode.ERROR, "SquidModelBase Link交叉引用"));
            logger.debug("Queue消息泡校验Middle：validateSquidLinkLoop" + ":" + messageBubbleList.size());
            PushMessagePacket.push(messageBubbleList, getToken());
        }
        else {
            if(loopNodeList.size() > 2){
                // 调用检查是否闭环的方法
                validateLoop(loopNodeList);
                // 闭环
                if(loopFlag){
                    
                    messageBubbleList.clear();
                    messageBubbleList.add(this.setMessageBubble(false, squidId, childId, name, MessageBubbleCode.ERROR_SQUID_LINK_LOOP.value(), MessageBubbleCode.ERROR, "SquidModelBase Link交叉引用"));
                    logger.debug("Queue消息泡校验Middle：validateSquidLinkLoop" + ":" + messageBubbleList.size());
                    PushMessagePacket.push(messageBubbleList, getToken());
                }
                else {
                    messageBubbleList.clear();
                    messageBubbleList.add(this.setMessageBubble(true, squidId, childId, name, MessageBubbleCode.ERROR_SQUID_LINK_LOOP.value(), MessageBubbleCode.ERROR, "SquidModelBase Link交叉引用"));
                    logger.debug("Queue消息泡校验Middle：validateSquidLinkLoop" + ":" + messageBubbleList.size());
                    PushMessagePacket.push(messageBubbleList, getToken());
                }
            }
            else {
                messageBubbleList.clear();
                messageBubbleList.add(this.setMessageBubble(true, squidId, childId, name, MessageBubbleCode.ERROR_SQUID_LINK_LOOP.value(), MessageBubbleCode.ERROR, "SquidModelBase Link交叉引用"));
                logger.debug("Queue消息泡校验Middle：validateSquidLinkLoop" + ":" + messageBubbleList.size());
                PushMessagePacket.push(messageBubbleList, getToken());
            }
        }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        //推送消息泡
        //PushMessagePacket.push(messageBubbleList, token);
    }
    
    /**
     * 
     * @param squidId
     * @param childId
     * @param name
     * @param code
     * @author bo.dang
     * @date 2014年6月10日
     */
    public void validateDataSquidLoop(int squidId, int childId, String name, int code){
        Map<String, String> paramsMap = new HashMap<String, String>();
        paramsMap.put("squid_flow_id", Integer.toString(childId, 10));
        
        List<SquidLink> squidLinkList = adapter.query2List(paramsMap, SquidLink.class);
        messageBubbleList.clear();
        if(StringUtils.isNull(squidLinkList)){
            messageBubbleList.add(this.setMessageBubble(false, squidId, childId, name, MessageBubbleCode.ERROR_SQUID_LINK_LOOP.value(), MessageBubbleCode.ERROR, "SquidLink没有闭环 不存在 交叉引用"));
        }
        PushMessagePacket.push(messageBubbleList, getToken());
    }
    
    public static void main1(String[] args) {
        List<TransformationLink> transformationLinkList = new ArrayList<TransformationLink>();
        TransformationLink transformationLink_1 = new TransformationLink();
        transformationLink_1.setFrom_transformation_id(1001);
        transformationLink_1.setTo_transformation_id(1005);
        transformationLinkList.add(transformationLink_1);
        
        TransformationLink transformationLink_2 = new TransformationLink();
        transformationLink_2.setFrom_transformation_id(1003);
        transformationLink_2.setTo_transformation_id(1009);
        transformationLinkList.add(transformationLink_2);
        
        TransformationLink transformationLink_3 = new TransformationLink();
        transformationLink_3.setFrom_transformation_id(1003);
        transformationLink_3.setTo_transformation_id(1001);
        transformationLinkList.add(transformationLink_3);
        
        TransformationLink transformationLink_4 = new TransformationLink();
        transformationLink_4.setFrom_transformation_id(1005);
        transformationLink_4.setTo_transformation_id(1003);
        transformationLinkList.add(transformationLink_4);
        
        TransformationLink transformationLink_5 = new TransformationLink();
        transformationLink_5.setFrom_transformation_id(1001);
        transformationLink_5.setTo_transformation_id(1006);
        transformationLinkList.add(transformationLink_5);
        
/*        TransformationLink transformationLink_1 = new TransformationLink();
        transformationLink_1.setFrom_transformation_id(1002);
        transformationLink_1.setTo_transformation_id(1003);
        transformationLinkList.add(transformationLink_1);
        
        TransformationLink transformationLink_2 = new TransformationLink();
        transformationLink_2.setFrom_transformation_id(1003);
        transformationLink_2.setTo_transformation_id(1004);
        transformationLinkList.add(transformationLink_2);
        
        TransformationLink transformationLink_3 = new TransformationLink();
        transformationLink_3.setFrom_transformation_id(1004);
        transformationLink_3.setTo_transformation_id(1005);
        transformationLinkList.add(transformationLink_3);
        
        TransformationLink transformationLink_4 = new TransformationLink();
        transformationLink_4.setFrom_transformation_id(1005);
        transformationLink_4.setTo_transformation_id(1006);
        transformationLinkList.add(transformationLink_4);
        
        TransformationLink transformationLink_5 = new TransformationLink();
        transformationLink_5.setFrom_transformation_id(1006);
        transformationLink_5.setTo_transformation_id(1007);
        transformationLinkList.add(transformationLink_5);
        
        TransformationLink transformationLink_6 = new TransformationLink();
        transformationLink_6.setFrom_transformation_id(1007);
        transformationLink_6.setTo_transformation_id(1008);
        transformationLinkList.add(transformationLink_6);*/
        
        List<LoopNode> loopNodeList = new ArrayList<LoopNode>();
        LoopNode loopNode = null;
        for(int i=0; i<transformationLinkList.size(); i++){
            loopNode = new LoopNode();
            loopNode.setFrom_id(transformationLinkList.get(i).getFrom_transformation_id());
            loopNode.setTo_id(transformationLinkList.get(i).getTo_transformation_id());
            loopNodeList.add(loopNode);
        }
        validateLoop(loopNodeList);
    }
    
    /**
     * 校验是否闭环
     * @param loopNodeList
     * @author bo.dang
     * @date 2014年6月10日
     */
    public static void validateLoop(List<LoopNode> loopNodeList){
        loopFlag = false;
        //newLoopNodeList.clear();
        int fromId = 0;
        int toId = 0;
        int totalCount = 0;
        LoopNode newLoopNode = null;
/*        if(StringUtils.isNotNull(newLoopNodeList)){
            loopNodeList = newLoopNodeList;
        }*/
        if(loopNodeList.size() > 2){
            List<LoopNode> copyNewLoopNodeList = new ArrayList<LoopNode>();
                for(int i=0; i<loopNodeList.size(); i++){
                    
                    fromId = loopNodeList.get(i).getFrom_id();
                    toId = loopNodeList.get(i).getTo_id();
                    int fromCount = 0;
                    int toCount = 0;
                    for(int j=0; j<loopNodeList.size(); j++){
                        if(toId ==  loopNodeList.get(j).getFrom_id()){
                            toCount ++;
                        }
                        if(fromId == loopNodeList.get(j).getTo_id()){
                            fromCount ++;
                        }
/*                        if(toCount > 0 && fromCount > 0){
                            newLoopNode = new LoopNode();
                            newLoopNode.setFrom_id(fromId);
                            newLoopNode.setTo_id(toId);
                            newLoopNodeList.add(newLoopNode);
                        }*/
                    }
                    
                    if(fromCount > 0 && toCount > 0){
                        newLoopNode = new LoopNode();
                        newLoopNode.setFrom_id(fromId);
                        newLoopNode.setTo_id(toId);
                        copyNewLoopNodeList.add(newLoopNode);
                        totalCount ++;
                    }
                }
                // 存在闭环,暂时把闭环分为1.最简单的闭环；2.复杂闭环
                // 1.最简单的闭环
              if(totalCount > 2 && totalCount == loopNodeList.size()){
                  loopFlag = true;
                  System.out.println("闭环");
              }
              // 2.复杂闭环
              else if(totalCount > 2 && totalCount < loopNodeList.size()){
 /*                 List<LoopNode> copyNewLoopNodeList = newLoopNodeList;
                  newLoopNodeList.clear();*/
                  System.out.println("继续检查是否闭环");
                  validateLoop(copyNewLoopNodeList);
              }
              else {
                  loopFlag = false;
                  System.out.println("没有闭环");
                  
              }
            }
            else {
                loopFlag = false;
                System.out.println("没有闭环");
                
            }
        
        //Link<T> Link = null;
        //Link
    }
/*    *//** 
     * 最简单的算法，但需要的空间比较高，一个Set集合 
     * @param link 
     * @param <T> 
     * @return 
     *//*  
    public static <T>  boolean containsCircle1(LinkedList<T> link) {
        if(link == null){  
            return false;  
        }  
        T value = link.element();  
        Set<T> values = new HashSet<T>();
        values.add(value);  
        while ((link = link.) != null){  
            if(link == null){  
                return false;  
            }  
            value = link.getValue();  
            if (values.contains(value)) {  
                return true;  
            }  
        }  
        return false;  
    }*/
    
    
    
	/**
	 * 校验SQL语法
	 * @param sql
	 * @author bo.dang
	 * @date 2014年5月26日
	 */
	public void checkSQLValidation(int squidId, int childId, String name, String sql){
	    if(StringUtils.isNull(sql)){
	           logger.info("数据校验 - 消息气泡 (校验SQL语法)");
/*	            MessageBubble messageBubble = new MessageBubble();
	            this.setMessageBubble(messageBubble, 0, 0, MessageBubbleCode.WARN_NO_SQL_EXPRESSION.value(), true, MessageBubbleCode.WARN);
	            this.setMessageBubble(squidId, childId, name, MessageBubbleCode.WARN_NO_SQL_EXPRESSION.value(), false, MessageBubbleCode.WARN, null);*/
	            
	            messageBubbleList.add(this.setMessageBubble(false, squidId, childId, name, MessageBubbleCode.WARN_NO_SQL_EXPRESSION.value(), MessageBubbleCode.WARN, "表达式为空"));
	    }
	    else {
	            messageBubbleList.add(this.setMessageBubble(true, squidId, childId, name, MessageBubbleCode.WARN_NO_SQL_EXPRESSION.value(), MessageBubbleCode.WARN, "表达式为空"));
	        try {
	            adapter.execute(sql);
	            messageBubbleList.add(this.setMessageBubble(true, squidId, childId, name, MessageBubbleCode.WARN_NO_SQL_EXPRESSION.value(), MessageBubbleCode.WARN, "表达式为空"));
	        } catch (DatabaseException e) {
	            // TODO
	            messageBubbleList.add(this.setMessageBubble(false, squidId, childId, name, MessageBubbleCode.WARN_NO_SQL_EXPRESSION.value(), MessageBubbleCode.ERROR, null));
	            logger.error("执行SQL异常", e);
	            try {
	                adapter.rollback();
	            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
	                logger.fatal("rollback err!", e1);
	            }
	        }
	        
	    }
	    PushMessagePacket.push(messageBubbleList, getToken());
	    
	}
    /**
     * 3.校验squid是否孤立 新增squid,删除squid,删除link,面板加载时触发
     * 
     * @param operateType 操作类型
     * @param type 枚举类型
     * @param squidLinks集合
     * @param infoPackets
     * @return
     */
    public MessageBubble isolateSquid(DMLType operateType, DSObjectType type, int squidId, int childId, String name) {
        MessageBubble messageBubble = null;
        try {
            logger.info("数据校验 - 消息气泡 (squid是否孤立)");
            //List<MessageBubble> messageBubbleList = new ArrayList<MessageBubble>();
/*            && (DSObjectType.DBSOURCE == type
                    || DSObjectType.EXTRACT == type
                    || DSObjectType.STAGE == type
                    || DSObjectType.DBSOURCE == type
                    || DSObjectType.REPORT == type
                    || DSObjectType.EXTRACT == type
                    || DSObjectType.XML_EXTRACT == type
                    || DSObjectType.DOC_EXTRACT == type
                    || DSObjectType.WEBLOGEXTRACT == type
                    || DSObjectType.WEBEXTRACT == type
                    || DSObjectType.WEIBOEXTRACT == type
                    || DSObjectType.LOGREP == type
                    || DSObjectType.SVM == type
                    || DSObjectType.NAIVEBAYES == type
                    || DSObjectType.KMEANS == type
                    || DSObjectType.ALS == type
                    || DSObjectType.LINEREG == type
                    || DSObjectType.RIDGEREG == type)*/
            // 单独创建squid
                //int squidId = 0;
                String queryfromSql= "select * from DS_SQUID_LINK where from_squid_id ="
                        + squidId + " or to_squid_id =" + squidId + "";
                List<SquidLink> squidLinkList = adapter.query2List(false, queryfromSql, null,SquidLink.class);
                // 如果没有检索到SquidLink的数据，那么说明这个Squid是孤立的,生成消息气泡
                if(StringUtils.isNull(squidLinkList) || squidLinkList.isEmpty()){
                    messageBubble = new MessageBubble(false, squidId, squidId, name, MessageBubbleCode.WARN_SQUID_NO_LINK.value(), MessageBubbleCode.WARN, null);
                }
                else {
                    // 消除消息泡
                    messageBubble = new MessageBubble(true, squidId, squidId, name, MessageBubbleCode.WARN_SQUID_NO_LINK.value(), MessageBubbleCode.WARN, null);
                }
                

            //推送消息泡
            //PushMessagePacket.push(messageBubbleList, token);
        } catch (Exception e) {
            logger.error("数据校验 - 消息气泡 (squid是否孤立)exception",e);
        }
        return messageBubble;
        
    }
    
    /**
     * 3.校验孤立Squid
     * @param squidId
     * @param childId
     * @param code
     * @author bo.dang
     * @throws SQLException 
     * @date 2014年6月5日
     */
    public void validateIsolateSquid(int squidId, int childId, String name, int code) {
        messageBubbleList.clear();
        try {
        callStatement = adapter.prepareCall(false, "call validateIsolateSquid(?, ?, ?, ?)");
        callStatement.setInt(1, squidId);
        callStatement.setInt(2, 0);
        callStatement.setInt(3, 0);
        callStatement.execute();
        //callStatement.registerOutParameter(3, Types.ARRAY);
        Boolean flag = callStatement.getBoolean(4);
        //Array a = callStatement.getArray("a");
        callStatement.close();
        if(!flag){
            messageBubbleList.add(new MessageBubble(false, squidId, squidId, name, MessageBubbleCode.WARN_SQUID_NO_LINK.value(), MessageBubbleCode.WARN, null));
        }
        else {
            // 消除消息泡
            messageBubbleList.add(new MessageBubble(true, squidId, squidId, name, MessageBubbleCode.WARN_SQUID_NO_LINK.value(), MessageBubbleCode.WARN, null));
        }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        PushMessagePacket.push(messageBubbleList, getToken());
    }
	
    /**
     * 3.校验squid是否孤立 新增squid,删除squid,删除link,面板加载时触发
     * 
     * @param operateType 操作类型
     * @param type 枚举类型
     * @param squidLinks集合
     * @param infoPackets
     * @return
     */
    public void isolateSquid(DMLType operateType, DSObjectType type, int squidId, int childId, String name, SquidLink squidLink) {
        try {
            logger.info("数据校验 - 消息气泡 (squid是否孤立)");
            //List<MessageBubble> messageBubbleList = new ArrayList<MessageBubble>();
            messageBubbleList.clear();
            Map<String, String> paramsMap = new HashMap<String, String>();
/*            && (DSObjectType.DBSOURCE == type
                    || DSObjectType.EXTRACT == type
                    || DSObjectType.STAGE == type
                    || DSObjectType.DBSOURCE == type
                    || DSObjectType.REPORT == type
                    || DSObjectType.EXTRACT == type
                    || DSObjectType.XML_EXTRACT == type
                    || DSObjectType.DOC_EXTRACT == type
                    || DSObjectType.WEBLOGEXTRACT == type
                    || DSObjectType.WEBEXTRACT == type
                    || DSObjectType.WEIBOEXTRACT == type
                    || DSObjectType.LOGREP == type
                    || DSObjectType.SVM == type
                    || DSObjectType.NAIVEBAYES == type
                    || DSObjectType.KMEANS == type
                    || DSObjectType.ALS == type
                    || DSObjectType.LINEREG == type
                    || DSObjectType.RIDGEREG == type)*/
            // 单独创建squid
            if (DMLType.INSERT == operateType && type != DSObjectType.SQUIDLINK) {
            if (DMLType.INSERT == operateType
                    && (DSObjectType.DBSOURCE == type
                            || DSObjectType.EXTRACT == type
                            || DSObjectType.STAGE == type
                            || DSObjectType.DBSOURCE == type
                            || DSObjectType.REPORT == type
                            || DSObjectType.EXTRACT == type
                            || DSObjectType.XML_EXTRACT == type
                            || DSObjectType.DOC_EXTRACT == type
                            || DSObjectType.WEBLOGEXTRACT == type
                            || DSObjectType.WEBEXTRACT == type
                            || DSObjectType.WEIBOEXTRACT == type
                            || DSObjectType.LOGREG == type
                            || DSObjectType.SVM == type
                            || DSObjectType.NAIVEBAYES == type
                            || DSObjectType.KMEANS == type
                            || DSObjectType.ALS == type
                            || DSObjectType.LINEREG == type
                            || DSObjectType.RIDGEREG == type)) {
                //int squidId = 0;
                String queryfromSql= "select * from DS_SQUID_LINK where from_squid_id ="
                        + squidId + " or to_squid_id =" + squidId + "";
                List<SquidLink> squidLinkList = adapter.query2List(false,queryfromSql, null,SquidLink.class);
                // 如果没有检索到SquidLink的数据，那么说明这个Squid是孤立的,生成消息气泡
                if(StringUtils.isNull(squidLinkList) || squidLinkList.isEmpty()){
                    messageBubbleList.add(new MessageBubble(false, squidId, squidId, name, MessageBubbleCode.WARN_SQUID_NO_LINK.value(), MessageBubbleCode.WARN, null));
                }
                else {
                    // 消除消息泡
                    messageBubbleList.add(new MessageBubble(true, squidId, squidId, name, MessageBubbleCode.WARN_SQUID_NO_LINK.value(), MessageBubbleCode.WARN, null));
                }
                
/*                paramsMap.put("to_squid_id", value)
                logger.info("数据校验 - 消息气泡 (squid是否孤立---单独创建squid)");
                MessageBubble messageBubble = new MessageBubble();
                this.setMessageBubble(messageBubble, infoPackets.get(0).getKey(), infoPackets.get(0).getKey(),MessageBubbleCode.WARN_SQUID_NO_LINK.value(), false, 0);
                messageBubbles.add(messageBubble);*/
                //PushMessagePacket.push(messageBubbleList, token);
            } else if (DMLType.INSERT == operateType
                    && type == DSObjectType.SQUIDLINK) {// 创建squidlink时，消息泡消失
                if(StringUtils.isNotNull(squidLink)){
                    // 创建SquidLink成功，ReprotSquid不再孤立，消泡
                    messageBubbleList.add(new MessageBubble(true, squidLink.getTo_squid_id(), squidLink.getTo_squid_id(), null, MessageBubbleCode.WARN_SQUID_NO_LINK.value(), MessageBubbleCode.WARN, null));
                }
                else
                {
                    // 消除消息气泡
                    messageBubbleList.add(new MessageBubble(false, squidLink.getTo_squid_id(), squidLink.getTo_squid_id(), null, MessageBubbleCode.WARN_SQUID_NO_LINK.value(), MessageBubbleCode.WARN, null));
                   
                }
                
            } else if (DMLType.DELETE == operateType
                    && type == DSObjectType.SQUIDLINK) {// 删除squidlink时，如果
                
                int from_squid_id = squidId;
                int to_squid_id = childId;
                //根据from_id查询squid是否孤立
                //List<MessageBubble> list=new ArrayList<MessageBubble>();
                String queryfromSql= "select * from DS_SQUID_LINK where from_squid_id="
                        + from_squid_id + " or to_squid_id=" + from_squid_id + "";
                List<SquidLink> fromSquidLinks = adapter.query2List(false, queryfromSql, null,SquidLink.class);
                if(fromSquidLinks.size()<1)
                {
                    logger.info("源squid孤立==================");
                    //根据from_id查询出squid的key
                    paramsMap.clear();
                    paramsMap.put("id",String.valueOf(from_squid_id) );
                    Squid fromSquid = adapter.query2Object(paramsMap, Squid.class);
//                  List<SquidModelBase> fromSquids=adapter.query2List(true, params, SquidModelBase.class);
//                  list.add(new MessageBubble(fromSquids.get(0).getKey(), fromSquids.get(0).getKey(), MessageBubbleCode.WARN_SQUID_NO_LINK.value(),false));
                    messageBubbleList.add(new MessageBubble(false, fromSquid.getId(), fromSquid.getId(), fromSquid.getName(), MessageBubbleCode.WARN_SQUID_NO_LINK.value(), MessageBubbleCode.WARN, null));
                }
                String querytoSql= "select * from DS_SQUID_LINK where from_squid_id="
                        + to_squid_id + " or to_squid_id=" + to_squid_id + "";;
                List<SquidLink> toSquidLinks=adapter.query2List(false,querytoSql, null,SquidLink.class);
                if(toSquidLinks.size()<1)
                {
                    logger.info("目标squid孤立==================");
                    //根据from_id查询出squid的key
                    paramsMap.clear();
                    paramsMap.put("id",String.valueOf(to_squid_id) );
                    Squid toSquid = adapter.query2Object(paramsMap, Squid.class);
                    //List<SquidModelBase> toSquids=adapter.query2List(true, params, SquidModelBase.class);
                    //list.add(new MessageBubble(toSquids.get(0).getKey(), toSquids.get(0).getKey(), MessageBubbleCode.WARN_SQUID_NO_LINK.value(),false));
                    messageBubbleList.add(new MessageBubble(false, toSquid.getId(), toSquid.getId(), toSquid.getName(), MessageBubbleCode.WARN_SQUID_NO_LINK.value(), MessageBubbleCode.WARN, null));
                }
            }
            //推送消息泡
            PushMessagePacket.push(messageBubbleList, getToken());
        } 
        }catch (Exception e) {
            logger.error("数据校验 - 消息气泡 (squid是否孤立)exception",e);
        }
    }
    
    /**
     * 5.StageSquid中没定义transformation的column
     * 
     * @author bo.dang
     * @throws SQLException 
     * @date 2014年6月4日
     */
    public void validateIsolateColumn(int squidId, int childId, String name, int code) {
        String sql = "select * from DS_TRANSFORMATION where squid_id =" + squidId + " or column_id =" + childId;
        try {                          
        Transformation transformation = adapter.query2Object(false, sql, null, Transformation.class);
        if(StringUtils.isNull(transformation)){
            // 推送消息泡
            messageBubbleList.add(setMessageBubble(false, squidId, childId, name, MessageBubbleCode.WARN_COLUMN_NO_TRANSFORMATION.value(), MessageBubbleCode.WARN, name + "没定义变换逻辑，运行时将导入空值（NULL）"));
        }
        else {
            messageBubbleList.add(setMessageBubble(true, squidId, childId, name, MessageBubbleCode.WARN_COLUMN_NO_TRANSFORMATION.value(), MessageBubbleCode.WARN, name + "没定义变换逻辑，运行时将导入空值（NULL）"));
        }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        //推送消息泡
        PushMessagePacket.push(messageBubbleList, getToken());
    }
	
    /**
     * 6.dataSquid落地的表名
     * @param stageSquid
     * @author bo.dang
     * @date 2014年6月4日
     */
    public void validateDestinationTableName(int squidId, int childId, String name, int code) {
        messageBubbleList.clear();
        logger.info("数据校验 - 消息气泡 (数据库表名规范)");
        String tableName = null;
        String filter = null;
        Map<String, String> paramsMap = new HashMap<String, String>();
        paramsMap.put("id", Integer.toString(squidId, 10));
        StageSquid stageSquid = adapter.query2Object(paramsMap, StageSquid.class);
            tableName = stageSquid.getTable_name();
            filter = stageSquid.getFilter();
/*            int squidId = stageSquid.getId();
            String name = stageSquid.getName();*/
            boolean isPersisted = stageSquid.isIs_persisted();
            // 校验SQL语法 filter
            SQLValidator.validateSquidFilter(filter, stageSquid.getId(), stageSquid.getId(), stageSquid.getName());
            
            if (StringUtils.isNotBlank(tableName)) {
                // 如果表名不为空,校验表名的正确性
                // 表名符合规范
                if (this.validateTableName(tableName)) {
                    logger.info("数据校验 - 消息气泡 (数据库表名规范---表名正确)");
/*                    this.setMessageBubble(messageBubble, stageSquids.get(i)
                            .getKey(), stageSquids.get(i).getKey(),
                            MessageBubbleCode.ERROR_DB_TABLE_NAME.value(),
                            true, 0);*/
                    messageBubbleList.add(setMessageBubble(true, squidId, squidId, name, MessageBubbleCode.ERROR_DB_TABLE_NAME.value(), MessageBubbleCode.ERROR, "表名正确"));
                } else {// 不符合规范
                    logger.info("数据校验 - 消息气泡 (数据库表名规范---表名不正确)");
/*                    this.setMessageBubble(messageBubble, stageSquids.get(i)
                            .getKey(), stageSquids.get(i).getKey(),
                            MessageBubbleCode.ERROR_DB_TABLE_NAME.value(),
                            false, 1);*/
                    messageBubbleList.add(setMessageBubble(false, squidId, squidId, name, MessageBubbleCode.ERROR_DB_TABLE_NAME.value(), MessageBubbleCode.ERROR, "表名不正确"));
                }
                // 消泡
                messageBubbleList.add(setMessageBubble(true, squidId, squidId, name, MessageBubbleCode.ERROR_NO_DB_TABLE_NAME.value(), MessageBubbleCode.ERROR, name + "已设置为落地，已指定落地表名"));
                
            } else {
                logger.info("数据校验 - 消息气泡 (数据库表名规范---表名不正确)");
                // 27.28.stageSquid的persist属性为true，但table_name为空
                if(isPersisted){
                    // 推送消息泡
                    messageBubbleList.add(setMessageBubble(false, squidId, squidId, name, MessageBubbleCode.ERROR_NO_DB_TABLE_NAME.value(), MessageBubbleCode.ERROR, name + "已设置为落地，但没指定落地表名"));
                    
                }
                else {
                    // 消泡
                    messageBubbleList.add(setMessageBubble(true, squidId, squidId, name, MessageBubbleCode.ERROR_NO_DB_TABLE_NAME.value(), MessageBubbleCode.ERROR, name + "没有设置为落地，没有指定落地表名"));
                }
/*                this.setMessageBubble(messageBubble, stageSquids.get(i)
                        .getKey(), stageSquids.get(i).getKey(),
                        MessageBubbleCode.ERROR_DB_TABLE_NAME.value(), false, 1);*/
            }
            //messageBubbles.add(messageBubble);
            PushMessagePacket.push(messageBubbleList, getToken());
        }
    
    /**
     * 7.dataSquid落地的datastore TODO
     * @param stageSquid
     * @author bo.dang
     * @date 2014年6月4日
     */
    
    
    /**
     * 8.29.dataSquid索引
     * @param stageSquid
     * @author bo.dang
     * @date 2014年6月4日
     */
    public void validateSquidIndexes(int squidId, int childId, String name, int code){
        messageBubbleList.clear();
        Map<String, String> paramsMap = new HashMap<String, String>();
        paramsMap.put("id", Integer.toString(squidId, 10));
        DataSquid dataSquid = adapter.query2Object(paramsMap, DataSquid.class);
        Boolean isIndexed = dataSquid.isIs_indexed();
        // Indexing设置为true，但indexes为空
        if(isIndexed){
            paramsMap.clear();
            paramsMap.put("squid_id", Integer.toString(squidId, 10));
            List<SquidIndexs> squidIndexsList = adapter.query2List(paramsMap, SquidIndexs.class);
        if(StringUtils.isNull(squidIndexsList)){
            messageBubbleList.add(setMessageBubble(false, squidId, childId, name, MessageBubbleCode.WARN_DATASQUID_NO_SQUIDINDEXES.value(), MessageBubbleCode.WARN, "索引设置不合法"));
        }
        else {
            messageBubbleList.add(setMessageBubble(true, squidId, childId, name, MessageBubbleCode.WARN_DATASQUID_NO_SQUIDINDEXES.value(), MessageBubbleCode.WARN, "索引设置合法"));
        }
        }
        PushMessagePacket.push(messageBubbleList, getToken());
    }
    
    /**
     * 43.校验发布路径未设置
     * @param squidId
     * @param childId
     * @param name
     * @author bo.dang
     * @date 2014年6月10日
     */
    public void validatePublishingFolder(int squidId, int childId, String name, int code){
        messageBubbleList.clear();
        Map<String, String> paramsMap = new HashMap<String, String>();
        paramsMap.put("id", Integer.toString(squidId, 10));
        ReportSquid reportSquid = adapter.query2Object(paramsMap, ReportSquid.class);
        if(StringUtils.isNotNull(reportSquid)){
            if(StringUtils.isNull(reportSquid.getFolder_id())){
                messageBubbleList.add(setMessageBubble(false, squidId, childId, name, MessageBubbleCode.WARN_REPORT_SQUID_NO_PUBLISHING_FOLDER.value(), MessageBubbleCode.WARN, "发布路径未设置"));
            }
            else {
                messageBubbleList.add(setMessageBubble(true, squidId, childId, name, MessageBubbleCode.WARN_REPORT_SQUID_NO_PUBLISHING_FOLDER.value(), MessageBubbleCode.WARN, "发布路径已设置"));
            }
        }
        PushMessagePacket.push(messageBubbleList, getToken());
    }
    
    /**
     * 44.校验报表模板定义
     * @param squidId
     * @param childId
     * @param name
     * @author bo.dang
     * @date 2014年6月10日
     */
    public void validateTemplateDefinition(int squidId, int childId, String name, int code){
        messageBubbleList.clear();
        Map<String, String> paramsMap = new HashMap<String, String>();
        paramsMap.put("id", Integer.toString(squidId, 10));
        ReportSquid reportSquid = adapter.query2Object(paramsMap, ReportSquid.class);
        if(StringUtils.isNotNull(reportSquid)){
            if(StringUtils.isNull(reportSquid.getReport_template())){
                messageBubbleList.add(setMessageBubble(false, squidId, childId, name, MessageBubbleCode.ERROR_REPORT_SQUID_NO_TEMPLATE_DEFINITION.value(), MessageBubbleCode.WARN, "报表模板设置非法"));
            }
            else {
                messageBubbleList.add(setMessageBubble(true, squidId, childId, name, MessageBubbleCode.ERROR_REPORT_SQUID_NO_TEMPLATE_DEFINITION.value(), MessageBubbleCode.WARN, "报表模板设置非法"));
            }
        }
        PushMessagePacket.push(messageBubbleList, getToken());
    }
    
    /**
     * 47.校验Training_percentage训练集所占比例
     * @param squidId
     * @param childId
     * @param name
     * @author bo.dang
     * @date 2014年6月10日
     */
    public void validateTrainingPercentage(int squidId, int childId, String name, int code){
        messageBubbleList.clear();
        Map<String, String> paramsMap = new HashMap<String, String>();
        paramsMap.put("id", Integer.toString(squidId, 10));
        DataMiningSquid dataMiningSquid = adapter.query2Object(paramsMap, DataMiningSquid.class);
        if(StringUtils.isNotNull(dataMiningSquid)){
            // 小于50%
            if(dataMiningSquid.getTraining_percentage() < 0.50){
                messageBubbleList.add(setMessageBubble(false, squidId, childId, name, MessageBubbleCode.WARN_DATA_MININGSQUID_TRAINING_PERCENTAGE.value(), MessageBubbleCode.WARN, "训练数据设置过小，可能会影响模型"));
            }
            else {
                messageBubbleList.add(setMessageBubble(true, squidId, childId, name, MessageBubbleCode.WARN_DATA_MININGSQUID_TRAINING_PERCENTAGE.value(), MessageBubbleCode.WARN, "训练数据设置正常"));
            }
        }
        PushMessagePacket.push(messageBubbleList, getToken());
    }
    
    /**
     * 48.校验ModelSquidId
     * @param squidId
     * @param childId
     * @param name
     * @author bo.dang
     * @date 2014年6月10日
     */
    public void validatePredictModelSquid(int squidId, int childId, String name, int code){
        messageBubbleList = new ArrayList<MessageBubble>();
        try {
        Map<String, String> paramsMap = new HashMap<String, String>();
        if(squidId != 0){
        //paramsMap.put("id", Integer.toString(childId, 10));
        paramsMap.put("squid_id", Integer.toString(squidId, 10));
        paramsMap.put("transformation_type_id", Integer.toString(TransformationTypeEnum.PREDICT.value(),10));
        Transformation transformation = null;
        int modelSquidId = 0;
        List<Transformation> transList = adapter.query2List(paramsMap, Transformation.class);
        for (int i = 0; i < transList.size(); i++) {
            transformation = transList.get(i);
        if(StringUtils.isNotNull(transformation)){
            modelSquidId = transformation.getModel_squid_id();
            paramsMap.clear();
            paramsMap.put("id", Integer.toString(modelSquidId, 10));
            Squid squid = adapter.query2Object(paramsMap, Squid.class);
            if(StringUtils.isNotNull(squid)){
                messageBubbleList.clear();
                messageBubbleList.add(setMessageBubble(true, squidId, childId, name, MessageBubbleCode.ERROR_PREDICT_MODEL_SQUID_BY_DELETED.value(), MessageBubbleCode.ERROR, "引用的模型已被删除"));
                logger.debug("Queue消息泡校验Middle：validatePredictModelSquid" + ":" + messageBubbleList.size());
                PushMessagePacket.push(messageBubbleList, getToken());
            }
            else {
                messageBubbleList.clear();
                messageBubbleList.add(setMessageBubble(false, squidId, childId, name, MessageBubbleCode.ERROR_PREDICT_MODEL_SQUID_BY_DELETED.value(), MessageBubbleCode.ERROR, "引用的模型已被删除"));
                logger.debug("Queue消息泡校验Middle：validatePredictModelSquid" + ":" + messageBubbleList.size());
                PushMessagePacket.push(messageBubbleList, getToken());
            }
        }
        }
        }
        else {
            paramsMap.clear();
            String sql = "select dst.model_squid_id as model_squid_id from ds_squid dss, ds_transformation dst where dss.squid_flow_id = " + childId + " and dss.id = dst.squid_id and dst.transformation_type_id = 65 and dst.model_squid_id != 0";
            List<Map<String, Object>> modelSquidMapList = adapter.query2List(sql, null);

            if(StringUtils.isNotNull(modelSquidMapList)){
                int modelSquid = 0;
                Squid squid = null;
                for(Map<String, Object> modelSquidMap : modelSquidMapList){
                    modelSquid = Integer.parseInt(modelSquidMap.get("MODEL_SQUID_ID").toString(), 10);
                    paramsMap.clear();
                    paramsMap.put("id", Integer.toString(modelSquid, 10));
                    squid = adapter.query2Object(paramsMap, Squid.class);
                    if(StringUtils.isNull(squid)){
                        messageBubbleList.clear();
                        messageBubbleList.add(setMessageBubble(false, squidId, childId, name, MessageBubbleCode.ERROR_PREDICT_MODEL_SQUID_BY_DELETED.value(), MessageBubbleCode.ERROR, "引用的模型已被删除"));
                        PushMessagePacket.push(messageBubbleList, getToken());
                    }
                    else {
                        messageBubbleList.clear();
                        messageBubbleList.add(setMessageBubble(true, squidId, childId, name, MessageBubbleCode.ERROR_PREDICT_MODEL_SQUID_BY_DELETED.value(), MessageBubbleCode.ERROR, "引用的模型已被删除"));
                        PushMessagePacket.push(messageBubbleList, getToken());
                        
                    }
                }
            }
            
        }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    /**
     * 49.校验报表squid的源数据是否被删除
     * @param squidId
     * @param childId
     * @param name
     * @param code
     * @author bo.dang
     * @date 2014年7月2日
     */
    public void validateReportSquidColumnsByDEL(int squidId, int childId, String name, int code){
        // 
        if(squidId != 0){
            messageBubbleList.clear();
            messageBubbleList.add(this.setMessageBubble(false, squidId, childId, name, MessageBubbleCode.WARN_REPORT_SQUID_COLUMNS_BY_DELETED.value(), MessageBubbleCode.WARN, "报表squid的源数据被修改或者删除，可能导致报表运行不正常"));
            logger.debug("Queue消息泡校验Middle：validateReportSquidColumnsByDEL" + ":" + messageBubbleList.size());
            PushMessagePacket.push(messageBubbleList, getToken());
        }
        else {
            String call = "{ ? = call validateReportSquidColumnsByDEL(?, ?, ?, ?) }";
            int level = MessageBubbleCode.WARN;
            String text = "报表squid的源数据被修改或删除，可能导致报表运行不正常";
            prepareCall(squidId, childId, name, code, call, level, text);
        }
        
    }
    
    /**
     * 51.校验Aggregation或者Group设置
     * 1.Group by 有一个原则就是: select 后面的所有列中，没有使用聚合函数的列，必须出现在group by后面
     * 2.要么全设置，要么全不设置
     * 1.可以选所有aggreation，不选group by;
     * 2.可以选所有group by, 不选aggreation;
     * 3.可以选所有aggreation，和可以选所有group by;
     * 4.select 后面的所有列中,没有使用聚合函数的列,必须出现在 group by 后面
     * @param squidId
     * @param childId
     * @param name
     * @param code
     * @author bo.dang
     * @date 2014年7月15日
     */
    public void validateAggregationOrGroup(int squidId, int childId, String name, int code){
        try {
            Boolean flag = false;
            Boolean validateFlag = false;
            String sql = null;
        if(squidId != 0){
            sql = "select dss.id as id, dsc.id as c_id, dsc.name as name, dsc.aggregation_type as aggregation_type, dsc.is_groupby as is_groupby from ds_squid dss inner join ds_column dsc on dss.id = dsc.squid_id where dss.squid_type_id = 3 and dss.id = " + squidId + " order by dsc.aggregation_type desc, dsc.is_groupby desc";
            Map<String, String> paramsMap = new HashMap<String, String>();
            paramsMap.put("id", Integer.toString(childId, 10));
            paramsMap.put("squid_id", Integer.toString(squidId, 10));
            Column column = adapter.query2Object(paramsMap, Column.class);
            // 如果Column为空或者Null, 那么应该是删除这个column
            if(StringUtils.isNull(column)){
                messageBubbleList.clear();
                messageBubbleList.add(this.setMessageBubble(true, squidId, childId, name, MessageBubbleCode.ERROR_AGGREGATION_OR_GROUP.value(), MessageBubbleCode.ERROR, "对应的Aggregation或者Group设置错误"));
                logger.debug("消息泡校验：validateAggregationOrGroup  (true, " + squidId + "," + childId + "," + name + "," + code + ") ");
                PushMessagePacket.push(messageBubbleList, getToken());
            }
        }
        else {
                sql = "select dss.id as id, dsc.id as c_id, dsc.name as name, dsc.aggregation_type as aggregation_type, dsc.is_groupby as is_groupby from ds_squid dss inner join ds_column dsc on dss.id = dsc.squid_id where dss.squid_type_id = 3 and dss.squid_flow_id = " + childId + " order by dss.id asc, dsc.aggregation_type desc, dsc.is_groupby desc";
        }
                List<Map<String, Object>> columnMapList = adapter.query2List4(false, sql, null);
                if(StringUtils.isNotNull(columnMapList) && !columnMapList.isEmpty()){
                    int aggregation_type = 0;
                    Boolean is_groupby = false;
                    Map<String, Object> columnMap = null;
                    for(int i = 0; i < columnMapList.size(); i++) {
                        columnMap = columnMapList.get(i);
                        squidId = Integer.parseInt(columnMap.get("ID").toString(), 10);
                        name = columnMap.get("NAME").toString();
                        childId = Integer.parseInt(columnMap.get("C_ID").toString(), 10);
                        aggregation_type = Integer.parseInt(columnMap.get("AGGREGATION_TYPE").toString(), 10);
                        is_groupby = columnMap.get("IS_GROUPBY").toString().equals("Y") ? true:false;
                        // 如果设置Aggregation或者Gropu
//                        if(aggregation_type != -1 || is_groupby){
                        if(aggregation_type >0 || is_groupby){
                            flag = true;
                        }
                        // 如果Column都没有设置Aggregation和Group
                        if(aggregation_type <=0 && !is_groupby){
                            validateFlag = true;
                        }
                        if((flag && !validateFlag) || (!flag && validateFlag)){
                            messageBubbleList.clear();
                            messageBubbleList.add(this.setMessageBubble(true, squidId, childId, name, MessageBubbleCode.ERROR_AGGREGATION_OR_GROUP.value(), MessageBubbleCode.ERROR, "对应的Aggregation或者Group设置错误"));
                            logger.debug("消息泡校验：validateAggregationOrGroup  (true, " + squidId + "," + childId + "," + name + "," + code + ") ");
                            PushMessagePacket.push(messageBubbleList, getToken());
                            //break;
                        }
                        
                        if(flag && validateFlag){
                            messageBubbleList.clear();
                            messageBubbleList.add(this.setMessageBubble(false, squidId, childId, name, MessageBubbleCode.ERROR_AGGREGATION_OR_GROUP.value(), MessageBubbleCode.ERROR, "对应的Aggregation或者Group设置错误"));
                            logger.debug("消息泡校验：validateAggregationOrGroup  (false, " + squidId + "," + childId + "," + name + "," + code + ") ");
                            PushMessagePacket.push(messageBubbleList, getToken());
                            //break;
                        }
                        
                        if(i<columnMapList.size()-1){
                            if(!columnMapList.get(i).get("ID").toString().equals(columnMapList.get(i+1).get("ID").toString())){
                                flag = false;
                                validateFlag = false;
                            }
                        }
                        
                    }
                }
            
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    /**
     * 
     * 检查同步状态
     * @param squidId
     * @param childId
     * @param name
     * @param code
     * @param status
     * @param text
     * @author bo.dang
     */
    public void checkSynchronized(int squidId, int childId, String name, int code, Boolean status, String text){
        messageBubbleList.clear();
        messageBubbleList.add(this.setMessageBubble(status, squidId, childId, name, MessageBubbleCode.ERROR_SYNCHRONIZED.value(), MessageBubbleCode.ERROR, text));
        logger.info("消息泡校验：checkSynchronized  (" + status  + "," + squidId + "," + childId + "," + name + "," + code + ") ");
        PushMessagePacket.push(messageBubbleList, getToken());
    }
    
    /**
     * 调用消息泡的Function
     * @param squidId
     * @param childId
     * @param name
     * @param code
     * @param call
     * @param level
     * @param text
     * @author bo.dang
     * @date 2014年6月12日
     */
    public void prepareCall(int squidId, int childId, String name, int code, String call, int level, String text){
        messageBubbleList = new ArrayList<MessageBubble>();
        if(StringUtils.isNotNull(call)){
        logger.debug("消息泡校验：" + call + ": (" + squidId + "," + childId + "," + name + "," + code + ") strat!");
        try {
//        callStatement = adapter.prepareCall(false, "{ ? = call validateMessageBubble(?, ?, ?, ?) }");
        callStatement = adapter.prepareCall(false, call);
        callStatement.setInt(1, squidId);
        callStatement.setInt(2, childId);
        callStatement.setString(3, name);
        callStatement.setInt(4, code);
        callStatement.execute();
        ResultSet rs = callStatement.getResultSet();
        ResultSetMetaData md = null; 
        List<Map<String, Object>> list = null;
        Map<String, Object> map=null;
        if(rs!=null){
            list = new ArrayList<Map<String, Object>>();
            md = rs.getMetaData();
            int cols = md.getColumnCount();
            while(rs.next()){
                map = new HashMap<String, Object>();
                for(int i=1; i<=cols; i++){
                    map.put(md.getColumnName(i), rs.getObject(i));
                }
                list.add(map);
            }
        }
        //callStatement.clearParameters();
        //logger.info("Queue消息泡校验Start：" + call + ":" + messageBubbleList.size());
        int t_id = 0;
        int t_c_id = 0;
        String t_name = null;
        Boolean status = false;
        int tempChildId = childId;
        if(StringUtils.isNotNull(list) && !list.isEmpty()){
            logger.debug("消息泡校验：" + call + ": " + list.size());
            int count = 0;
            for(Map<String, Object> resultMap : list){
                t_id = Integer.parseInt(resultMap.get("T_ID").toString(), 10);
                t_c_id = resultMap.get("T_C_ID") == null? 0:Integer.parseInt(resultMap.get("T_C_ID").toString(), 10);
                t_name = resultMap.get("T_NAME")== null?null:resultMap.get("T_NAME").toString();
                status = Boolean.valueOf(resultMap.get("STATUS").toString());
/*                if(squidId == 0 && t_c_id == 0){
                    t_c_id = t_id;
                }
                else if(squidId != 0 && childId != 0){
                    t_c_id = childId;
                }*/
/*                if(StringUtils.isNotNull(t_name)){
                    text = t_name + text;
                }*/
                logger.debug("消息泡校验：" + call + ": " + count++ + "," + status + "," + t_id + "," + t_c_id + "," + t_name + "," + code + "," + level + "," + text);
                messageBubbleList.clear();
                messageBubbleList.add(setMessageBubble(status, t_id, t_c_id, t_name, code, level, text));
                logger.debug("Queue消息泡校验Middle：" + call + ":" + messageBubbleList.size());
                PushMessagePacket.push(messageBubbleList, getToken());
            }
        }
        //callStatement.registerOutParameter(3, Types.ARRAY);
        logger.debug("消息泡校验：" + call + ": (" + squidId + "," + tempChildId + "," + name + "," + code + ") end!");
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            try {
                callStatement.close();
                adapter.rollback();
            } catch (SQLException e1) {
                // TODO Auto-generated catch block
                logger.debug("消息泡校验：" + call + ": (" + squidId + "," + childId + "," + name + "," + code + ") error!");
                e1.printStackTrace();
            }
            e.printStackTrace();
            logger.debug("消息泡校验：" + call + ": (" + squidId + "," + childId + "," + name + "," + code + ") error!");
        }
        }
    }
   
   /**
    * 建立adapter
    * @author bo.dang
    */
   public void openSessionForMessageBubble(IRelationalDataManager newAdapter){
       if(StringUtils.isNull(newAdapter) && StringUtils.isNull(this.adapter)){
           this.adapter = DataAdapterFactory.newInstance().getDataManagerByToken(
                   token);
           //this.adapter.openSession();
       } else if(StringUtils.isNotNull(newAdapter) && StringUtils.isNull(this.adapter) ){
           this.adapter = newAdapter;
       }
       if(StringUtils.isNotNull(this.adapter.getConnection())){
           try {
               if(this.adapter.getConnection().isClosed()){
                   this.adapter.openSession();
               }
           } catch (SQLException e) {
               // TODO Auto-generated catch block
               e.printStackTrace();
           }
       }
       
       if(StringUtils.isNull(this.adapter.getConnection())){
           this.adapter.openSession();
       }
   }
   
    public static void main(String[] args) {
/*        //add()和remove()方法在失败的时候会抛出异常(不推荐)
        Queue<String> queue = new LinkedList<String>();
        //添加元素
        queue.offer("a");
        queue.offer("b");
        queue.offer("c");
        queue.offer("d");
        queue.offer("e");
        for(String q : queue){
            System.out.println(q);
        }
        System.out.println("===");
        System.out.println("poll="+queue.poll()); //返回第一个元素，并在队列中删除
        for(String q : queue){
            System.out.println(q);
        }
        System.out.println("===");
        System.out.println("element="+queue.element()); //返回第一个元素 
        for(String q : queue){
            System.out.println(q);
        }
        System.out.println("===");
        System.out.println("peek="+queue.peek()); //返回第一个元素 
        for(String q : queue){
            System.out.println(q);
        }*/
        String regx = "[a-zA-Z][a-zA-Z0-9#$_]{0,29}";
        String reg = "adafdafd";
        PatternCompiler compiler = new Perl5Compiler();
        org.apache.oro.text.regex.Pattern pattern = null;
        try {
            pattern = compiler.compile(reg);
        } catch (MalformedPatternException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
	public static String getToken() {
		return token;
	}
	public static void setToken(String token) {
		MessageBubbleService.token = token;
	}
}
