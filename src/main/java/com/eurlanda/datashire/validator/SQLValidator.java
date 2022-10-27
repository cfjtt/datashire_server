/*
 * SQLValidator.java - 2013-11-25
 *
 * 版权声明: (c) 2013，悦岚（上海）数据服务有限公司，保留所有权利。
 *
 * 项目名称: datashire-server
 * 
 * 包结构说明：
 * 		validator可以提供消息气泡的数据校验、客户端请求的权限验证等。
 * 
 * 修改历史:
 * ===========================================
 *   修改人	     日期		     描述
 *   ---------------------------------------
 *   dang.lu  2013-11-25   create    
 * ===========================================
 */
package com.eurlanda.datashire.validator;

import com.akiban.sql.StandardException;
import com.akiban.sql.parser.SQLParser;
import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.entity.MessageBubble;
import com.eurlanda.datashire.entity.Squid;
import com.eurlanda.datashire.entity.TableExtractSquid;
import com.eurlanda.datashire.enumeration.MessageBubbleCode;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.sprint7.packet.PushMessagePacket;
import com.eurlanda.datashire.utility.StringUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * SQL语法校验
 *  1. Data SquidModelBase 的 filter 过滤条件
 *  2. Extract SquidModelBase 增量抽取条件(仅在启用增量抽取时有效)
 *  3. SquidModelBase Join 的 join_condition 连接条件
 *  
 *  4. GROUPBY错误 ：要查询的列（除聚合函数外）不在GROUP BY子句中
 * 
 * @author dang.lu 2013-11-25
 *
 */
public class SQLValidator {
	private static final Logger logger = LogManager.getLogger(SQLValidator.class);
	protected final IRelationalDataManager adapter;
	private static String token;
	private static final SQLParser SQL_PARSER = new SQLParser();
	public static final String SQL_SELECT = "SELECT * FROM A WHERE ";
	private static final String SQL_JOIN = "SELECT * FROM A JOIN B ON ";
	
	public SQLValidator(String token) {
		this.adapter = DataAdapterFactory.newInstance().getDataManagerByToken(token);
		this.token = token;
	}
	
	public SQLValidator(String token, IRelationalDataManager adapter) {
		this.adapter = adapter;
		this.token = token;
	}
	
	public static List<MessageBubble> messageBubbleList = new ArrayList<MessageBubble>();
	
    /**
     * 封装消息泡返回值
     */
    public static MessageBubble setMessageBubble(int squidId,
            int childId, String name, int code, boolean status, int level, String text) {
        //messageBubble.setSquid_id(squidId);
        //messageBubble.setChild_id(childId);
//        messageBubble.setCode(code);
//        messageBubble.setStatus(status);
//        messageBubble.setLevel(level);
        return new MessageBubble(status, squidId, childId, name, code, level, text);
    }
	/**
	 * Data SquidModelBase 的 filter 过滤条件(如果是ExtractSquid，同时验证增量抽取条件)
	 * @param squid_id
	 */
	public void checkSquidFilter(int squid_id){
		Map<String, Object> params = new HashMap<String, Object>(1);
		params.put("ID", squid_id);
		Squid squid = adapter.query2Object(true, params, Squid.class);
		if(squid!=null){
			checkSquidFilter(squid.getFilter(), squid.getKey());
			if(squid.getSquid_type()==SquidTypeEnum.EXTRACT.value()){
				TableExtractSquid extractSquid = adapter.query2Object(true, params, TableExtractSquid.class);
				if( extractSquid!=null&&extractSquid.isIs_incremental()){
					checkExtractSquidIncrement(extractSquid.getIncremental_expression(), squid.getKey());
				}
			}
		}
	}
	
	/**
	 * Data SquidModelBase 的 filter 过滤条件
	 * @param filter
	 * @param key
	 */
	public void checkSquidFilter(String filter, String key){
		if(StringUtils.isNotNull(filter)){
			String err = parse(SQL_SELECT+filter);
			PushMessagePacket.push(new MessageBubble(key, key, 
					MessageBubbleCode.ERROR_SQL_SYNTAX_FILTER.value(), StringUtils.isNull(err), err), token);
		}
	}
	
	/**
     * 2.Data SquidModelBase 的 filter 过滤条件
     * @param filter
     * @param key
     */
    public static void validateSquidFilter(String filter, int squidId, int childId, String name){
        messageBubbleList.clear();
        if(StringUtils.isNull(filter)){
            //String err = parse(SQL_SELECT+filter);
/*            PushMessagePacket.push(new MessageBubble(key, key, 
                    MessageBubbleCode.ERROR_SQL_SYNTAX_FILTER.value(), StringUtils.isNull(err), err), token);*/
            messageBubbleList.add(setMessageBubble(squidId, childId, name, MessageBubbleCode.WARN_NO_SQL_EXPRESSION.value(), false, MessageBubbleCode.WARN, "表达式为空"));
        }
        else {
            // 消除表达式为空的消息泡
            messageBubbleList.add(setMessageBubble(squidId, childId, name, MessageBubbleCode.WARN_NO_SQL_EXPRESSION.value(), true, MessageBubbleCode.WARN, "表达式不为空"));
            //adapter.execute(sql);
            // 校验信息
            String error = parse(SQL_SELECT+filter);
            // 如果校验信息为空,那么验证通过
            if(StringUtils.isNull(error)){
                // 消除消息泡
                messageBubbleList.add(setMessageBubble(squidId, childId, name, MessageBubbleCode.ERROR_SQL_SYNTAX_FILTER.value(), true, MessageBubbleCode.ERROR, "表达式校验正确"));
            }
            else {
                // 推送消息泡
                messageBubbleList.add(setMessageBubble(squidId, childId, name, MessageBubbleCode.ERROR_SQL_SYNTAX_FILTER.value(), false, MessageBubbleCode.ERROR, error));
            }
        
       }
        PushMessagePacket.push(messageBubbleList, token);
    }
    
    /**
     * 2.Data SquidModelBase 的 filter 过滤条件
     * @param filter
     * @param key
     */
    public static void validateJoin(String filter, int squidId, int childId, String name){
        messageBubbleList.clear();
        if(StringUtils.isNull(filter)){
            //String err = parse(SQL_SELECT+filter);
/*            PushMessagePacket.push(new MessageBubble(key, key, 
                    MessageBubbleCode.ERROR_SQL_SYNTAX_FILTER.value(), StringUtils.isNull(err), err), token);*/
            messageBubbleList.add(setMessageBubble(squidId, childId, name, MessageBubbleCode.WARN_NO_SQL_EXPRESSION.value(), false, MessageBubbleCode.WARN, "表达式为空"));
        }
        else {
            // 消除表达式为空的消息泡
            messageBubbleList.add(setMessageBubble(squidId, childId, name, MessageBubbleCode.WARN_NO_SQL_EXPRESSION.value(), true, MessageBubbleCode.WARN, "表达式不为空"));
            //adapter.execute(sql);
            // 校验信息
            String error = parse(SQL_SELECT+filter);
            // 如果校验信息为空,那么验证通过
            if(StringUtils.isNull(error)){
                // 消除消息泡
                messageBubbleList.add(setMessageBubble(squidId, childId, name, MessageBubbleCode.ERROR_SQL_SYNTAX_FILTER.value(), true, MessageBubbleCode.ERROR, "表达式校验正确"));
            }
            else {
                // 推送消息泡
                messageBubbleList.add(setMessageBubble(squidId, childId, name, MessageBubbleCode.ERROR_SQL_SYNTAX_FILTER.value(), false, MessageBubbleCode.ERROR, error));
            }
        
       }
        PushMessagePacket.push(messageBubbleList, token);
    }
	/**
	 * ExtractSquid 增量抽取条件
	 * @param increment
	 * @param key
	 */
	public void checkExtractSquidIncrement(String increment, String key){
		if(StringUtils.isNotNull(increment)){
			String err = parse(SQL_SELECT+increment);
			PushMessagePacket.push(new MessageBubble(key, key, 
					MessageBubbleCode.ERROR_SQL_SYNTAX_INCREMENT.value(), StringUtils.isNull(err), err), token);
		}
	}

	   /**
     * ExtractSquid 增量抽取条件
     * @param increment
     * @param key
     */
    public void validateExtractSquidIncrement(String increment, String key){
        if(StringUtils.isNotNull(increment)){
            String err = parse(SQL_SELECT+increment);
            PushMessagePacket.push(new MessageBubble(key, key, 
                    MessageBubbleCode.ERROR_SQL_SYNTAX_INCREMENT.value(), StringUtils.isNull(err), err), token);
        }
    }
	
	/**
	 * SquidModelBase Join 的 join_condition 连接条件
	 * @param squid_join_id
	 */
	public void checkJoinCondition(int squid_join_id){
		
	}
	
	public static final boolean isValidSQL(String sql){
		if(StringUtils.isNull(sql)){
			return false;
		}
		try {
		    SQL_PARSER.parseStatement(sql);
		} catch (StandardException e) {
			logger.warn("sql 语法校验失败 "+sql, e);
			return false;
		}
		return true;
	}
	
	/**
	 * 解析SQL语句，失败返回异常信息
	 * @param sql
	 * @return
	 */
	public static final String parse(String sql){
		if(StringUtils.isNull(sql)){
			return null;
		}
		try {
		    SQL_PARSER.parseStatement(sql);
		} catch (StandardException e) {
		    //logger.warn("sql 语法校验失败 "+sql, e);
			return "sql 语法校验失败 " + sql + e;
		}
		return null;
	}
	
	public static void main(String[] args) {
		//System.err.println(isValidSQL(" "));
		//System.out.println(isValidSQL("select 1 from dual"));
		System.out.println(parse("select 1 from dual where id in 2"));
		//System.err.println(isValidSQL("selecter 1 from dual"));
	}
	
}
