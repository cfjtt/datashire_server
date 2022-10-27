/*
 * MessageBubbleValidator.java - 2013-11-25
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

import com.eurlanda.datashire.entity.MessageBubble;
import com.eurlanda.datashire.entity.ReferenceColumn;
import com.eurlanda.datashire.entity.Squid;
import com.eurlanda.datashire.enumeration.MessageBubbleCode;
import com.eurlanda.datashire.sprint7.packet.PushMessagePacket;
import com.eurlanda.datashire.sprint7.service.squidflow.RepositoryServiceHelper;
import com.eurlanda.datashire.utility.StringUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * 数据校验 - 消息气泡
 * 
 * @author dang.lu 2013-11-25
 *
 */
public class MessageBubbleValidator{
	private static final Logger logger = LogManager.getLogger(MessageBubbleValidator.class);
	private RepositoryServiceHelper service;
	private final String token;
	
	public MessageBubbleValidator(String token) {
		this.service = new RepositoryServiceHelper(token);
		this.token = token;
	}

	/**
	 * 上游的column被删除 (引用列孤立/悬空)
	 */
	public void chekAllDeletedColumns(){
		logger.debug("数据校验 - 消息气泡 (上游的column被删除)");
		String sql = "SELECT * FROM DS_REFERENCE_COLUMN WHERE column_id NOT IN (SELECT ID FROM DS_SOURCE_COLUMN UNION ALL SELECT ID FROM DS_COLUMN)";
		List<ReferenceColumn> list = service.getAll(sql, ReferenceColumn.class);
		if(list!=null && !list.isEmpty()){
			int s = list.size();
			logger.debug("发现上游的column被删除："+s);
			List<MessageBubble> msg = new ArrayList<MessageBubble>(s);
			for(int i=0; i<s; i++){
				Squid hostSquid = service.getOne(Squid.class, list.get(i).getHost_squid_id());
				if(hostSquid!=null && StringUtils.isNotNull(hostSquid.getKey())){
					msg.add(new MessageBubble(hostSquid.getKey(), list.get(i).getKey(), MessageBubbleCode.ERROR_REFERENCE_COLUMN_DELETED.value(), false));
				}
			}
			PushMessagePacket.push(msg, token);
		}else{
			logger.debug("数据校验'上游的column被删除'正常。");
		}
	}
	
	/**
	 * 目标列（左边列）没有参与变换
	 */
	public void chekUnTransColumns(){
		logger.debug("数据校验 - 消息气泡 (目标列（左边列）没有参与变换)");
		String sql = "SELECT c.key child_key, s.key squid_key, "+
				MessageBubbleCode.ERROR_COLUMN_NO_TRANSFORMATION.value()
				+" code, 'N' succeed, 1 level FROM DS_COLUMN c, DS_SQUID s, DS_TRANSFORMATION t WHERE"
				+ " c.id=t.column_id AND c.squid_id=t.squid_id AND c.squid_id=s.id  AND t.squid_id=s.id"
				+ " AND t.id NOT IN (SELECT DISTINCT to_transformation_id FROM DS_TRANSFORMATION_LINK)";
		List<MessageBubble> msg = service.getAll(sql, MessageBubble.class);
		logger.debug("目标列（左边列）没有参与变换："+msg==null?0:msg.size());
		PushMessagePacket.push(msg, token);
	}
	
}
