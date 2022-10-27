/*
 * MessageQueueManager.java - 2013.10.20
 *
 * 版权声明: (c) 2013，悦岚（上海）数据服务有限公司，保留所有权利。
 *
 * 项目名称: datashire-server
 * 
 * 修改历史:
 * ===========================================
 *   修改人	     日期		     描述
 *   ---------------------------------------
 *   dang.lu  2013.10.20   create    
 * ===========================================
 */
package com.eurlanda.datashire.socket;

import com.eurlanda.datashire.utility.StringUtils;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;

/**
 * 维护socket通讯消息队列
 * 		
 * 	1.业务层处理完毕封装消息进栈
 * 	2.等待消息调度
 * 	3.当有通道可用时发送最先进栈的未锁定消息（FIFO），并将该消息锁定，即表示消息发送中）
 * 	4.通讯层发送成功后移除该消息
 * 	[全程监控消息进栈、出栈，消息积压情况]
 * 
 * TODO 消息积压处理！
 * @author dang.lu 2013.10.20
 *
 */
public class MessageQueueManager {
	
	private static Logger logger = Logger.getLogger(MessageQueueManager.class);

	/** 消息队列 */
	private static Hashtable<String, Vector<MessagePacket>> mq = new Hashtable<String, Vector<MessagePacket>>();
	
	public static int maxQueueLength=0; // 最大消息队列积压总数
	public static int totalIn; // 消息进栈总数
	public static int totalOut; // 消息出栈总数
	
	// 消息进栈命令号
	public static List<String> cmdInList = new ArrayList<String>();
	// 消息出栈命令号
	public static List<String> cmdOutList = new ArrayList<String>();
	// (后台准备发出去的)消息包长度
	public static Map<String, List<Integer>> MsgOutLen = new TreeMap<String, List<Integer>>();
	
	/** 消息入栈（存包） */
	public static void push(String token, MessagePacket packet){
		//String token = new String(packet.getToken(), 0, CommonConsts.PACKET_TOKEN_LENGTH);
		String cmd = packet.getCommandId()+packet.getChildCommandId();
		cmdInList.add(cmd);
		List<Integer> list = MsgOutLen.get(cmd);
		if(list!=null){
			list.add(packet.getData().length);
		}else{
			list = new ArrayList<Integer>();
			list.add(packet.getData().length);
			MsgOutLen.put(cmd, list);
		}
		
		logger.debug("[消息入栈] start \t编号="+packet.getId()+"\ttoken="+token+"\tcmd="+StringUtils.list2String(MessageQueueManager.cmdInList, 20));
		//对应token是否存在消息队列
		boolean firstMsg = false;
		if(mq.containsKey(token)){
		   mq.get(token).add(packet);
		}else{
		   firstMsg = true;
		   //创建新的消息队列
		   Vector<MessagePacket> vector=new Vector<MessagePacket>(1, 10);
		   //追加消息队列
		   vector.add(packet);
		   mq.put(token, vector);
		}
		totalIn++;
		maxQueueLength = Math.max(maxQueueLength, firstMsg?1:mq.get(token).size());
		logger.debug("[消息入栈] end \ttoken="+token+"\t消息积压="+(firstMsg?1:mq.get(token).size())+"\t最大积压="+MessageQueueManager.maxQueueLength);
	}
	
	/** 消息发送（取包） */
	public synchronized static MessagePacket peek(String token, int i){
		//获取数据包队列
		Vector<MessagePacket> vector = mq.get(token);
		int queueSize = vector==null?-1:vector.size();
//		if(queueSize>=1 && i>=queueSize){
//			logger.info("[遍历队列已满]\ttoken="+token+" ( 获取第 "+i+", 共 "+queueSize+" 个消息 )");
//			--i; // msg queue is not null, but query index > queueSize
//		}
		if(queueSize>=1&&i<queueSize){
			logger.debug("[获取待发送消息队列]\ttoken="+token+" ( 获取第 "+i+", 共 "+queueSize+" 个消息 )");
			//获取数据包第一个元素
			MessagePacket packetExpand=vector.get(i);
			if(packetExpand!=null){
				//判断是否锁定
				if(packetExpand.isLockFlag()){ // TODO 如果长期被（同一个通道）锁定，需要异常处理
					logger.info("[消息被锁定] 编号="+packetExpand.getId()+", 命令号"+packetExpand.getCommandId()+packetExpand.getChildCommandId());
					try {
						Thread.sleep(10);
					} catch (InterruptedException e) {
						logger.error("等待空闲消息被打扰！", e);
					}
					return peek(token, ++i);
				}else{
					packetExpand.setLockFlag(true); // 锁定该消息，准备发送
					return packetExpand;
				}
			}
		}else{
			logger.debug(i+"/"+queueSize+" [(待发送)消息队列为空/木有积压]\ttoken="+token);
		}
		return null;
	}
	
	/** 消息出栈（销毁）,并返回当前token消息积压总数 */
	public static int remove(String token, MessagePacket packet){
		cmdOutList.add(packet.getCommandId()+packet.getChildCommandId());
		logger.debug("[消息出栈] start \t编号="+packet.getId()+"\ttoken="+token+"\tcmd="+StringUtils.list2String(cmdOutList, 20));
		int i;
		Vector<MessagePacket> vector=mq.get(token);
		synchronized(vector){
			i = vector.indexOf(packet);
	    	if (i >= 0) {
	    		vector.removeElementAt(i);
	    	}
		}
		if (i < 0) {
    		logger.warn(i+"\t无此消息,token="+token+"\t"+packet);
    	}
    	totalOut++;
    	i=vector.size();
    	logger.debug("[消息出栈] end 剩余消息="+i+
	    			", 入栈总数="+totalIn+
	    			", 出栈总数="+totalOut+
	    			", 最大积压="+maxQueueLength+
	    			", 驻留时间="+(System.currentTimeMillis()-packet.getReceiveTime())
	    			);
    	if(i>0){
    		synchronized(vector){
        		for(MessagePacket p : vector){
    				logger.debug(i+"\t[消息积压] "+p.isLockFlag()+" 编号："+p.getId()+"， 命令号："+p.getCommandId()+p.getChildCommandId()/*+"， 内容："+new String(p.getData())*/);
    			}
    		}
    	}
    	return i;
	}
	
}	