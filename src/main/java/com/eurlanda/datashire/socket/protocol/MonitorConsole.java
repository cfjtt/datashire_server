package com.eurlanda.datashire.socket.protocol;

import org.apache.log4j.Logger;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MonitorConsole {

	private static Logger logger = Logger.getLogger(MonitorConsole.class);
	public static int serviceRequestCnt; // 业务调用总数
	int serviceResponseCnt; // 业务成功返回总数
	int maxActiveServiceCnt;// 最大活动业务数
	
	int msgRequestCnt; // 消息进栈总数
	int msgResponseCnt; // 消息出栈总数
	int maxActiveMsgCnt;// 最大消息积压数
	
	long totalMsgSizeReceived;  // 后台接受消息大小总量
	long totalMsgSizeResponsed; // 后台发送消息大小总量
	

	// TODO 根据不同数据库分别记录
	/** 连接池最大活动连接 */
	public static int maxActivePoolCnt;
	/** 连接池当前活动连接 */
	public static int activePoolCnt;
	/** 连接池活动连接总数 */
	public static int sumActivePoolCnt;
	/** 连接池当前空闲连接 */
	public static int idlePoolCnt;
	public static int sumUnclosedPoolCnt;
	
	/** <Connection.HashCode, StackTrace> */
	//public static Map<Integer, String> conMap = new HashMap<Integer, String>();
	/** <ThreadName, List<Connection>> */
	public static Map<String, List<Connection>> connectionMap = new HashMap<String, List<Connection>>();
	
	// key: cmd+childCmd
	Map<String, ServiceMonitor> serMap;
	JvmMonitor jvm;
	// key: table name
	private static Map<String, TableMonitor> tblMap = new HashMap<String, TableMonitor>(100);
	public static final int MaxTableLockTime = 3000;
	
	/** 更新操作，先将该表锁定，防止其它事务更新 */
	public synchronized static void lockTable(String tableName, String threadName){
		TableMonitor t = tblMap.get(tableName);
		if(t==null){
			t = new TableMonitor();
			t.setLockCounts(1);
			tblMap.put(tableName, t);
		}else{
			t.setLockCounts(t.getLockCounts()+1);
		}
		t.setTableName(tableName);
		t.setLocked(true);
		t.setLockTime(System.currentTimeMillis());
		t.setLockTreadName(threadName);
		logger.info("lock table "+t);
	}
	/** 解除锁定 */
	public synchronized static void unLockTable(String tableName){
		logger.info("unlock table "+tableName);
		TableMonitor t = tblMap.get(tableName);
		if(t!=null){
			t.setLocked(false);
		}
	}
	/** 查看某表是否被锁定 */
	public static boolean isLocked(String tableName){
		TableMonitor t = tblMap.get(tableName);
		if(t!=null){
			return t.isLocked();
		}
		return false;
	}
	
	public static void waitLock(String tableName){
		while(isLocked(tableName)){
			try {Thread.sleep(10);} catch (InterruptedException e) {}
			TableMonitor t = tblMap.get(tableName);
			if(System.currentTimeMillis()-t.getLockTime()>=MaxTableLockTime){
				logger.info("wait time over "+tableName);
				break;
			}
		}
	}
	
}
