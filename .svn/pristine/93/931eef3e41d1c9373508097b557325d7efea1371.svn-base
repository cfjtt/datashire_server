package com.eurlanda.datashire.utility;

import cn.com.jsoft.jframe.utils.ValidateUtils;
import com.eurlanda.datashire.enumeration.DataBaseType;
import com.eurlanda.datashire.validator.SquidValidationTask;
import org.jboss.netty.logging.InternalLogLevel;
import org.springframework.context.ApplicationContext;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 公共参数/常量类
 * 
 * @author dang.lu 2013.09.11
 *
 */
public class CommonConsts{
	private static Properties prop;
	private static long configLastModifyDate =0;
////////////////////////////////////// 数据库配置相关  ///////////////////////////////////////////
	
	/* 系统元数据 hsqldb连接配置
	 * server url:
	 *   HSQL_HOST = "jdbc:hsqldb:hsql://192.168.137.1:9001/"; */
	/** 系统数据库路径(默认数据库和仓库都放在这个目录下，子目录名即为数据库名) */
	public static String HSQL_DB_PATH;// 在setGlobalParam方法中实现。= getProperty("HsqlDB_Path");//null; //"C:/javac/eurlanda/hsqldb/";
	/** 数据库名称（系统权限库)
	 *  repository_db_name请看（大屏幕）：DS_SYS_REPOSITORY表）,注意不要和默认数据库冲突 */
	public static Integer HSQL_SYS_DB_TYPE = ValidateUtils.isEmpty(SysConf.getValue("HSQL_SYS_DB_TYPE"))?
				DataBaseType.HSQLDB.value():Integer.parseInt(SysConf.getValue("HSQL_SYS_DB_TYPE"));//hsqldb
	
	public static  String HSQL_SYS_DB_NAME = SysConf.getValue("HsqlDB_SysName");//"hsqlDB";
	// 数据库地址
	public static String HSQL_HOST_ADDRESS = SysConf.getValue("Hsql_Host_Address");
	/** 数据库路径 */
	public static String HSQL_HOST = HSQL_HOST_ADDRESS + HSQL_SYS_DB_NAME;

    /** 用来设置dbsquid host,代表使用的是系统自带的datasource */
    public static final String DATASHIRE_SERVER_DATABASE_HOST_SYMBOL = "—--**00DATASHIRE_SERVER_111DATABASE_HOST_SYMBOLasdfas";

	/** 数据库统一账号 */
	public static final String HSQL_USER_NAME = SysConf.getValue("HSQL_USER_NAME");;
	/** 数据库统一密码 */
	public static final String HSQL_PASSWORD = SysConf.getValue("HSQL_PASSWORD");;
	/** Repository元数据定义及初始化脚本(包含：建表、约束、初始化数据) */
	public static final String Repository_SQL_PATH = 
			System.getProperty("user.dir")+"/config/ds-hsql-repository.sql";
	/** 服务器版本号 */
	public static String VERSION;
////////////////////////////////////// others ///////////////////////////////////////////	

	/** 超级用户名称(系统固定参数配置,不分大小写，可以有前后空格) */
	public static final String SuperUserName = "superuser";
	/** 超级用户密码(服务启动时加载 默认6个1) */
	public static String SuperUserPwd = "111111"; // md5?
	/** 普通用户默认密码（添加用户时，如果没有设置密码，则使用该默认值） */
	public static final String DEFAULT_USER_PASSWORD = "111111"; // md5?
	public static String LimitedTime="2099-01-01";
	public static int MaxSquidCount = 99999;
	/** 每页显示记录数最大值 */
	public static final int MaxPageSize = 1000;
	/** 每页显示记录数最小值 */
	public static final int MinPageSize = 1;
	/** 日期显示格式 */
	public static final DateFormat FORMAT_TIMESTAMP = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	public static final DateFormat FORMAT_TIME = new SimpleDateFormat("HH:mm:ss");
	public static final DateFormat FORMAT_DATE = new SimpleDateFormat("yyyy-MM-dd");
	public static final String AutoIncrementColumn = "自增长列(AutoIncrementColumn)";
	
////////////////////////////////////// socket ///////////////////////////////////////////
	
	/** socket 日志级别 (表示netty将用哪一种日志级别记录日志,而不是只记录指定级别以上日志) */
	public static final InternalLogLevel SOCKET_LOG_LEVEL = InternalLogLevel.DEBUG;
	
	/** 默认token(32个0) 登录前状态 */
	public static final String DEFAULT_EMPTY_TOKEN = "00000000000000000000000000000000";
	/** 默认key(36个0) */
	public static final String DEFAULT_EMPTY_KEY = "000000000000000000000000000000000000";
	
	/** 心跳接口命令字 */
	public static final String CMD_ID_HEARTBEAT = "9999";
	/** 后台主动推送 - 消息气泡接口命令字(主命令号和子命令号一样，长度8位),key为默认值，token为service层接受值 */
	public static final String CMD_ID_MESSAGE_BUBBLE = "9001";

	/** 通讯字符编码 */
	public static final String Socket_Character_Encoding = "UTF-8";
	/** database extract squid 分割列过滤 key:databaseType, value: 需要过滤的类型 */
	public static final Map<Integer, Set<Integer>> SPLIT_COLUMN_FILTER = new HashMap<>();

	// 线程池(消息泡)
	public static ExecutorService THREAD_POOL = new ThreadPoolExecutor(1, 10, 10000, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
//	public static ExecutorService THREAD_POOL = new ThreadPoolExecutor(10, Integer.MAX_VALUE, 10000, TimeUnit.MILLISECONDS, new SynchronousQueue<Runnable>());
	// 是否执行消息泡校验逻辑
	public static Boolean executeValidationTaskFlag = true;
	// SquidFlow打开的时候，如果消息泡的状态是true,那么不向客户端发送请求
	public static Boolean executePushMessageBubbleFlag = false;
	/*
	 *  8 bytes    +  8bytes     + 36bytes +     4 bytes   + 4 bytes +      + 32 bytes
	 *  dataLength + sequenceId + guid    +     commandId + childCommandId + token    + data
	 */
	/** 分组数据长度 */
//	public static final int  PACKET_DATA_LENGTH = 8;
	// update yi.zhou
	public static final int  PACKET_DATA_LENGTH = 24;
	// update yi.zhou
//	public static final int  PACKET_DATA_LENGTH = 4;
	/** 前台guid/key长度 */
	public static final int PACKET_KEY_LENGTH = 36;
	/** 前台token长度 32 */
	public static final int PACKET_TOKEN_LENGTH = DEFAULT_EMPTY_TOKEN.length();
	/** 包头长度 (sequenceId + guid + commandId + childCommandId + token) */
	//public static final int PACKET_HEADER_LENGTH = 84;
	public static final int PACKET_HEADER_LENGTH = 80;


	/** 用户登录成功后，记录相关信息 (key:token) */
//	public static Map<String, User> UserSession = new ConcurrentHashMap<>();
	/**所有squidflow调度日志定时任务**/
	public static Map<String,Timer> squidflowTimer=new HashMap<String,Timer>();
	/**所有squidflow调度日志定时任务**/
	public static Map<String,TaskUtils> timerTaskMap=new HashMap<String,TaskUtils>();
	/**指定的squid调试日志定时任务**/
	public static Map<String,Timer> squidTimer=new HashMap<String,Timer>();
	/**终止timer的状态**/
	public static Map<String,Boolean> stopTimer=new HashMap<String,Boolean>();
	
	/**终止调试日志的状态**/
	public static Map<String,String> scheduleMap=new HashMap<String, String>();
	public static Map<String,String> scheduleToKenMap=new HashMap<String, String>();
	public static Map<String,Integer> scheduleTimeMap=new HashMap<String, Integer>();

	public static String DEFAULT_COLUMN_NAME = "col_";

	//public static Map<String,String> tokenAndAddress = new HashMap<>();
	/**
	 * spring bean容器
	 */
	public static ApplicationContext application_Context =null;
	/** 验证token是否合法 */
	public static boolean isValidToken(String token){
		return token!=null
                &&token.trim().length()==32;
//                &&UserSession.get(token)!=null;
	}
	/**数猎场登录时，允许的超时时间(五分钟)**/
	public static final int CLOUD_TIME_OUT = 1000*60*5;
	/**数列云的时候，允许同时运行的用户为100*/
	public static final int MAX_USER = 100;
	/**
	 * 读取配置文件
	 */
	private static void refreshProperties() {
		InputStream in =null;
		try{
			File configFile  = new File(Thread.currentThread().getContextClassLoader().getResource("config/config.properties").getFile());
			System.err.println("配置文件路径:"+configFile);
			if(!configFile.exists()) return ;
			if(configFile.lastModified()==configLastModifyDate) return ;
			System.err.println("读取系统配置:"+configFile);
			in = new FileInputStream(configFile);
			prop = new Properties();
			prop.load(in);
			in.close();
			configLastModifyDate = configFile.lastModified();
			setGlobalParam(prop);
			
			for(Object key : prop.keySet()){
				System.err.println(key+"="+prop.getProperty((String) key));
			}
			
		
		}catch(Exception e){
			System.err.println("刷新属性文件失败："+e.getMessage());
		}finally{
			if(in!=null){
				try {
					in.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * 测试
	 */
	private static ThreadLocal<DBSourceState> dbConnectionState = new ThreadLocal<>();

	/**
	 * 初始化状态
	 */
	public static void initDBSourceState() {
		dbConnectionState.set(new DBSourceState());
	}

	/**
	 * check state,  连接数 - 关闭连接数
	 * @return
     */
	public static int checkDBSourceState() {
		return dbConnectionState.get().getConnectionNumber() - dbConnectionState.get().getCloseNumber();
	}

	private static DBSourceState getOrGenState() {
		DBSourceState state = dbConnectionState.get();
		if(state == null) {
			state = new DBSourceState();
			dbConnectionState.set(state);
		}
		return state;
	}
	public static void incConnectionNumber(){
		DBSourceState state = getOrGenState();
		state.setConnectionNumber(state.getConnectionNumber() + 1);
	}
	public static int getConnectionNumber(){
		return getOrGenState().getConnectionNumber();
	}

	public static int getCloseNumber(){
		return getOrGenState().getCloseNumber();
	}
	public static void incCloseNumber() {
		DBSourceState state = getOrGenState();
		state.setCloseNumber(state.getCloseNumber() + 1);
	}

	private static class DBSourceState{
		private int connectionNumber = 0;
		private int closeNumber = 0;

		public int getConnectionNumber() {
			return connectionNumber;
		}

		public void setConnectionNumber(int connectionNumber) {
			this.connectionNumber = connectionNumber;
		}

		public int getCloseNumber() {
			return closeNumber;
		}

		public void setCloseNumber(int closeNumber) {
			this.closeNumber = closeNumber;
		}
	}

	/**
	 * 设置全局变量。
	 * @param prop
	 */
	private static void setGlobalParam(Properties prop){
		
		HSQL_DB_PATH = SysConf.getValue("HsqlDB_Path");
		HSQL_SYS_DB_NAME =SysConf.getValue("HsqlDB_SysName");
		
	}
	/**
	 * 取系统属性，系统属性在系统启动时会自动读取，并每隔1秒钟刷新一次。
	 * @date 2014-2-12
	 * @author jiwei.zhang
	 * @param key 属性名。
	 * @return
	 */
	public static String getProperty(String key){
		if(prop==null){
			refreshProperties();
		}
		return prop.getProperty(key);
	}
	
    /**
     * 加入队列
     * 
     * @author bo.dang
     * @date 2014年6月5日
     */
    public static void addValidationTask(SquidValidationTask task) {
    	boolean isPush = Boolean.parseBoolean(SysConf.getValue("executeValidationTaskFlag"));
     if(executeValidationTaskFlag&&isPush){
          THREAD_POOL.execute(task);
/*          try {
			//THREAD_POOL.invokeAll(null);
		} catch (InterruptedException e) {
			
			// TODO Auto-generated catch block e.printStackTrace();
			
		}*/
      }
    }
}
