package com.eurlanda.datashire.socket;

import com.alibaba.fastjson.JSONObject;
import com.eurlanda.datashire.common.rpc.IEngineService;
import com.eurlanda.datashire.common.rpc.IServerService;
import com.eurlanda.datashire.common.rpc.ServerServiceImpl;
import com.eurlanda.datashire.enumeration.DataBaseType;
import com.eurlanda.datashire.enumeration.datatype.DbBaseDatatype;
import com.eurlanda.datashire.server.utils.SysUtil;
import com.eurlanda.datashire.sprint7.plug.ParameterPlug;
import com.eurlanda.datashire.sprint7.service.squidflow.UserNewProcess;
import com.eurlanda.datashire.utility.CommonConsts;
import com.eurlanda.datashire.utility.DesUtils;
import com.eurlanda.datashire.utility.Language;
import com.eurlanda.datashire.utility.LogConsumerUtils;
import com.eurlanda.datashire.utility.StringUtils;
import com.eurlanda.datashire.utility.SysConf;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.jboss.netty.channel.DefaultChannelFuture;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Log4JLoggerFactory;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 服务器启动类
 * 
 * @author Fumin
 * 
 */
public class ServerEndPoint {

	static org.slf4j.Logger logger = LoggerFactory.getLogger(ServerEndPoint.class);

	/** 主服务对象 */
	private static Server mainServer;
	private static IServerService iServerService;
	private static IEngineService iEngineService;
	private static NettyTransceiver client;
	//临时下载文件路径
	public static String ftpDownload_Path = System.getProperty("java.io.tmpdir");
//	public static String ftpDownload_Path = System.getProperty("java.io.tmpdir") + "eurlanda_temp" + File.separator;
	public static int querytimeout = 0;
	/**  */
	protected static Map<String, Object> serverParameter;

	private static void initServerParams() throws Exception{
		// 获取参数集合
		ParameterPlug plug=new ParameterPlug();
		serverParameter=plug.getServerParameter();
		logger.debug("serverParameter: "+serverParameter);
		// 服务器版本号 1.7.0版本后，版本号保存在数据库中
		if(StringUtils.isNotNull(serverParameter.get("VERSION"))){
			CommonConsts.VERSION = String.valueOf(serverParameter.get("VERSION"));
		}
		if(StringUtils.isNotNull(serverParameter.get("SPLIT_COLUMN_FILTER"))) {
			String filter = serverParameter.get("SPLIT_COLUMN_FILTER").toString();
			Set<Map.Entry<String, Object>> set = JSONObject.parseObject(filter).entrySet();
			for(Map.Entry<String, Object> en : set) {
				String[] dataTypes = en.getValue().toString().split(",");
				Set<Integer> dtSet = new HashSet<>();
				for(String str : dataTypes) {
					dtSet.add(DbBaseDatatype.parse(str.trim()).value());
				}
				CommonConsts.SPLIT_COLUMN_FILTER.put(DataBaseType.parse(en.getKey().trim()).value(), dtSet);
			}
		}

		// 超级用户密码
		if(StringUtils.isNotNull(serverParameter.get("SuperUserPwd"))){
			CommonConsts.SuperUserPwd = String.valueOf(serverParameter.get("SuperUserPwd"));
		}
		//过期时间
		if(StringUtils.isNull(serverParameter.get("LimitedTime"))) {
			throw new Exception("数据库缺少配置项,请检查后启动");
		} else {
			CommonConsts.LimitedTime=DesUtils.decryptBasedDes(String.valueOf(serverParameter.get("LimitedTime")));
		}
		//license
		if(!serverParameter.containsKey("LicenseKey"))
		{
			throw new Exception("数据库缺少配置项,请检查后启动");
		}else
		{
			if(StringUtils.isNotNull(serverParameter.get("LicenseKey"))){
				UserNewProcess process = new UserNewProcess();
				int squidCnt = SysUtil.decodeLicenseKey(String.valueOf(serverParameter.get("LicenseKey")));
				CommonConsts.MaxSquidCount = squidCnt;
			}
		}
		// 启动日志同步客户端（装载日志）
		new Thread() {

			@Override
			public void run() {
					try {
						LogConsumerUtils.consumSFLog();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						logger.debug("客户端日志同步异常",e);
					}
				logger.debug("===========启动日志客户端=========");
			}
		}.start();
		// 定时推送日志
		new Thread() {
			@Override
			public void run() {
					try {
						LogConsumerUtils.pushSchedule();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						logger.debug("定时推送日志",e);
					}
				logger.debug("===========定时推送日志=========");
			}

		}.start();
	}
	
	public static Server getServerBean(String[] args){
		//创建Server对象
		Server server=new Server();
		//设置IP
		Object ip=serverParameter.get("ip");
		server.setServerIP(String.valueOf(ip==null?"":ip));
		//设置端口
		Object port=serverParameter.get("ServerPort");
		server.setServerPort(port==null?9000:Integer.parseInt(String.valueOf(port)));
		//server.setServerPort(9090);
		//设置语言
		Object language=serverParameter.get("language");
		server.setLanguage(language==null?Language.zh_CN:Language.zh_CN);
		//设置数据编码
		//Object encoding=serverParameter.get("encoding");
		//server.setDataEncoding(encoding==null?"utf-8":String.valueOf(encoding));
		//设置输入缓冲大小
		Object input=serverParameter.get("input");
		server.setInputBufferSize(input==null?9999:Integer.parseInt(String.valueOf(input)));
		//设置输出缓冲大小
		Object output=serverParameter.get("output");
		server.setOutputBufferSize(output==null?9999:Integer.parseInt(String.valueOf(output)));
		
		//初始化查询过期时间
		querytimeout = Integer.valueOf(SysConf.getValue("sql.queryTimeout"));
		// 运行时指定参数（配置参数优先级： 运行时指定 > 参数表配置 > 程序默认值）
		if(args!=null){
			if(args.length>=1){ // socket 端口号
				if(StringUtils.isNotNull(args[0])){
					server.setServerPort(Integer.parseInt(args[0].trim()));
				}
			}
		}
		return server;

	}
	
	/**
	 * 设置服务对象
	 * */
	public static void setMainServer(Server s) {
		logger.trace("setMainServer");
		mainServer = s;
		AgreementProtocol.instantHandler();
		AgreementProtocol.instantProtocol();
		mainServer.setProtocols(AgreementProtocol.AGREEMENT_PROTOCOL);
	}

	/**
	 * 得到服务对象
	 * */
	public static Server getMainServer() {
		return mainServer;
	}

	public static void initRPCServer(){
		try {
			Integer port = Integer.valueOf(SysConf.getValue("rpc.self.port"));
			// 创建一个远程对象
			DefaultChannelFuture.setUseDeadLockChecker(false);
			iServerService = new ServerServiceImpl();
			SpecificResponder reponse = new SpecificResponder(IServerService.class, iServerService);
			new NettyServer(reponse, new InetSocketAddress(port));
			logger.info("RPC service started，listen on port:" + port);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static IEngineService getEngineService() throws NumberFormatException, IOException {
		if (isEngineNotConnected()) {
			synchronized (ServerEndPoint.class){
				if (isEngineNotConnected()) {
					logger.debug("初始化engine连接");
					DefaultChannelFuture.setUseDeadLockChecker(false);
					client = new NettyTransceiver(new InetSocketAddress(SysConf.getValue("rpc.engine.addr"), Integer.parseInt(SysConf.getValue("rpc.engine.port"))));
					iEngineService = SpecificRequestor.getClient(IEngineService.class, client);
				}
//						iEngineService = SpecificRequestor.getClient(IEngineService.class, client);
			}
		}
		return iEngineService;
	}
	
	public static boolean isEngineNotConnected(){
		if(iEngineService == null) {
            logger.debug("iEngineService 未初始化..");
        }
        if(client==null) {
        	logger.debug("client==null");
        }else if(!client.isConnected()) {
        	logger.debug("!client.isConnected()");
        }
        return iEngineService == null;
	}
	
	/**
	 * main入口
	 * */
	public static void main(String[] args) throws IOException,Exception { 
		final long startTime = System.currentTimeMillis();
		logger.info("datashire server start up!");
		// 启动spring
		ApplicationContext context = new ClassPathXmlApplicationContext(new String[] {"config/applicationContext.xml"});
		CommonConsts.application_Context=context;
		// 1. 初始化系统配置参数
		initServerParams();
		// 设置RPC接口的的端口绑定
		initRPCServer();
		//getEngineService();
		// 2. 设置server运行时参数
		ServerEndPoint.setMainServer(getServerBean(args));
		// 3. 指定netty日志处理器
		InternalLoggerFactory.setDefaultFactory(new Log4JLoggerFactory());
		// 4. 启动server-socket（netty）
		ServerEndPoint.getMainServer().Start();
		// 5. 启动server-socket 心跳 (keep TCP alive)
		Heartbeat.TimerScanning();
		long endTime = System.currentTimeMillis();
		logger.info("datashire server start up sucessfully in "+(endTime-startTime)+" ms.");
	}
}
