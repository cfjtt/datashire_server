package com.eurlanda.datashire.socket;

import com.eurlanda.datashire.socket.protocol.Protocol;
import com.eurlanda.datashire.socket.protocol.ProtocolCallPacket;
import com.eurlanda.datashire.utility.Language;
import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

public class Server {

	static Logger logger = Logger.getLogger(Server.class);

	/**
	 * 通过Spring注入 协议列表，int表示协议命令标示，Protocol表示协议接口对
	 */
	private Map<String, Protocol> protocols;

	/**
	 * 协议数据池，key为token,值为Socket链接队列
	 */
	private static Map<String, List<ProtocolCallPacket>> protocolCallPacketPool;

	/**
	 * 主服务IP
	 * */
	private String serverIP;

	/**
	 * 语言
	 */
	private Language language;

	/**
	 * 服务端口
	 * */
	private int serverPort;

	/**
	 * 数据包种子，最大65535
	 * */
	private short sequenceSeed = 0;

	/**
	 * 系统目录
	 * */
	private String systemDir;

	/**
	 * BufferdOutputStream buffer size
	 * */
	private int outputBufferSize;

	/**
	 * BufferdInputStream buffer size
	 * */
	private int inputBufferSize;

	/**
	 * 协议数据编码
	 */
	//private String dataEncoding;

	/**
	 * 得到系统目录
	 * */
	public String getSystemDir() {
		logger.trace("getSystemDir");
		return systemDir;
	}

	/**
	 * 设置系统目录
	 * 
	 * @param path
	 *            路径
	 * */
	public void setSystemDir(String path) {
		logger.debug("setSystemDir start");
		logger.debug("source code:" + path);
		systemDir = path;
		logger.debug("setSystemDir end");
	}

	/**
	 * 得到输出缓冲区
	 * */
	public int getOutputBufferSize() {
		logger.trace("getOutputBufferSize");
		return this.outputBufferSize;
	}

	/**
	 * 设置输出缓冲区
	 * 
	 * @param size
	 *            大小
	 * */
	public void setOutputBufferSize(int size) {
		logger.debug("setOutputBufferSize start");
		logger.debug("source code:" + size);
		this.outputBufferSize = size;
		logger.debug("setOutputBufferSize end");
	}

	/**
	 * 得到输入缓冲区
	 * */
	public int getInputBufferSize() {
		logger.trace("getInputBufferSize");
		return this.inputBufferSize;
	}

	/**
	 * 设置输入缓冲区
	 * 
	 * @param size
	 *            大小
	 * */
	public void setInputBufferSize(int size) {
		logger.debug("setInputBufferSize start");
		logger.debug("source code:" + size);
		this.inputBufferSize = size;
		logger.debug("setInputBufferSize end");
	}

	/**
	 * 获得字符串编码方式（针对协议数据段）
	 * */
	//public String getDataEncoding() {
	//	logger.trace("getDataEncoding");
	//	return this.dataEncoding;
	//}

	/**
	 * 设置字符串编码方式（针对协议数据段）
	 * */
	//public void setDataEncoding(String e) {
	//	logger.debug("setDataEncoding start");
	//	logger.debug("source code:" + e);
	//	this.dataEncoding = e;
	//	logger.debug("setDataEncoding end");
	//}

	/**
	 * 构造函数
	 * */
	public Server() {
		logger.trace("Server");
		if (systemDir == "") {
			setSystemDir(System.getProperty("user.dir"));
		}
		if (outputBufferSize == 0) {
			setOutputBufferSize(1024);
		}

	}

	/**
	 * 程序入口方法，内部进行Socket服务初始化，加载Channel工厂和管道工厂，配置BOSS和Worker线程池
	 * (服务端启动代码)
	 */
	public void Start() {
		logger.debug("Start method start");
		// Configure the Server.
		//启动和任务处理流程
		ServerBootstrap bootstrap = new ServerBootstrap(
				//多线程处理机制
				new NioServerSocketChannelFactory(
						Executors.newCachedThreadPool(),
						Executors.newCachedThreadPool()));
		//传给ChannelFactory工厂处理
		bootstrap.setOption("child.tcpNoDelay", true);
		bootstrap.setOption("child.keepAlive", true);
		//bootstrap.setOption("reuseAddress", true);
		//bootstrap.setOption("child.linger", 60);
		//bootstrap.setOption("child.TIMEOUT", 120);
		bootstrap.setPipelineFactory(new ServerPipelineFactory());
		bootstrap.bind(new InetSocketAddress(this.serverPort));
		try {
			logger.info("bind at address = "+InetAddress.getLocalHost()+"\t(local access address: 127.0.0.1 or localhost.), port = "+this.serverPort);
		} catch (UnknownHostException e) {
			logger.warn("get remote ip error.(see also cmd.exe ipconfig/all)\t(local access address: 127.0.0.1 or localhost.), port = "+this.serverPort);
		}
	}

	/**
	 * 设置协议列表
	 * */
	public void setProtocols(Map<String, Protocol> protocols) {
		logger.debug("setProtocols start");
		logger.debug("source code:" + protocols);
		this.protocols = protocols;
		logger.debug("setProtocols end");
	}

	/**
	 * 获得协议列表
	 * */
	public Map<String, Protocol> getProtocols() {
		return this.protocols;
	}

	/**
	 * 根据协议id，查找某个协议
	 * 
	 * @param id
	 *            协议ID
	 * */
	public Protocol findProtocol(String id) {
		return protocols.get(id);
	}

	/**
	 * 得到服务器IP
	 * */
	public String getServerIP() {
		logger.trace("getServerIP");
		return this.serverIP;
	}

	/**
	 * 设置服务器IP
	 * */
	public void setServerIP(String ip) {
		logger.debug("SetServerIP "+this.serverIP+"\t"+ip);
//		logger.debug("setServerIP start");
//		logger.debug("source code:" + ip);
//		if (StringUtils.isNull(this.serverIP)) {
//			logger.debug("serverIP is not be set, try to auto get....");
//			ip = ServerEndPoint.getLocalIP();
//			logger.debug(String.format(
//					"serverIP that just try to auto get is %s", ip));
//		}
		this.serverIP = ip;
		//logger.debug("setServerIP end");
	}

	/**
	 * 设置服务端口
	 * */
	public void setServerPort(int port) {
		logger.debug("setServerPort start");
		logger.debug("source code:" + port);
		this.serverPort = port;
		logger.debug("setServerPort end");
	}

	/**
	 * 获得服务端口
	 * */
	public int getServerPort() {
		logger.trace("getServerPort");
		return this.serverPort;
	}

	/**
	 * 得到下一个数据包序号
	 * */
	public short getNextSequence() {
		logger.trace("getNextSequence");
		if (this.sequenceSeed >= 0xFFFF) {
			this.sequenceSeed = 0;
		}
		this.sequenceSeed++;
		logger.trace("nextSequence:" + this.sequenceSeed);
		return this.sequenceSeed;
	}

	/**
	 * 得到当前数据包序号
	 * */
	public short getCurrentSequence() {
		logger.trace("getCurrentSequence");
		return this.sequenceSeed;
	}

	public static Map<String, List<ProtocolCallPacket>> getProtocolCallPacketPool() {
		return protocolCallPacketPool;
	}

	public static void setProtocolCallPacketPool(
			Map<String, List<ProtocolCallPacket>> protocolCallPacketPool) {
		Server.protocolCallPacketPool = protocolCallPacketPool;
	}

	public Language getLanguage() {
		return language;
	}

	public void setLanguage(Language language) {
		this.language = language;
	}
}