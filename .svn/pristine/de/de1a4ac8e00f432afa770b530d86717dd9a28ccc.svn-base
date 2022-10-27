package com.eurlanda.datashire.socket.protocol;

import com.eurlanda.datashire.server.utils.TokenUtil;
import com.eurlanda.datashire.socket.Agreement;
import com.eurlanda.datashire.socket.AgreementProtocol;
import com.eurlanda.datashire.socket.MessagePacket;
import com.eurlanda.datashire.socket.MessageQueueManager;
import com.eurlanda.datashire.socket.ProtocolDecoder;
import com.eurlanda.datashire.utility.CommonConsts;
import com.eurlanda.datashire.utility.DebugUtil;
import com.eurlanda.datashire.utility.StringUtils;
import net.sf.json.JSONObject;
import net.sf.json.JsonConfig;
import org.apache.log4j.Logger;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * <h3>文件名称：</h3>
 * ProtocolThreadPoolTask.java
 * <h3>内容摘要：</h3>
 * 协议执行的线程任务对象，用于提交协议的异步请求，并返回结果后，触发回调
 * <h3>其他说明：</h3>
 * <h4>调用关系：</h4>
 * 被线程池调用的可运行对象，实现自 {@link Runnable} 接口，调用者为 {@code Server#threadPool} 
 * <h4>应用场景：</h4>
 * 处理具体协议任务处理，运行在线程池环境
 */
public class ProtocolThreadPoolTask{

	private static Logger logger = Logger.getLogger(ProtocolThreadPoolTask.class);

	private static final JsonConfig JSON_CONFIG = new JsonConfig();
	
	/**
	 * 协议对象
	 */
	private AbstractProtocol protocol;
	
	/**
	 * 协议回调对象
	 */
	private ProtocolCallback protocolCallback;

	/**
	 * 构造方法
	 * @param packet    上行协议数据包
	 * @param protocol    上行协议
	 * @param protocolCallback    协议回调对象
	 */
	public ProtocolThreadPoolTask(AbstractProtocol protocol, ProtocolCallback protocolCallback){
		//super("ServiceThread-"+protocolCallback.getChannelPacket().getId());
		this.protocol = protocol;
		this.protocolCallback = protocolCallback;
	}
	
	/**
	 * 设置Protocol
	 * @param protocol 协议
	 * */
	public void setAbstractProtocol(AbstractProtocol protocol){
		this.protocol = protocol;
	}
	
	/**
	 * 获得当前Protocol
	 * */
	public AbstractProtocol getAbstractProtocol(){
		return this.protocol;
	}
	
	/**
	 * 设置ProtocolCallback
	 * @param callback 回调对象
	 * */
	public void setProtocolCallback(ProtocolCallback callback){
		this.protocolCallback = callback;
	}
	
	/**
	 * 获得ProtocolCallback
	 * */
	public ProtocolCallback getProtocolCallback(){
		return this.protocolCallback;
	}
	
	/**
	 * 实现接口run方法，内部执行协议具体请求
	 */
	public void start(){
		logger.trace("service invoke run start");
		//得到Listner关联的上行数据包
		MessagePacket packet = protocolCallback.getChannelPacket();
		/*
		 *  获取处理类 
		 *  zjweii:此处未考虑多线程，存在重大BUG，会导致客户端消息发送错位a->b。
		 */
		Object service = AgreementProtocol.AGREEMENT_HANDLER.get(packet.getCommandId());
		// 获取对应参数定义
		ServiceConf serviceConf = Agreement.ServiceConfMap.get(packet.getCommandId().concat(packet.getChildCommandId()));
		/*
		 *  修复方法。
		 */
		try {
			
			String beanName=service.getClass().getSimpleName();
			String firstName = beanName.substring(0,1).toLowerCase();
			beanName=beanName.replaceFirst("^\\w", firstName);
			//service = service.getClass().newInstance();
			//serviceConf = serviceConf.getClass().newInstance();
			CommonConsts.application_Context.getBean(beanName);
		} catch (Exception e1) {
			throw new RuntimeException("未找到spring bean");
		}
		
		
		Class<?> cls=null;
		Method meth = null;
		Object retobj = null;
		
		try {
			cls=service.getClass();
			
			// 传说中的依赖注入。。。(可以考虑统一配置，权限部分不需要token和key)
			Field field=cls.getDeclaredField("token");
			field.setAccessible(true);
			field.set(service, StringUtils.bytes2Str(packet.getToken()));
			field=cls.getDeclaredField("key");
			field.setAccessible(true);
			field.set(service, StringUtils.bytes2Str(packet.getGuid()));

			// 将token 和 key 保存至 ThreadLocal中
			TokenUtil.setTokenAndKey(StringUtils.bytes2Str(packet.getToken()), StringUtils.bytes2Str(packet.getGuid()));

			//入参类型为空的情况
			if(Void.TYPE.equals(serviceConf.getArgType()))
			{
				meth = cls.getMethod(serviceConf.getMethodName());
			}else
			{
				meth = cls.getMethod(serviceConf.getMethodName(), serviceConf.getArgType());
			}

			// 消息包内容
			String info = JSONObject.fromObject(StringUtils.bytes2Str(packet.getData())).getString("info");
			if(DebugUtil.isDebugenabled()) synchronized (this) {totalActive++;}
		
			long s = System.currentTimeMillis();
			if (Void.TYPE.equals(serviceConf.getArgType())) {
				retobj = meth.invoke(service);
			} else {
				retobj = meth.invoke(service, info);
			}
			long e = System.currentTimeMillis();
			
			if(serviceConf.isReplaceKey()){ // 替换包头
				packet.setGuid(StringUtils.str2Bytes(field.get(service).toString()));
			}
			
			if(DebugUtil.isDebugenabled()){
				synchronized (this) {
					totalSucceed++;
					if(totalActive-totalSucceed>maxActive){
						maxActive=totalActive-totalSucceed;
					}
				}
				p(packet.getCommandId().concat(packet.getChildCommandId()).concat(meth.getName()), (int)(e-s));
			}
		} catch (Exception e) {
			retobj = "{\"code\":-9999,\"desc\":\""+e+"\"}";
			logger.error(MessageFormat.format("{0}.{1} service error!", cls==null?null:cls.getSimpleName(), meth==null?null:meth.getName()), e);
		}
		
		// 不需要返回消息的部分接口 (接口参数统一配置，可参考相关文档)
		
		// zjweii ： 不需要
		if(serviceConf!=null && serviceConf.isSendResponse()){
			packet.setData(StringUtils.str2Bytes(JSONObject.fromObject(
					retobj==null?"":retobj.toString(), JSON_CONFIG).toString()));
			this.protocol.endInvoke(this.protocolCallback);
		}
		logger.trace("run end");
		// 将token 和 key 从 ThreadLocal中 移除
		TokenUtil.removeTokenAndKey();
	}

	///////////// add by ludang at 2013.10.13 (test code only in dev-mode) ///////////////////
	private static int totalActive;//业务调用总数
	private static int totalSucceed;//成功总数
	private static int maxActive=-1;//最大在线业务数（某时刻未完成的）
	private static int conTotalUsedCount;//数据库连接累计使用总数
	private static int conTotalUnClosedCount;//数据库连接累计未关闭总数
	
	private static Map<String, List<Integer>> m = new TreeMap<String, List<Integer>>();
	private void p(String cmd, int timeUsed){
		// 业务监控
		logger.debug("[业务计数]\t"+cmd
				+"\t最大活动业务数："+maxActive+"\t成功："+totalSucceed+"\t总数："+totalActive);
		List<Integer> timeUsedList = m.get(cmd);
		if(timeUsedList!=null){
			timeUsedList.add(timeUsed);
		}else{
			timeUsedList = new ArrayList<Integer>();
			timeUsedList.add(timeUsed);
			m.put(cmd, timeUsedList);
		}
		Collections.sort(timeUsedList);
		logger.debug("[方法用时]\t"+cmd+"\t耗时："+timeUsed+"\t执行次数："+timeUsedList.size()
				+"\t"+Arrays.toString(getMinMaxSumAvg(timeUsedList))+"\t"+StringUtils.list2String(timeUsedList, 20));
		
		// 连接池监控
		String tn = Thread.currentThread().getName();
		List<Connection> l = MonitorConsole.connectionMap.get(tn);
		if(l!=null){
			int connectionOpened = l.size();
			int unClosedCount = 0;
			//String stackTrace = "";
			try {
				for(int i=0; i<connectionOpened; i++){
					Connection c = l.get(i);
					if(c!=null && !c.isClosed()){
						unClosedCount++;
						//int hc = c.hashCode();
						//stackTrace += DataSourceManager.conMap.get(hc)+"\r\n";
						c.close();
						//DataSourceManager.conMap.remove(hc);
					}
				}
			} catch (SQLException e) {
				logger.error("this is a mistake.", e);
			}
			if(unClosedCount!=0){
				//stackTrace = "\t未关闭连接调用堆栈：\r\n"+stackTrace.trim();
				conTotalUnClosedCount+=unClosedCount;
			}
			conTotalUsedCount+=connectionOpened;
			MonitorConsole.connectionMap.remove(tn);
			logger.debug("[连接池使用情况]\t"+cmd
					+"\t本次使用："+ connectionOpened+"\t累计使用："+conTotalUsedCount+
					"\t本次未关闭："+unClosedCount+"\t累计未关闭："+conTotalUnClosedCount);
		}
	}
	
	/** 系统运行情况（开发模式）*/
	public static void sysRunStatus(){
		StringBuilder s = new StringBuilder(500);
		s.append("\r\n************************************* system running status at "+new SimpleDateFormat("yyyy.MM.dd HH.mm.ss").format(new Date())+" *****************************************************************");
		s.append("\r\n[连接池] 活动="+MonitorConsole.activePoolCnt+
				"， 空闲="+MonitorConsole.idlePoolCnt+
				"， Max(活动)="+MonitorConsole.maxActivePoolCnt+
				"， Sum(活动)="+MonitorConsole.sumActivePoolCnt+
				"， Sum(未关闭)="+conTotalUnClosedCount);
		
		s.append("\r\n[消息池]\t消息最大积压数："+MessageQueueManager.maxQueueLength
				+"\t进栈总数："+MessageQueueManager.totalIn+"\t出栈总数："+MessageQueueManager.totalOut+"\t发送失败："+(MessageQueueManager.totalOut-MessageQueueManager.totalIn));
		
		s.append("\r\n[业务数]\t最大活动业务数："+maxActive+"\t成功业务："+totalSucceed+"\t调用总数："+totalActive+"\t调用失败："+(totalActive-totalSucceed));
		int[] a;
		Iterator<String> it=m.keySet().iterator();
		s.append("\r\n\r\n命令号\t\t调用次数\t进栈总数\t出栈总数\tMin\tMax\tSum\tAvg\tService Name");
		while(it.hasNext()){
			String cmd = it.next();
			List<Integer> list = m.get(cmd);
			a = getMinMaxSumAvg(list);
			s.append("\r\n"+cmd.substring(0,8)
					+"\t"+list.size()
					+"\t"+ProtocolDecoder.MsgInLen.get(cmd.substring(0,8)).size()
					+"\t"+(MessageQueueManager.MsgOutLen.get(cmd.substring(0,8))==null?-1:MessageQueueManager.MsgOutLen.get(cmd.substring(0,8)).size())
					+"\t"+a[0] +"\t"+a[1] +"\t"+a[2] +"\t"+a[3]
					+"\t"+cmd.substring(8,cmd.length())
					);
		}
		
		s.append("\r\n\r\n命令号\t\t消息进栈长度统计(MinMaxSumAvg)\t\t消息出栈长度统计(MinMaxSumAvg)");
		it=m.keySet().iterator();
		while(it.hasNext()){
			String cmd = it.next();
			s.append("\r\n"+cmd.substring(0,8));
			List<Integer> list = ProtocolDecoder.MsgInLen.get(cmd.substring(0,8));
			if(list!=null && !list.isEmpty()){
				a = getMinMaxSumAvg(list);
				s.append("\t"+a[0] +"\t"+a[1] +"\t"+a[2] +"\t"+StringUtils.byte2String(a[3]) );
			}
			
			list = MessageQueueManager.MsgOutLen.get(cmd.substring(0,8));
			if(list!=null && !list.isEmpty()){
				a = getMinMaxSumAvg(list);
				s.append("\t\t"+a[0] +"\t"+a[1] +"\t"+a[2] +"\t"+StringUtils.byte2String(a[3]) );
			}
			s.append("\t"+cmd.substring(8,cmd.length()));
		}
		s.append("\r\n***************************************************************************************************************************************************\r\n");
		logger.debug(s.toString());
	}
	
	// 过滤部分接口入参、出参数据太多情况（应该要考虑系统间接口只在必要时调用，且只传递必要参数）
	@Deprecated static Set<String> retFilterSet = new TreeSet<String>();
	@Deprecated static Set<String> paramFilterSet = new TreeSet<String>();
	static{
		paramFilterSet.add("00000001");
		paramFilterSet.add("00010016");
		paramFilterSet.add("00010016");
		paramFilterSet.add("00010020");
		paramFilterSet.add("00010020");
		paramFilterSet.add("00010022");
		paramFilterSet.add("00010022");
		paramFilterSet.add("00010023");
		paramFilterSet.add("00010023");
		paramFilterSet.add("00010039");
		paramFilterSet.add("00010039");
		paramFilterSet.add("00010040");
		paramFilterSet.add("00010040");
		paramFilterSet.add("00010041");
		paramFilterSet.add("00010041");
		paramFilterSet.add("00010045");
		paramFilterSet.add("00010045");
		paramFilterSet.add("00010104");
		paramFilterSet.add("00050009");

		retFilterSet.add("00000001");
		retFilterSet.add("00000001");
		retFilterSet.add("00010029");
		retFilterSet.add("00010029");
		retFilterSet.add("00010039");
		retFilterSet.add("00010039");
		retFilterSet.add("00010041");
		retFilterSet.add("00010041");
		retFilterSet.add("00010046");
		retFilterSet.add("00010046");
		retFilterSet.add("00010047");
		retFilterSet.add("00010047");
		retFilterSet.add("00010102");
		retFilterSet.add("00010102");
		retFilterSet.add("00010104");
		retFilterSet.add("00050001");
		retFilterSet.add("00050001");
		retFilterSet.add("00050010");
	}

	@Deprecated
	private final static int[] MathUtilAry = new int[4]; // MinMaxSumAvg
	public static int[] getMinMaxSumAvg(List<Integer> list){
		//min
		MathUtilAry[0]=Integer.MAX_VALUE;
		//max
		MathUtilAry[1]=Integer.MIN_VALUE;
		//sum
		MathUtilAry[2]=0;
		//avg
		MathUtilAry[3]=0;
		int tmp=0;
		for(int i=0; i<list.size(); i++){
			tmp = list.get(i);
			if(tmp>=MathUtilAry[1])MathUtilAry[1]=tmp;
			if(tmp<=MathUtilAry[0])MathUtilAry[0]=tmp;
			MathUtilAry[2] +=tmp; 
		}
		MathUtilAry[3]=MathUtilAry[2]/list.size();
		return MathUtilAry;
	}
	
}