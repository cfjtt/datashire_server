package com.eurlanda.datashire.socket;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.Channel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Vector;

/**
 * 
 * <p>
 * Title : 客户端连接监测
 * </p>
 * <p>
 * Description:
 * </p>
 * <p>
 * Author :何科敏 Sep 16, 2013
 * </p>
 * <p>
 * update :何科敏 Sep 16, 2013
 * </p>
 * <p>
 * Department : JAVA后端研发部
 * </p>
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司 </p>
 */
public class Heartbeat{

	/**
	 * 创建类日志对象
	 */
	static Logger logger = Logger.getLogger(Heartbeat.class);

	/**
	 * 系统扫描间隔时间，精确 单位：秒
	 */
	protected static int HEARTBEAT_SCANNING_FREQUENCY = 5000;

	/**
	 * 设置允许超过的最大心跳频率
	 */
	protected static final int MESSAGE_MAX_FREQUENCY = 5;

	public static void TimerScanning() { // 心跳只有一个
		//new HeartbeatThread("TimerScanning").start();
		// 创建定时器
		Timer timer = new Timer();
		// 制定执行任务的业务,在1秒后执行此任务,每次间隔3秒
		timer.schedule(new TimerTask() {
			@Override
			public void run() {
				//scanning();
			}
		}, 1000, HEARTBEAT_SCANNING_FREQUENCY);
	}

	/**
	 * <p>
	 * 作用描述：根据指定参数，对channel扫描检测
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 */
	private static void scanning() {
		//logger.trace("心跳开始");
		Hashtable<String, Vector<ChannelExpand>> allChannels = ChannelManager.getChannelInfo();
		// 判断channelExpands是否等于NUll
		if (!allChannels.isEmpty()) {
			// 获取远程地址
			String token = null;
			// 获取远程连接对象
			Vector<ChannelExpand> vector = null;
			// 失效的客户端连接
			List<ChannelExpand> invalidChannels = null;
			// 得到指定的key
			Enumeration<String> enumeration = allChannels.keys();
			while (enumeration.hasMoreElements()) {
				// 获取远程地址
				token = enumeration.nextElement();
				// 获取远程连接对象
				vector = allChannels.get(token);
				if (vector != null && !vector.isEmpty()) {
					invalidChannels = new ArrayList<ChannelExpand>(3);
					for (ChannelExpand channelExpand : vector) {
						channelExpand.setStopTime(channelExpand.getStopTime()+1);
						if (channelExpand.getStopTime() > MESSAGE_MAX_FREQUENCY) {
							channelExpand.getChannle().close();
							invalidChannels.add(channelExpand);
						}
					}
					if(!invalidChannels.isEmpty()){
						ChannelManager.removeQueueChannel(token, invalidChannels);
					}
				}
				
			}
		}
		//logger.trace("心跳结束");
	}
	
	public static void updateHearbeat(Channel channel) {
		//new HeartbeatThread("updateHearbeat", channel).start();
		// 这么一点事情还需要单独线程处理么？ (捡了芝麻丢了西瓜)
		// 获得所有客户端信息组
		Collection<Vector<ChannelExpand>> collection = ChannelManager.getChannelInfo().values();
		// 逐步遍历客户端信息组
		for (Vector<ChannelExpand> vector : collection) {
			Enumeration<ChannelExpand> enumeration = vector.elements();
			// 下一个remote
			while (enumeration.hasMoreElements()) {
				// 获取一个客户端信息
				ChannelExpand channelExpand = enumeration.nextElement();
				// 判断是否同一个客户端
				if (channelExpand.getChannle().getRemoteAddress() == channel.getRemoteAddress()) {
					// 设置客户端与服务端失去
					channelExpand.setStopTime(0);
					// 设置客户端与服务端进行了正常的互动
					//channelExpand.setAcceptFlag(true);
					logger.trace("keep this channel alive succeed! "+channelExpand.getChannle().getRemoteAddress());
				}
			}
		}
	}
}
