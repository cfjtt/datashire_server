package com.eurlanda.datashire.socket;
import org.jboss.netty.channel.Channel;

/**
 * 
 * <p>
 * Title :
 * </p>
 * <p>
 * Description:
 * </p>
 * <p>
 * Author :何科敏 Sep 12, 2013
 * </p>
 * <p>
 * update :何科敏 Sep 12, 2013
 * </p>
 * <p>
 * Department : JAVA后端研发部
 * </p>
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司 </p>
 */
public class ChannelExpand {

	/**
	 * 定义token
	 */
	private String token;

	/**
	 * 失去连接次数
	 */
	private int stopTime;

	/**
	 * 是否正常互动
	 */
	//private boolean acceptFlag = false;
	
	/**
	 * 是否锁定
	 */
	private boolean LockFlag;
	
	private String lockThreadName;
	private String unLockThreadName;

	/**
	 * 通道对象
	 */
	private Channel channle;
	
	public ChannelExpand() {
	}
	
	public ChannelExpand(Channel channle) {
		this.channle = channle;
	}

	/**
	 * @return token
	 */
	public String getToken() {
		return token;
	}

	/**
	 * @param token
	 */
	public void setToken(String token) {
		this.token = token;
	}

	/**
	 * @return stopTime
	 */
	public int getStopTime() {
		return stopTime;
	}

	/**
	 * @param stopTime
	 */
	public void setStopTime(int stopTime) {
		this.stopTime = stopTime;
	}

	/**
	 * @return acceptFlag
	 */
//	public boolean isAcceptFlag() {
//		return acceptFlag;
//	}
//
//	/**
//	 * @param acceptFlag
//	 */
//	public void setAcceptFlag(boolean acceptFlag) {
//		this.acceptFlag = acceptFlag;
//	}

	/**
	 * @return channle
	 */
	public Channel getChannle() {
		return channle;
	}

	/**
	 * @param channle
	 */
	public void setChannle(Channel channle) {
		this.channle = channle;
	}

	/**
	 * @return lockFlag
	 */
	public boolean isLockFlag() {
		return LockFlag;
	}

	/**
	 * @param lockFlag
	 */
	public void setLockFlag(boolean lockFlag, String threadName) {
		LockFlag = lockFlag;
		if(LockFlag){
			lockThreadName = threadName;
		}else{
			unLockThreadName = threadName;
		}
	}
	
	public void close(){
		if(this.channle!=null){
		  this.getChannle().close();
		}
	}

	@Override
	public String toString() {
		return " Channel" + channle.getRemoteAddress() + "[ stopTime=" + stopTime
				//+ ", acceptFlag=" + acceptFlag 
				+ ", LockFlag=" + LockFlag +"("+lockThreadName+"/"+unLockThreadName
				+ "), [Channel] Bound=" + channle.isBound()
				+", Connected="+channle.isConnected()
				+", Open="+channle.isOpen()
				+", Readable="+channle.isReadable()
				+", Writable="+channle.isWritable()
				+", id="+channle.getId()+", token="+token+ "]";
	}
	
	
}
