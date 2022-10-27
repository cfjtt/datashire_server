package com.eurlanda.datashire.sprint7.socket;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.Channel;
/**
 * channel包装。
 * @date 2014-4-18
 * @author jiwei.zhang
 *
 */
public class ChannelWrapper{
	private Channel channel;
	private boolean locked= false;
	private static Logger logger = Logger.getLogger(ChannelWrapper.class);

	public ChannelWrapper() {
		super();
	}

	public ChannelWrapper(Channel channel) {
		super();
		this.channel = channel;
	}

	public Channel getChannel() {
		return channel;
	}

	public void setChannel(Channel channel) {
		this.channel = channel;
	}

	public boolean isLocked() {
		return locked;
	}

	public void setLocked(boolean locked) {
		this.locked = locked;
	}
	/**
	 * 向通道写入消息
	 * @date 2014-4-18
	 * @author jiwei.zhang
	 * @param msg 消息。
	 */
	public void writeMessage(Object msg){
		try{
			this.locked=true;
			if(this.channel!=null&&this.channel.isOpen()){
				this.channel.write(msg);
			}
		}catch (Exception e) {
			e.printStackTrace();
		}finally{
			this.locked=false;
			//logger.debug(Thread.currentThread()+",写入完成，释放写入锁。");
		}
	}

	@Override
	public String toString() {
		return "ChannelWrapper [locked=" + locked + ", channel=" + channel + "]";
	}


}
