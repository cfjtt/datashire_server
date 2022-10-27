package com.eurlanda.datashire.sprint7.socket;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IDBAdapter;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.socket.MessagePacket;
import org.jboss.netty.channel.Channel;

import java.util.HashMap;
import java.util.Map;

/**
 * Socket 请求上下文
 * 
 * @date 2014-4-16
 * @author jiwei.zhang
 * 
 */
public class SocketRequestContext {
	private MessagePacket messagePacket;
	private Channel channel;
	private String token;
	private IDBAdapter repositoryAdapter;
	protected static final IRelationalDataManager systemDbAdapter = DataAdapterFactory.getDefaultDataManager();

	private static Map<Thread, SocketRequestContext> contexts = new HashMap<Thread, SocketRequestContext>();

	public SocketRequestContext(MessagePacket objectMsg) {
		messagePacket = objectMsg;
		channel = objectMsg.getChannel();
		token = new String(objectMsg.getToken(), 0, 32);
		Thread cur = Thread.currentThread();
		contexts.put(cur, this);
//		DataAdapterFactory adapterFactory = DataAdapterFactory.newInstance();
//		this.repositoryAdapter = adapterFactory.getTokenAdapter(token);
	}

	public MessagePacket getMessagePacket() {
		return messagePacket;
	}

	public void setMessagePacket(MessagePacket messagePacket) {
		this.messagePacket = messagePacket;
	}

	public Channel getChannel() {
		return channel;
	}

	public void setChannel(Channel channel) {
		this.channel = channel;
	}

	public String getToken() {
		return token;
	}

	public void setToken(String token) {
		this.token = token;
	}

	public IDBAdapter getRepositoryAdapter() {
		return repositoryAdapter;
	}

	public void setRepositoryAdapter(IDBAdapter repositoryAdapter) {
		this.repositoryAdapter = repositoryAdapter;
	}

	/**
	 * 取系统adapter.
	 * 
	 * @date 2014-4-21
	 * @author jiwei.zhang
	 * @return
	 */
	public IRelationalDataManager getSystemDbAdapter() {
		return systemDbAdapter;
	}

	/**
	 * 取得当前线程中缓存的RequestContenxt对象。
	 * 
	 * @date 2014-4-21
	 * @author jiwei.zhang
	 * @return
	 */
	public static SocketRequestContext getCurrentRequestContext() {
		Thread cur = Thread.currentThread();
		return contexts.get(cur);
	}

	/**
	 * 删除当前线程中缓存的RequestContenxt对象。
	 * 
	 * @date 2014-4-21
	 * @author jiwei.zhang
	 */
	public static void removeCurrentRequestContext() {
		Thread cur = Thread.currentThread();
		contexts.remove(cur);
	}
}
