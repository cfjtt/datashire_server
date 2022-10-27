package com.eurlanda.datashire.socket;

import org.jboss.netty.channel.Channel;

/**
 * 
 * 
 * <h3>文件名称：</h3>
 * ChannelPacket.java
 * <h3>内容摘要：</h3>
 * {@code Channel} 数据包封装，是对上行下行数据包的底层数据格式映射
 * <h3>其他说明：</h3>
 * <h4>数据格式描述：</h4>
 * <table border=1>
 * <tr>
 * <td>数据包长度</td><td colspan=5>数据包头</td><td>实际数据</td>
 * </tr>
 * <tr>
 * <td>length</td><td>sequenceId</td><td>commandId</td><td>childCommandId</td><td>optional</td><td>data</td>
 * </tr>
 * <tr>
 * <td>00 00 00 00</td><td>00 00</td><td>00 00</td><td>00 00 00 00</td><td>00</td><td>00 00 ...</td>
 * </tr>
 * <tr>
 * <td>4字节 int</td><td>2字节 short</td><td>2字节 short</td><td>4字节 int</td><td>1字节 byte</td><td>00 00 ...</td>
 * </tr>
 * <tr>
 * <td>值为数据长度+头长度，即data.length+11</td><td>数据包序列ID</td><td>针对接收到的数据包序列ID</td><td>协议ID</td><td>子命令ID</td><td>保留位</td><td>数据段</td>
 * </tr>
 * </table>
 * <h4>调用关系：</h4>
 * {@link ProtocolDecoder}, {@link ProtocolEncoder} 这两个协议编码器使用这个类进行网络封包
 * <h4>应用场景：</h4>
 * 对{@code Channel} 操作write(Object message)的时候，message需要传递这个类的实例，然后底层{@link ProtocolEncoder}才可以正确编码执行下发
 */
public class MessagePacket {
	

	/**
	 * Channel 消息通道
	 */
	private Channel channel;
	
	public Channel getChannel() {
		return channel;
	}

	public void setChannel(Channel channel) {
		this.channel = channel;
	}

	/** 编号，全局唯一 */
	private int id;
	
	private static int currPacketIndex;
	
	private synchronized static int geturrPacketID(){
		//System.out.println(currPacketIndex+"\t"+Thread.currentThread().getName());
		return currPacketIndex++;
	}
	
	/** 消息接收时间 */
	private long receiveTime;// = System.currentTimeMillis();

	/* 是否需要（发送）响应（消息） */
	//private boolean responseNeeded;
	/* 消息处理优先级 */
	//private int		priority;
	/* 消息发送成功后处理类型：丢弃、保存 等*/
	//private int		operateTypeWhileDone;
	// 消息版本号
	// 签名串
	// 压缩标志
	
	/**
	 * 数据包序列号(TODO 消息包互相独立又有业务关联的，可以优先处理序列号最小的)
	 */
	private int sequenceId; // 其实这个可以去掉
	
	/**
	 * GUID
	 */
	private byte[] guid;
	
	private int dsObjectType;
	
	/**
	 * 协议命令，格式0x0000-0xFFFF
	 */
	private String commandId;
	
	/**
	 * 子命令，某一个协议命令下，格式0x00000000-0xFFFFFFFF，按父命令分组标示
	 */
	private String childCommandId;
	
	/**
	 * token
	 */
	private byte[] token;
	
	/**
	 * 包的数量
	 */
	private int totalPacketCount; // 其实这个可以去掉
	
	/**
	 * 当前包的编号,默认为1
	 */
	private int currentPacket; // 其实这个可以去掉
	
	/**
	 * 保存数据包中的实际数据
	 */
	private byte[] data;
	
	/**
	 * 表示数据长度，即包头长度11个字节+实际数据长度
	 */
	private int length; // Integer.MAX_VALUE = 2GB 已足够
	
	/**
	 * 是否锁定 (发送消息时锁定消息，发送完成删除，或者根据定义是删除、备份还是做其他处理)
	 */
	private boolean LockFlag=false;

	/**
	 * @return lockFlag
	 */
	public boolean isLockFlag() {
		return LockFlag;
	}

	/**
	 * @param lockFlag
	 */
	public void setLockFlag(boolean lockFlag) {
		LockFlag = lockFlag;
	}
	
	/**
	 * 构造方法
	 */
	public MessagePacket(){
		//this.currentPacket = 1;
		//this.totalPacketCount = 1;
		this.receiveTime = System.currentTimeMillis();
		this.id = geturrPacketID();
	}

	/**
	 * 得到协议子命令
	 */
	public String getChildCommandId(){
		return this.childCommandId;
	}

	/**
	 * 得到协议命令
	 */
	public String getCommandId(){
		return this.commandId;
	}

	/**
	 * 得到数据
	 */
	public byte[] getData(){
		return this.data;
	}

	/**
	 * 得到数据包长度
	 */
	public int getLength(){
		return this.length;
	}

	/**
	 * 设置协议子命令
	 * 
	 * @param cid
	 */
	public void setChildCommandId(String cid){
		this.childCommandId = cid;
	}

	/**
	 * 设置协议命令
	 * 
	 * @param id
	 */
	public void setCommandId(String id){
		this.commandId = id;
	}

	/**
	 * 设置数据
	 * 
	 * @param data
	 */
	public void setData(byte[] data){
		this.data = data;
	}

	/**
	 * 设计数据长度
	 * 
	 * @param length
	 */
	public void setLength(int length){
		this.length = length;
	}

	/**
	 * @return sequenceId
	 */
	public int getSequenceId() {
		return sequenceId;
	}

	/**
	 * @param sequenceId 要设置的 sequenceId
	 */
	public void setSequenceId(int sequenceId) {
		this.sequenceId = sequenceId;
	}

	public byte[] getGuid() {
		return guid;
	}

	public void setGuid(byte[] guid) {
		this.guid = guid;
	}

	public byte[] getToken() {
		return token;
	}

	public void setToken(byte[] token) {
		this.token = token;
	}

	public int getTotalPacketCount() {
		return totalPacketCount;
	}

	public void setTotalPacketCount(int totalPacketCount) {
		this.totalPacketCount = totalPacketCount;
	}

	public int getCurrentPacket() {
		return currentPacket;
	}

	public void setCurrentPacket(int currentPacket) {
		this.currentPacket = currentPacket;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public long getReceiveTime() {
		return receiveTime;
	}

	public void setReceiveTime(long receiveTime) {
		this.receiveTime = receiveTime;
	}
	
	public int getDsObjectType() {
		return dsObjectType;
	}

	public void setDsObjectType(int dsObjectType) {
		this.dsObjectType = dsObjectType;
	}
}