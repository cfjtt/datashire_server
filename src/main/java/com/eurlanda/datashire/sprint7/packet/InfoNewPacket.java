package com.eurlanda.datashire.sprint7.packet;



public class InfoNewPacket {

	private int id;// 主键
	private int repositoryId;//仓库Id
	private int squidFlowId;//squidflowId
	private int type;

	public InfoNewPacket(int id, int type) {
		super();
		this.id = id;
		this.type = type;

	}

	/**
	 * 构造器
	 */
	public InfoNewPacket() {

	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public int getRepositoryId() {
		return repositoryId;
	}

	public void setRepositoryId(int repositoryId) {
		this.repositoryId = repositoryId;
	}

	public int getSquidFlowId() {
		return squidFlowId;
	}

	public void setSquidFlowId(int squidFlowId) {
		this.squidFlowId = squidFlowId;
	}

}
