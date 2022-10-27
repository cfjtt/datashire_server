package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;

import java.sql.Types;

/**
 * squidflow状态实体类
 * @author lei.bin
 *
 */
@MultitableMapping(pk="id", name = "DS_SYS_SQUID_FLOW_STATUS", desc = "")
public class SquidFlowStatus {
	@ColumnMpping(name="id", desc="", nullable=false, precision=0, type=Types.INTEGER, valueReg=">=1")
	private int id;
	@ColumnMpping(name="team_id", desc="", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private int team_id;
	@ColumnMpping(name="repository_id", desc="", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private int repository_id;
	@ColumnMpping(name="project_id", desc="", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private int project_id;
	@ColumnMpping(name="squid_flow_id", desc="", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private int squid_flow_id;
	@ColumnMpping(name="status", desc="0:正常状态，1：加锁，2：squidfolw被调度(创建或者暂停)", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private int squid_flow_status;
	@ColumnMpping(name="owner_client_token", desc="", nullable=true, precision=50, type=Types.VARCHAR, valueReg="")
	private String owner_client_token;
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public int getTeam_id() {
		return team_id;
	}
	public void setTeam_id(int team_id) {
		this.team_id = team_id;
	}
	public int getRepository_id() {
		return repository_id;
	}
	public void setRepository_id(int repository_id) {
		this.repository_id = repository_id;
	}
	public int getProject_id() {
		return project_id;
	}
	public void setProject_id(int project_id) {
		this.project_id = project_id;
	}
	public int getSquid_flow_id() {
		return squid_flow_id;
	}
	public void setSquid_flow_id(int squid_flow_id) {
		this.squid_flow_id = squid_flow_id;
	}
	
	public int getSquid_flow_status() {
		return squid_flow_status;
	}
	public void setSquid_flow_status(int squid_flow_status) {
		this.squid_flow_status = squid_flow_status;
	}
	public String getOwner_client_token() {
		return owner_client_token;
	}
	public void setOwner_client_token(String owner_client_token) {
		this.owner_client_token = owner_client_token;
	}

}
