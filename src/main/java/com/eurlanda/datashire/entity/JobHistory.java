package com.eurlanda.datashire.entity;


public class JobHistory {
	private String task_id;
	private int repository_id;
	private int squid_flow_id;
	private int status;
	private String debug_squids;
	private String destination_squids;
	private String caller;
	private String create_time;
	private String update_time;
	private String repository_name;
	private String squid_flow_name;
	private String squid_name;
	private String message;
	private int log_id;
	private int job_id;
	private int num;
	
	public int getNum() {
		return num;
	}
	public void setNum(int num) {
		this.num = num;
	}
	public int getJob_id() {
		return job_id;
	}
	public void setJob_id(int job_id) {
		this.job_id = job_id;
	}
	public String getTask_id() {
		return task_id;
	}
	public void setTask_id(String task_id) {
		this.task_id = task_id;
	}
	
	public int getRepository_id() {
		return repository_id;
	}
	public void setRepository_id(int repository_id) {
		this.repository_id = repository_id;
	}
	public int getSquid_flow_id() {
		return squid_flow_id;
	}
	public void setSquid_flow_id(int squid_flow_id) {
		this.squid_flow_id = squid_flow_id;
	}
	public int getStatus() {
		return status;
	}
	public void setStatus(int status) {
		this.status = status;
	}
	public String getDebug_squids() {
		return debug_squids;
	}
	public void setDebug_squids(String debug_squids) {
		this.debug_squids = debug_squids;
	}
	public String getDestination_squids() {
		return destination_squids;
	}
	public void setDestination_squids(String destination_squids) {
		this.destination_squids = destination_squids;
	}
	public String getCaller() {
		return caller;
	}
	public void setCaller(String caller) {
		this.caller = caller;
	}
	public String getCreate_time() {
		return create_time;
	}
	public void setCreate_time(String create_time) {
		this.create_time = create_time;
	}
	public String getUpdate_time() {
		return update_time;
	}
	public void setUpdate_time(String update_time) {
		this.update_time = update_time;
	}
	public String getRepository_name() {
		return repository_name;
	}
	public void setRepository_name(String repository_name) {
		this.repository_name = repository_name;
	}
	public String getSquid_flow_name() {
		return squid_flow_name;
	}
	public void setSquid_flow_name(String squid_flow_name) {
		this.squid_flow_name = squid_flow_name;
	}
	public String getMessage() {
		return message;
	}
	public void setMessage(String message) {
		this.message = message;
	}
	public int getLog_id() {
		return log_id;
	}
	public void setLog_id(int log_id) {
		this.log_id = log_id;
	}
	public String getSquid_name() {
		return squid_name;
	}
	public void setSquid_name(String squid_name) {
		this.squid_name = squid_name;
	}
	
}
