package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;

import java.sql.Types;

@MultitableMapping(pk="id", name = "DS_SYS_JOB_SCHEDULE", desc = "")
public class JobSchedule {
	@ColumnMpping(name="id", desc="", nullable=false, precision=0, type=Types.INTEGER, valueReg=">=1")
   private int id;
	@ColumnMpping(name="team_id", desc="", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
   private int team_id;
	@ColumnMpping(name="repository_id", desc="", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
   private int repository_id;
	@ColumnMpping(name="project_id", desc="", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
   private int project_id;
	@ColumnMpping(name="project_name", desc="", nullable=true, precision=100, type=Types.VARCHAR, valueReg="")
   private String project_name;
	@ColumnMpping(name="squid_flow_id", desc="", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
   private int squid_flow_id;
	@ColumnMpping(name="squid_flow_name", desc="", nullable=true, precision=100, type=Types.VARCHAR, valueReg="")
   private String squid_flow_name;
	@ColumnMpping(name="schedule_type", desc="", nullable=true, precision=50, type=Types.VARCHAR, valueReg="")
   private String schedule_type;
	@ColumnMpping(name="schedule_begin_date", desc="", nullable=true, precision=0, type=Types.TIMESTAMP, valueReg="")
   private String schedule_begin_date;
	@ColumnMpping(name="schedule_end_date", desc="", nullable=true, precision=0, type=Types.TIMESTAMP, valueReg="")
   private String schedule_end_date;
	@ColumnMpping(name="schedule_valid", desc="", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
   private int schedule_valid;
	@ColumnMpping(name="day_dely", desc="", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
   private int day_dely;
	@ColumnMpping(name="day_run_count", desc="", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
   private int day_run_count;
	@ColumnMpping(name="day_begin_date", desc="", nullable=true, precision=0, type=Types.TIME, valueReg="")
   private String day_begin_date;
	@ColumnMpping(name="day_end_date", desc="", nullable=true, precision=0, type=Types.TIME, valueReg="")
   private String day_end_date;
	@ColumnMpping(name="day_run_dely", desc="", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
   private int day_run_dely;
	@ColumnMpping(name="week_day", desc="", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
   private int week_day;
	@ColumnMpping(name="week_begin_date", desc="", nullable=true, precision=0, type=Types.TIME, valueReg="")
   private String week_begin_date;
	@ColumnMpping(name="month_day", desc="", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
   private int month_day;
	@ColumnMpping(name="month_begin_date", desc="", nullable=true, precision=0, type=Types.TIME, valueReg="")
   private String month_begin_date;
	@ColumnMpping(name="last_scheduled_date", desc="", nullable=true, precision=0, type=Types.TIMESTAMP, valueReg="")
   private String last_scheduled_date;
	@ColumnMpping(name="creation_date", desc="", nullable=true, precision=0, type=Types.TIMESTAMP, valueReg="")
   private String creation_date;
	@ColumnMpping(name="status", desc="", nullable=true, precision=1, type=Types.VARCHAR, valueReg="")
   private boolean job_status;//暂停false,正常true
	@ColumnMpping(name="object_type", desc="", nullable=true, precision=1, type=Types.INTEGER, valueReg="")
	private int object_type;
	@ColumnMpping(name="squid_id", desc="", nullable=true, precision=1, type=Types.INTEGER, valueReg="")
	private int squid_id;
	@ColumnMpping(name="day_once_off_time", desc="", nullable=true, precision=1, type=Types.TIME, valueReg="")
	private String day_once_off_time;
	@ColumnMpping(name="enable_email_sending", desc="是否发送邮件", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private int enable_email_sending;
	@ColumnMpping(name="email_address", desc="邮件地址", nullable=true, precision=500, type=Types.VARCHAR, valueReg="")
	private String email_address;
	
	public int getEnable_email_sending() {
		return enable_email_sending;
	}
	public void setEnable_email_sending(int enable_email_sending) {
		this.enable_email_sending = enable_email_sending;
	}
	public String getEmail_address() {
		return email_address;
	}
	public void setEmail_address(String email_address) {
		this.email_address = email_address;
	}
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
	public String getProject_name() {
		return project_name;
	}
	public void setProject_name(String project_name) {
		this.project_name = project_name;
	}
	public int getSquid_flow_id() {
		return squid_flow_id;
	}
	public void setSquid_flow_id(int squid_flow_id) {
		this.squid_flow_id = squid_flow_id;
	}
	public String getSquid_flow_name() {
		return squid_flow_name;
	}
	public void setSquid_flow_name(String squid_flow_name) {
		this.squid_flow_name = squid_flow_name;
	}
	public String getSchedule_type() {
		return schedule_type;
	}
	public void setSchedule_type(String schedule_type) {
		this.schedule_type = schedule_type;
	}
	public String getSchedule_begin_date() {
		return schedule_begin_date;
	}
	public void setSchedule_begin_date(String schedule_begin_date) {
		this.schedule_begin_date = schedule_begin_date;
	}
	public String getSchedule_end_date() {
		return schedule_end_date;
	}
	public void setSchedule_end_date(String schedule_end_date) {
		this.schedule_end_date = schedule_end_date;
	}
	public int getSchedule_valid() {
		return schedule_valid;
	}
	public void setSchedule_valid(int schedule_valid) {
		this.schedule_valid = schedule_valid;
	}
	public int getDay_dely() {
		return day_dely;
	}
	public void setDay_dely(int day_dely) {
		this.day_dely = day_dely;
	}
	public int getDay_run_count() {
		return day_run_count;
	}
	public void setDay_run_count(int day_run_count) {
		this.day_run_count = day_run_count;
	}
	public String getDay_begin_date() {
		return day_begin_date;
	}
	public void setDay_begin_date(String day_begin_date) {
		this.day_begin_date = day_begin_date;
	}
	public String getDay_end_date() {
		return day_end_date;
	}
	public void setDay_end_date(String day_end_date) {
		this.day_end_date = day_end_date;
	}
	public int getDay_run_dely() {
		return day_run_dely;
	}
	public void setDay_run_dely(int day_run_dely) {
		this.day_run_dely = day_run_dely;
	}
	public int getWeek_day() {
		return week_day;
	}
	public void setWeek_day(int week_day) {
		this.week_day = week_day;
	}
	public String getWeek_begin_date() {
		return week_begin_date;
	}
	public void setWeek_begin_date(String week_begin_date) {
		this.week_begin_date = week_begin_date;
	}
	public int getMonth_day() {
		return month_day;
	}
	public void setMonth_day(int month_day) {
		this.month_day = month_day;
	}
	public String getMonth_begin_date() {
		return month_begin_date;
	}
	public void setMonth_begin_date(String month_begin_date) {
		this.month_begin_date = month_begin_date;
	}
	public String getLast_scheduled_date() {
		return last_scheduled_date;
	}
	public void setLast_scheduled_date(String last_scheduled_date) {
		this.last_scheduled_date = last_scheduled_date;
	}
	public String getCreation_date() {
		return creation_date;
	}
	public void setCreation_date(String creation_date) {
		this.creation_date = creation_date;
	}
	public boolean isJob_status() {
		return job_status;
	}
	public void setJob_status(boolean job_status) {
		this.job_status = job_status;
	}
    public int getObject_type() {
        return object_type;
    }
    public void setObject_type(int object_type) {
        this.object_type = object_type;
    }
    public int getSquid_id() {
        return squid_id;
    }
    public void setSquid_id(int squid_id) {
        this.squid_id = squid_id;
    }
    public String getDay_once_off_time() {
        return day_once_off_time;
    }
    public void setDay_once_off_time(String day_once_off_time) {
        this.day_once_off_time = day_once_off_time;
    }

	
}
