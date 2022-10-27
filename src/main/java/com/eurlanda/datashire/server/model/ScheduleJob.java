package com.eurlanda.datashire.server.model;

import java.util.Date;

public class ScheduleJob {
    private Integer id;

    private Integer squid_flow_id;

    private Integer schedule_type;

    private String cron_expression;

    private Integer enable_email;

    private String email_address;

    private Integer job_status;

    private Date createTime;

    private Date updateTime;

    private String name;

    private String comment;

    private Integer repository_id;
    private Integer project_id;
    private String key;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getSquid_flow_id() {
        return squid_flow_id;
    }

    public void setSquid_flow_id(Integer squid_flow_id) {
        this.squid_flow_id = squid_flow_id;
    }

    public Integer getSchedule_type() {
        return schedule_type;
    }

    public void setSchedule_type(Integer schedule_Type) {
        this.schedule_type = schedule_Type;
    }

    public String getCron_expression() {
        return cron_expression;
    }
    public void setCron_expression(String cron_expression) {
        this.cron_expression = cron_expression;
    }

    public Integer getEnable_email() {
        return enable_email;
    }

    public void setEnable_email(Integer enable_email) {
        this.enable_email = enable_email;
    }

    public String getEmail_address() {
        return email_address;
    }

    public void setEmail_address(String email_address) {
        this.email_address = email_address;
    }

    public Integer getJob_status() {
        return job_status;
    }

    public void setJob_status(Integer job_status) {
        this.job_status = job_status;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public Integer getRepository_id() {
        return repository_id;
    }

    public void setRepository_id(Integer repository_id) {
        this.repository_id = repository_id;
    }

    public Integer getProject_id() {
        return project_id;
    }

    public void setProject_id(Integer project_id) {
        this.project_id = project_id;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }



    @Override
    public String toString() {
        return "ScheduleJob{" +
                "id=" + id +
                ", squid_flow_id=" + squid_flow_id +
                ", schedule_type=" + schedule_type +
                ", cron_expression='" + cron_expression + '\'' +
                ", enable_email=" + enable_email +
                ", email_address='" + email_address + '\'' +
                ", job_status=" + job_status +
                ", createTime=" + createTime +
                ", updateTime=" + updateTime +
                ", name='" + name + '\'' +
                ", comment='" + comment + '\'' +
                ", repository_id=" + repository_id +
                ", project_id=" + project_id +
                ", key='" + key + '\'' +
                '}';
    }
}