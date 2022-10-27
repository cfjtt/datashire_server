package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;

import java.sql.Types;
import java.util.Date;

/**
 * Created by yhc on 3/8/2016.
 */
@MultitableMapping(pk="ID", name = "DS_SYS_APPLICATION_STATUS", desc = "")
public class ApplicationStatus extends DSBaseModel {

    @ColumnMpping(name="repository_id", desc="", nullable=true, precision=0, type= Types.INTEGER, valueReg="")
    private int repository_id;
    public int getRepository_id() {
        return repository_id;
    }
    public void setRepository_id(int repository_id) {
        this.repository_id = repository_id;
    }

    @ColumnMpping(name="project_id", desc="", nullable=true, precision=0, type= Types.INTEGER, valueReg="")
    private int project_id;
    public int getProject_id() {
        return project_id;
    }
    public void setProject_id(int project_id) {
        this.project_id = project_id;
    }

    @ColumnMpping(name="squidflow_id", desc="", nullable=true, precision=0, type= Types.INTEGER, valueReg="")
    private int squidflow_id;
    public int getSquidflow_id() {
        return squidflow_id;
    }
    public void setSquidflow_id(int squidflow_id) {
        this.squidflow_id = squidflow_id;
    }

    @ColumnMpping(name="application_id", desc="", nullable=true, precision=100, type= Types.VARCHAR, valueReg="")
    private String application_id;
    public String getApplication_id() {
        return application_id;
    }
    public void setApplication_id(String application_id) {
        this.application_id = application_id;
    }
    @ColumnMpping(name="status", desc="", nullable=true, precision=20, type= Types.VARCHAR, valueReg="")
    private String app_status;
    public String getApp_status() {
        return app_status;
    }
    public void setApp_status(String app_status) {
        this.app_status = app_status;
    }

    @ColumnMpping(name="config", desc="", nullable=true, precision=500, type= Types.VARCHAR, valueReg="")
    private String config;
    public String getConfig() {
        return config;
    }
    public void setConfig(String config) {
        this.config = config;
    }

    @ColumnMpping(name="create_date", desc="", nullable=true, precision=0, type= Types.DATE, valueReg="")
    private Date create_date;
    public Date getCreate_date() {
        return create_date;
    }
    public void setCreate_date(Date create_date) {
        this.create_date = create_date;
    }

    @ColumnMpping(name="update_date", desc="", nullable=true, precision=0, type= Types.DATE, valueReg="")
    private Date update_date;
    public Date getUpdate_date() {
        return update_date;
    }
    public void setUpdate_date(Date update_date) {
        this.update_date = update_date;
    }

    @ColumnMpping(name="stop_date", desc="", nullable=true, precision=0, type= Types.DATE, valueReg="")
    private Date stop_date;
    public Date getStop_date() {
        return stop_date;
    }
    public void setStop_date(Date stop_date) {
        this.stop_date = stop_date;
    }

    @ColumnMpping(name="launch_user_id", desc="", nullable=true, precision=0, type= Types.INTEGER, valueReg="")
    private int launch_user_id;
    public int getLaunch_user_id() {
        return launch_user_id;
    }
    public void setLaunch_user_id(int launch_user_id) {
        this.launch_user_id = launch_user_id;
    }

    /**
     * 客户端用于显示squidflow的路径
     */
    private String squidflow_url;
    public String getSquidflow_url() {
        return squidflow_url;
    }
    public void setSquidflow_url(String squidflow_url) {
        this.squidflow_url = squidflow_url;
    }
}
