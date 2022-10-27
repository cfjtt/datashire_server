package com.eurlanda.datashire.squidVersion;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;
import com.eurlanda.datashire.entity.DSBaseModel;

import java.sql.Types;

/**
 * 历史版本实体类
 * Created by Administrator on 2016/9/22.
 */
@MultitableMapping(pk = "", name = {"SQUID_FLOW_HISTORY"}, desc = "")
public class SquidFlowHistory extends DSBaseModel {


    @ColumnMpping(name = "SQUID_FLOW_ID", desc = "", nullable = false, precision = 0, type = Types.INTEGER, valueReg = ">=1")
    private int squidFlowId;

    @ColumnMpping(name = "SQUID_FLOW_NAME", desc = "", nullable = false, precision = 200, type = Types.NVARCHAR, valueReg = "")
    private String squidFlowName;

    @ColumnMpping(name = "PROJECT_ID", desc = "", nullable = false, precision = 0, type = Types.INTEGER, valueReg = ">=1")
    private int projectId;

    @ColumnMpping(name = "PROJECT_NAME", desc = "", nullable = true, precision = 200, type = Types.NVARCHAR, valueReg = "")
    private String projectName;

    @ColumnMpping(name = "REPOSITORY_ID", desc = "", nullable = false, precision = 0, type = Types.INTEGER, valueReg = ">=1")
    private int repositoryId;

    @ColumnMpping(name = "COMMIT_TIME", desc = "", nullable = false, precision = 0, type = Types.TIMESTAMP, valueReg = "")
    private String submitDate;

    @ColumnMpping(name = "USER_ID", desc = "", nullable = true, precision = 0, type = Types.INTEGER, valueReg = ">=0")
    private int userId;

    @ColumnMpping(name = "SUBMIT_USERNAME", desc = "", nullable = true, precision = 1000, type = Types.NVARCHAR, valueReg = "")
    private String submitUserName;

    @ColumnMpping(name = "COMMENTS", desc = "", nullable = true, precision = 1000, type = Types.NVARCHAR, valueReg = "")
    private String comments;

    @ColumnMpping(name = "VERSION", desc = "", nullable = false, precision = 0, type = Types.INTEGER, valueReg = ">=1")
    private int version;

    @ColumnMpping(name = "SQUID_FLOW_TYPE", desc = "", nullable = false, precision = 0, type = Types.INTEGER, valueReg = ">=0")
    private int squidFlowType;

    @ColumnMpping(name = "FIELD_TYPE", desc = "", nullable = false, precision = 0, type = Types.INTEGER, valueReg = ">=0")
    private int dataShireFieldType;

    public SquidFlowHistory() {
    }

    public SquidFlowHistory(int squidFlowId, String squidFlowName,
                            int projectId, String projectName,
                            int repositoryId, String submitDate,
                            int userId, String comments,
                            int squidFlowType, String submitUserName,
                            int dataShireFieldType) {
        this.squidFlowId = squidFlowId;
        this.squidFlowName = squidFlowName;
        this.projectId = projectId;
        this.projectName = projectName;
        this.repositoryId = repositoryId;
        this.submitDate = submitDate;
        this.userId = userId;
        this.comments = comments;
        this.squidFlowType = squidFlowType;
        this.submitUserName = submitUserName;
        this.dataShireFieldType = dataShireFieldType;
    }

    public int getSquidFlowId() {
        return squidFlowId;
    }

    public void setSquidFlowId(int squidFlowId) {
        this.squidFlowId = squidFlowId;
    }

    public String getSquidFlowName() {
        return squidFlowName;
    }

    public void setSquidFlowName(String squidFlowName) {
        this.squidFlowName = squidFlowName;
    }

    public int getProjectId() {
        return projectId;
    }

    public void setProjectId(int projectId) {
        this.projectId = projectId;
    }

    public String getProjectName() {
        return projectName;
    }

    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }

    public int getRepositoryId() {
        return repositoryId;
    }

    public void setRepositoryId(int repositoryId) {
        this.repositoryId = repositoryId;
    }

    public String getSubmitDate() {
        return submitDate;
    }

    public void setSubmitDate(String submitDate) {
        this.submitDate = submitDate;
    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public String getComments() {
        return comments;
    }

    public void setComments(String comments) {
        this.comments = comments;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public int getSquidFlowType() {
        return squidFlowType;
    }

    public void setSquidFlowType(int squidFlowType) {
        this.squidFlowType = squidFlowType;
    }

    public String getSubmitUserName() {
        return submitUserName;
    }

    public void setSubmitUserName(String submitUserName) {
        this.submitUserName = submitUserName;
    }

    public int getDataShireFieldType() {
        return dataShireFieldType;
    }

    public void setDataShireFieldType(int dataShireFieldType) {
        this.dataShireFieldType = dataShireFieldType;
    }

    @Override
    public String toString() {
        return "SquidFlowHistory{" +
                "squidFlowId=" + squidFlowId +
                ", squidFlowName='" + squidFlowName + '\'' +
                ", projectId=" + projectId +
                ", projectName='" + projectName + '\'' +
                ", repositoryId=" + repositoryId +
                ", submitDate='" + submitDate + '\'' +
                ", userId=" + userId +
                ", comments='" + comments + '\'' +
                ", version=" + version +
                ", squidFlowType=" + squidFlowType +
                '}';
    }
}
