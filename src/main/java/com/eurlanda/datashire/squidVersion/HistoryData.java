package com.eurlanda.datashire.squidVersion;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;
import com.eurlanda.datashire.entity.DSBaseModel;

import java.sql.Types;

/**
 * 版本的历史数据
 * Created by Administrator on 2016/9/22.
 */
@MultitableMapping(pk = "", name = {"HISTORY_VERSION_DATA"}, desc = "")
public class HistoryData extends DSBaseModel {

    @ColumnMpping(name = "REPOSITORY_Id", desc = "", nullable = false, precision = 0, type = Types.INTEGER, valueReg = ">=1")
    private int RepositoryId;

    @ColumnMpping(name = "SQUIDFLOWHISTORYID", desc = "", nullable = false, precision = 0, type = Types.INTEGER, valueReg = ">=1")
    private int squidFlowHistoryId;

    @ColumnMpping(name = "SQUID_FLOW_ID", desc = "", nullable = false, precision = 0, type = Types.INTEGER, valueReg = ">=1")
    private int squidFlowId;

    @ColumnMpping(name = "COMMIT_TIME", desc = "提交时间", nullable = false, precision = 0, type = Types.TIMESTAMP, valueReg = "")
    private String commit_time;

    @ColumnMpping(name = "DATA", desc = "提交时数据", nullable = false, precision = 0, type = Types.LONGVARCHAR, valueReg = "")
    private String data;

    @ColumnMpping(name = "SQUIDTYPE", desc = "提交时squid类别", nullable = false, precision = 0, type = Types.INTEGER, valueReg = ">=1")
    private Integer squidType;

    @ColumnMpping(name = "PROCESSTYPE", desc = "调度进度的级别", nullable = false, precision = 0, type = Types.INTEGER, valueReg = ">=1")
    private int processType;

    @ColumnMpping(name = "SQUID_FLOW_TYPE", desc = "squidflow级别", nullable = false, precision = 0, type = Types.INTEGER, valueReg = ">=0")
    private int squidFlowType;

    public HistoryData() {
    }

    public HistoryData(int repositoryId, int squidFlowHistoryId, int squidFlowId,
                       String commit_time, String data,
                       Integer squidType, int processType,
                       int squidFlowType) {
        RepositoryId = repositoryId;
        this.squidFlowHistoryId = squidFlowHistoryId;
        this.squidFlowId = squidFlowId;
        this.commit_time = commit_time;
        this.data = data;
        this.squidType = squidType;
        this.processType = processType;
        this.squidFlowType = squidFlowType;
    }

    public int getRepositoryId() {
        return RepositoryId;
    }

    public void setRepositoryId(int repositoryId) {
        RepositoryId = repositoryId;
    }

    public int getSquidFlowHistoryId() {
        return squidFlowHistoryId;
    }

    public void setSquidFlowHistoryId(int squidFlowHistoryId) {
        this.squidFlowHistoryId = squidFlowHistoryId;
    }

    public int getSquidFlowId() {
        return squidFlowId;
    }

    public void setSquidFlowId(int squidFlowId) {
        this.squidFlowId = squidFlowId;
    }

    public String getCommit_time() {
        return commit_time;
    }

    public void setCommit_time(String commit_time) {
        this.commit_time = commit_time;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public Integer getSquidType() {
        return squidType;
    }

    public void setSquidType(Integer squidType) {
        this.squidType = squidType;
    }

    public int getProcessType() {
        return processType;
    }

    public void setProcessType(int processType) {
        this.processType = processType;
    }

    public int getSquidFlowType() {
        return squidFlowType;
    }

    public void setSquidFlowType(int squidFlowType) {
        this.squidFlowType = squidFlowType;
    }
}
