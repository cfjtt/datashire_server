package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.enumeration.SchedulerLogStatus;
import com.eurlanda.datashire.utility.MessageCode;

import java.util.List;
import java.util.Map;

/**
 * 编译状态实体类
 * Created by Eurlanda-dev on 2016/8/8.
 */
public class BuildInfo extends DSBaseModel {

    private int squidId;

    private int messageCode;

    //对应SchedulerLogStatus枚举(DEBUG=1,INFO=2,WARNING=3,ERROR=4)
    private int infoType;

    private int repositoryId;

    private int squidFlowId;

    private List<String> subTargetName;

    //默认为false,最后一条数据为true
    private boolean lastBuildInfo;

    public BuildInfo() {}

    public BuildInfo(Squid squid,Map<String,Object> map){
        this.setSquidId(squid.getId());
        this.setRepositoryId(Integer.parseInt(map.get("RepositoryId") + ""));
        this.setMessageCode(MessageCode.COMPILE_SUCCESS.value());
        this.setInfoType(SchedulerLogStatus.INFO.getValue());
        this.setSquidFlowId(squid.getSquidflow_id());
        this.setSubTargetName(null);
        this.setLastBuildInfo((boolean)map.get("isLast"));
    }

    public int getSquidId() {
        return squidId;
    }

    public void setSquidId(int squidId) {
        this.squidId = squidId;
    }

    public int getMessageCode() {
        return messageCode;
    }

    public void setMessageCode(int messageCode) {
        this.messageCode = messageCode;
    }

    public int getInfoType() {
        return infoType;
    }

    public void setInfoType(int infoType) {
        this.infoType = infoType;
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

    public List<String> getSubTargetName() {
        return subTargetName;
    }

    public void setSubTargetName(List<String> subTargetName) {
        this.subTargetName = subTargetName;
    }

    public boolean isLastBuildInfo() {
        return lastBuildInfo;
    }

    public void setLastBuildInfo(boolean lastBuildInfo) {
        this.lastBuildInfo = lastBuildInfo;
    }
}
