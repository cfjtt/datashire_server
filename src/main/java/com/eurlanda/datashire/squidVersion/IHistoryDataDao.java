package com.eurlanda.datashire.squidVersion;

import java.util.List;

/**
 * 版本数据操作类
 * Created by Administrator on 2016/10/19.
 */
public interface IHistoryDataDao {
    /**
     * 查询出不同进度下的版本数据
     */
    public List<HistoryData> getVersionDataByProcess(SquidFlowHistory squidFlowHistory, int processType) throws Exception;

    /**
     * 查询出版本下的squid类别
     */
    public List<Integer> getVersionSquidType(SquidFlowHistory squidFlowHistory) throws Exception;

    /**
     * 根据squidType,processType查询版本数据
     */
    public List<HistoryData> getVersionDataByProcessAndSquidType(SquidFlowHistory squidFlowHistory, int processType, int squidTypeId) throws Exception;
}
