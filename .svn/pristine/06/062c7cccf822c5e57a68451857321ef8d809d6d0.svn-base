package com.eurlanda.datashire.squidVersion;

import com.eurlanda.datashire.utility.DatabaseException;

import java.util.List;

/**
 * 版本列表操作类
 * Created by Administrator on 2016/10/19.
 */
public interface ISquidFlowHistoryDao {
    /**
     * 根据respository，squidFlowId获取历史版本信息
     *
     * @param respositoryId
     * @param squidFlowId
     * @return
     */
    public List<SquidFlowHistory> getSquidFlowHistoryBySquidFlow(int respositoryId, int squidFlowId, int userId, int squidFlowType, int datashireFieldType) throws Exception;

    /**
     * 根据RespositoryID获取SquidFlowHistory信息
     *
     * @param respositoryId
     * @return
     */
    public List<SquidFlowHistory> getSquidFlowHistoryByRespository(int respositoryId, int userId, int dataShireFieldType) throws Exception;

    /**
     * 根据projectId获取历史版本信息
     *
     * @return
     * @throws DatabaseException
     */
    public List<SquidFlowHistory> getSquidFlowHistoryByProjectId(int projectId, int userId, int repositoryId, int dataShireFieldType) throws Exception;

    /**
     * 删除squidFlowHistory
     *
     * @param id
     * @return
     * @throws DatabaseException
     */
    public boolean delSquidFlowHistory(int id) throws DatabaseException;
}
