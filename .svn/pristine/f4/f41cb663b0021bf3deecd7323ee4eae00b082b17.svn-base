package com.eurlanda.datashire.squidVersion;

import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.dao.impl.BaseDaoImpl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2016/10/19.
 */
public class HistoryDataDaoImpl extends BaseDaoImpl implements IHistoryDataDao {

    public HistoryDataDaoImpl() {
    }

    public HistoryDataDaoImpl(IRelationalDataManager adapter) {
        this.adapter = adapter;
    }

    /**
     * 查询出不同精度下的数据
     *
     * @param squidFlowHistory
     * @param processType
     * @return
     * @throws Exception
     */
    @Override
    public List<HistoryData> getVersionDataByProcess(SquidFlowHistory squidFlowHistory, int processType)
            throws Exception {
        String sql = "select * from history_version_data where squid_flow_id=" + squidFlowHistory.getSquidFlowId()
                + " and repository_id=" + squidFlowHistory.getRepositoryId() + " and squidFlowHistoryId="
                + squidFlowHistory.getId() + " and processType=" + processType + " order by squidType";
        List<HistoryData> historyDataList = adapter.query2List(sql, null, HistoryData.class);
        return historyDataList;
    }

    /**
     * 查询出数据中一共有多少squidType
     *
     * @param squidFlowHistory
     * @return
     * @throws Exception
     */
    @Override
    public List<Integer> getVersionSquidType(SquidFlowHistory squidFlowHistory) throws Exception {
        String sql = "select distinct squidType from history_version_data where  squid_flow_id="
                + squidFlowHistory.getSquidFlowId() + " and repository_id=" + squidFlowHistory.getRepositoryId()
                + " and squidFlowHistoryId=" + squidFlowHistory.getId();
        List<Map<String, Object>> squidTypeId = adapter.query2List(sql, null);
        List<Integer> squidTypeIds = new ArrayList<>();
        for (Map<String, Object> map : squidTypeId) {
            if ((Integer) map.get("SQUIDTYPE") == -1) {
                continue;
            }
            squidTypeIds.add((Integer) map.get("SQUIDTYPE"));
        }
        return squidTypeIds;
    }

    /**
     * 根据squidType,processType查询版本信息
     */
    @Override
    public List<HistoryData> getVersionDataByProcessAndSquidType(SquidFlowHistory squidFlowHistory, int processType,
                                                                 int squidTypeId) throws Exception {
        String sql = "select * from history_version_data where squid_flow_id=" + squidFlowHistory.getSquidFlowId()
                + " and repository_id=" + squidFlowHistory.getRepositoryId() + " and squidFlowHistoryId="
                + squidFlowHistory.getId() + " and processType=" + processType + " and squidType = " + squidTypeId;
        List<HistoryData> historyDataList = adapter.query2List(sql, null, HistoryData.class);
        return historyDataList;
    }

}
