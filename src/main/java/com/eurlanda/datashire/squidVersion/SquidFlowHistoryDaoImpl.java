package com.eurlanda.datashire.squidVersion;

import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.dao.impl.BaseDaoImpl;
import com.eurlanda.datashire.utility.DatabaseException;

import java.util.List;

/**
 * Created by Administrator on 2016/10/19.
 */
public class SquidFlowHistoryDaoImpl extends BaseDaoImpl implements ISquidFlowHistoryDao {
    public SquidFlowHistoryDaoImpl() {
    }

    public SquidFlowHistoryDaoImpl(IRelationalDataManager adapter) {
        this.adapter = adapter;
    }

    @Override
    public List<SquidFlowHistory> getSquidFlowHistoryBySquidFlow(int respositoryId, int squidFlowId, int userId, int squidFlowType, int dataShireFieldType) throws Exception {
        String sql = "select * from SQUID_FLOW_HISTORY where SQUID_FLOW_ID = " + squidFlowId + " and REPOSITORY_ID = " + respositoryId + " and  USER_ID=" + userId + " and squid_flow_type=" + squidFlowType + " and field_type=" + dataShireFieldType;
        List<SquidFlowHistory> squidFlowHistoryList = adapter.query2List(sql, null, SquidFlowHistory.class);
        return squidFlowHistoryList;
    }

    @Override
    public List<SquidFlowHistory> getSquidFlowHistoryByRespository(int respositoryId, int userId, int dataShireFieldType) throws Exception {
        String sql = "select * from SQUID_FLOW_HISTORY where  REPOSITORY_ID = " + respositoryId + " and USER_ID=" + userId + " and field_type=" + dataShireFieldType;
        List<SquidFlowHistory> squidFlowHistoryList = adapter.query2List(sql, null, SquidFlowHistory.class);
        return squidFlowHistoryList;
    }

    @Override
    public List<SquidFlowHistory> getSquidFlowHistoryByProjectId(int projectId, int userId, int repositoryId, int dataShireFieldType) throws Exception {
        String sql = "select * from SQUID_FLOW_HISTORY where  project_id = " + projectId + " and USER_ID=" + userId + " and repository_id=" + repositoryId + " and field_type=" + dataShireFieldType;
        List<SquidFlowHistory> squidFlowHistoryList = adapter.query2List(sql, null, SquidFlowHistory.class);
        return squidFlowHistoryList;
    }

    @Override
    public boolean delSquidFlowHistory(int id) throws DatabaseException {
        if (id < 0) {
            return false;
        }
        String sql = "delete sf,hv from squid_flow_history sf inner join history_version_data hv on sf.id = hv.squidFlowHistoryId where sf.id=" + id;
        return adapter.execute(sql) > 0 ? true : false;
    }
}
