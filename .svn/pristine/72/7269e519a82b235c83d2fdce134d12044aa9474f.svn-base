package com.eurlanda.datashire.dao;

import com.eurlanda.datashire.entity.FindModel;
import com.eurlanda.datashire.entity.FindResult;
import com.eurlanda.datashire.entity.Repository;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface IRepositoryDao extends IBaseDao {
	/**
     * 查询squid对应的repository_id
     * @param adapter3
     * @param squidId
     * @return
     * @throws SQLException
     */
    public int getRepositoryIdBySquid(int squidId) throws SQLException;

    /**
     * 根据squid查找project
     * @param squidId
     * @return
     * @throws SQLException
     */
    public int getProjectIdBySquid(int squidId) throws SQLException;
    /**
     * 获取某Repository下的所有的dataminingSquid
     * @param repositoryId
     * @param filterIds
     * @return
     * @throws SQLException
     */
    public List<Map<String, Object>> getAllDataMiningSquidInRepository(boolean isFromCurrentProject,int repositoryId,
    		String filterIds) throws SQLException;
    
    /**
     * 获取某Repository下的所有的Quantify
     * @param repositoryId
     * @param filterIds
     * @return
     * @throws SQLException
     */
    public List<Map<String, Object>> getAllQuantifySquidInRepository(
			boolean isFromCurrentProject,int repositoryId, String filterIds) throws SQLException;
    /**
     * 获取所有的repository集合
     * @return
     * @throws SQLException
     */
    public List<Repository> getAllRepository() throws SQLException;
	
    /**
     * 获取当前查找的结果集
     * @param fm
     * @return
     */
    public List<FindResult> getFindResultByParams(FindModel fm) throws Exception;
    
    /**
     * 保存license的值
     * @param info
     * @throws Exception
     */
    public void saveLicenseKey(String info) throws Exception;
    
    /**
     * 获取license的值
     * @return
     * @throws Exception
     */
    public String getLicenseKey() throws Exception;
}
