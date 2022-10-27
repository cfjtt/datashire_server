package com.eurlanda.datashire.dao;

import com.eurlanda.datashire.entity.Project;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface IProjectDao extends IBaseDao{
	/**
	 * 获取project表的部分字段
	 * @return
	 * @throws SQLException
	 */
	public List<Map<String, Object>> getSomeProject(String value) throws SQLException ;
	/**
	 * 根据repositoryId获取所有的project集合
	 * @return
	 * @throws SQLException
	 */
	public List<Project> getAllProject(int repositoryId) throws SQLException;
}
