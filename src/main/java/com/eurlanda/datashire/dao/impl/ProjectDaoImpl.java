package com.eurlanda.datashire.dao.impl;

import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.dao.IProjectDao;
import com.eurlanda.datashire.entity.Project;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProjectDaoImpl extends BaseDaoImpl implements IProjectDao {
	public ProjectDaoImpl() {
	}

	public ProjectDaoImpl(IRelationalDataManager adapter) {
		this.adapter = adapter;
	}
	@Override
	public List<Map<String, Object>> getSomeProject(String value) throws SQLException {
		// TODO Auto-generated method stub
		List<Map<String, Object>> proName=adapter.query2List(true, "select id,name from DS_PROJECT where id in"+value, null);
		return proName;
	}

	@Override
	public List<Project> getAllProject(int repositoryId) throws SQLException {
		// TODO Auto-generated method stub
		Map<String, Object> paramMap = new HashMap<String, Object>();
		paramMap.put("repository_id", repositoryId);
		List<Project> projects=adapter.query2List2(true, paramMap,Project.class);
		return projects;
	}

}
