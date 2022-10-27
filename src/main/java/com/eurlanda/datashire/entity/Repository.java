package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;

import java.sql.Types;
import java.util.List;
/**
 * 
 * 
 * <p>
 * Title : 
 * </p>
 * <p>
 * Description: 
 * </p>
 * <p>
 * Author :赵春花 2013-9-5
 * </p>
 * <p>
 * update :赵春花 2013-9-5
 * </p>
 * <p>
 * Department :  JAVA后端研发部
 * </p>
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司
 * </p>
 */
@MultitableMapping(pk="", name = "DS_SYS_REPOSITORY", desc = "元数据仓库配置信息表")
public class Repository extends DSBaseModel {

	@ColumnMpping(name="description", desc="描述", nullable=true, precision=500, type=Types.VARCHAR, valueReg="")
	private String description;
	
	@ColumnMpping(name="repository_db_name", desc="数据库名称/文件夹名(唯一约束)", nullable=true, precision=100, type=Types.VARCHAR, valueReg="")
	private String repository_db_name;
	
	@ColumnMpping(name="type", desc="type", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private int repository_type;
	
	@ColumnMpping(name="team_id", desc="team_id", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private int team_id;

	@ColumnMpping(name="status_id", desc="状态ID", nullable=true, precision=0, type=Types.INTEGER, valueReg=">=1")
	private int status_id;
	@ColumnMpping(name="packageId", desc="状态ID", nullable=true, precision=0, type=Types.INTEGER, valueReg=">=1")
	private Integer packageId;
	/**
	 * project对象集合
	 */
	private List<Project> projectList;
	//路径
	private String repository_path;
	//是否校验
	private boolean validate;

	public String getDescription() {
		return description;
	}

	public String getRepository_path() {
		return repository_path;
	}

	public void setRepository_path(String repositoryPath) {
		repository_path = repositoryPath;
	}

	public boolean isValidate() {
		return validate;
	}

	public void setValidate(boolean validate) {
		this.validate = validate;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public List<Project> getProjectList() {
		return projectList;
	}

	public void setProjectList(List<Project> projectList) {
		this.projectList = projectList;
	}

	public String getRepository_db_name() {
		return repository_db_name;
	}

	public void setRepository_db_name(String repositoryDbName) {
		repository_db_name = repositoryDbName;
	}

	public int getTeam_id() {
		return team_id;
	}

	public void setTeam_id(int teamId) {
		team_id = teamId;
	}

	public int getRepository_type() {
		return repository_type;
	}

	public void setRepository_type(int repositoryType) {
		repository_type = repositoryType;
	}

	public Integer getPackageId() {
		return packageId;
	}

	public void setPackageId(Integer packageId) {
		this.packageId = packageId;
	}

	@Override
	public void setStatus(int status) {
		super.setStatus(status);
		this.setStatus_id(status);
	}
	
	public int getStatus_id() {
		return status_id;
	}

	public void setStatus_id(int status_id) {
		this.status_id = status_id;
	}

	@Override
	public String toString() {
		return "Repository [description=" + description + ", projectList=" + projectList + ", repository_db_name="
				+ repository_db_name + ", repository_path=" + repository_path + ", repository_type=" + repository_type
				+ ", team_id=" + team_id + ", validate=" + validate + "]";
	}

}