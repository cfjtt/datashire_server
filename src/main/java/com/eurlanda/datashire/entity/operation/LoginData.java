package com.eurlanda.datashire.entity.operation;

import com.eurlanda.datashire.entity.Repository;
import com.eurlanda.datashire.server.model.User;

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
 * Author :赵春花 2013-9-10
 * </p>
 * <p>
 * update :赵春花 2013-9-10
 * </p>
 * <p>
 * Department :  JAVA后端研发部
 * </p>
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司
 * </p>
 */
public class LoginData {
	/***
	 * 数据库连接信息
	 */
	private Repository repository;
	
	/***
	 * 用户登录信息
	 */
	private User userInfo;
	
	/***
	 * 构造器
	 */
	public LoginData(){
		
	}

	/***
	 * 构造器
	 * @param repository
	 * @param userInfo
	 */
	public LoginData(Repository repository, User userInfo) {
		super();
		this.repository = repository;
		this.userInfo = userInfo;
	}

	public Repository getRepository() {
		return repository;
	}

	public void setRepository(Repository repository) {
		this.repository = repository;
	}

	public User getUser() {
		return userInfo;
	}

	public void setUser(User userInfo) {
		this.userInfo = userInfo;
	}
	
}
