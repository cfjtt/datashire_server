package com.eurlanda.datashire.sprint7.service.user;

public interface ILoginService {
	
	/**
	 * 登录（普通用户、超级用户）
	 * @param info
	 * @return
	 */
	public String login(String info);
	
	/**
	 * 登出
	 * @param info
	 * @return
	 */
	public String logout();
	
}
