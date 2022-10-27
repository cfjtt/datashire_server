package com.eurlanda.datashire.sprint7.service.workspace;
/**
 * 数据校验类，前端调用该类进行数据有效性校验，校验的具体内容为ds前缀的表(不包含ds_sys系统表),枚举类型的表，以及具有主外键约束的表
 * 
 * @version 1.0
 * @author lei.bin
 * @created 2013-09-17
 */
public interface IRepositoryValidationService{
/**
	 * 查询所有的元数据表信息,将校验结果返回给前端,具体操作由子类完成
	 * @param info 
	 * @return json对象
	 */
	public String queryAllTableName(String info);
	/**
	 * 校验的具体内容为ds前缀的表(不包含ds_sys系统表),枚举类型的表，以及具有主外键约束的表
	 * @param info 
	 * @return json对象
	 */
	public String checkAllTable(String info);
	
}