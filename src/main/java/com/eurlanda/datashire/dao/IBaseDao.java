package com.eurlanda.datashire.dao;

import com.eurlanda.datashire.adapter.IRelationalDataManager;

import java.util.List;
import java.util.Map;

/**
 * 统一数据访问
 * @author yi.zhou
 *
 */
public interface IBaseDao {
	/**
	 * 获取默认的链接对象，不自动提交事物
	 * @return
	 */
	public IRelationalDataManager getAdaDataManager();
	
	/**
	 * 设置默认的链接对象
	 * @param adapter
	 */
	public void setAdaDataManager(IRelationalDataManager adapter);
	
	/**
	 * 获取某实体类型的编号为某id的对象
	 * @param id 实体对象的编号
	 * @param c 实体对象的类型
	 * @return
	 */
	public <T> T getObjectById(int id, Class<T> c);
	
	
	/**
	 * 获取实体类型的列表
	 * @param id 实体对象的编号
	 * @param c 实体对象的类型
	 * @return
	 */
	public <T> List<T> getObjectAll(Class<T> c);
	
	
	/**
	 * 获取实体类型的列表
	 * @param c 实体对象的类型
	 * @return
	 */
	public <T> List<Map<String, Object>> getObjectForMap(Class<T> c) throws Exception;
	
	/**
	 * 获取实体类型的列表
	 * @param c 实体对象的类型
	 * @param filters 过滤字段
	 * @return
	 */
	public <T> List<Map<String, Object>> getObjectForMap(Class<T> c, List<String> filters) throws Exception;
	


	/**
	 * 获取实体类型的列表
	 * @param c 实体对象的类型
	 * @param filters 过滤字段
	 * @return
	 */
	public <T> List<Map<String, Object>> getObjectForMap(Class<T> c, List<String> filters,Map<String,String> where) throws Exception;
	/**
	 * 通用的添加数据方法
	 * @param obj
	 * @return
	 */
	public int insert2(Object obj) throws Exception ;
	
	/**
	 * 通用的删除数据方法
	 * @param id 对象的唯一标示号
	 * @param c 对象的类型
	 * @return
	 * @throws Exception
	 */
	public <T> int delete(int id, Class<T> c) throws Exception;
	
	/**
	 * 通用的修改数据方法
	 * @param id 对象
	 * @param c 对象的类型
	 * @return
	 * @throws Exception
	 */
	public boolean update(Object obj) throws Exception;

	/**
	 * 通过ID获取Squid的类型，具体数值是数字，对应枚举
	 * @return
	 * @throws Exception
     */
	public int getSquidTypeById(int id) throws Exception;
}
