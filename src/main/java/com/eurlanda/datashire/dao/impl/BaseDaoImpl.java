package com.eurlanda.datashire.dao.impl;

import cn.com.jsoft.jframe.utils.ValidateUtils;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.annotation.MultitableMapping;
import com.eurlanda.datashire.dao.IBaseDao;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * 统一数据访问
 * @author eurlanda
 *
 */
public class BaseDaoImpl implements IBaseDao{
	
	static Logger logger = Logger.getLogger(BaseDaoImpl.class);// 记录日志
	
	protected IRelationalDataManager adapter;
	
	/**
	 * 获取默认的数据库链接，同时开启事务，不自动提交事务
	 */
	@Override
	public IRelationalDataManager getAdaDataManager(){
		return adapter;
	}
	
	/**
	 * 设置默认的数据库链接
	 */
	public void setAdaDataManager(IRelationalDataManager adapter){
		this.adapter = adapter;
	}

	@Override
	public <T> T getObjectById(int id, Class<T> c) {
		Map<String, Object> paramMap = new HashMap<String, Object>();
		paramMap.put("id", id);
		return adapter.query2Object(true, paramMap, c);
	}
	
	@Override
	public <T> List<T> getObjectAll(Class<T> c) {
		Map<String, Object> paramMap = new HashMap<String, Object>();
		return adapter.query2List2(true, paramMap, c);
	}
	
	@Override
	public <T> List<Map<String, Object>> getObjectForMap(Class<T> c) throws Exception {
		String tableName="";
		MultitableMapping tm = c.newInstance().getClass().getAnnotation(MultitableMapping.class);
		if(tm!=null&&tm.name()!=null&&tm.name().length>0){
			int i = tm.name().length - 1;
			if (!ValidateUtils.isEmpty(tm.name()[i])){
				tableName = tm.name()[i];
			}
		}
		String sql  = "select  * from " + tableName;
		return adapter.query2List(true, sql, null);
	}

	@Override
	public <T> List<Map<String, Object>> getObjectForMap(Class<T> c, List<String> filters) throws Exception {
		String tableName="";
		MultitableMapping tm = c.newInstance().getClass().getAnnotation(MultitableMapping.class);
		if(tm!=null&&tm.name()!=null&&tm.name().length>0){
			int i = tm.name().length - 1;
			if (!ValidateUtils.isEmpty(tm.name()[i])){
				tableName = tm.name()[i];
			}
		}
		String sql  = "select  * from " + tableName;
		if (filters!=null&&filters.size()>0){
			String temp = filters.toString();
			temp = temp.substring(1);
			temp = temp.substring(0, temp.length()-1);
			
			sql = "select " + temp + " from " + tableName;
		}
		return adapter.query2List(true, sql, null);
	}
	

	public <T> List<Map<String, Object>> getObjectForMap(Class<T> c, List<String> filters, Map<String,String> where) throws Exception {
		String tableName="";
		MultitableMapping tm = c.newInstance().getClass().getAnnotation(MultitableMapping.class);
		if(tm!=null&&tm.name()!=null&&tm.name().length>0){
			int i = tm.name().length - 1;
			if (!ValidateUtils.isEmpty(tm.name()[i])){
				tableName = tm.name()[i];
			}
		}
		String sql  = "select  * from " + tableName;
		if (filters!=null&&filters.size()>0){
			String temp = filters.toString();
			temp = temp.substring(1);
			temp = temp.substring(0, temp.length()-1);
			sql = "select " + temp + " from " + tableName;
		}
		if(where!=null){
			int i = 0;
			StringBuffer sb = new StringBuffer();
			sb.append(sql);
			sb.append(" where ");
			for (Entry<String,String> entry : where.entrySet()) {
				if(i!=0){
					sb.append(" and ");
				}
				sb.append(entry.getKey()+"='"+entry.getValue()+"'");
				i++;
			}
			sql = sb.toString();
		}
		return adapter.query2List(true, sql, null);
	}
	
	@Override
	public int insert2(Object obj) throws Exception {
		return adapter.insert2(obj);
	}

	@Override
	public <T> int delete(int id, Class<T> c) throws Exception{
		Map<String, String> params = new HashMap<String, String>();
		params.put("id", id+"");
		return adapter.delete(params, c);
	}

	@Override
	public boolean update(Object obj) throws Exception {
		return adapter.update2(obj);
	}

	@Override
	public int getSquidTypeById(int id) throws Exception {
		String sql = "select SQUID_TYPE_ID from DS_SQUID WHERE ID = " + id;
		List<Map<String, Object>> list = adapter.query2List(true, sql, null);
		int type_id = 0;
		if(list!=null && !list.isEmpty()){
			type_id = Integer.valueOf(list.get(0).get("SQUID_TYPE_ID").toString());
		}
		return type_id;
	}
}
