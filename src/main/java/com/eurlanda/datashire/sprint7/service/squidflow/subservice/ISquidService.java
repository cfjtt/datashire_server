package com.eurlanda.datashire.sprint7.service.squidflow.subservice;

import com.eurlanda.datashire.entity.Squid;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.utility.ReturnValue;

import java.util.Map;

/**
 * SquidModelBase 增删查改
 * 
 * @author dang.lu 2013.11.12
 *
 */
public interface ISquidService{
/**
	 * 根据squid_id获取Squid
	 * @param squid_id
	 * @return
	 */
	public Squid getSquid(int squid_id);
	/**
	 * 根据squid删除
	 * @param squid
	 * @param out
	 * @return
	 */
	public boolean deleteSquid(int squid, ReturnValue out);
	/**
	 * 根据枚举类型去删除子类
	 * @param squid
	 * @param type
	 * @return
	 */
	public boolean deleteChildrenSquid(int squid,DSObjectType type,Map<String, String> paramMap,ReturnValue out);
	
}