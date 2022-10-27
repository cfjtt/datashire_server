package com.eurlanda.datashire.sprint7.service.squidflow.subservice;

import com.eurlanda.datashire.utility.ReturnValue;

/**
 * 通用校验表是否被抽取
 * @author lei.bin
 *
 */
public interface ICheckExtractService{
/**
	 * 更新DS_SOURCE_TABLE中is_extracted
	 * 
	 * @param tableName
	 * @param squid_id
	 */
	public void updateExtract(String tableName, int squid_id, String type,
			ReturnValue out,String key,String token);
	
}