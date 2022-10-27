package com.eurlanda.datashire.sprint7.service.squidflow.subservice;

import com.eurlanda.datashire.utility.ReturnValue;

/**
 * 删除Transformation业务处理类
 * @author lei.bin
 *
 */
public interface ITransformationService{
/**
	 * 根据transformation_id删除transformation以及关联表信息
	 * 
	 * @param trans_id
	 * @param out
	 * @return
	 */
	public boolean deleteTransformation(int trans_id, ReturnValue out);
	
}