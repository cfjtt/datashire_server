package com.eurlanda.datashire.sprint7.service.squidflow.subservice;

import com.eurlanda.datashire.utility.ReturnValue;

/**
 * 删除Column处理类
 * @author lei.bin
 *
 */
public interface IColumnService{
/**
	 * 根据id删除column
	 * 
	 * @param id
	 * @param out
	 * @return
	 */
	public boolean deleteColumn(int id, ReturnValue out);
	
}