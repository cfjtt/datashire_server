package com.eurlanda.datashire.sprint7.service.squidflow.subservice;

import com.eurlanda.datashire.utility.ReturnValue;

/**
 * 删除ReferenceCloumn处理类
 * @author eurlanda01
 *
 */
public interface IReferenceColumnService{
/**
	 * 根据id删除ReferenceCloumn
	 * @param column_id
	 * @return
	 * @throws SQLException
	 */
	public boolean deleteReferenceColumn(int column_id,ReturnValue out);
	
}