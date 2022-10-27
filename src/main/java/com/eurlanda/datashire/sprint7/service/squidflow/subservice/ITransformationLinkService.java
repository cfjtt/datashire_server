package com.eurlanda.datashire.sprint7.service.squidflow.subservice;

import com.eurlanda.datashire.utility.ReturnValue;

/**
 * 删除TransformationLink处理类
 * @author lei.bin
 *
 */
public interface ITransformationLinkService{
/**
	 * 删除TransformationLink，同时更改引用列is_referenced='N'
	 * @param TransformationLink_id
	 * @return
	 */
	public boolean deleteTransLink(int TransformationLink_id,ReturnValue out);
	
}