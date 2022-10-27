package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.entity.SpecialTransformationAndLinks;
import com.eurlanda.datashire.entity.TransformationLink;
import com.eurlanda.datashire.utility.ReturnValue;

import java.util.List;

public interface ITransAndLinkspProcess{
/**
	 * 拖动transformation打断transformationlink业务处理类
	 * 
	 * @return
	 */
	public List<SpecialTransformationAndLinks> drapTransformationAndLink(
			String info, ReturnValue out);
	/**
	 * 对transformationLink对象的赋值
	 * 
	 * @param transformationLink
	 * @param from_id
	 * @param to_id
	 */
	public void setTransfromation(TransformationLink transformationLink,
			int from_id, int to_id);
	
}