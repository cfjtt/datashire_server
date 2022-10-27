package com.eurlanda.datashire.dao;

import com.eurlanda.datashire.entity.TemplateDataInfo;
import com.eurlanda.datashire.entity.TemplateTypeInfo;

import java.util.List;

public interface ITemplateTypeDao extends IBaseDao {
	
	/**
	 * 获取TemplateType表中的所有类型集合
	 * @return
	 */
	public List<TemplateTypeInfo> getTemplateTypeList();
	
	/**
	 * 根据指定的数据类型及集合个数返回相关的数据集合
	 * @param tempType
	 * @return
	 */
	public List<TemplateDataInfo> getTemplateDataListByType(int tempType, int topCount);
}
