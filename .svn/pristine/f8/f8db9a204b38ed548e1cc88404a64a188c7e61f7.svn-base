package com.eurlanda.datashire.dao.impl;

import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.dao.ITemplateTypeDao;
import com.eurlanda.datashire.entity.TemplateDataInfo;
import com.eurlanda.datashire.entity.TemplateTypeInfo;

import java.util.List;

public class TemplateTypeDaoImpl extends BaseDaoImpl implements ITemplateTypeDao {

	public TemplateTypeDaoImpl(){
	}
	
	public TemplateTypeDaoImpl(IRelationalDataManager adapter){
		this.adapter = adapter;
	}
	
	@Override
	public List<TemplateTypeInfo> getTemplateTypeList() {
		List<TemplateTypeInfo> list = adapter.query2List(true, null, TemplateTypeInfo.class);
		return list;
	}

	@Override
	public List<TemplateDataInfo> getTemplateDataListByType(int tempType,
			int topCount) {
		return adapter.query2List(true, 
				"select * from ds_sys_template_data where type_id = " + 
						tempType + " limit 0," + topCount, 
				null, TemplateDataInfo.class);
	}
	
}
