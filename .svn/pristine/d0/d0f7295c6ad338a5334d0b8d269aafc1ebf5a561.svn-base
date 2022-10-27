package com.eurlanda.datashire.sprint7.service.squidflow.subservice;

import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.entity.StageSquid;
import com.eurlanda.datashire.entity.operation.StageSquidAndSquidLink;

/**
 *
 * @author dang.lu 2013.11.12
 *
 */
public interface IStageSquidService{
/**
	 * 从extract右键link到stage(该stage可能新创建，或者已经存在上游extract)
	 * 
	 * @param squidAndSquidLink
	 * @return
	 */
	public void link2StageSquid(StageSquidAndSquidLink squidAndSquidLink);
	/** 从datasquid的目标列选择若干column放到stagesquid上 */
	public void drag2StageSquid(StageSquidAndSquidLink squidAndSquidLink);
	/** 获取StageSquid详细信息（如果不在同一个事务里，adapter连接打开、关闭需要调用函数自己处理） */
	public  void setStageSquidData(String token, boolean inSession, IRelationalDataManager adapter, StageSquid stageSquid);
	
}