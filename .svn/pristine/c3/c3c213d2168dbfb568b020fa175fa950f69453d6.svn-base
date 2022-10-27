package com.eurlanda.datashire.sprint7.service.squidflow.subservice;

import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.entity.TableExtractSquid;
import com.eurlanda.datashire.entity.operation.ExtractSquidAndSquidLink;
import com.eurlanda.datashire.utility.ReturnValue;

import java.util.List;

/**
 * Extract SquidModelBase 业务处理
 * 1.拖表创建，支持批量创建（从source table list选择一个或多个到面板空白区）
 * 2.拖列创建（从source table中选择一列或多列到面板空白区或空的extract上）
 * 3.详细信息加载（query details when load squid flow）
 * 
 * @author dang.lu 2013.11.12
 *
 */
public interface IExtractService{
/** 直接从table list拖拽创建，有可能多选或全选(需要考虑性能问题，可以持续优化)  */
	public  void drag2ExtractSquid(List<ExtractSquidAndSquidLink> voList, ReturnValue out);
	/** 获取ExtractSquid详细信息（如果不在同一个事务里，adapter连接打开、关闭需要调用函数自己处理） */
	public  void setExtractSquidData(String token, final TableExtractSquid squid, boolean inSession, IRelationalDataManager adapter);
	
}