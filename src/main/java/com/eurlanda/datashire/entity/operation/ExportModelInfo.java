package com.eurlanda.datashire.entity.operation;
/**
 * 导出数据结构
 */

import com.eurlanda.datashire.entity.IOFlow;

import java.util.List;

public class ExportModelInfo {
	
	private MetaModuleInfo meta;
	private List<IOFlow> ioflowList;
	private List<SquidFlowModuleInfo> squidflowList;
	
	public MetaModuleInfo getMeta() {
		return meta;
	}
	public void setMeta(MetaModuleInfo meta) {
		this.meta = meta;
	}
	public List<IOFlow> getIoflowList() {
		return ioflowList;
	}
	public void setIoflowList(List<IOFlow> ioflowList) {
		this.ioflowList = ioflowList;
	}
	public List<SquidFlowModuleInfo> getSquidflowList() {
		return squidflowList;
	}
	public void setSquidflowList(List<SquidFlowModuleInfo> squidflowList) {
		this.squidflowList = squidflowList;
	}
	
}
