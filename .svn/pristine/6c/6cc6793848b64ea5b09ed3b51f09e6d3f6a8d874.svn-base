package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.MultitableMapping;
import com.eurlanda.datashire.enumeration.DSObjectType;

import java.util.List;

@MultitableMapping(name = {"DS_SQUID"}, pk="ID", desc = "默认从HTTPExtractSquitList拖拽创建")
public class HttpExtractSquid extends DataSquid {

	{
		this.setType(DSObjectType.HTTPEXTRACT.value());
	}
	// 第三方链接参数的集合，打开squidflow推送时使用
	private List<ThirdPartyParams> urlParams;
	private List<ThirdPartyParams> contentParams;
	public List<ThirdPartyParams> getUrlParams() {
		return urlParams;
	}
	public void setUrlParams(List<ThirdPartyParams> urlParams) {
		this.urlParams = urlParams;
	}
	public List<ThirdPartyParams> getContentParams() {
		return contentParams;
	}
	public void setContentParams(List<ThirdPartyParams> contentParams) {
		this.contentParams = contentParams;
	}
	
}
