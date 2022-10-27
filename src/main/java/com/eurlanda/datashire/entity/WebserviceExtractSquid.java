package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.MultitableMapping;
import com.eurlanda.datashire.enumeration.DSObjectType;

import java.util.List;

@MultitableMapping(name = {"DS_SQUID"}, pk="ID", desc = "默认从WebServiceExtractSquit拖拽创建")
public class WebserviceExtractSquid extends DataSquid {
	{
		this.setType(DSObjectType.WEBSERVICEEXTRACT.value());
	}
	// 第三方链接参数的集合，打开squidflow推送时使用
	private List<ThirdPartyParams> urlParams;//
	private List<ThirdPartyParams> headerParams;//wsdl header XML中的参数
	private List<ThirdPartyParams> contentParams;//wsdl body XML中的参数 或者post
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
	public List<ThirdPartyParams> getHeaderParams() {
		return headerParams;
	}
	public void setHeaderParams(List<ThirdPartyParams> headerParams) {
		this.headerParams = headerParams;
	}
	
	
}
