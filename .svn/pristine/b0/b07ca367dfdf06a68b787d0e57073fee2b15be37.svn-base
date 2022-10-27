package com.eurlanda.datashire.entity.dest;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;
import com.eurlanda.datashire.entity.Squid;

import java.sql.Types;

@MultitableMapping(pk="", name = {"DS_SQUID"}, desc = "")
public class DestWSSquid extends Squid {
	
	@ColumnMpping(name="service_name", desc="", nullable=true, precision=100, type=Types.VARCHAR, valueReg="")
	private String service_name;
	
	@ColumnMpping(name="wsdl", desc="", nullable=true, precision=200, type=Types.VARCHAR, valueReg="")
	private String wsdl;
	
	@ColumnMpping(name="is_realtime", desc="", nullable=true, precision=1, type=Types.VARCHAR, valueReg="")
	private boolean is_realtime;
	
	@ColumnMpping(name="callback_url", desc="", nullable=true, precision=200, type=Types.VARCHAR, valueReg="")
	private String callback_url;
	
	@ColumnMpping(name="allowed_services", desc="", nullable=true, precision=200, type=Types.VARCHAR, valueReg="")
	private String allowed_services;

	public String getService_name() {
		return service_name;
	}

	public void setService_name(String service_name) {
		this.service_name = service_name;
	}

	public String getWsdl() {
		return wsdl;
	}

	public void setWsdl(String wsdl) {
		this.wsdl = wsdl;
	}

	public boolean isIs_realtime() {
		return is_realtime;
	}

	public void setIs_realtime(boolean is_realtime) {
		this.is_realtime = is_realtime;
	}

	public String getCallback_url() {
		return callback_url;
	}

	public void setCallback_url(String callback_url) {
		this.callback_url = callback_url;
	}

	public String getAllowed_services() {
		return allowed_services;
	}

	public void setAllowed_services(String allowed_services) {
		this.allowed_services = allowed_services;
	}
}
