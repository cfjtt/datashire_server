package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;

import java.sql.Types;
import java.util.List;

/**
 * webservice连接squid实体类
 * @author lei.bin
 *
 */
@MultitableMapping(pk="ID", name = {"DS_SQUID", "DS_WEBSERVICE_CONNECTION"}, desc = "")
public class WebserviceSquid extends Squid {
	@ColumnMpping(name="is_restful", desc="", nullable=true, precision=1, type=Types.VARCHAR, valueReg="")
	private boolean is_restful; 
	@ColumnMpping(name="address", desc="", nullable=true, precision=200, type=Types.VARCHAR, valueReg="")
	private String address;
	
	private String portName;
	private String domainName;

	private List<SourceTable> sourceTableList;
	
	
	public List<SourceTable> getSourceTableList() {
		return sourceTableList;
	}
	public void setSourceTableList(List<SourceTable> sourceTableList) {
		this.sourceTableList = sourceTableList;
	}
	public boolean isIs_restful() {
		return is_restful;
	}
	public boolean getIs_restful() {
		return is_restful;
	}
	public void setIs_restful(boolean is_restful) {
		this.is_restful = is_restful;
	}
	public String getAddress() {
		return address;
	}
	public void setAddress(String address) {
		this.address = address;
	}
	
}
