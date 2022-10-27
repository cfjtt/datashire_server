package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;
import com.eurlanda.datashire.enumeration.DSObjectType;

import java.sql.Types;
import java.util.List;

/**
 * 
 * 
 * <p>
 * Title : 
 * </p>
 * <p>
 * Description: stream stage squid
 * </p>
 * <p>
 * Department :  JAVA后端研发部
 * </p>
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司
 * </p>
 */
@MultitableMapping(name = {"DS_SQUID"}, pk="ID", desc = "")
public class StreamStageSquid extends DataSquid implements StreamSquid {

	{
		super.setType(DSObjectType.STREAM_STAGE.value());
	}

	public long getWINDOW_DURATION() {
		return WINDOW_DURATION;
	}

	public void setWINDOW_DURATION(long WINDOW_DURATION) {
		this.WINDOW_DURATION = WINDOW_DURATION;
	}

	@ColumnMpping(name = "WINDOW_DURATION", desc = "WINDOW的时间间隔", nullable = true, precision = 1, type = Types.VARCHAR, valueReg = "")
	private long WINDOW_DURATION;

	public boolean getENABLE_WINDOW() {
		return ENABLE_WINDOW;
	}

	public void setENABLE_WINDOW(boolean ENABLE_WINDOW) {
		this.ENABLE_WINDOW = ENABLE_WINDOW;
	}

	@ColumnMpping(name = "ENABLE_WINDOW", desc = "是否启用WINDOW", nullable = true, precision = 1, type = Types.VARCHAR, valueReg = "")
	private boolean  ENABLE_WINDOW;


	
	private List<SquidJoin> joins;
	{
		this.setType(DSObjectType.STREAM_STAGE.value());
	}
	public List<SquidJoin> getJoins() {
		return joins;
	}

	public void setJoins(List<SquidJoin> joins) {
		this.joins = joins;
	}

}