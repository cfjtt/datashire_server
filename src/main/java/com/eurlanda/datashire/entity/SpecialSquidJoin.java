package com.eurlanda.datashire.entity;

/**
 * 对join进行操作，返回给前端的对象类
 * @author lei.bin
 *2013-10-21
 */
public class SpecialSquidJoin extends DSBaseModel {
	/* Join对象 */
	private SquidJoin squidJoin;
	/* StageSquid对象 */
	private StageSquid stageSquid;
	/* SquidLink对象 */
	private SquidLink squidLink;
	/* 返回的操作代码 */
	private int code;
	/*condition校验状态，-1 表示未验证， 1为校验通过  0 为校验失败*/
	private int validationState ; 

	public SquidJoin getSquidJoin() {
		return squidJoin;
	}

	public void setSquidJoin(SquidJoin squidJoin) {
		this.squidJoin = squidJoin;
	}

	public StageSquid getStageSquid() {
		return stageSquid;
	}

	public void setStageSquid(StageSquid stageSquid) {
		this.stageSquid = stageSquid;
	}

	public SquidLink getSquidLink() {
		return squidLink;
	}

	public void setSquidLink(SquidLink squidLink) {
		this.squidLink = squidLink;
	}

	public int getCode() {
		return code;
	}

	public void setCode(int code) {
		this.code = code;
	}

	public int getValidationState() {
		return validationState;
	}

	public void setValidationState(int validationState) {
		this.validationState = validationState;
	}

}
