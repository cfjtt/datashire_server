package com.eurlanda.datashire.entity.operation;

/**
 * 项目查询条件类
 * @author Fumin
 *
 */
public class ProjectCondition {

	private String creator;
	
	private String beginCreationDate;
	
	private String endCreationDate;
	
	private String beginModificationDate;
	
	private String endModificationDate;
	
	/**
	 * 构造器
	 */
	public ProjectCondition(){
		
	}

	public String getCreator() {
		return creator;
	}

	public void setCreator(String creator) {
		this.creator = creator;
	}

	public String getBeginCreationDate() {
		return beginCreationDate;
	}

	public void setBeginCreationDate(String beginCreationDate) {
		this.beginCreationDate = beginCreationDate;
	}

	public String getEndCreationDate() {
		return endCreationDate;
	}

	public void setEndCreationDate(String endCreationDate) {
		this.endCreationDate = endCreationDate;
	}

	public String getBeginModificationDate() {
		return beginModificationDate;
	}

	public void setBeginModificationDate(String beginModificationDate) {
		this.beginModificationDate = beginModificationDate;
	}

	public String getEndModificationDate() {
		return endModificationDate;
	}

	public void setEndModificationDate(String endModificationDate) {
		this.endModificationDate = endModificationDate;
	}
}
