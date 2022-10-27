package com.eurlanda.datashire.entity.operation;

import com.eurlanda.datashire.entity.Column;
import com.eurlanda.datashire.entity.Transformation;
import com.eurlanda.datashire.entity.TransformationLink;

/**
 * 业务类
 * 
 * <p>
 * Title : 
 * </p>
 * <p>
 * Description: 
 * </p>
 * <p>
 * Author :赵春花 2013-9-12
 * </p>
 * <p>
 * update :赵春花 2013-9-12
 * </p>
 * <p>
 * Department :  JAVA后端研发部
 * </p>
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司
 * </p>
 */
public class TransformationAndCloumn {
	private Column column;
	private Transformation transformation;
	private TransformationLink transformationLink;
	public TransformationAndCloumn() {
	}
	public Column getColumn() {
		return column;
	}
	public void setColumn(Column column) {
		this.column = column;
	}
	public Transformation getTransformation() {
		return transformation;
	}
	public void setTransformation(Transformation transformation) {
		this.transformation = transformation;
	}
	public TransformationLink getTransformationLink() {
		return transformationLink;
	}
	public void setTransformationLink(TransformationLink transformationLink) {
		this.transformationLink = transformationLink;
	}
}
