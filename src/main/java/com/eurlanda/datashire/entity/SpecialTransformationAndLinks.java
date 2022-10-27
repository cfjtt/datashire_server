package com.eurlanda.datashire.entity;

import java.util.List;

/**
 * 新增transformation打断transformationlink接口类
 * @author lei。bin
 *
 */
public class SpecialTransformationAndLinks {
    private Transformation transformation;
    private List<TransformationLink> transformationLinks;
	public Transformation getTransformation() {
		return transformation;
	}
	public void setTransformation(Transformation transformation) {
		this.transformation = transformation;
	}
	public List<TransformationLink> getTransformationLinks() {
		return transformationLinks;
	}
	public void setTransformationLinks(List<TransformationLink> transformationLinks) {
		this.transformationLinks = transformationLinks;
	}
    
}
