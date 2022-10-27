package com.eurlanda.datashire.entity.operation;

import com.eurlanda.datashire.entity.TransformationInputs;

import java.util.ArrayList;
import java.util.List;

public class DataSquidCollectionPropertyId {
	
	public DataSquidCollectionPropertyId(){
		this.squidId = 0;
		this.columnIds = new ArrayList<Integer>();
		this.referenceColumnIds = new ArrayList<Integer>();
		this.transformationIds = new ArrayList<Integer>();
		this.transformLinkIds = new ArrayList<Integer>();
		this.updateInputs = new ArrayList<TransformationInputs>();
		this.deleteInputs = new ArrayList<TransformationInputs>();
	}
	
	public DataSquidCollectionPropertyId(int squidId, List<Integer> columnIds,
			List<Integer> referenceColumnIds, List<Integer> transformationIds,
			List<Integer> transformLinkIds,
			List<TransformationInputs> updateInputs,
			List<TransformationInputs> deleteInputs) {
		super();
		this.squidId = squidId;
		this.columnIds = columnIds;
		this.referenceColumnIds = referenceColumnIds;
		this.transformationIds = transformationIds;
		this.transformLinkIds = transformLinkIds;
		this.updateInputs = updateInputs;
		this.deleteInputs = deleteInputs;
	}

	public int squidId;
	public List<Integer> columnIds;
	public List<Integer> referenceColumnIds;
	public List<Integer> transformationIds;
	public List<Integer> transformLinkIds;
	public List<TransformationInputs> updateInputs;
	public List<TransformationInputs> deleteInputs;

	public int getSquidId() {
		return squidId;
	}
	public void setSquidId(int squidId) {
		this.squidId = squidId;
	}
	public List<Integer> getColumnIds() {
		return columnIds;
	}
	public void setColumnIds(List<Integer> columnIds) {
		this.columnIds = columnIds;
	}
	public List<Integer> getReferenceColumnIds() {
		return referenceColumnIds;
	}
	public void setReferenceColumnIds(List<Integer> referenceColumnIds) {
		this.referenceColumnIds = referenceColumnIds;
	}
	public List<Integer> getTransformationIds() {
		return transformationIds;
	}
	public void setTransformationIds(List<Integer> transformationIds) {
		this.transformationIds = transformationIds;
	}
	public List<Integer> getTransformLinkIds() {
		return transformLinkIds;
	}
	public void setTransformLinkIds(List<Integer> transformLinkIds) {
		this.transformLinkIds = transformLinkIds;
	}
	public List<TransformationInputs> getUpdateInputs() {
		return updateInputs;
	}
	public void setUpdateInputs(List<TransformationInputs> updateInputs) {
		this.updateInputs = updateInputs;
	}
	public List<TransformationInputs> getDeleteInputs() {
		return deleteInputs;
	}
	public void setDeleteInputs(List<TransformationInputs> deleteInputs) {
		this.deleteInputs = deleteInputs;
	}
}
