package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;

import java.sql.Types;

/**
 * TransformationLink对象
 * 
 * <p>
 * Title : 
 * </p>
 * <p>
 * Description: 
 * </p>
 * <p>
 * Author :赵春花 2013-9-5
 * </p>
 * <p>
 * update :赵春花 2013-9-5
 * </p>
 * <p>
 * Department :  JAVA后端研发部
 * </p>
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司
 * </p>
 */
@MultitableMapping(name = "DS_TRANSFORMATION_LINK", pk="ID", desc = "")
public class TransformationLink extends DSBaseModel {

	@ColumnMpping(name="from_transformation_id", desc="fk:DS_TRANSFORMATION.id", nullable=false, precision=0, type=Types.INTEGER, valueReg=">=1")
	private int from_transformation_id;
	@ColumnMpping(name="in_order", desc="", nullable=false, precision=0, type=Types.INTEGER, valueReg="")
	private int in_order;
	@ColumnMpping(name="to_transformation_id", desc="fk:DS_TRANSFORMATION.id", nullable=false, precision=0, type=Types.INTEGER, valueReg=">=1")
	private int to_transformation_id;
	
	private int source_input_index;

	@Override
	public String toString() {
		return "TransLink-"+id+"\t[ "+ from_transformation_id + " -> "+to_transformation_id+ "\tin_order="+in_order+" ]";
	}

	public TransformationLink(){

	}

	public int getFrom_transformation_id() {
		return from_transformation_id;
	}

	public void setFrom_transformation_id(int fromTransformationId) {
		from_transformation_id = fromTransformationId;
	}

	public int getIn_order() {
		return in_order;
	}

	public void setIn_order(int inOrder) {
		in_order = inOrder;
	}

	public int getTo_transformation_id() {
		return to_transformation_id;
	}

	public void setTo_transformation_id(int toTransformationId) {
		to_transformation_id = toTransformationId;
	}

	public int getSource_input_index() {
		return source_input_index;
	}

	public void setSource_input_index(int source_input_index) {
		this.source_input_index = source_input_index;
	}
}