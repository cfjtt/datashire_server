package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;

import javax.xml.bind.annotation.XmlAttribute;
import java.sql.Types;

/**
 * ds_trans_inputs实体类
 *
 * @author yi.zhou
 */
@MultitableMapping(pk = "", name = "DS_TRAN_INPUTS", desc = "")
public class TransformationInputs extends DSBaseModel implements Comparable<TransformationInputs> {

    @ColumnMpping(name = "transformation_id", desc = "fk:DS_TRANSFORMATION.id", nullable = false, precision = 0, type = Types.INTEGER, valueReg = ">=1")
    private int transformationId;
    @ColumnMpping(name = "relative_order", desc = "", nullable = false, precision = 0, type = Types.INTEGER, valueReg = "")
    private int relative_order;

    @ColumnMpping(name = "source_transform_id", desc = "fk:DS_TRANSFORMATION.id", nullable = false, precision = 0, type = Types.INTEGER, valueReg = ">=1")
    private int source_transform_id;

    //客户端显示用，不存表
    private String sourceTransformationName;

    //客户端使用
    @ColumnMpping(name = "INPUT_DATA_TYPE", desc = "", nullable = true, precision = 0, type = Types.INTEGER, valueReg = "")
    private int input_Data_Type;

    @ColumnMpping(name = "DESCRIPTION", desc = "", nullable = true, precision = 500, type = Types.VARCHAR, valueReg = "")
    private String description;

    @ColumnMpping(name = "source_tran_output_index", desc = "", nullable = true, precision = 0, type = Types.INTEGER, valueReg = "")
    private int source_tran_output_index;

    @ColumnMpping(name = "in_condition", desc = "", nullable = true, precision = 1000, type = Types.VARCHAR, valueReg = "")
    private String in_condition;

    @ColumnMpping(name = "input_value", desc = "", nullable = true, precision = 50, type = Types.VARCHAR, valueReg = "")
    private String input_value;

    public int getTransformationId() {
        return transformationId;
    }

    public void setTransformationId(int transformationId) {
        this.transformationId = transformationId;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @XmlAttribute(name = "relative_order")
    public int getRelative_order() {
        return relative_order;
    }

    public void setRelative_order(int relativeOrder) {
        relative_order = relativeOrder;
    }

    public int getSource_transform_id() {
        return source_transform_id;
    }

    public void setSource_transform_id(int sourceTransformId) {
        source_transform_id = sourceTransformId;
    }

    public String getSourceTransformationName() {
        return sourceTransformationName;
    }

    public void setSourceTransformationName(String sourceTransformationName) {
        this.sourceTransformationName = sourceTransformationName;
    }

    public int getInput_Data_Type() {
        return input_Data_Type;
    }

    public void setInput_Data_Type(int input_Data_Type) {
        this.input_Data_Type = input_Data_Type;
    }

    public int getSource_tran_output_index() {
        return source_tran_output_index;
    }

    public void setSource_tran_output_index(int sourceTranOutputIndex) {
        source_tran_output_index = sourceTranOutputIndex;
    }

    public String getIn_condition() {
        return in_condition;
    }

    public void setIn_condition(String in_condition) {
        this.in_condition = in_condition;
    }

    public void setInput_value(String input_value) {
        this.input_value = input_value;
    }

    public String getInput_value() {
        return this.input_value;
    }


    //重写自定义排序(TreeMap和TreeSet时使用)
    @Override
    public int compareTo(TransformationInputs o) {
        return this.getRelative_order() - o.getRelative_order();
    }
}
