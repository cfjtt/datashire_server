package com.eurlanda.datashire.server.model;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;
import com.eurlanda.datashire.entity.DataSquid;

import java.sql.Types;

/**
 * Created by test on 2017/11/24.
 */
@MultitableMapping(pk="", name = {"DS_SQUID"}, desc = "")
public class PivotSquid extends DataSquid {

    @ColumnMpping(name = "pivot_column_value", desc = "pivot列值,List<string> 转成的json", nullable = true, precision = 0x7fffffff, type = Types.VARCHAR, valueReg = "")
    private String pivotColumnValue;
    @ColumnMpping(name = "aggregation_type", desc = "聚合方式", nullable = true, precision = 0, type = Types.INTEGER, valueReg = "")
    private Integer aggregationType;
    //pivot列
    @ColumnMpping(name = "pivot_column_id", desc = "pivot列字段id", nullable = true, precision = 0, type = Types.INTEGER, valueReg = "")
    private Integer pivotColumnId;
    //value列
    @ColumnMpping(name = "value_column_id", desc = "value列对应的columnId", nullable = true, precision = 0, type = Types.INTEGER, valueReg = "")
    private Integer valueColumnId;
    //分组列Id,多个id用","分开
    @ColumnMpping(name = "group_by_column_ids", desc = "分组列columnId,多个用','隔开", nullable = true, precision = 500, type = Types.VARCHAR, valueReg = "")
    private String groupByColumnIds;

    public String getGroupByColumnIds() {
        return groupByColumnIds;
    }

    public void setGroupByColumnIds(String groupByColumnIds) {
        this.groupByColumnIds = groupByColumnIds;
    }

    public String getPivotColumnValue() {
        return pivotColumnValue;
    }

    public void setPivotColumnValue(String pivotColumnValue) {
        this.pivotColumnValue = pivotColumnValue;
    }

    public Integer getAggregationType() {
        return aggregationType;
    }

    public void setAggregationType(Integer aggregationType) {
        this.aggregationType = aggregationType;
    }

    public Integer getPivotColumnId() {
        return pivotColumnId;
    }

    public void setPivotColumnId(Integer pivotColumnId) {
        this.pivotColumnId = pivotColumnId;
    }

    public Integer getValueColumnId() {
        return valueColumnId;
    }

    public void setValueColumnId(Integer valueColumnId) {
        this.valueColumnId = valueColumnId;
    }
}
