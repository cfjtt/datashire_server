package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;

import java.io.Serializable;
import java.sql.Types;

/**
 * Created by Eurlanda on 2017/5/5.
 */
@MultitableMapping(name = {"DS_STATISTICS_DEFINITION"}, pk = "ID", desc = "")
public class StatisticsMessage implements Serializable {
    @ColumnMpping(name = "id", desc = "", nullable = false, precision = 0, type = Types.INTEGER, valueReg = ">=1")
    private int id;
    @ColumnMpping(name = "alias_name", desc = "别名", nullable = true, precision = 100, type = Types.VARCHAR, valueReg = "")
    private String aliasName;
    @ColumnMpping(name = "statistics_name", desc = "自定义类名", nullable = true, precision = 100, type = Types.VARCHAR, valueReg = "")
    private String statistics_name;
    @ColumnMpping(name = "data_mapping", desc = "referenceColumn", nullable = true, precision = Integer.MAX_VALUE, type = Types.VARCHAR, valueReg = "")
    private String data_mapping;
    @ColumnMpping(name = "parameter", desc = "", nullable = true, precision = Integer.MAX_VALUE, type = Types.VARCHAR, valueReg = "")
    private String parameter;
    @ColumnMpping(name = "output_mapping", desc = "对应column", nullable = true, precision = Integer.MAX_VALUE, type = Types.VARCHAR, valueReg = "")
    private String output_mapping;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getAliasName() {
        return aliasName;
    }

    public void setAliasName(String aliasName) {
        this.aliasName = aliasName;
    }

    public String getStatistics_name() {
        return statistics_name;
    }

    public void setStatistics_name(String statistics_name) {
        this.statistics_name = statistics_name;
    }

    public String getData_mapping() {
        return data_mapping;
    }

    public void setData_mapping(String data_mapping) {
        this.data_mapping = data_mapping;
    }

    public String getParameter() {
        return parameter;
    }

    public void setParameter(String parameter) {
        this.parameter = parameter;
    }

    public String getOutput_mapping() {
        return output_mapping;
    }

    public void setOutput_mapping(String output_mapping) {
        this.output_mapping = output_mapping;
    }
}
