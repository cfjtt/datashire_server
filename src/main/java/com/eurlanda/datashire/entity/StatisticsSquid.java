package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;
import com.eurlanda.datashire.enumeration.DSObjectType;

import java.sql.Types;
import java.util.List;

/**
 * Created by Eurlanda on 2017/5/5.
 */
@MultitableMapping(name = { "DS_SQUID"}, pk = "ID", desc = "")
public class StatisticsSquid extends DataSquid{
    {
        this.setSquid_type(DSObjectType.STATISTICS.value());
    }
    @ColumnMpping(name="statistics_name", desc="统计算法名", nullable=true, precision=255, type= Types.VARCHAR, valueReg="")
    private String statistics_name;
    private List<String> statisticsNames;
    private List<StatisticsDataMapColumn> statisticsDataMapColumns;
    private List<StatisticsParameterColumn> statisticsParametersColumns;
    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getStatistics_name() {
        return statistics_name;
    }

    public void setStatistics_name(String statistics_name) {
        this.statistics_name = statistics_name;
    }

    public List<StatisticsDataMapColumn> getStatisticsDataMapColumns() {
        return statisticsDataMapColumns;
    }

    public void setStatisticsDataMapColumns(List<StatisticsDataMapColumn> statisticsDataMapColumns) {
        this.statisticsDataMapColumns = statisticsDataMapColumns;
    }

    public List<StatisticsParameterColumn> getStatisticsParametersColumns() {
        return statisticsParametersColumns;
    }

    public void setStatisticsParametersColumns(List<StatisticsParameterColumn> statisticsParametersColumns) {
        this.statisticsParametersColumns = statisticsParametersColumns;
    }

    public List<String> getStatisticsNames() {
        return statisticsNames;
    }

    public void setStatisticsNames(List<String> statisticsNames) {
        this.statisticsNames = statisticsNames;
    }
}
