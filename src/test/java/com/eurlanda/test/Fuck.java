package com.eurlanda.test;

//import org.apache.poi.hssf.record.formula.functions.T;

import org.apache.poi.ss.formula.functions.T;

/**
 * Created by e56 on 2015/5/22.
 */
public class Fuck {
    private String columnName;
    private Class<T> clazz;


    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public void setClazz(Class<T> clazz) {
        this.clazz = clazz;
    }

    public String getColumnName() {
        return columnName;
    }

    public Class<T> getClazz() {
        return clazz;
    }
}
