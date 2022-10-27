package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;
import com.eurlanda.datashire.enumeration.DSObjectType;

import java.sql.Types;
import java.util.List;

/**
 * Created by Eurlanda on 2017/4/24.
 */
@MultitableMapping(name = { "DS_SQUID"}, pk = "ID", desc = "")
public class UserDefinedSquid extends DataSquid {
    {
        this.setSquid_type(DSObjectType.USERDEFINED.value());
    }
    @ColumnMpping(name="selectClassName", desc="自定义类名", nullable=true, precision=255, type= Types.VARCHAR, valueReg="")
    private String selectClassName;
    private List<String> classNames;
    private List<UserDefinedMappingColumn> userDefinedMappingColumns;
    private List<UserDefinedParameterColumn> UserDefinedParameterColumns;

    public String getSelectClassName() {
        return selectClassName;
    }

    public void setSelectClassName(String selectClassName) {
        this.selectClassName = selectClassName;
    }

    public List<String> getClassNames() {
        return classNames;
    }

    public void setClassNames(List<String> classNames) {
        this.classNames = classNames;
    }

    public List<UserDefinedMappingColumn> getUserDefinedMappingColumns() {
        return userDefinedMappingColumns;
    }

    public void setUserDefinedMappingColumns(List<UserDefinedMappingColumn> userDefinedMappingColumns) {
        this.userDefinedMappingColumns = userDefinedMappingColumns;
    }

    public List<UserDefinedParameterColumn> getUserDefinedParameterColumns() {
        return UserDefinedParameterColumns;
    }

    public void setUserDefinedParameterColumns(List<UserDefinedParameterColumn> userDefinedParameterColumns) {
        UserDefinedParameterColumns = userDefinedParameterColumns;
    }
}
