package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;

import java.io.Serializable;
import java.sql.Types;
import java.util.Date;

/**
 * Created by Eurlanda on 2017/4/25.
 */
@MultitableMapping(name = { "THIRD_JAR_DEFINITION" }, pk = "ID", desc = "")
public class ThirdJarDefintion implements Serializable{
    @ColumnMpping(name="id", desc="", nullable=false, precision=0, type=Types.INTEGER, valueReg=">=1")
    private  int id;
    @ColumnMpping(name="alias_name", desc="别名", nullable=true, precision=100, type= Types.VARCHAR, valueReg="")
    private String aliasName;
    @ColumnMpping(name="class_name", desc="自定义类名", nullable=true, precision=100, type= Types.VARCHAR, valueReg="")
    private String className;
    @ColumnMpping(name="data_mapping", desc="referenceColumn", nullable=true, precision=Integer.MAX_VALUE, type= Types.VARCHAR, valueReg="")
    private String data_mapping;
    @ColumnMpping(name="parameter", desc="", nullable=true, precision=Integer.MAX_VALUE, type= Types.VARCHAR, valueReg="")
    private String parameter;
    @ColumnMpping(name="output_mapping", desc="对应column", nullable=true, precision=Integer.MAX_VALUE, type= Types.VARCHAR, valueReg="")
    private String output_mapping;
    @ColumnMpping(name="author", desc="", nullable=true, precision=50, type= Types.VARCHAR, valueReg="")
    private String author;
    @ColumnMpping(name="create_date", desc="", nullable=true, precision=0, type= Types.DATE, valueReg="")
    private Date create_date;
    @ColumnMpping(name="update_date", desc="", nullable=true, precision=0, type= Types.DATE, valueReg="")
    private Date update_date;

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

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
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

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public Date getCreate_date() {
        return create_date;
    }

    public void setCreate_date(Date create_date) {
        this.create_date = create_date;
    }

    public Date getUpdate_date() {
        return update_date;
    }

    public void setUpdate_date(Date update_date) {
        this.update_date = update_date;
    }
}
