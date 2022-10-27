package com.eurlanda.datashire.server.model;

/**
 * Created by eurlanda - new 2 on 2017/6/27.
 * Trans的配置实体类
 */
public class TransformationType {
    private int id;
    private String code;
    private int output_data_type;
    private String description;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public int getOutput_data_type() {
        return output_data_type;
    }

    public void setOutput_data_type(int output_data_type) {
        this.output_data_type = output_data_type;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}
