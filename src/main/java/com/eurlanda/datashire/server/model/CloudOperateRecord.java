package com.eurlanda.datashire.server.model;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;

import java.sql.Types;
import java.util.Date;

@MultitableMapping(pk="ID", name = "DS_CLOUD_OPERATE_RECORD", desc = "")
public class CloudOperateRecord {
    @ColumnMpping(name="id", desc="", nullable=true, precision=0, type= Types.INTEGER, valueReg="")
    private int id;
    @ColumnMpping(name="operate_time", desc="", nullable=true, precision=0, type= Types.DATE, valueReg="")
    private Date operate_time;
    @ColumnMpping(name="content", desc="", nullable=true, precision=255, type= Types.VARCHAR, valueReg="")
    private String content;
    @ColumnMpping(name="user_id", desc="", nullable=true, precision=0, type= Types.INTEGER, valueReg="")
    private int user_id;
    @ColumnMpping(name="space_id", desc="", nullable=true, precision=0, type= Types.INTEGER, valueReg="")
    private int space_id;
    @ColumnMpping(name="operate_type", desc="", nullable=true, precision=0, type= Types.INTEGER, valueReg="")
    private int operate_type;
    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public Date getOperate_time() {
        return operate_time;
    }

    public void setOperate_time(Date operate_time) {
        this.operate_time = operate_time;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public int getUser_id() {
        return user_id;
    }

    public void setUser_id(int user_id) {
        this.user_id = user_id;
    }

    public int getSpace_id() {
        return space_id;
    }

    public void setSpace_id(int space_id) {
        this.space_id = space_id;
    }

    public int getOperate_type() {
        return operate_type;
    }

    public void setOperate_type(int operate_type) {
        this.operate_type = operate_type;
    }
}
