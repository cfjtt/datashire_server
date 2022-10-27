package com.eurlanda.datashire.server.model.Base;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;
import com.eurlanda.datashire.entity.DSVariable;

import java.sql.Types;
import java.util.List;

/**
 * 所有 Squid 的父类
 * TODO: 老的实体类也是从这个类派生，这里最后会把老版本全部改掉
 */
@MultitableMapping(pk="ID", name = "DS_SQUID", desc = "")
public class SquidModelBase extends BaseObject{

    @ColumnMpping(name="squid_flow_id", desc="fk:DS_SQUID_FLOW", nullable=true, precision=0, type=Types.INTEGER, valueReg=">=1")
    protected Integer squidflow_id;

    @ColumnMpping(name="squid_type_id", desc="fk:SquidTypeEnum", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
    protected Integer squid_type;

    @ColumnMpping(name="location_x", desc="x", nullable=true, precision=0, type= Types.INTEGER, valueReg="")
    protected Integer location_x;

    @ColumnMpping(name="location_y", desc="x", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
    protected Integer location_y;

    @ColumnMpping(name="squid_height", desc="h", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
    protected Integer squid_height;

    @ColumnMpping(name="squid_width", desc="w", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
    protected Integer squid_width;

    @ColumnMpping(name="design_status", desc="设计期的状态,比如pending, error等", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
    protected Integer design_status;

    @ColumnMpping(name="description", desc="描述", nullable=true, precision=300, type=Types.VARCHAR, valueReg="")
    protected String description;

    /**
     *断点标志
     */
    private boolean break_flag;
    /**
     *查看数据
     */
    private boolean data_viewer_flag;
    /**
     *运行至本squid
     */
    private boolean run_to_here_flag;
    /**
     * 运行时的状态״̬
     */
    private int runTimeStatus;

    /**
     * Squid中变量的集合
     */
    private List<DSVariable> variables;

    public Integer getSquidflow_id() {
        return squidflow_id;
    }

    public void setSquidflow_id(Integer squidflow_id) {
        this.squidflow_id = squidflow_id;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Integer getSquid_type() {
        return squid_type;
    }

    public void setSquid_type(Integer squid_type) {
        this.squid_type = squid_type;
    }

    public Integer getLocation_x() {
        return location_x;
    }

    public void setLocation_x(Integer location_x) {
        this.location_x = location_x;
    }

    public Integer getLocation_y() {
        return location_y;
    }

    public void setLocation_y(Integer location_y) {
        this.location_y = location_y;
    }

    public Integer getSquid_height() {
        return squid_height;
    }

    public void setSquid_height(Integer squid_height) {
        this.squid_height = squid_height;
    }

    public Integer getSquid_width() {
        return squid_width;
    }

    public void setSquid_width(Integer squid_width) {
        this.squid_width = squid_width;
    }

    public Integer getDesign_status() {
        return design_status;
    }

    public void setDesign_status(Integer design_status) {
        this.design_status = design_status;
    }

    public boolean isBreak_flag() {
        return break_flag;
    }

    public void setBreak_flag(boolean break_flag) {
        this.break_flag = break_flag;
    }

    public boolean isData_viewer_flag() {
        return data_viewer_flag;
    }

    public void setData_viewer_flag(boolean data_viewer_flag) {
        this.data_viewer_flag = data_viewer_flag;
    }

    public boolean isRun_to_here_flag() {
        return run_to_here_flag;
    }

    public void setRun_to_here_flag(boolean run_to_here_flag) {
        this.run_to_here_flag = run_to_here_flag;
    }

    public int getRunTimeStatus() {
        return runTimeStatus;
    }

    public void setRunTimeStatus(int runTimeStatus) {
        this.runTimeStatus = runTimeStatus;
    }

    public List<DSVariable> getVariables() {
        return variables;
    }

    public void setVariables(List<DSVariable> variables) {
        this.variables = variables;
    }
}