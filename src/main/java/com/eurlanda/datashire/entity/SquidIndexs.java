package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;
import com.eurlanda.datashire.enumeration.DSObjectType;

import java.sql.Types;
/**
 * 
 * @author bo.dang
 * @date 2014年5月15日
 */
@MultitableMapping(name = {"DS_SQUID", "DS_INDEXES"}, pk="ID", desc = "")
public class SquidIndexs extends Squid {

    {
        this.setType(DSObjectType.SQUID.value());
    }
    @ColumnMpping(name="squid_id", desc="", nullable=true, precision=1, type=Types.INTEGER, valueReg="")
    private int squid_id;
    @ColumnMpping(name="index_name", desc="", nullable=true, precision=1, type=Types.VARCHAR, valueReg="")
    private int index_name;
    @ColumnMpping(name="index_type", desc="", nullable=true, precision=1, type=Types.INTEGER, valueReg="")
    private int index_type;
    @ColumnMpping(name="column_id10", desc="", nullable=true, precision=1, type=Types.INTEGER, valueReg="")
    private int column_id10;
    @ColumnMpping(name="column_id9", desc="", nullable=true, precision=1, type=Types.INTEGER, valueReg="")
    private int column_id9;
    @ColumnMpping(name="column_id8", desc="", nullable=true, precision=1, type=Types.INTEGER, valueReg="")
    private int column_id8;
    @ColumnMpping(name="column_id7", desc="", nullable=true, precision=1, type=Types.INTEGER, valueReg="")
    private int column_id7;
    @ColumnMpping(name="column_id6", desc="", nullable=true, precision=1, type=Types.INTEGER, valueReg="")
    private int column_id6;
    @ColumnMpping(name="column_id5", desc="", nullable=true, precision=1, type=Types.INTEGER, valueReg="")
    private int column_id5;
    @ColumnMpping(name="column_id4", desc="", nullable=true, precision=1, type=Types.INTEGER, valueReg="")
    private int column_id4;
    @ColumnMpping(name="column_id3", desc="", nullable=true, precision=1, type=Types.INTEGER, valueReg="")
    private int column_id3;
    @ColumnMpping(name="column_id2", desc="", nullable=true, precision=1, type=Types.INTEGER, valueReg="")
    private int column_id2;
    @ColumnMpping(name="column_id1", desc="", nullable=true, precision=1, type=Types.INTEGER, valueReg="")
    private int column_id1;
    
    public int getSquid_id() {
        return squid_id;
    }
    public void setSquid_id(int squid_id) {
        this.squid_id = squid_id;
    }
    public int getIndex_name() {
        return index_name;
    }
    public void setIndex_name(int index_name) {
        this.index_name = index_name;
    }
    public int getIndex_type() {
        return index_type;
    }
    public void setIndex_type(int index_type) {
        this.index_type = index_type;
    }
    public int getColumn_id10() {
        return column_id10;
    }
    public void setColumn_id10(int column_id10) {
        this.column_id10 = column_id10;
    }
    public int getColumn_id9() {
        return column_id9;
    }
    public void setColumn_id9(int column_id9) {
        this.column_id9 = column_id9;
    }
    public int getColumn_id8() {
        return column_id8;
    }
    public void setColumn_id8(int column_id8) {
        this.column_id8 = column_id8;
    }
    public int getColumn_id7() {
        return column_id7;
    }
    public void setColumn_id7(int column_id7) {
        this.column_id7 = column_id7;
    }
    public int getColumn_id6() {
        return column_id6;
    }
    public void setColumn_id6(int column_id6) {
        this.column_id6 = column_id6;
    }
    public int getColumn_id5() {
        return column_id5;
    }
    public void setColumn_id5(int column_id5) {
        this.column_id5 = column_id5;
    }
    public int getColumn_id4() {
        return column_id4;
    }
    public void setColumn_id4(int column_id4) {
        this.column_id4 = column_id4;
    }
    public int getColumn_id3() {
        return column_id3;
    }
    public void setColumn_id3(int column_id3) {
        this.column_id3 = column_id3;
    }
    public int getColumn_id2() {
        return column_id2;
    }
    public void setColumn_id2(int column_id2) {
        this.column_id2 = column_id2;
    }
    public int getColumn_id1() {
        return column_id1;
    }
    public void setColumn_id1(int column_id1) {
        this.column_id1 = column_id1;
    }
 
    
    
}
