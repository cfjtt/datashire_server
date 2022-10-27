package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;

import java.sql.Types;
/**
 * 
 * @author bo.dang
 * @date 2014年5月15日
 */
@MultitableMapping(name = "DS_INDEXES", pk="ID", desc = "")
public class SquidIndexes{

    @ColumnMpping(name="id", desc="", nullable=true, precision=1, type=Types.INTEGER, valueReg="")
    private int id;
    @ColumnMpping(name="squid_id", desc="", nullable=true, precision=1, type=Types.INTEGER, valueReg="")
    private int squid_id;
    @ColumnMpping(name="index_name", desc="", nullable=true, precision=100, type=Types.VARCHAR, valueReg="")
    private String index_name;
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
    // column_id1对应的column_name
    private String column_name_1;
    // column_id2对应的column_name
    private String column_name_2;
    // column_id3对应的column_name
    private String column_name_3;
    // column_id4对应的column_name
    private String column_name_4;
    // column_id5对应的column_name
    private String column_name_5;
    // column_id6对应的column_name
    private String column_name_6;
    // column_id7对应的column_name
    private String column_name_7;
    // column_id8对应的column_name
    private String column_name_8;
    // column_id9对应的column_name
    private String column_name_9;
    // column_id10对应的column_name
    private String column_name_10;
    
    public int getSquid_id() {
        return squid_id;
    }
    public void setSquid_id(int squid_id) {
        this.squid_id = squid_id;
    }
    public String getIndex_name() {
        return index_name;
    }
    public void setIndex_name(String index_name) {
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
    public int getId() {
        return id;
    }
    public void setId(int id) {
        this.id = id;
    }
	public String getColumn_name_1() {
		return column_name_1;
	}
	public void setColumn_name_1(String column_name_1) {
		this.column_name_1 = column_name_1;
	}
	public String getColumn_name_2() {
		return column_name_2;
	}
	public void setColumn_name_2(String column_name_2) {
		this.column_name_2 = column_name_2;
	}
	public String getColumn_name_3() {
		return column_name_3;
	}
	public void setColumn_name_3(String column_name_3) {
		this.column_name_3 = column_name_3;
	}
	public String getColumn_name_4() {
		return column_name_4;
	}
	public void setColumn_name_4(String column_name_4) {
		this.column_name_4 = column_name_4;
	}
	public String getColumn_name_5() {
		return column_name_5;
	}
	public void setColumn_name_5(String column_name_5) {
		this.column_name_5 = column_name_5;
	}
	public String getColumn_name_6() {
		return column_name_6;
	}
	public void setColumn_name_6(String column_name_6) {
		this.column_name_6 = column_name_6;
	}
	public String getColumn_name_7() {
		return column_name_7;
	}
	public void setColumn_name_7(String column_name_7) {
		this.column_name_7 = column_name_7;
	}
	public String getColumn_name_8() {
		return column_name_8;
	}
	public void setColumn_name_8(String column_name_8) {
		this.column_name_8 = column_name_8;
	}
	public String getColumn_name_9() {
		return column_name_9;
	}
	public void setColumn_name_9(String column_name_9) {
		this.column_name_9 = column_name_9;
	}
	public String getColumn_name_10() {
		return column_name_10;
	}
	public void setColumn_name_10(String column_name_10) {
		this.column_name_10 = column_name_10;
	}
 
    
    
}
